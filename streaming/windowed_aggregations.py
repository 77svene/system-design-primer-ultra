"""
Real-time Stream Processing Extensions for System Design Primer
Extends MapReduce examples with Apache Flink/Spark Streaming equivalents for real-time analytics, exactly-once processing, and windowed aggregations.
"""

import time
import json
import threading
import queue
import logging
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Optional, Tuple, Iterator
from dataclasses import dataclass, field
from enum import Enum
import heapq
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessingTimeCharacteristic(Enum):
    """Time characteristic for stream processing"""
    EVENT_TIME = "event_time"
    PROCESSING_TIME = "processing_time"
    INGESTION_TIME = "ingestion_time"


class WindowType(Enum):
    """Types of windows for stream processing"""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    GLOBAL = "global"


class WatermarkStrategy(Enum):
    """Watermark strategies for event time processing"""
    BOUNDED_OUT_OF_ORDER = "bounded_out_of_order"
    PUNCTUATED = "punctuated"
    MONOTONOUS = "monotonous"


@dataclass
class Watermark:
    """Watermark for event time processing"""
    timestamp: float
    stream_id: str
    partition: int = 0
    
    def __lt__(self, other):
        return self.timestamp < other.timestamp


@dataclass
class StreamRecord:
    """Record in a stream"""
    key: Any
    value: Any
    event_time: float
    processing_time: float
    partition: int = 0
    offset: int = 0
    stream_id: str = ""
    
    def __post_init__(self):
        if not self.stream_id:
            self.stream_id = str(uuid.uuid4())


@dataclass
class Window:
    """Window for aggregations"""
    start: float
    end: float
    window_type: WindowType
    key: Any = None
    
    def contains(self, timestamp: float) -> bool:
        return self.start <= timestamp < self.end
    
    def __lt__(self, other):
        return self.end < other.end


class StateBackend:
    """State backend for exactly-once processing"""
    
    def __init__(self, checkpoint_interval: int = 1000):
        self.state = {}
        self.checkpoint_interval = checkpoint_interval
        self.last_checkpoint = 0
        self.checkpoint_counter = 0
        self.lock = threading.RLock()
        
    def get_state(self, key: Any) -> Any:
        with self.lock:
            return self.state.get(key)
    
    def set_state(self, key: Any, value: Any):
        with self.lock:
            self.state[key] = value
            self.checkpoint_counter += 1
            
            if self.checkpoint_counter - self.last_checkpoint >= self.checkpoint_interval:
                self._checkpoint()
    
    def _checkpoint(self):
        """Create checkpoint for exactly-once semantics"""
        self.last_checkpoint = self.checkpoint_counter
        # In production, this would write to persistent storage
        logger.debug(f"Checkpoint created at counter {self.checkpoint_counter}")


class WindowedAggregator:
    """Base class for windowed aggregations"""
    
    def __init__(self, 
                 window_size: int,
                 window_slide: int = None,
                 window_type: WindowType = WindowType.TUMBLING,
                 watermark_strategy: WatermarkStrategy = WatermarkStrategy.BOUNDED_OUT_OF_ORDER,
                 allowed_lateness: int = 0):
        self.window_size = window_size
        self.window_slide = window_slide or window_size
        self.window_type = window_type
        self.watermark_strategy = watermark_strategy
        self.allowed_lateness = allowed_lateness
        self.state_backend = StateBackend()
        self.windows: Dict[Window, Dict] = defaultdict(dict)
        self.watermarks: Dict[int, Watermark] = {}
        self.late_records = 0
        self.processed_records = 0
        
    def _get_windows_for_timestamp(self, timestamp: float) -> List[Window]:
        """Get all windows that contain the given timestamp"""
        windows = []
        
        if self.window_type == WindowType.TUMBLING:
            window_start = (timestamp // self.window_size) * self.window_size
            windows.append(Window(window_start, window_start + self.window_size, self.window_type))
            
        elif self.window_type == WindowType.SLIDING:
            # Calculate all sliding windows that contain this timestamp
            latest_start = timestamp - self.window_size + self.window_slide
            window_start = (latest_start // self.window_slide) * self.window_slide
            
            while window_start <= timestamp:
                window_end = window_start + self.window_size
                if window_start <= timestamp < window_end:
                    windows.append(Window(window_start, window_end, self.window_type))
                window_start += self.window_slide
                
        elif self.window_type == WindowType.SESSION:
            # Session windows based on gap
            gap = self.window_slide or 300  # Default 5 minute gap
            session_start = timestamp
            session_end = timestamp + gap
            windows.append(Window(session_start, session_end, self.window_type))
            
        return windows
    
    def process_record(self, record: StreamRecord) -> List[Tuple[Window, Any]]:
        """Process a single record and return window results"""
        self.processed_records += 1
        
        # Check if record is late
        watermark = self.watermarks.get(record.partition)
        if watermark and record.event_time < watermark.timestamp - self.allowed_lateness:
            self.late_records += 1
            logger.warning(f"Late record detected: {record.event_time} < watermark {watermark.timestamp}")
            return []
        
        # Get windows for this record
        windows = self._get_windows_for_timestamp(record.event_time)
        results = []
        
        for window in windows:
            # Update window state
            window_state = self.state_backend.get_state(window)
            if window_state is None:
                window_state = {"count": 0, "sum": 0, "values": []}
            
            # Apply aggregation function
            window_state["count"] += 1
            window_state["sum"] += record.value
            window_state["values"].append(record.value)
            
            self.state_backend.set_state(window, window_state)
            self.windows[window] = window_state
            
            # Emit result if window is complete
            if self._is_window_complete(window):
                result = self._aggregate_window(window, window_state)
                results.append((window, result))
                
        return results
    
    def _is_window_complete(self, window: Window) -> bool:
        """Check if window is complete based on watermarks"""
        if self.window_type == WindowType.GLOBAL:
            return False  # Global windows never complete
        
        watermark = self.watermarks.get(window.key or 0)
        if watermark is None:
            return False
            
        return watermark.timestamp > window.end + self.allowed_lateness
    
    def _aggregate_window(self, window: Window, window_state: Dict) -> Any:
        """Aggregate window results"""
        # Default aggregation: average
        if window_state["count"] > 0:
            return window_state["sum"] / window_state["count"]
        return 0
    
    def update_watermark(self, partition: int, timestamp: float):
        """Update watermark for a partition"""
        self.watermarks[partition] = Watermark(timestamp, "", partition)


class TumblingWindowAggregator(WindowedAggregator):
    """Tumbling window aggregation"""
    
    def __init__(self, window_size: int, **kwargs):
        super().__init__(window_size, window_type=WindowType.TUMBLING, **kwargs)


class SlidingWindowAggregator(WindowedAggregator):
    """Sliding window aggregation"""
    
    def __init__(self, window_size: int, slide_interval: int, **kwargs):
        super().__init__(window_size, window_slide=slide_interval, 
                        window_type=WindowType.SLIDING, **kwargs)


class SessionWindowAggregator(WindowedAggregator):
    """Session window aggregation"""
    
    def __init__(self, session_gap: int, **kwargs):
        super().__init__(session_gap, window_slide=session_gap, 
                        window_type=WindowType.SESSION, **kwargs)


class StreamProcessor:
    """Main stream processor class"""
    
    def __init__(self, 
                 time_characteristic: ProcessingTimeCharacteristic = ProcessingTimeCharacteristic.EVENT_TIME,
                 parallelism: int = 1):
        self.time_characteristic = time_characteristic
        self.parallelism = parallelism
        self.operators = []
        self.source_queues = {}
        self.sink_queues = {}
        self.running = False
        self.threads = []
        self.metrics = {
            "records_processed": 0,
            "records_emitted": 0,
            "watermarks_updated": 0,
            "windows_completed": 0
        }
        
    def add_source(self, name: str, queue: queue.Queue):
        """Add a source queue"""
        self.source_queues[name] = queue
        
    def add_sink(self, name: str, queue: queue.Queue):
        """Add a sink queue"""
        self.sink_queues[name] = queue
        
    def add_operator(self, operator: WindowedAggregator):
        """Add a processing operator"""
        self.operators.append(operator)
        
    def _process_source(self, source_name: str):
        """Process records from a source"""
        source_queue = self.source_queues[source_name]
        
        while self.running:
            try:
                record = source_queue.get(timeout=1)
                self.metrics["records_processed"] += 1
                
                # Process through all operators
                for operator in self.operators:
                    results = operator.process_record(record)
                    
                    for window, result in results:
                        self.metrics["windows_completed"] += 1
                        # Emit to sinks
                        for sink_name, sink_queue in self.sink_queues.items():
                            output_record = StreamRecord(
                                key=window,
                                value=result,
                                event_time=record.event_time,
                                processing_time=time.time(),
                                stream_id=f"window_{window.start}_{window.end}"
                            )
                            sink_queue.put(output_record)
                            self.metrics["records_emitted"] += 1
                            
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error processing record: {e}")
                
    def _generate_watermarks(self):
        """Generate watermarks based on processing time"""
        while self.running:
            current_time = time.time()
            
            for operator in self.operators:
                for partition in range(self.parallelism):
                    # Simple watermark generation: current time - allowed lateness
                    watermark_time = current_time - operator.allowed_lateness
                    operator.update_watermark(partition, watermark_time)
                    self.metrics["watermarks_updated"] += 1
                    
            time.sleep(1)  # Update watermarks every second
            
    def start(self):
        """Start the stream processor"""
        self.running = True
        
        # Start source processing threads
        for source_name in self.source_queues:
            thread = threading.Thread(
                target=self._process_source,
                args=(source_name,),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
            
        # Start watermark generation thread
        watermark_thread = threading.Thread(
            target=self._generate_watermarks,
            daemon=True
        )
        watermark_thread.start()
        self.threads.append(watermark_thread)
        
        logger.info(f"Stream processor started with {len(self.source_queues)} sources")
        
    def stop(self):
        """Stop the stream processor"""
        self.running = False
        for thread in self.threads:
            thread.join(timeout=5)
        logger.info("Stream processor stopped")


class KafkaSimulator:
    """Simulates Kafka message queue for testing"""
    
    def __init__(self, num_partitions: int = 3):
        self.num_partitions = num_partitions
        self.partitions = [queue.Queue() for _ in range(num_partitions)]
        self.consumer_groups = defaultdict(list)
        
    def produce(self, topic: str, key: Any, value: Any, partition: int = None):
        """Produce a message to Kafka"""
        if partition is None:
            partition = hash(key) % self.num_partitions
            
        record = StreamRecord(
            key=key,
            value=value,
            event_time=time.time(),
            processing_time=time.time(),
            partition=partition,
            offset=self.partitions[partition].qsize(),
            stream_id=topic
        )
        
        self.partitions[partition].put(record)
        return record
        
    def consume(self, topic: str, consumer_group: str, timeout: float = 1.0) -> Optional[StreamRecord]:
        """Consume a message from Kafka"""
        if consumer_group not in self.consumer_groups:
            self.consumer_groups[consumer_group] = [0] * self.num_partitions
            
        for partition in range(self.num_partitions):
            try:
                record = self.partitions[partition].get(timeout=timeout)
                self.consumer_groups[consumer_group][partition] += 1
                return record
            except queue.Empty:
                continue
                
        return None


class ExactlyOnceProcessor:
    """Implements exactly-once processing semantics"""
    
    def __init__(self, checkpoint_dir: str = "/tmp/checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        self.state = {}
        self.transaction_log = []
        self.current_transaction = None
        
    def begin_transaction(self):
        """Begin a new transaction"""
        self.current_transaction = {
            "id": str(uuid.uuid4()),
            "start_time": time.time(),
            "operations": []
        }
        
    def commit_transaction(self):
        """Commit current transaction"""
        if self.current_transaction:
            self.current_transaction["commit_time"] = time.time()
            self.transaction_log.append(self.current_transaction)
            self._write_checkpoint()
            self.current_transaction = None
            
    def rollback_transaction(self):
        """Rollback current transaction"""
        if self.current_transaction:
            logger.warning(f"Rolling back transaction {self.current_transaction['id']}")
            self.current_transaction = None
            
    def _write_checkpoint(self):
        """Write checkpoint to disk"""
        # In production, this would write to persistent storage
        checkpoint_data = {
            "state": self.state,
            "transaction_log": self.transaction_log[-100:],  # Keep last 100 transactions
            "timestamp": time.time()
        }
        logger.debug(f"Checkpoint written with {len(self.state)} state entries")
        
    def update_state(self, key: Any, value: Any):
        """Update state within current transaction"""
        if self.current_transaction:
            self.current_transaction["operations"].append({
                "type": "update",
                "key": key,
                "value": value,
                "timestamp": time.time()
            })
            self.state[key] = value


class StreamJoinOperator:
    """Joins two streams based on key and time window"""
    
    def __init__(self, 
                 join_window: int,
                 join_type: str = "inner"):
        self.join_window = join_window
        self.join_type = join_type
        self.left_buffer = defaultdict(deque)
        self.right_buffer = defaultdict(deque)
        self.state_backend = StateBackend()
        
    def process_left(self, record: StreamRecord) -> List[Tuple]:
        """Process record from left stream"""
        results = []
        key = record.key
        
        # Add to left buffer
        self.left_buffer[key].append(record)
        self._cleanup_buffer(self.left_buffer[key], record.event_time)
        
        # Check for matches in right buffer
        for right_record in self.right_buffer.get(key, []):
            if abs(record.event_time - right_record.event_time) <= self.join_window:
                joined = self._join_records(record, right_record)
                results.append(joined)
                
        return results
    
    def process_right(self, record: StreamRecord) -> List[Tuple]:
        """Process record from right stream"""
        results = []
        key = record.key
        
        # Add to right buffer
        self.right_buffer[key].append(record)
        self._cleanup_buffer(self.right_buffer[key], record.event_time)
        
        # Check for matches in left buffer
        for left_record in self.left_buffer.get(key, []):
            if abs(record.event_time - left_record.event_time) <= self.join_window:
                joined = self._join_records(left_record, record)
                results.append(joined)
                
        return results
    
    def _cleanup_buffer(self, buffer: deque, current_time: float):
        """Remove old records from buffer"""
        while buffer and current_time - buffer[0].event_time > self.join_window:
            buffer.popleft()
            
    def _join_records(self, left: StreamRecord, right: StreamRecord) -> Tuple:
        """Join two records"""
        return (left.key, left.value, right.value, {
            "left_time": left.event_time,
            "right_time": right.event_time,
            "join_time": time.time()
        })


class PatternDetector:
    """Detects patterns in streams (Complex Event Processing)"""
    
    def __init__(self, pattern: List[Dict], within: int):
        self.pattern = pattern
        self.within = within
        self.active_sequences = defaultdict(list)
        self.completed_patterns = []
        
    def process_event(self, event: StreamRecord) -> List[Dict]:
        """Process event and detect patterns"""
        key = event.key
        results = []
        
        # Check if event matches any pattern step
        for seq in self.active_sequences[key]:
            next_step = seq["next_step"]
            if self._matches_pattern_step(event, self.pattern[next_step]):
                seq["events"].append(event)
                seq["next_step"] += 1
                
                # Check if pattern is complete
                if seq["next_step"] >= len(self.pattern):
                    results.append({
                        "pattern": self.pattern,
                        "events": seq["events"],
                        "start_time": seq["events"][0].event_time,
                        "end_time": event.event_time
                    })
                    self.completed_patterns.append(seq)
                    
        # Start new sequence if first pattern step matches
        if self._matches_pattern_step(event, self.pattern[0]):
            self.active_sequences[key].append({
                "events": [event],
                "next_step": 1,
                "start_time": event.event_time
            })
            
        # Clean up old sequences
        self._cleanup_sequences(key, event.event_time)
        
        return results
    
    def _matches_pattern_step(self, event: StreamRecord, pattern_step: Dict) -> bool:
        """Check if event matches pattern step"""
        # Simple pattern matching - in production this would be more sophisticated
        for field, condition in pattern_step.items():
            if field == "value":
                if isinstance(condition, dict):
                    if "min" in condition and event.value < condition["min"]:
                        return False
                    if "max" in condition and event.value > condition["max"]:
                        return False
                elif event.value != condition:
                    return False
        return True
    
    def _cleanup_sequences(self, key: Any, current_time: float):
        """Clean up expired sequences"""
        self.active_sequences[key] = [
            seq for seq in self.active_sequences[key]
            if current_time - seq["start_time"] <= self.within
        ]


# Example Usage and Integration with Existing Codebase
class StreamingExample:
    """Example demonstrating streaming capabilities"""
    
    @staticmethod
    def word_count_streaming():
        """Streaming version of word count from MapReduce examples"""
        # Create Kafka simulator
        kafka = KafkaSimulator(num_partitions=3)
        
        # Create stream processor
        processor = StreamProcessor(
            time_characteristic=ProcessingTimeCharacteristic.EVENT_TIME,
            parallelism=3
        )
        
        # Create tumbling window aggregator for word counts
        word_counter = TumblingWindowAggregator(
            window_size=60,  # 1 minute windows
            allowed_lateness=30  # 30 seconds allowed lateness
        )
        
        processor.add_operator(word_counter)
        
        # Create queues for source and sink
        source_queue = queue.Queue()
        sink_queue = queue.Queue()
        
        processor.add_source("words", source_queue)
        processor.add_sink("counts", sink_queue)
        
        # Start processor
        processor.start()
        
        # Simulate streaming data
        words = ["hello", "world", "streaming", "processing", "real-time"]
        
        try:
            for i in range(100):
                word = words[i % len(words)]
                record = StreamRecord(
                    key=word,
                    value=1,
                    event_time=time.time(),
                    processing_time=time.time(),
                    partition=i % 3
                )
                source_queue.put(record)
                time.sleep(0.1)  # Simulate streaming rate
                
                # Check for results
                try:
                    result = sink_queue.get_nowait()
                    logger.info(f"Window result: {result.key} -> {result.value}")
                except queue.Empty:
                    pass
                    
        except KeyboardInterrupt:
            pass
        finally:
            processor.stop()
            
    @staticmethod
    def session_analytics():
        """Session-based analytics example"""
        # Create session window aggregator
        session_aggregator = SessionWindowAggregator(
            session_gap=300,  # 5 minute session gap
            allowed_lateness=60
        )
        
        # Create pattern detector for user behavior
        pattern_detector = PatternDetector(
            pattern=[
                {"value": {"min": 1}},  # First event
                {"value": {"min": 2}},  # Second event
                {"value": {"min": 3}}   # Third event
            ],
            within=600  # 10 minutes
        )
        
        # Simulate user session
        user_id = "user_123"
        events = [
            StreamRecord(key=user_id, value=1, event_time=time.time(), processing_time=time.time()),
            StreamRecord(key=user_id, value=2, event_time=time.time() + 60, processing_time=time.time()),
            StreamRecord(key=user_id, value=3, event_time=time.time() + 120, processing_time=time.time()),
        ]
        
        for event in events:
            # Process through session aggregator
            results = session_aggregator.process_record(event)
            for window, result in results:
                logger.info(f"Session window {window.start}-{window.end}: {result}")
                
            # Process through pattern detector
            patterns = pattern_detector.process_event(event)
            for pattern in patterns:
                logger.info(f"Pattern detected: {pattern}")


# Integration with existing system design primer examples
def integrate_with_existing_examples():
    """Integrate streaming examples with existing MapReduce examples"""
    
    # Example: Extend the word count from solutions/object_oriented_design/hash_table
    # This would be a streaming version of the word count example
    
    class StreamingWordCount:
        """Streaming word count extending hash table example"""
        
        def __init__(self, window_size: int = 60):
            self.window_size = window_size
            self.aggregator = TumblingWindowAggregator(window_size)
            self.word_counts = defaultdict(int)
            
        def process_stream(self, stream: Iterator[str]):
            """Process stream of words"""
            for word in stream:
                record = StreamRecord(
                    key=word,
                    value=1,
                    event_time=time.time(),
                    processing_time=time.time()
                )
                
                results = self.aggregator.process_record(record)
                for window, count in results:
                    self.word_counts[word] = count
                    
                yield word, self.word_counts[word]
    
    return StreamingWordCount


if __name__ == "__main__":
    # Run examples
    logger.info("Starting streaming examples...")
    
    # Example 1: Word count streaming
    StreamingExample.word_count_streaming()
    
    # Example 2: Session analytics
    StreamingExample.session_analytics()
    
    logger.info("Streaming examples completed")