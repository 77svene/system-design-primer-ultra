# streaming/exactly_once_processing.py

"""
Real-time Stream Processing Extensions for System Design Primer.

This module extends MapReduce examples with Apache Flink/Spark Streaming equivalents
for real-time analytics, exactly-once processing, and windowed aggregations.

Implements:
- Streaming versions of batch processing examples
- Watermarking for event time processing
- State management for complex event processing
- Integration with message queues like Kafka
- Exactly-once processing semantics
- Windowed aggregations with event time
"""

import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict, deque
from abc import ABC, abstractmethod
import threading
from concurrent.futures import ThreadPoolExecutor
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProcessingGuarantee(Enum):
    """Processing guarantee semantics for stream processing."""
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"
    AT_MOST_ONCE = "at_most_once"


class WindowType(Enum):
    """Types of windowing strategies for stream processing."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


class WatermarkStrategy(Enum):
    """Watermark strategies for event time processing."""
    BOUNDED_OUT_OF_ORDER = "bounded_out_of_order"
    PUNCTUATED = "punctuated"
    MONOTONOUS = "monotonous"


@dataclass
class Event:
    """Base event class for stream processing."""
    event_id: str
    event_time: datetime
    processing_time: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)
    partition_key: Optional[str] = None
    source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for serialization."""
        result = asdict(self)
        result['event_time'] = self.event_time.isoformat()
        result['processing_time'] = self.processing_time.isoformat()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create event from dictionary."""
        data['event_time'] = datetime.fromisoformat(data['event_time'])
        data['processing_time'] = datetime.fromisoformat(data['processing_time'])
        return cls(**data)


@dataclass
class Window:
    """Represents a window in stream processing."""
    window_id: str
    window_type: WindowType
    start_time: datetime
    end_time: datetime
    max_timestamp: datetime = field(default_factory=datetime.now)
    is_complete: bool = False
    data: List[Event] = field(default_factory=list)
    aggregated_result: Optional[Dict[str, Any]] = None
    
    def add_event(self, event: Event) -> bool:
        """Add event to window if it belongs to this window."""
        if self.start_time <= event.event_time < self.end_time:
            self.data.append(event)
            if event.event_time > self.max_timestamp:
                self.max_timestamp = event.event_time
            return True
        return False
    
    def is_late(self, event: Event, watermark: datetime) -> bool:
        """Check if event is late based on watermark."""
        return event.event_time < watermark


@dataclass
class Watermark:
    """Watermark for event time processing."""
    timestamp: datetime
    partition: Optional[str] = None
    source: Optional[str] = None
    
    def __lt__(self, other: 'Watermark') -> bool:
        return self.timestamp < other.timestamp
    
    def __le__(self, other: 'Watermark') -> bool:
        return self.timestamp <= other.timestamp


@dataclass
class Checkpoint:
    """Checkpoint for exactly-once processing."""
    checkpoint_id: str
    timestamp: datetime
    state_snapshot: Dict[str, Any]
    offsets: Dict[str, int]  # Topic/partition -> offset mapping
    watermark: Optional[Watermark] = None
    is_complete: bool = False


class StateBackend(ABC):
    """Abstract base class for state backends."""
    
    @abstractmethod
    def get_state(self, key: str) -> Optional[Any]:
        """Get state for a key."""
        pass
    
    @abstractmethod
    def set_state(self, key: str, value: Any) -> None:
        """Set state for a key."""
        pass
    
    @abstractmethod
    def checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Create checkpoint of current state."""
        pass
    
    @abstractmethod
    def restore(self, checkpoint_id: str) -> None:
        """Restore state from checkpoint."""
        pass


class InMemoryStateBackend(StateBackend):
    """In-memory state backend for development and testing."""
    
    def __init__(self):
        self.state_store: Dict[str, Any] = {}
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
    
    def get_state(self, key: str) -> Optional[Any]:
        return self.state_store.get(key)
    
    def set_state(self, key: str, value: Any) -> None:
        self.state_store[key] = value
    
    def checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        snapshot = self.state_store.copy()
        self.checkpoints[checkpoint_id] = snapshot
        return snapshot
    
    def restore(self, checkpoint_id: str) -> None:
        if checkpoint_id in self.checkpoints:
            self.state_store = self.checkpoints[checkpoint_id].copy()


class StreamProcessor(ABC):
    """Abstract base class for stream processors."""
    
    def __init__(self, 
                 processing_guarantee: ProcessingGuarantee = ProcessingGuarantee.EXACTLY_ONCE,
                 watermark_strategy: WatermarkStrategy = WatermarkStrategy.BOUNDED_OUT_OF_ORDER,
                 max_out_of_order_delay: timedelta = timedelta(seconds=5)):
        self.processing_guarantee = processing_guarantee
        self.watermark_strategy = watermark_strategy
        self.max_out_of_order_delay = max_out_of_order_delay
        self.state_backend = InMemoryStateBackend()
        self.watermarks: Dict[str, Watermark] = {}
        self.windows: Dict[str, Window] = {}
        self.pending_events: Dict[str, deque] = defaultdict(deque)
        self.checkpoint_interval = timedelta(seconds=30)
        self.last_checkpoint_time = datetime.now()
        self.checkpoint_counter = 0
        self.is_running = False
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    @abstractmethod
    def process_event(self, event: Event) -> None:
        """Process a single event."""
        pass
    
    @abstractmethod
    def create_window(self, window_type: WindowType, 
                      start_time: datetime, end_time: datetime) -> Window:
        """Create a new window."""
        pass
    
    def update_watermark(self, partition: str, timestamp: datetime) -> None:
        """Update watermark for a partition."""
        if partition not in self.watermarks or timestamp > self.watermarks[partition].timestamp:
            self.watermarks[partition] = Watermark(
                timestamp=timestamp,
                partition=partition,
                source=self.__class__.__name__
            )
    
    def get_watermark(self, partition: str) -> Optional[Watermark]:
        """Get current watermark for a partition."""
        return self.watermarks.get(partition)
    
    def handle_late_event(self, event: Event, watermark: Watermark) -> None:
        """Handle late-arriving event."""
        logger.warning(f"Late event detected: {event.event_id} at {event.event_time}, "
                      f"watermark: {watermark.timestamp}")
        # For exactly-once processing, we might want to buffer late events
        # or send them to a side output for special handling
        if event.partition_key:
            self.pending_events[event.partition_key].append(event)
    
    def trigger_window(self, window_id: str) -> Optional[Dict[str, Any]]:
        """Trigger computation for a completed window."""
        if window_id not in self.windows:
            return None
        
        window = self.windows[window_id]
        if not window.is_complete:
            # Process window data
            result = self.compute_window_result(window)
            window.aggregated_result = result
            window.is_complete = True
            
            # Emit result
            self.emit_window_result(window)
            
            # Clean up old windows (keep last 100)
            if len(self.windows) > 100:
                oldest_key = min(self.windows.keys(), 
                               key=lambda k: self.windows[k].start_time)
                del self.windows[oldest_key]
            
            return result
        return window.aggregated_result
    
    @abstractmethod
    def compute_window_result(self, window: Window) -> Dict[str, Any]:
        """Compute aggregated result for a window."""
        pass
    
    @abstractmethod
    def emit_window_result(self, window: Window) -> None:
        """Emit window result to downstream systems."""
        pass
    
    def create_checkpoint(self) -> Checkpoint:
        """Create a checkpoint for exactly-once processing."""
        self.checkpoint_counter += 1
        checkpoint_id = f"checkpoint_{self.checkpoint_counter}_{int(time.time())}"
        
        # Get state snapshot
        state_snapshot = self.state_backend.checkpoint(checkpoint_id)
        
        # Get current offsets (simulated)
        offsets = self.get_current_offsets()
        
        # Get current watermark
        watermark = self.get_global_watermark()
        
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            timestamp=datetime.now(),
            state_snapshot=state_snapshot,
            offsets=offsets,
            watermark=watermark,
            is_complete=True
        )
        
        logger.info(f"Created checkpoint: {checkpoint_id}")
        self.last_checkpoint_time = datetime.now()
        return checkpoint
    
    def restore_from_checkpoint(self, checkpoint: Checkpoint) -> None:
        """Restore state from checkpoint."""
        self.state_backend.restore(checkpoint.checkpoint_id)
        if checkpoint.watermark:
            for partition in checkpoint.watermark.partition or []:
                self.watermarks[partition] = checkpoint.watermark
        logger.info(f"Restored from checkpoint: {checkpoint.checkpoint_id}")
    
    def get_current_offsets(self) -> Dict[str, int]:
        """Get current offsets for all partitions (simulated)."""
        # In real implementation, this would come from Kafka consumer
        return {"topic_partition_0": 0, "topic_partition_1": 0}
    
    def get_global_watermark(self) -> Optional[Watermark]:
        """Get global watermark (minimum across all partitions)."""
        if not self.watermarks:
            return None
        return min(self.watermarks.values(), key=lambda w: w.timestamp)
    
    def should_checkpoint(self) -> bool:
        """Check if it's time to create a checkpoint."""
        return (datetime.now() - self.last_checkpoint_time) >= self.checkpoint_interval
    
    def start(self) -> None:
        """Start the stream processor."""
        self.is_running = True
        logger.info("Stream processor started")
    
    def stop(self) -> None:
        """Stop the stream processor."""
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("Stream processor stopped")


class WordCountStreamProcessor(StreamProcessor):
    """Streaming word count processor with windowed aggregations."""
    
    def __init__(self, 
                 window_size: timedelta = timedelta(minutes=1),
                 window_type: WindowType = WindowType.TUMBLING,
                 **kwargs):
        super().__init__(**kwargs)
        self.window_size = window_size
        self.window_type = window_type
        self.word_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.window_results: Dict[str, Dict[str, int]] = {}
    
    def process_event(self, event: Event) -> None:
        """Process a word count event."""
        if 'text' not in event.data:
            logger.warning(f"Event {event.event_id} missing 'text' field")
            return
        
        text = event.data['text'].lower()
        words = text.split()
        
        # Update watermark based on event time
        if event.partition_key:
            self.update_watermark(event.partition_key, event.event_time)
        
        # Get current watermark
        watermark = self.get_watermark(event.partition_key) if event.partition_key else None
        
        # Check if event is late
        if watermark and event.event_time < watermark.timestamp - self.max_out_of_order_delay:
            self.handle_late_event(event, watermark)
            return
        
        # Determine window for event
        window_start = self.get_window_start(event.event_time)
        window_end = window_start + self.window_size
        window_id = f"window_{window_start.isoformat()}_{window_end.isoformat()}"
        
        # Create or get window
        if window_id not in self.windows:
            self.windows[window_id] = self.create_window(
                self.window_type, window_start, window_end
            )
        
        window = self.windows[window_id]
        window.add_event(event)
        
        # Update word counts for this window
        for word in words:
            self.word_counts[window_id][word] += 1
        
        # Check if window should be triggered
        if watermark and window_end <= watermark.timestamp:
            self.trigger_window(window_id)
        
        # Create checkpoint if needed
        if self.should_checkpoint():
            self.create_checkpoint()
    
    def create_window(self, window_type: WindowType, 
                      start_time: datetime, end_time: datetime) -> Window:
        """Create a new window for word counting."""
        window_id = f"window_{start_time.isoformat()}_{end_time.isoformat()}"
        return Window(
            window_id=window_id,
            window_type=window_type,
            start_time=start_time,
            end_time=end_time
        )
    
    def compute_window_result(self, window: Window) -> Dict[str, Any]:
        """Compute word count result for a window."""
        window_id = window.window_id
        word_counts = self.word_counts.get(window_id, {})
        
        # Sort by count descending
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        
        result = {
            'window_id': window_id,
            'window_start': window.start_time.isoformat(),
            'window_end': window.end_time.isoformat(),
            'total_words': sum(word_counts.values()),
            'unique_words': len(word_counts),
            'top_words': sorted_words[:10],  # Top 10 words
            'word_counts': dict(word_counts),
            'event_count': len(window.data),
            'processing_time': datetime.now().isoformat()
        }
        
        # Store result
        self.window_results[window_id] = result
        
        return result
    
    def emit_window_result(self, window: Window) -> None:
        """Emit window result to downstream systems."""
        result = window.aggregated_result
        if result:
            logger.info(f"Window result: {result['window_id']} - "
                       f"Total words: {result['total_words']}, "
                       f"Unique words: {result['unique_words']}")
            
            # In real implementation, this would send to Kafka, database, etc.
            # For now, we just log it
    
    def get_window_start(self, event_time: datetime) -> datetime:
        """Calculate window start time based on event time."""
        if self.window_type == WindowType.TUMBLING:
            # Tumbling windows: fixed size, non-overlapping
            window_seconds = self.window_size.total_seconds()
            timestamp_seconds = event_time.timestamp()
            window_start_seconds = (timestamp_seconds // window_seconds) * window_seconds
            return datetime.fromtimestamp(window_start_seconds)
        elif self.window_type == WindowType.SLIDING:
            # Sliding windows: fixed size, overlapping
            # For simplicity, use same logic as tumbling
            return self.get_window_start(event_time)
        elif self.window_type == WindowType.SESSION:
            # Session windows: dynamic size based on gaps
            # For simplicity, use tumbling logic
            return self.get_window_start(event_time)
        else:
            return event_time.replace(second=0, microsecond=0)


class ClickStreamProcessor(StreamProcessor):
    """Streaming click stream processor for user behavior analytics."""
    
    def __init__(self, 
                 session_timeout: timedelta = timedelta(minutes=30),
                 **kwargs):
        super().__init__(**kwargs)
        self.session_timeout = session_timeout
        self.user_sessions: Dict[str, Dict[str, Any]] = {}
        self.user_profiles: Dict[str, Dict[str, Any]] = {}
        self.click_patterns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    def process_event(self, event: Event) -> None:
        """Process a click stream event."""
        required_fields = ['user_id', 'page', 'action']
        for field in required_fields:
            if field not in event.data:
                logger.warning(f"Click event {event.event_id} missing '{field}' field")
                return
        
        user_id = event.data['user_id']
        page = event.data['page']
        action = event.data['action']
        
        # Update watermark
        if event.partition_key:
            self.update_watermark(event.partition_key, event.event_time)
        
        # Get or create user session
        session = self.get_or_create_session(user_id, event.event_time)
        
        # Update session with click event
        session['last_activity'] = event.event_time
        session['clicks'].append({
            'timestamp': event.event_time.isoformat(),
            'page': page,
            'action': action,
            'data': event.data
        })
        
        # Update user profile
        self.update_user_profile(user_id, page, action)
        
        # Detect patterns
        self.detect_click_patterns(user_id, session, event)
        
        # Check for session timeout
        watermark = self.get_watermark(event.partition_key) if event.partition_key else None
        if watermark:
            self.check_session_timeouts(watermark.timestamp)
        
        # Create checkpoint if needed
        if self.should_checkpoint():
            self.create_checkpoint()
    
    def get_or_create_session(self, user_id: str, event_time: datetime) -> Dict[str, Any]:
        """Get existing session or create new one for user."""
        if user_id not in self.user_sessions:
            session_id = str(uuid.uuid4())
            self.user_sessions[user_id] = {
                'session_id': session_id,
                'user_id': user_id,
                'start_time': event_time,
                'last_activity': event_time,
                'clicks': [],
                'pages_visited': set(),
                'total_time': timedelta(0)
            }
        
        return self.user_sessions[user_id]
    
    def update_user_profile(self, user_id: str, page: str, action: str) -> None:
        """Update user profile based on click behavior."""
        if user_id not in self.user_profiles:
            self.user_profiles[user_id] = {
                'user_id': user_id,
                'first_seen': datetime.now(),
                'last_seen': datetime.now(),
                'total_sessions': 0,
                'total_clicks': 0,
                'favorite_pages': defaultdict(int),
                'action_counts': defaultdict(int)
            }
        
        profile = self.user_profiles[user_id]
        profile['last_seen'] = datetime.now()
        profile['total_clicks'] += 1
        profile['favorite_pages'][page] += 1
        profile['action_counts'][action] += 1
    
    def detect_click_patterns(self, user_id: str, session: Dict[str, Any], 
                             event: Event) -> None:
        """Detect click patterns for anomaly detection or recommendations."""
        clicks = session['clicks']
        if len(clicks) < 2:
            return
        
        # Simple pattern: rapid clicking
        recent_clicks = clicks[-5:]  # Last 5 clicks
        if len(recent_clicks) >= 3:
            time_diffs = []
            for i in range(1, len(recent_clicks)):
                prev_time = datetime.fromisoformat(recent_clicks[i-1]['timestamp'])
                curr_time = datetime.fromisoformat(recent_clicks[i]['timestamp'])
                time_diffs.append((curr_time - prev_time).total_seconds())
            
            avg_time_diff = sum(time_diffs) / len(time_diffs)
            if avg_time_diff < 1.0:  # Less than 1 second between clicks
                pattern = {
                    'user_id': user_id,
                    'pattern_type': 'rapid_clicking',
                    'timestamp': event.event_time.isoformat(),
                    'avg_time_between_clicks': avg_time_diff,
                    'session_id': session['session_id']
                }
                self.click_patterns[user_id].append(pattern)
                logger.info(f"Detected rapid clicking pattern for user {user_id}")
    
    def check_session_timeouts(self, watermark_time: datetime) -> None:
        """Check for sessions that have timed out."""
        timeout_threshold = watermark_time - self.session_timeout
        
        sessions_to_end = []
        for user_id, session in self.user_sessions.items():
            if session['last_activity'] < timeout_threshold:
                sessions_to_end.append(user_id)
        
        for user_id in sessions_to_end:
            self.end_session(user_id, watermark_time)
    
    def end_session(self, user_id: str, end_time: datetime) -> None:
        """End a user session and compute session metrics."""
        if user_id not in self.user_sessions:
            return
        
        session = self.user_sessions[user_id]
        session_duration = end_time - session['start_time']
        session['total_time'] = session_duration
        session['end_time'] = end_time
        
        # Compute session metrics
        metrics = {
            'session_id': session['session_id'],
            'user_id': user_id,
            'duration_seconds': session_duration.total_seconds(),
            'click_count': len(session['clicks']),
            'pages_visited': len(session['pages_visited']),
            'clicks_per_minute': len(session['clicks']) / max(1, session_duration.total_seconds() / 60)
        }
        
        logger.info(f"Session ended: {metrics}")
        
        # Update user profile
        if user_id in self.user_profiles:
            self.user_profiles[user_id]['total_sessions'] += 1
        
        # Remove session
        del self.user_sessions[user_id]
    
    def create_window(self, window_type: WindowType, 
                      start_time: datetime, end_time: datetime) -> Window:
        """Create window for click stream analysis."""
        window_id = f"click_window_{start_time.isoformat()}_{end_time.isoformat()}"
        return Window(
            window_id=window_id,
            window_type=window_type,
            start_time=start_time,
            end_time=end_time
        )
    
    def compute_window_result(self, window: Window) -> Dict[str, Any]:
        """Compute click stream analytics for a window."""
        # Aggregate metrics from all sessions in window
        total_sessions = len(self.user_sessions)
        total_clicks = sum(len(session['clicks']) for session in self.user_sessions.values())
        
        # Get top pages
        page_counts = defaultdict(int)
        for session in self.user_sessions.values():
            for click in session['clicks']:
                page_counts[click['page']] += 1
        
        top_pages = sorted(page_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        result = {
            'window_id': window.window_id,
            'window_start': window.start_time.isoformat(),
            'window_end': window.end_time.isoformat(),
            'total_sessions': total_sessions,
            'total_clicks': total_clicks,
            'avg_clicks_per_session': total_clicks / max(1, total_sessions),
            'top_pages': top_pages,
            'unique_users': len(set(session['user_id'] for session in self.user_sessions.values())),
            'processing_time': datetime.now().isoformat()
        }
        
        return result
    
    def emit_window_result(self, window: Window) -> None:
        """Emit click stream analytics result."""
        result = window.aggregated_result
        if result:
            logger.info(f"Click analytics: {result['window_id']} - "
                       f"Sessions: {result['total_sessions']}, "
                       f"Clicks: {result['total_clicks']}")


class KafkaSimulator:
    """Simulates Kafka message queue for testing."""
    
    def __init__(self):
        self.topics: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.consumers: Dict[str, List[Callable]] = defaultdict(list)
        self.producer_offsets: Dict[str, int] = defaultdict(int)
    
    def create_topic(self, topic: str, partitions: int = 1) -> None:
        """Create a Kafka topic."""
        logger.info(f"Created topic: {topic} with {partitions} partitions")
    
    def produce(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> int:
        """Produce message to topic."""
        message = {
            'offset': self.producer_offsets[topic],
            'key': key,
            'value': value,
            'timestamp': datetime.now().isoformat()
        }
        self.topics[topic].append(message)
        self.producer_offsets[topic] += 1
        
        # Notify consumers
        for consumer in self.consumers[topic]:
            consumer(message)
        
        return message['offset']
    
    def consume(self, topic: str, callback: Callable) -> None:
        """Register consumer for topic."""
        self.consumers[topic].append(callback)
    
    def get_messages(self, topic: str, since_offset: int = 0) -> List[Dict[str, Any]]:
        """Get messages from topic since offset."""
        return [msg for msg in self.topics[topic] if msg['offset'] >= since_offset]


class StreamProcessingPipeline:
    """Complete stream processing pipeline with Kafka integration."""
    
    def __init__(self, 
                 processor: StreamProcessor,
                 input_topic: str,
                 output_topic: str,
                 kafka_simulator: Optional[KafkaSimulator] = None):
        self.processor = processor
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.kafka = kafka_simulator or KafkaSimulator()
        self.consumer_offset = 0
        self.is_running = False
    
    def start(self) -> None:
        """Start the stream processing pipeline."""
        self.is_running = True
        self.processor.start()
        
        # Register as consumer
        self.kafka.consume(self.input_topic, self._process_message)
        
        logger.info(f"Pipeline started: {self.input_topic} -> {self.output_topic}")
    
    def stop(self) -> None:
        """Stop the pipeline."""
        self.is_running = False
        self.processor.stop()
        logger.info("Pipeline stopped")
    
    def _process_message(self, message: Dict[str, Any]) -> None:
        """Process incoming message from Kafka."""
        try:
            # Convert message to Event
            event_data = message['value']
            event = Event.from_dict(event_data)
            
            # Process event
            self.processor.process_event(event)
            
            # Update consumer offset
            self.consumer_offset = max(self.consumer_offset, message['offset'] + 1)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def produce_result(self, result: Dict[str, Any]) -> None:
        """Produce processing result to output topic."""
        self.kafka.produce(
            topic=self.output_topic,
            key=result.get('window_id'),
            value=result
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            'input_topic': self.input_topic,
            'output_topic': self.output_topic,
            'consumer_offset': self.consumer_offset,
            'is_running': self.is_running,
            'processor_type': self.processor.__class__.__name__,
            'processing_guarantee': self.processor.processing_guarantee.value,
            'watermark_strategy': self.processor.watermark_strategy.value,
            'active_windows': len(self.processor.windows),
            'pending_events': sum(len(events) for events in self.processor.pending_events.values())
        }


# Example usage and demonstration
def demonstrate_stream_processing():
    """Demonstrate stream processing capabilities."""
    print("=== Real-time Stream Processing Demonstration ===\n")
    
    # Create Kafka simulator
    kafka = KafkaSimulator()
    kafka.create_topic("word-count-input", partitions=2)
    kafka.create_topic("word-count-output", partitions=1)
    
    # Create word count processor
    word_count_processor = WordCountStreamProcessor(
        window_size=timedelta(seconds=30),
        window_type=WindowType.TUMBLING,
        processing_guarantee=ProcessingGuarantee.EXACTLY_ONCE,
        watermark_strategy=WatermarkStrategy.BOUNDED_OUT_OF_ORDER,
        max_out_of_order_delay=timedelta(seconds=2)
    )
    
    # Create pipeline
    pipeline = StreamProcessingPipeline(
        processor=word_count_processor,
        input_topic="word-count-input",
        output_topic="word-count-output",
        kafka_simulator=kafka
    )
    
    # Start pipeline
    pipeline.start()
    
    # Simulate events
    events = [
        Event(
            event_id="1",
            event_time=datetime.now() - timedelta(seconds=5),
            data={"text": "hello world hello"},
            partition_key="partition-0",
            source="web-server"
        ),
        Event(
            event_id="2",
            event_time=datetime.now() - timedelta(seconds=3),
            data={"text": "hello stream processing"},
            partition_key="partition-0",
            source="web-server"
        ),
        Event(
            event_id="3",
            event_time=datetime.now() - timedelta(seconds=1),
            data={"text": "stream processing is awesome"},
            partition_key="partition-1",
            source="mobile-app"
        ),
        # Late event (should be handled)
        Event(
            event_id="4",
            event_time=datetime.now() - timedelta(seconds=10),
            data={"text": "late event"},
            partition_key="partition-0",
            source="batch-import"
        )
    ]
    
    print("Producing events to Kafka...")
    for event in events:
        kafka.produce(
            topic="word-count-input",
            key=event.partition_key,
            value=event.to_dict()
        )
        time.sleep(0.1)  # Simulate real-time arrival
    
    # Wait for processing
    time.sleep(2)
    
    # Show pipeline stats
    print("\nPipeline Statistics:")
    stats = pipeline.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Show window results
    print("\nWindow Results:")
    for window_id, window in word_count_processor.windows.items():
        if window.aggregated_result:
            result = window.aggregated_result
            print(f"  Window: {result['window_id']}")
            print(f"    Total words: {result['total_words']}")
            print(f"    Unique words: {result['unique_words']}")
            print(f"    Top words: {result['top_words'][:5]}")
    
    # Stop pipeline
    pipeline.stop()
    
    print("\n=== Demonstration Complete ===")


if __name__ == "__main__":
    demonstrate_stream_processing()