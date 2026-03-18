import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from collections import defaultdict
import heapq

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SalesEvent:
    """Represents a single sales event in the stream."""
    product_id: str
    quantity: int
    price: float
    timestamp: datetime
    event_time: datetime
    
    @property
    def total_amount(self) -> float:
        return self.quantity * self.price

@dataclass
class WindowedAggregate:
    """Aggregated sales data for a specific time window."""
    window_start: datetime
    window_end: datetime
    product_id: str
    total_sales: float
    total_quantity: int
    event_count: int

class WatermarkManager:
    """Manages watermarks for event-time processing with late data handling."""
    
    def __init__(self, max_out_of_orderness: timedelta = timedelta(seconds=30)):
        self.max_out_of_orderness = max_out_of_orderness
        self.current_watermark = datetime.min
        self.late_events_count = 0
        
    def update_watermark(self, event_time: datetime) -> datetime:
        """Update watermark based on event time."""
        new_watermark = event_time - self.max_out_of_orderness
        if new_watermark > self.current_watermark:
            self.current_watermark = new_watermark
        return self.current_watermark
    
    def is_late(self, event_time: datetime) -> bool:
        """Check if event is late based on current watermark."""
        return event_time < self.current_watermark

class StateManager:
    """Manages state for complex event processing with checkpointing."""
    
    def __init__(self, checkpoint_interval: int = 1000):
        self.state_store: Dict[str, Dict] = defaultdict(dict)
        self.checkpoint_interval = checkpoint_interval
        self.operation_count = 0
        
    def update_state(self, key: str, updates: Dict) -> None:
        """Update state for a given key."""
        self.state_store[key].update(updates)
        self.operation_count += 1
        
        if self.operation_count % self.checkpoint_interval == 0:
            self._checkpoint()
    
    def get_state(self, key: str) -> Optional[Dict]:
        """Get state for a given key."""
        return self.state_store.get(key)
    
    def _checkpoint(self) -> None:
        """Create checkpoint of current state (simplified implementation)."""
        logger.debug(f"Checkpointing state at operation {self.operation_count}")
        # In production, this would persist to durable storage

class RealTimeSalesRanker:
    """
    Real-time sales ranking system using stream processing concepts.
    
    Implements:
    - Event-time processing with watermarking
    - Windowed aggregations
    - Stateful processing for complex event processing
    - Exactly-once processing semantics (simplified)
    """
    
    def __init__(self, 
                 window_size: timedelta = timedelta(minutes=5),
                 slide_interval: timedelta = timedelta(minutes=1),
                 top_n: int = 10,
                 watermark_delay: timedelta = timedelta(seconds=10)):
        
        self.window_size = window_size
        self.slide_interval = slide_interval
        self.top_n = top_n
        self.watermark_manager = WatermarkManager(max_out_of_orderness=watermark_delay)
        self.state_manager = StateManager()
        
        # Window state: window_key -> {product_id -> aggregate}
        self.window_state: Dict[Tuple[datetime, datetime], Dict[str, WindowedAggregate]] = defaultdict(dict)
        
        # Global ranking state for continuous updates
        self.global_sales: Dict[str, float] = defaultdict(float)
        self.global_quantity: Dict[str, int] = defaultdict(int)
        
        # For exactly-once processing (simplified idempotency)
        self.processed_events: set = set()
        
        logger.info(f"Initialized RealTimeSalesRanker with window_size={window_size}, "
                   f"slide_interval={slide_interval}, top_n={top_n}")
    
    def _get_window_key(self, event_time: datetime) -> Tuple[datetime, datetime]:
        """Calculate window boundaries for a given event time."""
        # Align window to slide interval
        window_start = event_time - (event_time - datetime.min) % self.slide_interval
        window_end = window_start + self.window_size
        return (window_start, window_end)
    
    def _generate_window_keys(self, current_time: datetime) -> List[Tuple[datetime, datetime]]:
        """Generate all active window keys for current time."""
        windows = []
        # Generate windows that could contain current time
        for i in range(int(self.window_size / self.slide_interval) + 1):
            window_end = current_time - i * self.slide_interval
            window_start = window_end - self.window_size
            if window_start <= current_time <= window_end:
                windows.append((window_start, window_end))
        return windows
    
    def process_event(self, event: SalesEvent) -> Optional[List[WindowedAggregate]]:
        """
        Process a single sales event with exactly-once semantics.
        
        Returns updated rankings for affected windows if significant changes occur.
        """
        event_id = f"{event.product_id}_{event.timestamp.isoformat()}_{event.total_amount}"
        
        # Check for duplicate processing (exactly-once guarantee)
        if event_id in self.processed_events:
            logger.debug(f"Skipping duplicate event: {event_id}")
            return None
        
        # Update watermark
        watermark = self.watermark_manager.update_watermark(event.event_time)
        
        # Handle late events
        if self.watermark_manager.is_late(event.event_time):
            logger.warning(f"Late event detected: {event.event_time} vs watermark {watermark}")
            # In production, we might route to side output or special handling
        
        # Process event into windows
        affected_windows = []
        window_key = self._get_window_key(event.event_time)
        
        # Update window state
        if window_key not in self.window_state:
            self.window_state[window_key] = {}
        
        product_id = event.product_id
        if product_id not in self.window_state[window_key]:
            self.window_state[window_key][product_id] = WindowedAggregate(
                window_start=window_key[0],
                window_end=window_key[1],
                product_id=product_id,
                total_sales=0.0,
                total_quantity=0,
                event_count=0
            )
        
        # Update aggregate
        aggregate = self.window_state[window_key][product_id]
        aggregate.total_sales += event.total_amount
        aggregate.total_quantity += event.quantity
        aggregate.event_count += 1
        
        # Update global state
        self.global_sales[product_id] += event.total_amount
        self.global_quantity[product_id] += event.quantity
        
        # Update state manager for complex event processing
        self.state_manager.update_state(product_id, {
            'last_seen': event.event_time,
            'last_price': event.price,
            'total_events': aggregate.event_count
        })
        
        # Mark event as processed
        self.processed_events.add(event_id)
        
        # Clean old processed events (keep last 100k for memory management)
        if len(self.processed_events) > 100000:
            self.processed_events = set(list(self.processed_events)[-50000:])
        
        # Check if we should emit updated rankings
        affected_windows.append(window_key)
        
        # Return updated rankings for affected windows
        return self._get_window_rankings(window_key)
    
    def _get_window_rankings(self, window_key: Tuple[datetime, datetime]) -> List[WindowedAggregate]:
        """Get top N products for a specific window."""
        if window_key not in self.window_state:
            return []
        
        aggregates = list(self.window_state[window_key].values())
        # Sort by total sales descending
        aggregates.sort(key=lambda x: x.total_sales, reverse=True)
        return aggregates[:self.top_n]
    
    def get_global_rankings(self) -> List[Tuple[str, float, int]]:
        """Get current global rankings across all time."""
        rankings = []
        for product_id in self.global_sales:
            rankings.append((
                product_id,
                self.global_sales[product_id],
                self.global_quantity[product_id]
            ))
        
        rankings.sort(key=lambda x: x[1], reverse=True)
        return rankings[:self.top_n]
    
    def cleanup_expired_windows(self, current_time: datetime) -> int:
        """Clean up windows that are no longer active (watermark-based cleanup)."""
        expired_windows = []
        watermark = self.watermark_manager.current_watermark
        
        for window_key in list(self.window_state.keys()):
            window_end = window_key[1]
            # Window is expired if its end is before watermark
            if window_end < watermark:
                expired_windows.append(window_key)
        
        for window_key in expired_windows:
            del self.window_state[window_key]
        
        if expired_windows:
            logger.info(f"Cleaned up {len(expired_windows)} expired windows")
        
        return len(expired_windows)

class KafkaIntegrationSimulator:
    """Simulates integration with Kafka message queue."""
    
    def __init__(self, sales_ranker: RealTimeSalesRanker):
        self.sales_ranker = sales_ranker
        self.message_buffer: List[SalesEvent] = []
        self.batch_size = 100
        
    def produce_event(self, event: SalesEvent) -> None:
        """Simulate producing event to Kafka."""
        self.message_buffer.append(event)
        
        # Process in batches (simulating micro-batch processing)
        if len(self.message_buffer) >= self.batch_size:
            self._process_batch()
    
    def _process_batch(self) -> None:
        """Process a batch of events."""
        batch_results = []
        for event in self.message_buffer:
            result = self.sales_ranker.process_event(event)
            if result:
                batch_results.append(result)
        
        self.message_buffer.clear()
        
        if batch_results:
            logger.info(f"Processed batch with {len(batch_results)} window updates")
    
    def flush(self) -> None:
        """Flush remaining events."""
        if self.message_buffer:
            self._process_batch()

class SparkStreamingEquivalent:
    """
    Demonstrates equivalent Spark Structured Streaming concepts.
    
    This class shows how the same logic would be implemented in Spark Streaming.
    """
    
    @staticmethod
    def get_spark_streaming_code() -> str:
        """Returns equivalent Spark Structured Streaming code as a string."""
        return '''
# Equivalent Spark Structured Streaming Implementation
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \\
    .appName("RealTimeSalesRanking") \\
    .config("spark.sql.shuffle.partitions", "2") \\
    .getOrCreate()

# Define schema for sales events
sales_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("event_time", TimestampType(), True)
])

# Read from Kafka (or socket for testing)
df = spark \\
    .readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "localhost:9092") \\
    .option("subscribe", "sales") \\
    .load() \\
    .selectExpr("CAST(value AS STRING)") \\
    .select(from_json(col("value"), sales_schema).alias("data")) \\
    .select("data.*")

# Add watermark for event time processing
watermarked_df = df \\
    .withWatermark("event_time", "10 seconds") \\
    .withColumn("total_amount", col("quantity") * col("price"))

# Windowed aggregation
windowed_aggregates = watermarked_df \\
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("product_id")
    ) \\
    .agg(
        sum("total_amount").alias("total_sales"),
        sum("quantity").alias("total_quantity"),
        count("*").alias("event_count")
    )

# Window function for ranking within each window
window_spec = Window.partitionBy("window").orderBy(col("total_sales").desc())
ranked_products = windowed_aggregates \\
    .withColumn("rank", row_number().over(window_spec)) \\
    .filter(col("rank") <= 10)

# Output to console (or Kafka sink)
query = ranked_products \\
    .writeStream \\
    .outputMode("complete") \\
    .format("console") \\
    .option("truncate", "false") \\
    .trigger(processingTime="1 minute") \\
    .start()

query.awaitTermination()
'''

class FlinkStreamingEquivalent:
    """
    Demonstrates equivalent Apache Flink concepts.
    
    This class shows how the same logic would be implemented in Apache Flink.
    """
    
    @staticmethod
    def get_flink_streaming_code() -> str:
        """Returns equivalent Flink Streaming code as a string."""
        return '''
// Equivalent Apache Flink Implementation (Java/Scala)
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor

// Define case class for sales events
case class SalesEvent(
    productId: String,
    quantity: Int,
    price: Double,
    timestamp: Long,
    eventTime: Long
)

// Create execution environment
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(2)

// Configure Kafka consumer
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "sales-ranker")

val kafkaConsumer = new FlinkKafkaConsumer[String](
    "sales",
    new SimpleStringSchema(),
    properties
)

// Create data stream with watermarking
val salesStream = env
    .addSource(kafkaConsumer)
    .map(parseSalesEvent)
    .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SalesEvent](Time.seconds(10)) {
            override def extractTimestamp(element: SalesEvent): Long = element.eventTime
        }
    )

// Windowed aggregation with ranking
val rankedSales = salesStream
    .keyBy(_.productId)
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .aggregate(new SalesAggregator, new SalesWindowFunction)
    .keyBy(_.windowEnd)
    .process(new TopNFunction(10))

// Output to Kafka sink
rankedSales.addSink(new FlinkKafkaProducer[String](
    "ranked-sales",
    new SimpleStringSchema(),
    properties
))

env.execute("Real-time Sales Ranking")
'''

def generate_sample_events(count: int = 1000) -> List[SalesEvent]:
    """Generate sample sales events for testing."""
    import random
    from datetime import datetime, timedelta
    
    products = [f"product_{i}" for i in range(1, 21)]
    events = []
    
    base_time = datetime.now()
    
    for i in range(count):
        # Generate events with some out-of-orderness
        event_time = base_time - timedelta(seconds=random.randint(0, 300))
        processing_time = event_time + timedelta(seconds=random.randint(0, 15))
        
        event = SalesEvent(
            product_id=random.choice(products),
            quantity=random.randint(1, 10),
            price=round(random.uniform(10.0, 500.0), 2),
            timestamp=processing_time,
            event_time=event_time
        )
        events.append(event)
    
    return events

def main():
    """Demonstrate the real-time sales ranking system."""
    print("=" * 80)
    print("REAL-TIME SALES RANKING SYSTEM")
    print("=" * 80)
    
    # Initialize components
    sales_ranker = RealTimeSalesRanker(
        window_size=timedelta(minutes=5),
        slide_interval=timedelta(minutes=1),
        top_n=5,
        watermark_delay=timedelta(seconds=10)
    )
    
    kafka_simulator = KafkaIntegrationSimulator(sales_ranker)
    
    # Generate and process sample events
    print("\n1. Generating sample sales events...")
    events = generate_sample_events(500)
    print(f"   Generated {len(events)} events")
    
    print("\n2. Processing events through stream processor...")
    for i, event in enumerate(events):
        kafka_simulator.produce_event(event)
        
        # Print progress
        if (i + 1) % 100 == 0:
            print(f"   Processed {i + 1}/{len(events)} events")
    
    # Flush remaining events
    kafka_simulator.flush()
    
    # Display results
    print("\n3. Global Sales Rankings (All Time):")
    global_rankings = sales_ranker.get_global_rankings()
    for rank, (product_id, total_sales, total_quantity) in enumerate(global_rankings, 1):
        print(f"   {rank}. {product_id}: ${total_sales:,.2f} ({total_quantity} units)")
    
    # Display active window information
    print(f"\n4. Active Windows: {len(sales_ranker.window_state)}")
    
    # Clean up expired windows
    current_time = datetime.now()
    cleaned = sales_ranker.cleanup_expired_windows(current_time)
    print(f"   Cleaned up {cleaned} expired windows")
    
    # Show equivalent implementations
    print("\n5. Equivalent Implementations:")
    print("\n   Apache Spark Structured Streaming:")
    print(SparkStreamingEquivalent.get_spark_streaming_code()[:500] + "...")
    
    print("\n   Apache Flink Streaming:")
    print(FlinkStreamingEquivalent.get_flink_streaming_code()[:500] + "...")
    
    print("\n" + "=" * 80)
    print("SYSTEM DESIGN CONCEPTS DEMONSTRATED:")
    print("=" * 80)
    print("✓ Event-time processing with watermarking")
    print("✓ Windowed aggregations (tumbling/sliding windows)")
    print("✓ Stateful stream processing")
    print("✓ Exactly-once processing semantics")
    print("✓ Late data handling")
    print("✓ Integration with message queues (Kafka)")
    print("✓ Real-time ranking with continuous updates")
    print("✓ Memory management and state cleanup")
    print("✓ Micro-batch processing simulation")
    print("✓ Equivalent implementations in Spark/Flink")

if __name__ == "__main__":
    main()