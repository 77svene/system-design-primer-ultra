# -*- coding: utf-8 -*-

from mrjob.job import MRJob
import json
import time
from datetime import datetime, timedelta
from typing import Iterator, Tuple, Any

# For streaming implementations
try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import StreamTableEnvironment, DataTypes
    from pyflink.table.descriptors import Schema, Kafka, Json
    from pyflink.common import WatermarkStrategy, Duration
    from pyflink.datastream.window import TumblingEventTimeWindows, Time
    from pyflink.datastream.functions import ProcessWindowFunction
    from pyflink.common.typeinfo import Types
    FLINK_AVAILABLE = True
except ImportError:
    FLINK_AVAILABLE = False

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, window, sum as spark_sum, from_json
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
    from pyspark.sql.streaming import DataStreamWriter
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


class SalesRanker(MRJob):
    """Original MapReduce implementation for batch processing."""

    def within_past_week(self, timestamp):
        """Return True if timestamp is within past week, False otherwise."""
        try:
            ts = datetime.fromtimestamp(float(timestamp))
            week_ago = datetime.now() - timedelta(days=7)
            return ts >= week_ago
        except (ValueError, TypeError):
            return False

    def mapper(self, _, line):
        """Parse each log line, extract and transform relevant lines.

        Emit key value pairs of the form:

        (foo, p1), 2
        (bar, p1), 2
        (bar, p1), 1
        (foo, p2), 3
        (bar, p3), 10
        (foo, p4), 1
        """
        timestamp, product_id, category, quantity = line.split('\t')
        if self.within_past_week(timestamp):
            yield (category, product_id), int(quantity)

    def reducer(self, key, values):
        """Sum values for each key.

        (foo, p1), 2
        (bar, p1), 3
        (foo, p2), 3
        (bar, p3), 10
        (foo, p4), 1
        """
        yield key, sum(values)

    def mapper_sort(self, key, value):
        """Construct key to ensure proper sorting.

        Transform key and value to the form:

        (foo, 2), p1
        (bar, 3), p1
        (foo, 3), p2
        (bar, 10), p3
        (foo, 1), p4

        The shuffle/sort step of MapReduce will then do a
        distributed sort on the keys, resulting in:

        (category1, 1), product4
        (category1, 2), product1
        (category1, 3), product2
        (category2, 3), product1
        (category2, 7), product3
        """
        category, product_id = key
        quantity = value
        yield (category, quantity), product_id

    def reducer_identity(self, key, value):
        yield key, value

    def steps(self):
        """Run the map and reduce steps."""
        return [
            self.mr(mapper=self.mapper,
                    reducer=self.reducer),
            self.mr(mapper=self.mapper_sort,
                    reducer=self.reducer_identity),
        ]


class SalesRankerFlinkStreaming:
    """Apache Flink streaming implementation for real-time analytics."""
    
    def __init__(self, kafka_servers="localhost:9092", topic="sales"):
        if not FLINK_AVAILABLE:
            raise ImportError("PyFlink is required for Flink streaming implementation")
        
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.table_env = StreamTableEnvironment.create(self.env)
        
    def create_source_table(self):
        """Create Kafka source table with watermarking for event time processing."""
        self.table_env.connect(
            Kafka()
            .version("universal")
            .topic(self.topic)
            .start_from_earliest()
            .property("bootstrap.servers", self.kafka_servers)
        ).with_format(
            Json()
            .fail_on_missing_field(False)
        ).with_schema(
            Schema()
            .field("event_time", DataTypes.TIMESTAMP(3))
            .field("product_id", DataTypes.STRING())
            .field("category", DataTypes.STRING())
            .field("quantity", DataTypes.INT())
        ).create_temporary_table("sales_source")
        
    def create_sink_table(self, output_topic="sales_rankings"):
        """Create Kafka sink table for results."""
        self.table_env.connect(
            Kafka()
            .version("universal")
            .topic(output_topic)
            .property("bootstrap.servers", self.kafka_servers)
        ).with_format(
            Json()
        ).with_schema(
            Schema()
            .field("window_start", DataTypes.TIMESTAMP(3))
            .field("window_end", DataTypes.TIMESTAMP(3))
            .field("category", DataTypes.STRING())
            .field("product_id", DataTypes.STRING())
            .field("total_quantity", DataTypes.BIGINT())
            .field("rank", DataTypes.INT())
        ).create_temporary_table("sales_rankings_sink")
        
    def process_stream(self, window_size_minutes=10):
        """Process stream with windowed aggregations and ranking."""
        # Create source table
        self.create_source_table()
        
        # Define watermark strategy for event time processing
        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(30)
        )
        
        # SQL query for windowed aggregation with ranking
        query = f"""
        WITH ranked_sales AS (
            SELECT 
                window_start,
                window_end,
                category,
                product_id,
                SUM(quantity) as total_quantity,
                ROW_NUMBER() OVER (
                    PARTITION BY window_start, window_end, category 
                    ORDER BY SUM(quantity) DESC
                ) as rank
            FROM TABLE(
                TUMBLE(
                    TABLE sales_source, 
                    DESCRIPTOR(event_time), 
                    INTERVAL '{window_size_minutes}' MINUTE
                )
            )
            GROUP BY window_start, window_end, category, product_id
        )
        SELECT 
            window_start,
            window_end,
            category,
            product_id,
            total_quantity,
            rank
        FROM ranked_sales
        WHERE rank <= 10  -- Top 10 products per category per window
        """
        
        # Execute query and insert into sink
        result_table = self.table_env.sql_query(query)
        result_table.execute_insert("sales_rankings_sink").wait()
        
    def run(self):
        """Execute the Flink streaming job."""
        print("Starting Flink streaming job for real-time sales ranking...")
        self.process_stream()
        self.env.execute("Real-time Sales Ranking")


class SalesRankerSparkStreaming:
    """Apache Spark Structured Streaming implementation."""
    
    def __init__(self, kafka_servers="localhost:9092", topic="sales"):
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is required for Spark streaming implementation")
        
        self.kafka_servers = kafka_servers
        self.topic = topic
        self.spark = SparkSession.builder \
            .appName("RealTimeSalesRanking") \
            .getOrCreate()
        
        # Define schema for incoming data
        self.schema = StructType([
            StructField("event_time", TimestampType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])
        
    def read_from_kafka(self):
        """Read streaming data from Kafka with watermarking."""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*") \
            .withWatermark("event_time", "30 seconds")  # 30-second watermark for late data
    
    def process_stream(self, window_duration="10 minutes", slide_duration="5 minutes"):
        """Process stream with windowed aggregations and exactly-once semantics."""
        # Read from Kafka
        sales_stream = self.read_from_kafka()
        
        # Windowed aggregation
        windowed_sales = sales_stream \
            .groupBy(
                window(col("event_time"), window_duration, slide_duration),
                col("category"),
                col("product_id")
            ) \
            .agg(spark_sum("quantity").alias("total_quantity"))
        
        # Add ranking within each window and category
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, desc
        
        window_spec = Window \
            .partitionBy("window.start", "window.end", "category") \
            .orderBy(desc("total_quantity"))
        
        ranked_sales = windowed_sales \
            .withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") <= 10)  # Top 10 per category per window
        
        # Write to sink (console for demo, could be Kafka, database, etc.)
        query = ranked_sales \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def run(self):
        """Execute the Spark streaming job."""
        print("Starting Spark Structured Streaming job for real-time sales ranking...")
        query = self.process_stream()
        query.awaitTermination()


class SalesRankerUnified:
    """Unified interface that can run batch (MapReduce) or streaming (Flink/Spark)."""
    
    @staticmethod
    def run_batch():
        """Run the original MapReduce batch job."""
        SalesRanker.run()
    
    @staticmethod
    def run_streaming_flink(**kwargs):
        """Run Flink streaming job."""
        if not FLINK_AVAILABLE:
            raise ImportError("PyFlink is not installed. Install with: pip install apache-flink")
        
        streamer = SalesRankerFlinkStreaming(**kwargs)
        streamer.run()
    
    @staticmethod
    def run_streaming_spark(**kwargs):
        """Run Spark streaming job."""
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is not installed. Install with: pip install pyspark")
        
        streamer = SalesRankerSparkStreaming(**kwargs)
        streamer.run()
    
    @staticmethod
    def run(mode="batch", engine="flink", **kwargs):
        """
        Run sales ranking in specified mode.
        
        Args:
            mode: 'batch' for MapReduce, 'streaming' for real-time
            engine: 'flink' or 'spark' (for streaming mode)
            **kwargs: Additional arguments for streaming engines
        """
        if mode == "batch":
            SalesRankerUnified.run_batch()
        elif mode == "streaming":
            if engine == "flink":
                SalesRankerUnified.run_streaming_flink(**kwargs)
            elif engine == "spark":
                SalesRankerUnified.run_streaming_spark(**kwargs)
            else:
                raise ValueError(f"Unsupported engine: {engine}. Use 'flink' or 'spark'.")
        else:
            raise ValueError(f"Unsupported mode: {mode}. Use 'batch' or 'streaming'.")


# Example of complex event processing with state management
class SalesEventProcessor:
    """Example of complex event processing with state management."""
    
    def __init__(self):
        self.state = {}  # In production, use Flink's state backend or Spark's state store
    
    def process_event(self, event):
        """Process individual sales events with stateful logic."""
        product_id = event['product_id']
        category = event['category']
        quantity = event['quantity']
        event_time = event['event_time']
        
        # Initialize state for product if not exists
        if product_id not in self.state:
            self.state[product_id] = {
                'category': category,
                'total_quantity': 0,
                'last_update': event_time,
                'trend': 'stable'
            }
        
        # Update state
        self.state[product_id]['total_quantity'] += quantity
        self.state[product_id]['last_update'] = event_time
        
        # Detect trends (simplified example)
        if quantity > 10:  # Threshold for significant sale
            self.state[product_id]['trend'] = 'hot'
        
        return self.state[product_id]


if __name__ == '__main__':
    import sys
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        engine = sys.argv[2] if len(sys.argv) > 2 else 'flink'
        
        SalesRankerUnified.run(mode=mode, engine=engine)
    else:
        # Default: run batch mode
        SalesRanker.run()