# -*- coding: utf-8 -*-

import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Generator, Tuple
from mrjob.job import MRJob
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.trace import Status, StatusCode
from opentelemetry.metrics import Counter, Histogram
import psutil
import os

# Initialize OpenTelemetry
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

metrics.set_meter_provider(MeterProvider())
meter = metrics.get_meter(__name__)

# Custom metrics
records_processed_counter = meter.create_counter(
    name="mint.records_processed",
    description="Number of records processed in mapper",
    unit="1"
)

records_emitted_counter = meter.create_counter(
    name="mint.records_emitted", 
    description="Number of records emitted by mapper",
    unit="1"
)

budget_notifications_counter = meter.create_counter(
    name="mint.budget_notifications",
    description="Number of budget notifications triggered",
    unit="1"
)

cache_hit_counter = meter.create_counter(
    name="mint.cache_hits",
    description="Number of cache hits",
    unit="1"
)

cache_miss_counter = meter.create_counter(
    name="mint.cache_misses",
    description="Number of cache misses",
    unit="1"
)

processing_duration_histogram = meter.create_histogram(
    name="mint.processing_duration",
    description="Duration of processing operations",
    unit="ms"
)

memory_usage_gauge = meter.create_observable_gauge(
    name="mint.memory_usage",
    callbacks=[lambda: [metrics.Observation(psutil.Process().memory_info().rss / 1024 / 1024)]],
    description="Memory usage in MB",
    unit="MB"
)

cpu_usage_gauge = meter.create_observable_gauge(
    name="mint.cpu_usage",
    callbacks=[lambda: [metrics.Observation(psutil.cpu_percent())]],
    description="CPU usage percentage",
    unit="%"
)

# Structured logging configuration
class StructuredLogger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        # Create JSON formatter
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s", "extra": %(extra)s}'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def info(self, message: str, **kwargs):
        extra = json.dumps(kwargs)
        self.logger.info(message, extra={'extra': extra})
    
    def error(self, message: str, **kwargs):
        extra = json.dumps(kwargs)
        self.logger.error(message, extra={'extra': extra})
    
    def warning(self, message: str, **kwargs):
        extra = json.dumps(kwargs)
        self.logger.warning(message, extra={'extra': extra})

logger = StructuredLogger("mint_mapreduce")

class HealthCheck:
    """Health check system for monitoring job status"""
    
    def __init__(self):
        self.start_time = time.time()
        self.last_heartbeat = time.time()
        self.healthy = True
        self.readiness = True
        self.error_count = 0
        self.max_errors = 10
    
    def heartbeat(self):
        """Update heartbeat timestamp"""
        self.last_heartbeat = time.time()
    
    def record_error(self):
        """Record an error occurrence"""
        self.error_count += 1
        if self.error_count >= self.max_errors:
            self.healthy = False
            self.readiness = False
    
    def check_health(self) -> Dict[str, Any]:
        """Return health status"""
        uptime = time.time() - self.start_time
        time_since_heartbeat = time.time() - self.last_heartbeat
        
        return {
            "status": "healthy" if self.healthy else "unhealthy",
            "readiness": "ready" if self.readiness else "not_ready",
            "uptime_seconds": uptime,
            "last_heartbeat_seconds_ago": time_since_heartbeat,
            "error_count": self.error_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    def check_readiness(self) -> bool:
        """Check if system is ready to process requests"""
        return self.readiness and self.healthy

# Global health check instance
health_check = HealthCheck()

class SpendingByCategory(MRJob):
    """MapReduce job for categorizing spending with full observability"""
    
    def __init__(self, categorizer=None):
        super(SpendingByCategory, self).__init__()
        self.categorizer = categorizer
        self.cache = {}  # Simple cache for categorization
        self.transaction_categories = {}  # Track transaction categorization
        self.escalation_paths = {}  # Track escalation paths for budget notifications
        self.performance_metrics = {
            "mapper_calls": 0,
            "reducer_calls": 0,
            "total_processing_time": 0
        }
        
        logger.info("Initialized SpendingByCategory job", 
                   job_id=os.getpid(),
                   start_time=datetime.utcnow().isoformat())
    
    def current_year_month(self) -> str:
        """Return the current year and month."""
        with tracer.start_as_current_span("current_year_month") as span:
            current = datetime.now().strftime("%Y-%m")
            span.set_attribute("current_year_month", current)
            return current
    
    def extract_year_month(self, timestamp: str) -> str:
        """Return the year and month portions of the timestamp."""
        with tracer.start_as_current_span("extract_year_month") as span:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                result = dt.strftime("%Y-%m")
                span.set_attribute("extracted_year_month", result)
                span.set_status(Status(StatusCode.OK))
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error("Failed to extract year month", 
                           timestamp=timestamp, 
                           error=str(e))
                health_check.record_error()
                raise
    
    def categorize_transaction(self, category: str) -> str:
        """Categorize transaction with caching and metrics"""
        with tracer.start_as_current_span("categorize_transaction") as span:
            span.set_attribute("input_category", category)
            
            # Check cache first
            if category in self.cache:
                cache_hit_counter.add(1)
                span.set_attribute("cache_hit", True)
                logger.info("Cache hit for category", category=category)
                return self.cache[category]
            
            cache_miss_counter.add(1)
            span.set_attribute("cache_hit", False)
            
            # Use categorizer if available
            if self.categorizer:
                categorized = self.categorizer.categorize(category)
            else:
                categorized = category.lower().strip()
            
            # Cache result
            self.cache[category] = categorized
            
            # Track categorization
            self.transaction_categories[category] = categorized
            
            logger.info("Categorized transaction", 
                       original=category, 
                       categorized=categorized)
            
            span.set_attribute("categorized_result", categorized)
            return categorized
    
    def handle_budget_notifications(self, key: Tuple[str, str], total: float) -> None:
        """Call notification API if nearing or exceeded budget with escalation tracking."""
        with tracer.start_as_current_span("handle_budget_notifications") as span:
            period, category = key
            span.set_attributes({
                "period": period,
                "category": category,
                "total_amount": total
            })
            
            # Simulate budget threshold check
            budget_threshold = 1000  # Example threshold
            
            if total > budget_threshold:
                # Track escalation path
                escalation_id = f"{period}_{category}_{int(time.time())}"
                self.escalation_paths[escalation_id] = {
                    "period": period,
                    "category": category,
                    "total": total,
                    "threshold": budget_threshold,
                    "escalation_time": datetime.utcnow().isoformat(),
                    "status": "escalated"
                }
                
                budget_notifications_counter.add(1, {"type": "exceeded"})
                
                logger.warning("Budget exceeded - escalation triggered",
                             period=period,
                             category=category,
                             total=total,
                             threshold=budget_threshold,
                             escalation_id=escalation_id)
                
                span.add_event("budget_exceeded", {
                    "escalation_id": escalation_id,
                    "total": total,
                    "threshold": budget_threshold
                })
                
                # Simulate notification API call
                self._send_notification(period, category, total, "exceeded")
                
            elif total > budget_threshold * 0.8:  # 80% threshold
                budget_notifications_counter.add(1, {"type": "warning"})
                
                logger.warning("Budget nearing limit",
                             period=period,
                             category=category,
                             total=total,
                             threshold=budget_threshold)
                
                span.add_event("budget_warning", {
                    "total": total,
                    "threshold": budget_threshold
                })
                
                self._send_notification(period, category, total, "warning")
    
    def _send_notification(self, period: str, category: str, total: float, level: str) -> None:
        """Simulate sending notification with tracing"""
        with tracer.start_as_current_span("send_notification") as span:
            span.set_attributes({
                "period": period,
                "category": category,
                "total": total,
                "notification_level": level
            })
            
            # Simulate API call
            time.sleep(0.01)  # Simulate network delay
            
            logger.info("Notification sent",
                       period=period,
                       category=category,
                       total=total,
                       level=level)
    
    def mapper(self, _, line: str) -> Generator[Tuple[Tuple[str, str], float], None, None]:
        """Parse each log line, extract and transform relevant lines.
        
        Emit key value pairs of the form:
        
        (2016-01, shopping), 25
        (2016-01, shopping), 100
        (2016-01, gas), 50
        """
        start_time = time.time()
        self.performance_metrics["mapper_calls"] += 1
        
        with tracer.start_as_current_span("mapper") as span:
            health_check.heartbeat()
            
            try:
                # Parse line
                parts = line.split('\t')
                if len(parts) != 3:
                    logger.warning("Invalid line format", line=line)
                    return
                
                timestamp, category, amount_str = parts
                
                # Validate and convert amount
                try:
                    amount = float(amount_str)
                except ValueError:
                    logger.error("Invalid amount format", 
                               amount_str=amount_str,
                               line=line)
                    return
                
                # Extract period
                period = self.extract_year_month(timestamp)
                
                # Categorize transaction
                categorized = self.categorize_transaction(category)
                
                span.set_attributes({
                    "timestamp": timestamp,
                    "original_category": category,
                    "categorized": categorized,
                    "amount": amount,
                    "period": period
                })
                
                records_processed_counter.add(1)
                
                # Only process current month
                if period == self.current_year_month():
                    records_emitted_counter.add(1)
                    
                    logger.info("Emitting record",
                              period=period,
                              category=categorized,
                              amount=amount)
                    
                    yield (period, categorized), amount
                
                # Record processing duration
                processing_time = (time.time() - start_time) * 1000  # Convert to ms
                processing_duration_histogram.record(processing_time, {"operation": "mapper"})
                self.performance_metrics["total_processing_time"] += processing_time
                
                span.set_status(Status(StatusCode.OK))
                
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error("Mapper error", 
                           line=line, 
                           error=str(e),
                           traceback=str(e.__traceback__))
                health_check.record_error()
                raise
    
    def reducer(self, key: Tuple[str, str], values: Generator[float, None, None]) -> Generator[Tuple[Tuple[str, str], float], None, None]:
        """Sum values for each key.
        
        (2016-01, shopping), 125
        (2016-01, gas), 50
        """
        start_time = time.time()
        self.performance_metrics["reducer_calls"] += 1
        
        with tracer.start_as_current_span("reducer") as span:
            health_check.heartbeat()
            
            try:
                period, category = key
                span.set_attributes({
                    "period": period,
                    "category": category
                })
                
                # Calculate total
                total = 0
                value_count = 0
                for value in values:
                    total += value
                    value_count += 1
                
                span.set_attributes({
                    "total": total,
                    "value_count": value_count
                })
                
                logger.info("Reducing key",
                          period=period,
                          category=category,
                          total=total,
                          value_count=value_count)
                
                # Handle budget notifications
                self.handle_budget_notifications(key, total)
                
                # Record processing duration
                processing_time = (time.time() - start_time) * 1000
                processing_duration_histogram.record(processing_time, {"operation": "reducer"})
                self.performance_metrics["total_processing_time"] += processing_time
                
                span.set_status(Status(StatusCode.OK))
                yield key, total
                
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error("Reducer error",
                           key=str(key),
                           error=str(e),
                           traceback=str(e.__traceback__))
                health_check.record_error()
                raise
    
    def steps(self):
        """Run the map and reduce steps."""
        with tracer.start_as_current_span("steps") as span:
            logger.info("Starting MapReduce steps")
            
            steps = [
                self.mr(mapper=self.mapper,
                        reducer=self.reducer)
            ]
            
            span.set_attribute("num_steps", len(steps))
            return steps
    
    def run(self):
        """Override run method to add final metrics and health reporting"""
        with tracer.start_as_current_span("job_execution") as span:
            logger.info("Starting job execution",
                       job_id=os.getpid(),
                       start_time=datetime.utcnow().isoformat())
            
            try:
                result = super(SpendingByCategory, self).run()
                
                # Log final metrics
                logger.info("Job completed successfully",
                          job_id=os.getpid(),
                          mapper_calls=self.performance_metrics["mapper_calls"],
                          reducer_calls=self.performance_metrics["reducer_calls"],
                          total_processing_time_ms=self.performance_metrics["total_processing_time"],
                          cache_size=len(self.cache),
                          categorizations=len(self.transaction_categories),
                          escalations=len(self.escalation_paths))
                
                span.set_status(Status(StatusCode.OK))
                return result
                
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error("Job execution failed",
                           job_id=os.getpid(),
                           error=str(e),
                           traceback=str(e.__traceback__))
                health_check.record_error()
                raise
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status"""
        return health_check.check_health()
    
    def get_readiness_status(self) -> bool:
        """Check if job is ready"""
        return health_check.check_readiness()
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of collected metrics"""
        return {
            "performance": self.performance_metrics,
            "cache_stats": {
                "size": len(self.cache),
                "hit_rate": cache_hit_counter.get().sum / max(cache_hit_counter.get().sum + cache_miss_counter.get().sum, 1)
            },
            "categorizations": len(self.transaction_categories),
            "escalations": len(self.escalation_paths),
            "health": self.get_health_status()
        }


if __name__ == '__main__':
    SpendingByCategory.run()