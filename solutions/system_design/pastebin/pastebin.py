# -*- coding: utf-8 -*-

import logging
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional
from mrjob.job import MRJob
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.metrics import Observation
import psutil
import os

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize OpenTelemetry
resource = Resource.create({
    "service.name": "pastebin-hitcounts",
    "service.version": "1.0.0",
    "deployment.environment": os.getenv("ENVIRONMENT", "development")
})

# Set up tracing
trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter())
)
trace.set_tracer_provider(trace_provider)
tracer = trace.get_tracer(__name__)

# Set up metrics
metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(),
    export_interval_millis=5000
)
metric_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(metric_provider)
meter = metrics.get_meter(__name__)

# Custom metrics
lines_processed_counter = meter.create_counter(
    name="pastebin.lines_processed",
    description="Total number of log lines processed",
    unit="1"
)

errors_counter = meter.create_counter(
    name="pastebin.errors",
    description="Total number of processing errors",
    unit="1"
)

processing_time_histogram = meter.create_histogram(
    name="pastebin.processing_time",
    description="Time taken to process log lines",
    unit="ms"
)

urls_extracted_counter = meter.create_counter(
    name="pastebin.urls_extracted",
    description="Number of URLs successfully extracted",
    unit="1"
)

period_distribution_counter = meter.create_counter(
    name="pastebin.period_distribution",
    description="Distribution of log entries by time period",
    unit="1"
)

cache_hit_gauge = meter.create_observable_gauge(
    name="pastebin.cache_hit_rate",
    callbacks=[lambda x: [Observation(0.85, {})]],  # Simulated cache hit rate
    description="Cache hit rate for URL lookups",
    unit="percent"
)

memory_usage_gauge = meter.create_observable_gauge(
    name="pastebin.memory_usage",
    callbacks=[lambda x: [Observation(psutil.Process().memory_info().rss / 1024 / 1024, {})]],
    description="Memory usage in MB",
    unit="MB"
)

# Health check endpoint simulation
health_status = {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


class HitCounts(MRJob):
    """MapReduce job for counting pastebin hit counts with full observability."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()
        self.processed_count = 0
        self.error_count = 0
        self.url_cache = {}  # Simple cache for URL lookups
        logger.info("HitCounts job initialized", extra={
            "job_id": self.options.job_name if hasattr(self.options, 'job_name') else "unknown",
            "environment": os.getenv("ENVIRONMENT", "development")
        })

    def extract_url(self, line: str) -> Optional[str]:
        """Extract the generated url from the log line with error handling."""
        with tracer.start_as_current_span("extract_url") as span:
            try:
                # Simulated URL extraction logic
                # In real implementation, parse actual log format
                parts = line.split()
                if len(parts) >= 3:
                    url = parts[2]  # Assume URL is third field
                    
                    # Cache lookup with metrics
                    if url in self.url_cache:
                        span.set_attribute("cache.hit", True)
                    else:
                        span.set_attribute("cache.hit", False)
                        self.url_cache[url] = True
                    
                    span.set_attribute("url", url)
                    span.set_status(Status(StatusCode.OK))
                    urls_extracted_counter.add(1, {"operation": "extract"})
                    return url
                else:
                    span.set_status(Status(StatusCode.ERROR, "Invalid log format"))
                    errors_counter.add(1, {"error_type": "invalid_format"})
                    return None
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                errors_counter.add(1, {"error_type": "extraction_error"})
                logger.error(f"Error extracting URL: {e}", extra={
                    "line": line[:100],  # Truncate for logging
                    "error": str(e)
                })
                return None

    def extract_year_month(self, line: str) -> Optional[str]:
        """Return the year and month portions of the timestamp with validation."""
        with tracer.start_as_current_span("extract_year_month") as span:
            try:
                # Simulated timestamp extraction
                # In real implementation, parse actual timestamp format
                parts = line.split()
                if len(parts) >= 1:
                    timestamp_str = parts[0]  # Assume timestamp is first field
                    
                    # Parse timestamp (simplified)
                    # Real implementation would use datetime.strptime
                    if len(timestamp_str) >= 7:  # At least YYYY-MM
                        period = timestamp_str[:7]  # Extract YYYY-MM
                        span.set_attribute("period", period)
                        span.set_attribute("timestamp", timestamp_str)
                        span.set_status(Status(StatusCode.OK))
                        period_distribution_counter.add(1, {"period": period})
                        return period
                    else:
                        span.set_status(Status(StatusCode.ERROR, "Invalid timestamp format"))
                        errors_counter.add(1, {"error_type": "invalid_timestamp"})
                        return None
                else:
                    span.set_status(Status(StatusCode.ERROR, "No timestamp found"))
                    errors_counter.add(1, {"error_type": "missing_timestamp"})
                    return None
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                errors_counter.add(1, {"error_type": "timestamp_error"})
                logger.error(f"Error extracting timestamp: {e}", extra={
                    "line": line[:100],
                    "error": str(e)
                })
                return None

    def mapper(self, _, line):
        """Parse each log line, extract and transform relevant lines with observability."""
        with tracer.start_as_current_span("mapper") as span:
            start_time = time.time()
            span.set_attributes({
                "line_length": len(line),
                "mapper_id": self.options.mr_job_id if hasattr(self.options, 'mr_job_id') else "unknown"
            })
            
            try:
                self.processed_count += 1
                lines_processed_counter.add(1, {"phase": "mapper"})
                
                url = self.extract_url(line)
                period = self.extract_year_month(line)
                
                if url and period:
                    span.set_attributes({
                        "url_extracted": True,
                        "period_extracted": True
                    })
                    yield (period, url), 1
                    
                    # Log successful processing
                    logger.debug("Successfully processed line", extra={
                        "period": period,
                        "url": url,
                        "line_number": self.processed_count
                    })
                else:
                    span.set_attributes({
                        "url_extracted": url is not None,
                        "period_extracted": period is not None
                    })
                    logger.warning("Failed to extract data from line", extra={
                        "line": line[:100],
                        "line_number": self.processed_count
                    })
                
                # Record processing time
                processing_time = (time.time() - start_time) * 1000
                processing_time_histogram.record(processing_time, {
                    "phase": "mapper",
                    "success": "true" if url and period else "false"
                })
                
            except Exception as e:
                self.error_count += 1
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                errors_counter.add(1, {"phase": "mapper", "error_type": "unexpected_error"})
                logger.error(f"Unexpected error in mapper: {e}", extra={
                    "line": line[:100],
                    "error": str(e),
                    "line_number": self.processed_count
                })

    def reducer(self, key, values):
        """Sum values for each key with observability."""
        with tracer.start_as_current_span("reducer") as span:
            start_time = time.time()
            period, url = key
            span.set_attributes({
                "period": period,
                "url": url,
                "reducer_id": self.options.mr_job_id if hasattr(self.options, 'mr_job_id') else "unknown"
            })
            
            try:
                total = sum(values)
                span.set_attribute("total_hits", total)
                
                # Log aggregation result
                logger.info("Aggregated hits for URL", extra={
                    "period": period,
                    "url": url,
                    "total_hits": total
                })
                
                yield key, total
                
                # Record processing time
                processing_time = (time.time() - start_time) * 1000
                processing_time_histogram.record(processing_time, {
                    "phase": "reducer",
                    "success": "true"
                })
                
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                errors_counter.add(1, {"phase": "reducer", "error_type": "aggregation_error"})
                logger.error(f"Error in reducer: {e}", extra={
                    "key": str(key),
                    "error": str(e)
                })

    def steps(self):
        """Run the map and reduce steps with observability."""
        with tracer.start_as_current_span("job_steps") as span:
            span.set_attributes({
                "job_name": self.options.job_name if hasattr(self.options, 'job_name') else "hitcounts",
                "total_steps": 1
            })
            
            logger.info("Starting MapReduce job", extra={
                "job_config": {
                    "input_paths": self.options.input_paths if hasattr(self.options, 'input_paths') else [],
                    "output_path": self.options.output_dir if hasattr(self.options, 'output_dir') else None
                }
            })
            
            return [
                self.mr(mapper=self.mapper,
                        reducer=self.reducer)
            ]

    def configure_args(self):
        """Configure command line arguments with observability options."""
        super(HitCounts, self).configure_args()
        
        # Add observability-specific arguments
        self.add_passthru_arg(
            '--enable-tracing',
            dest='enable_tracing',
            default=True,
            help='Enable distributed tracing'
        )
        self.add_passthru_arg(
            '--enable-metrics',
            dest='enable_metrics',
            default=True,
            help='Enable metrics collection'
        )
        self.add_passthru_arg(
            '--log-level',
            dest='log_level',
            default='INFO',
            help='Logging level'
        )

    def job_runner_kwargs(self):
        """Add observability configuration to job runner."""
        kwargs = super(HitCounts, self).job_runner_kwargs()
        
        # Add observability metadata
        kwargs['job_name'] = f"pastebin-hitcounts-{int(time.time())}"
        kwargs['extra_args'] = kwargs.get('extra_args', [])
        kwargs['extra_args'].extend([
            '--jobconf', f'mapreduce.job.name=pastebin-hitcounts-{int(time.time())}',
            '--jobconf', 'mapreduce.map.log.level=INFO',
            '--jobconf', 'mapreduce.reduce.log.level=INFO'
        ])
        
        return kwargs

    def run(self):
        """Override run method to add final observability reporting."""
        with tracer.start_as_current_span("job_execution") as span:
            span.set_attributes({
                "job_type": "mapreduce",
                "component": "pastebin_hitcounts"
            })
            
            logger.info("Starting job execution", extra={
                "pid": os.getpid(),
                "start_time": datetime.utcnow().isoformat()
            })
            
            try:
                result = super(HitCounts, self).run()
                
                # Calculate final metrics
                total_time = time.time() - self.start_time
                
                # Log job completion
                logger.info("Job completed successfully", extra={
                    "total_lines_processed": self.processed_count,
                    "total_errors": self.error_count,
                    "total_time_seconds": total_time,
                    "lines_per_second": self.processed_count / total_time if total_time > 0 else 0,
                    "error_rate": self.error_count / self.processed_count if self.processed_count > 0 else 0,
                    "cache_size": len(self.url_cache),
                    "end_time": datetime.utcnow().isoformat()
                })
                
                span.set_attributes({
                    "total_lines_processed": self.processed_count,
                    "total_errors": self.error_count,
                    "total_time_seconds": total_time,
                    "success": True
                })
                
                # Export final metrics
                self._export_final_metrics()
                
                return result
                
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error(f"Job failed: {e}", extra={
                    "error": str(e),
                    "processed_count": self.processed_count,
                    "error_count": self.error_count
                })
                raise

    def _export_final_metrics(self):
        """Export final job metrics."""
        # Create a final span for metrics export
        with tracer.start_as_current_span("final_metrics_export") as span:
            span.set_attributes({
                "processed_count": self.processed_count,
                "error_count": self.error_count,
                "cache_hit_rate": 0.85  # Would be calculated from actual cache stats
            })
            
            # Record final metrics
            lines_processed_counter.add(
                self.processed_count,
                {"phase": "total", "job": "pastebin"}
            )
            
            if self.error_count > 0:
                errors_counter.add(
                    self.error_count,
                    {"phase": "total", "job": "pastebin"}
                )
            
            logger.info("Exported final metrics", extra={
                "metrics_exported": True,
                "export_time": datetime.utcnow().isoformat()
            })


def health_check() -> Dict[str, Any]:
    """Health check endpoint for the service."""
    with tracer.start_as_current_span("health_check") as span:
        try:
            # Check system resources
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            status = "healthy"
            if cpu_percent > 90 or memory.percent > 90 or disk.percent > 90:
                status = "degraded"
            
            health_data = {
                "status": status,
                "timestamp": datetime.utcnow().isoformat(),
                "version": "1.0.0",
                "checks": {
                    "cpu": {"status": "ok" if cpu_percent < 90 else "warning", "value": cpu_percent},
                    "memory": {"status": "ok" if memory.percent < 90 else "warning", "value": memory.percent},
                    "disk": {"status": "ok" if disk.percent < 90 else "warning", "value": disk.percent}
                }
            }
            
            span.set_attributes({
                "health_status": status,
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent
            })
            
            logger.info("Health check completed", extra=health_data)
            return health_data
            
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }


def readiness_check() -> Dict[str, Any]:
    """Readiness probe for the service."""
    with tracer.start_as_current_span("readiness_check") as span:
        try:
            # Check if service is ready to accept work
            # In a real implementation, this would check dependencies
            ready = True
            dependencies = {
                "hdfs": "available",  # Simulated
                "database": "available",  # Simulated
                "cache": "available"  # Simulated
            }
            
            readiness_data = {
                "ready": ready,
                "timestamp": datetime.utcnow().isoformat(),
                "dependencies": dependencies
            }
            
            span.set_attributes({
                "ready": ready,
                "dependency_count": len(dependencies)
            })
            
            logger.info("Readiness check completed", extra=readiness_data)
            return readiness_data
            
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Readiness check failed: {e}")
            return {
                "ready": False,
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            }


if __name__ == '__main__':
    # Start health check server in background (simplified)
    import threading
    
    def run_health_server():
        """Simulate health check server."""
        while True:
            health_check()
            time.sleep(30)  # Check every 30 seconds
    
    # Start health check thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    
    # Run the job
    HitCounts.run()