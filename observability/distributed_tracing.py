# observability/distributed_tracing.py
"""
Observable System Design Patterns — Instrumentation Module
Implements OpenTelemetry-compatible instrumentation with structured logging,
metrics collection, distributed tracing, health checks, and Grafana dashboards.
"""

import json
import logging
import time
import threading
import functools
import inspect
import uuid
import os
import sys
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from contextlib import contextmanager
from collections import defaultdict
import http.server
import socketserver
from concurrent.futures import ThreadPoolExecutor

# OpenTelemetry imports (graceful degradation if not installed)
try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.context import Context
    from opentelemetry.trace.status import Status, StatusCode
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    # Fallback stubs
    class trace:
        @staticmethod
        def get_tracer(*args, **kwargs):
            return type('Tracer', (), {
                'start_span': lambda *a, **k: type('Span', (), {
                    '__enter__': lambda s: s,
                    '__exit__': lambda s, *a: None,
                    'set_attribute': lambda s, k, v: None,
                    'set_status': lambda s, status: None,
                    'end': lambda s: None,
                    'get_span_context': lambda s: type('Context', (), {'trace_id': 0, 'span_id': 0})()
                })()
            })()
    
    class metrics:
        @staticmethod
        def get_meter(*args, **kwargs):
            return type('Meter', (), {
                'create_counter': lambda *a, **k: type('Counter', (), {'add': lambda s, v, attrs=None: None})(),
                'create_histogram': lambda *a, **k: type('Histogram', (), {'record': lambda s, v, attrs=None: None})(),
                'create_observable_gauge': lambda *a, **k: None
            })()

# Structured logging configuration
class StructuredLogger:
    """JSON-structured logger with trace context injection."""
    
    def __init__(self, name: str, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers = []
        
        # Create console handler with JSON formatter
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(self.JsonFormatter())
        self.logger.addHandler(handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    class JsonFormatter(logging.Formatter):
        def format(self, record):
            log_data = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'level': record.levelname,
                'logger': record.name,
                'message': record.getMessage(),
                'module': record.module,
                'function': record.funcName,
                'line': record.lineno,
            }
            
            # Add exception info if present
            if record.exc_info:
                log_data['exception'] = self.formatException(record.exc_info)
            
            # Add extra fields
            for key, value in record.__dict__.items():
                if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName',
                              'levelname', 'lineno', 'module', 'msecs', 'pathname',
                              'process', 'processName', 'relativeCreated', 'thread',
                              'threadName', 'exc_info', 'exc_text', 'stack_info']:
                    log_data[key] = value
            
            return json.dumps(log_data)
    
    def _inject_trace_context(self, extra: Dict) -> Dict:
        """Inject current trace context into log extra fields."""
        if OPENTELEMETRY_AVAILABLE:
            span = trace.get_current_span()
            if span and span.get_span_context().is_valid:
                ctx = span.get_span_context()
                extra['trace_id'] = format(ctx.trace_id, '032x')
                extra['span_id'] = format(ctx.span_id, '016x')
        return extra
    
    def info(self, msg: str, **kwargs):
        extra = self._inject_trace_context(kwargs)
        self.logger.info(msg, extra=extra)
    
    def error(self, msg: str, **kwargs):
        extra = self._inject_trace_context(kwargs)
        self.logger.error(msg, extra=extra)
    
    def warning(self, msg: str, **kwargs):
        extra = self._inject_trace_context(kwargs)
        self.logger.warning(msg, extra=extra)
    
    def debug(self, msg: str, **kwargs):
        extra = self._inject_trace_context(kwargs)
        self.logger.debug(msg, extra=extra)

# Metrics collection
class MetricsCollector:
    """Centralized metrics collection with OpenTelemetry compatibility."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.counters = defaultdict(lambda: defaultdict(int))
        self.histograms = defaultdict(list)
        self.gauges = {}
        self._lock = threading.RLock()
        
        if OPENTELEMETRY_AVAILABLE:
            self._setup_opentelemetry()
    
    def _setup_opentelemetry(self):
        """Initialize OpenTelemetry metrics if available."""
        try:
            metric_reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'localhost:4317')),
                export_interval_millis=5000
            )
            provider = MeterProvider(metric_readers=[metric_reader])
            metrics.set_meter_provider(provider)
            self.meter = metrics.get_meter(self.service_name)
            
            # Create standard metrics
            self.request_counter = self.meter.create_counter(
                name="http.server.requests",
                description="Total HTTP requests",
                unit="1"
            )
            
            self.request_duration = self.meter.create_histogram(
                name="http.server.duration",
                description="HTTP request duration",
                unit="ms"
            )
            
            self.cache_hits = self.meter.create_counter(
                name="cache.hits",
                description="Cache hit count",
                unit="1"
            )
            
            self.cache_misses = self.meter.create_counter(
                name="cache.misses",
                description="Cache miss count",
                unit="1"
            )
            
        except Exception as e:
            logging.warning(f"Failed to setup OpenTelemetry metrics: {e}")
    
    def increment_counter(self, name: str, value: int = 1, tags: Optional[Dict] = None):
        """Increment a counter metric."""
        with self._lock:
            tag_key = json.dumps(tags or {}, sort_keys=True)
            self.counters[name][tag_key] += value
            
            if OPENTELEMETRY_AVAILABLE and hasattr(self, 'request_counter'):
                self.request_counter.add(value, tags or {})
    
    def record_histogram(self, name: str, value: float, tags: Optional[Dict] = None):
        """Record a histogram value."""
        with self._lock:
            self.histograms[name].append((value, tags or {}))
            
            if OPENTELEMETRY_AVAILABLE and hasattr(self, 'request_duration'):
                self.request_duration.record(value, tags or {})
    
    def set_gauge(self, name: str, value: float, tags: Optional[Dict] = None):
        """Set a gauge value."""
        with self._lock:
            self.gauges[name] = (value, tags or {})
    
    def get_metrics_snapshot(self) -> Dict:
        """Get current metrics snapshot."""
        with self._lock:
            return {
                'counters': dict(self.counters),
                'histograms': {k: list(v) for k, v in self.histograms.items()},
                'gauges': dict(self.gauges),
                'timestamp': datetime.utcnow().isoformat()
            }

# Distributed tracing
class DistributedTracer:
    """Distributed tracing with OpenTelemetry compatibility."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.tracer = None
        
        if OPENTELEMETRY_AVAILABLE:
            self._setup_tracing()
    
    def _setup_tracing(self):
        """Initialize OpenTelemetry tracing."""
        try:
            trace.set_tracer_provider(TracerProvider())
            otlp_exporter = OTLPSpanExporter(
                endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'localhost:4317')
            )
            span_processor = BatchSpanProcessor(otlp_exporter)
            trace.get_tracer_provider().add_span_processor(span_processor)
            self.tracer = trace.get_tracer(self.service_name)
            self.propagator = TraceContextTextMapPropagator()
        except Exception as e:
            logging.warning(f"Failed to setup OpenTelemetry tracing: {e}")
    
    @contextmanager
    def start_span(self, name: str, attributes: Optional[Dict] = None):
        """Start a new span with context management."""
        if not OPENTELEMETRY_AVAILABLE or not self.tracer:
            # Fallback: yield a dummy span
            yield type('DummySpan', (), {
                'set_attribute': lambda k, v: None,
                'set_status': lambda status: None
            })()
            return
        
        span = self.tracer.start_span(name)
        
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)
        
        try:
            yield span
            span.set_status(Status(StatusCode.OK))
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            raise
        finally:
            span.end()
    
    def inject_context(self, carrier: Dict):
        """Inject trace context into carrier."""
        if OPENTELEMETRY_AVAILABLE and hasattr(self, 'propagator'):
            self.propagator.inject(carrier)
    
    def extract_context(self, carrier: Dict) -> Context:
        """Extract trace context from carrier."""
        if OPENTELEMETRY_AVAILABLE and hasattr(self, 'propagator'):
            return self.propagator.extract(carrier)
        return Context()

# Health checks and readiness probes
class HealthCheck:
    """Health check and readiness probe implementation."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.checks = {}
        self.ready = False
        self._lock = threading.RLock()
    
    def register_check(self, name: str, check_func: Callable[[], bool]):
        """Register a health check function."""
        with self._lock:
            self.checks[name] = check_func
    
    def run_checks(self) -> Dict[str, Any]:
        """Run all registered health checks."""
        results = {}
        all_healthy = True
        
        with self._lock:
            for name, check_func in self.checks.items():
                try:
                    healthy = check_func()
                    results[name] = {
                        'status': 'healthy' if healthy else 'unhealthy',
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    if not healthy:
                        all_healthy = False
                except Exception as e:
                    results[name] = {
                        'status': 'error',
                        'error': str(e),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    all_healthy = False
        
        return {
            'service': self.service_name,
            'status': 'healthy' if all_healthy else 'unhealthy',
            'checks': results,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def set_ready(self, ready: bool):
        """Set readiness status."""
        with self._lock:
            self.ready = ready
    
    def is_ready(self) -> bool:
        """Check if service is ready."""
        with self._lock:
            return self.ready and len(self.checks) > 0

# Performance dashboard templates
class GrafanaDashboard:
    """Grafana dashboard template generator."""
    
    @staticmethod
    def generate_system_dashboard(service_name: str) -> Dict:
        """Generate a comprehensive system dashboard template."""
        return {
            "dashboard": {
                "title": f"{service_name} - System Performance",
                "tags": ["system", "performance", service_name.lower()],
                "timezone": "browser",
                "panels": [
                    {
                        "title": "Request Rate",
                        "type": "graph",
                        "targets": [{
                            "expr": f'sum(rate(http_server_requests_total{{service="{service_name}"}}[5m])) by (method, path)',
                            "legendFormat": "{{method}} {{path}}"
                        }],
                        "yaxes": [{"format": "reqps"}, {"format": "short"}]
                    },
                    {
                        "title": "Request Duration (p95)",
                        "type": "graph",
                        "targets": [{
                            "expr": f'histogram_quantile(0.95, sum(rate(http_server_duration_seconds_bucket{{service="{service_name}"}}[5m])) by (le, method, path))',
                            "legendFormat": "{{method}} {{path}}"
                        }],
                        "yaxes": [{"format": "s"}, {"format": "short"}]
                    },
                    {
                        "title": "Error Rate",
                        "type": "graph",
                        "targets": [{
                            "expr": f'sum(rate(http_server_requests_total{{service="{service_name}",status=~"5.."}}[5m])) / sum(rate(http_server_requests_total{{service="{service_name}"}}[5m]))',
                            "legendFormat": "Error Rate"
                        }],
                        "yaxes": [{"format": "percentunit"}, {"format": "short"}]
                    },
                    {
                        "title": "Cache Hit Rate",
                        "type": "stat",
                        "targets": [{
                            "expr": f'sum(rate(cache_hits_total{{service="{service_name}"}}[5m])) / (sum(rate(cache_hits_total{{service="{service_name}"}}[5m])) + sum(rate(cache_misses_total{{service="{service_name}"}}[5m])))',
                            "legendFormat": "Hit Rate"
                        }],
                        "gauge": {"maxValue": 1, "minValue": 0}
                    },
                    {
                        "title": "Active Spans",
                        "type": "stat",
                        "targets": [{
                            "expr": f'sum(traces_active_total{{service="{service_name}"}})',
                            "legendFormat": "Active Traces"
                        }]
                    },
                    {
                        "title": "Memory Usage",
                        "type": "graph",
                        "targets": [{
                            "expr": f'process_resident_memory_bytes{{service="{service_name}"}}',
                            "legendFormat": "Resident Memory"
                        }],
                        "yaxes": [{"format": "bytes"}, {"format": "short"}]
                    }
                ],
                "refresh": "10s",
                "schemaVersion": 27,
                "version": 1
            },
            "overwrite": True
        }
    
    @staticmethod
    def generate_cache_dashboard(service_name: str) -> Dict:
        """Generate cache-specific dashboard template."""
        return {
            "dashboard": {
                "title": f"{service_name} - Cache Performance",
                "tags": ["cache", service_name.lower()],
                "panels": [
                    {
                        "title": "Cache Operations",
                        "type": "graph",
                        "targets": [
                            {"expr": f'rate(cache_hits_total{{service="{service_name}"}}[5m])', "legendFormat": "Hits"},
                            {"expr": f'rate(cache_misses_total{{service="{service_name}"}}[5m])', "legendFormat": "Misses"},
                            {"expr": f'rate(cache_evictions_total{{service="{service_name}"}}[5m])', "legendFormat": "Evictions"}
                        ]
                    },
                    {
                        "title": "Cache Size",
                        "type": "stat",
                        "targets": [{
                            "expr": f'cache_size_items{{service="{service_name}"}}',
                            "legendFormat": "Items"
                        }]
                    },
                    {
                        "title": "Cache Hit Ratio",
                        "type": "gauge",
                        "targets": [{
                            "expr": f'sum(rate(cache_hits_total{{service="{service_name}"}}[5m])) / (sum(rate(cache_hits_total{{service="{service_name}"}}[5m])) + sum(rate(cache_misses_total{{service="{service_name}"}}[5m])))',
                            "legendFormat": "Hit Ratio"
                        }],
                        "gauge": {"maxValue": 1, "minValue": 0}
                    }
                ]
            }
        }

# Instrumentation decorators
def instrument_method(tracer: DistributedTracer, metrics: MetricsCollector, logger: StructuredLogger):
    """Decorator to instrument methods with tracing, metrics, and logging."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            method_name = func.__name__
            class_name = args[0].__class__.__name__ if args and hasattr(args[0], '__class__') else ''
            span_name = f"{class_name}.{method_name}" if class_name else method_name
            
            # Extract trace context from kwargs if present
            trace_context = kwargs.pop('_trace_context', {})
            
            # Start span
            with tracer.start_span(span_name, attributes={
                'method': method_name,
                'class': class_name,
                'service': tracer.service_name
            }) as span:
                # Record start time
                start_time = time.time()
                
                try:
                    # Execute method
                    result = func(*args, **kwargs)
                    
                    # Record success metrics
                    duration = (time.time() - start_time) * 1000  # Convert to ms
                    metrics.increment_counter('method.calls', 1, {
                        'method': method_name,
                        'class': class_name,
                        'status': 'success'
                    })
                    metrics.record_histogram('method.duration', duration, {
                        'method': method_name,
                        'class': class_name
                    })
                    
                    logger.info(f"Method {method_name} completed", 
                               duration_ms=duration,
                               method=method_name,
                               class_name=class_name)
                    
                    return result
                    
                except Exception as e:
                    # Record error metrics
                    duration = (time.time() - start_time) * 1000
                    metrics.increment_counter('method.errors', 1, {
                        'method': method_name,
                        'class': class_name,
                        'error_type': type(e).__name__
                    })
                    
                    logger.error(f"Method {method_name} failed",
                                error=str(e),
                                error_type=type(e).__name__,
                                duration_ms=duration,
                                method=method_name,
                                class_name=class_name)
                    
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
        
        return wrapper
    return decorator

# Observable base class for existing implementations
class Observable:
    """Base class for making existing implementations observable."""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.logger = StructuredLogger(service_name)
        self.metrics = MetricsCollector(service_name)
        self.tracer = DistributedTracer(service_name)
        self.health_check = HealthCheck(service_name)
        
        # Register default health checks
        self.health_check.register_check('memory', self._check_memory)
        self.health_check.register_check('threads', self._check_threads)
        
        # Set ready after initialization
        self.health_check.set_ready(True)
    
    def _check_memory(self) -> bool:
        """Check memory usage health."""
        try:
            import psutil
            process = psutil.Process()
            memory_percent = process.memory_percent()
            return memory_percent < 90  # Less than 90% memory usage
        except ImportError:
            return True  # psutil not available, assume healthy
    
    def _check_threads(self) -> bool:
        """Check thread health."""
        try:
            import threading
            active_threads = threading.active_count()
            return active_threads < 100  # Less than 100 active threads
        except:
            return True
    
    def get_health_status(self) -> Dict:
        """Get current health status."""
        return self.health_check.run_checks()
    
    def get_metrics(self) -> Dict:
        """Get current metrics."""
        return self.metrics.get_metrics_snapshot()
    
    def instrument_class(self, cls: type) -> type:
        """Class decorator to instrument all public methods."""
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
            if not name.startswith('_'):
                setattr(cls, name, instrument_method(self.tracer, self.metrics, self.logger)(method))
        return cls

# Health check HTTP server
class HealthCheckServer:
    """HTTP server for health check and readiness probes."""
    
    def __init__(self, health_check: HealthCheck, port: int = 8080):
        self.health_check = health_check
        self.port = port
        self.server = None
        self.thread = None
    
    def start(self):
        """Start the health check server in a background thread."""
        handler = self._create_handler()
        self.server = socketserver.TCPServer(("", self.port), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        logging.info(f"Health check server started on port {self.port}")
    
    def stop(self):
        """Stop the health check server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            if self.thread:
                self.thread.join(timeout=5)
    
    def _create_handler(self):
        health_check = self.health_check
        
        class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
            def do_GET(self):
                if self.path == '/health':
                    self._handle_health()
                elif self.path == '/ready':
                    self._handle_ready()
                elif self.path == '/metrics':
                    self._handle_metrics()
                else:
                    self.send_response(404)
                    self.end_headers()
            
            def _handle_health(self):
                status = health_check.run_checks()
                self.send_response(200 if status['status'] == 'healthy' else 503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(status).encode())
            
            def _handle_ready(self):
                ready = health_check.is_ready()
                self.send_response(200 if ready else 503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'ready': ready}).encode())
            
            def _handle_metrics(self):
                # This would typically export Prometheus metrics
                self.send_response(200)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(b"# Metrics endpoint - implement Prometheus export\n")
        
        return HealthCheckHandler

# Integration with existing codebase
def instrument_existing_module(module_path: str, service_name: str):
    """Instrument an existing module with observability features."""
    try:
        # Import the module
        module = __import__(module_path, fromlist=[''])
        
        # Create observable instance
        observable = Observable(service_name)
        
        # Instrument all classes in the module
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and obj.__module__ == module_path:
                observable.instrument_class(obj)
                logging.info(f"Instrumented class {name} in {module_path}")
        
        return observable
    except Exception as e:
        logging.error(f"Failed to instrument module {module_path}: {e}")
        return None

# Example usage and integration
if __name__ == "__main__":
    # Example: Instrument the LRU cache
    observable = instrument_existing_module(
        'solutions.object_oriented_design.lru_cache.lru_cache',
        'lru-cache-service'
    )
    
    if observable:
        # Start health check server
        health_server = HealthCheckServer(observable.health_check, port=8081)
        health_server.start()
        
        # Generate Grafana dashboards
        system_dashboard = GrafanaDashboard.generate_system_dashboard('lru-cache-service')
        cache_dashboard = GrafanaDashboard.generate_cache_dashboard('lru-cache-service')
        
        # Save dashboards to files
        with open('grafana_system_dashboard.json', 'w') as f:
            json.dump(system_dashboard, f, indent=2)
        
        with open('grafana_cache_dashboard.json', 'w') as f:
            json.dump(cache_dashboard, f, indent=2)
        
        print("Observability instrumentation complete!")
        print("Health check available at: http://localhost:8081/health")
        print("Readiness probe at: http://localhost:8081/ready")
        print("Metrics at: http://localhost:8081/metrics")
        print("Grafana dashboards saved to grafana_system_dashboard.json and grafana_cache_dashboard.json")
        
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            health_server.stop()
            print("\nShutting down observability instrumentation...")

# Export public API
__all__ = [
    'StructuredLogger',
    'MetricsCollector',
    'DistributedTracer',
    'HealthCheck',
    'GrafanaDashboard',
    'Observable',
    'HealthCheckServer',
    'instrument_method',
    'instrument_existing_module'
]