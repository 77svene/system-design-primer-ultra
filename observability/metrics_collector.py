import os
import time
import threading
import json
import logging
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from functools import wraps
from contextlib import contextmanager
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver

# OpenTelemetry imports
try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagators.composite import CompositeHTTPPropagator
    from opentelemetry.metrics import Observation
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    print("OpenTelemetry not installed. Install with: pip install opentelemetry-api opentelemetry-sdk opentelemetry-exporter-otlp")

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(trace_id)s - %(span_id)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('system_design_primer_ultra.log')
    ]
)

class MetricType(Enum):
    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"
    UP_DOWN_COUNTER = "up_down_counter"

@dataclass
class MetricDefinition:
    name: str
    description: str
    unit: str
    metric_type: MetricType
    labels: List[str]

@dataclass
class HealthStatus:
    status: str
    timestamp: float
    checks: Dict[str, bool]
    details: Dict[str, Any]

class StructuredLogger:
    """Structured logging with trace context propagation"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.name = name
        
    def _get_trace_context(self) -> Dict[str, str]:
        """Extract trace context for logging"""
        if OPENTELEMETRY_AVAILABLE:
            carrier = {}
            TraceContextTextMapPropagator().inject(carrier)
            return {
                'trace_id': carrier.get('traceparent', '').split('-')[1] if 'traceparent' in carrier else '0',
                'span_id': carrier.get('traceparent', '').split('-')[2] if 'traceparent' in carrier else '0'
            }
        return {'trace_id': '0', 'span_id': '0'}
    
    def info(self, message: str, **kwargs):
        extra = self._get_trace_context()
        extra.update(kwargs)
        self.logger.info(message, extra=extra)
    
    def error(self, message: str, **kwargs):
        extra = self._get_trace_context()
        extra.update(kwargs)
        self.logger.error(message, extra=extra)
    
    def warning(self, message: str, **kwargs):
        extra = self._get_trace_context()
        extra.update(kwargs)
        self.logger.warning(message, extra=extra)
    
    def debug(self, message: str, **kwargs):
        extra = self._get_trace_context()
        extra.update(kwargs)
        self.logger.debug(message, extra=extra)

class MetricsCollector:
    """OpenTelemetry-compatible metrics collector with custom business metrics"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self.meter = None
        self.tracer = None
        self.metrics_registry = {}
        self.custom_metrics = {}
        self.health_checks = {}
        self.readiness_checks = {}
        self.performance_data = []
        self.logger = StructuredLogger("metrics_collector")
        
        # Initialize OpenTelemetry if available
        if OPENTELEMETRY_AVAILABLE:
            self._init_opentelemetry()
        else:
            self.logger.warning("OpenTelemetry not available, using fallback metrics")
            self._init_fallback_metrics()
    
    def _init_opentelemetry(self):
        """Initialize OpenTelemetry SDK"""
        try:
            # Create resource
            resource = Resource.create({
                "service.name": "system-design-primer-ultra",
                "service.version": "1.0.0",
                "deployment.environment": os.getenv("ENVIRONMENT", "development")
            })
            
            # Initialize tracing
            trace.set_tracer_provider(TracerProvider(resource=resource))
            self.tracer = trace.get_tracer(__name__)
            
            # Initialize metrics
            metric_reader = PrometheusMetricReader()
            metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
            self.meter = metrics.get_meter(__name__)
            
            # Register standard metrics
            self._register_standard_metrics()
            
            self.logger.info("OpenTelemetry initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize OpenTelemetry: {e}")
            self._init_fallback_metrics()
    
    def _init_fallback_metrics(self):
        """Initialize fallback metrics when OpenTelemetry is not available"""
        self.meter = None
        self.tracer = None
        self.logger.info("Using fallback metrics collection")
    
    def _register_standard_metrics(self):
        """Register standard system metrics"""
        if not OPENTELEMETRY_AVAILABLE or not self.meter:
            return
            
        # System metrics
        self.request_counter = self.meter.create_counter(
            name="system.requests.total",
            description="Total number of requests",
            unit="1"
        )
        
        self.request_duration = self.meter.create_histogram(
            name="system.request.duration",
            description="Request duration in seconds",
            unit="s"
        )
        
        self.error_counter = self.meter.create_counter(
            name="system.errors.total",
            description="Total number of errors",
            unit="1"
        )
        
        # Business metrics
        self.cache_hits = self.meter.create_counter(
            name="business.cache.hits",
            description="Cache hit count",
            unit="1"
        )
        
        self.cache_misses = self.meter.create_counter(
            name="business.cache.misses",
            description="Cache miss count",
            unit="1"
        )
        
        self.active_users = self.meter.create_up_down_counter(
            name="business.users.active",
            description="Number of active users",
            unit="1"
        )
        
        self.escalations = self.meter.create_counter(
            name="business.escalations.total",
            description="Total escalations",
            unit="1"
        )
        
        self.transactions = self.meter.create_counter(
            name="business.transactions.total",
            description="Total transactions",
            unit="1"
        )
    
    def register_custom_metric(self, definition: MetricDefinition):
        """Register a custom metric"""
        if not OPENTELEMETRY_AVAILABLE or not self.meter:
            self.custom_metrics[definition.name] = {
                "definition": definition,
                "values": []
            }
            return
        
        try:
            if definition.metric_type == MetricType.COUNTER:
                metric = self.meter.create_counter(
                    name=definition.name,
                    description=definition.description,
                    unit=definition.unit
                )
            elif definition.metric_type == MetricType.HISTOGRAM:
                metric = self.meter.create_histogram(
                    name=definition.name,
                    description=definition.description,
                    unit=definition.unit
                )
            elif definition.metric_type == MetricType.GAUGE:
                metric = self.meter.create_observable_gauge(
                    name=definition.name,
                    description=definition.description,
                    unit=definition.unit,
                    callbacks=[self._create_gauge_callback(definition.name)]
                )
            elif definition.metric_type == MetricType.UP_DOWN_COUNTER:
                metric = self.meter.create_up_down_counter(
                    name=definition.name,
                    description=definition.description,
                    unit=definition.unit
                )
            else:
                raise ValueError(f"Unknown metric type: {definition.metric_type}")
            
            self.metrics_registry[definition.name] = metric
            self.logger.info(f"Registered custom metric: {definition.name}")
            
        except Exception as e:
            self.logger.error(f"Failed to register metric {definition.name}: {e}")
    
    def _create_gauge_callback(self, metric_name: str):
        """Create callback for observable gauge metrics"""
        def callback(options):
            if metric_name in self.custom_metrics:
                for value in self.custom_metrics[metric_name]["values"]:
                    yield Observation(value["value"], value.get("labels", {}))
        return callback
    
    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a metric value"""
        if labels is None:
            labels = {}
        
        if OPENTELEMETRY_AVAILABLE and name in self.metrics_registry:
            try:
                metric = self.metrics_registry[name]
                if hasattr(metric, 'add'):
                    metric.add(value, labels)
                elif hasattr(metric, 'record'):
                    metric.record(value, labels)
            except Exception as e:
                self.logger.error(f"Failed to record metric {name}: {e}")
        
        # Store for fallback
        if name not in self.custom_metrics:
            self.custom_metrics[name] = {"values": []}
        
        self.custom_metrics[name]["values"].append({
            "value": value,
            "labels": labels,
            "timestamp": time.time()
        })
        
        # Keep only last 1000 values
        if len(self.custom_metrics[name]["values"]) > 1000:
            self.custom_metrics[name]["values"] = self.custom_metrics[name]["values"][-1000:]
    
    @contextmanager
    def trace_span(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Context manager for distributed tracing"""
        if OPENTELEMETRY_AVAILABLE and self.tracer:
            with self.tracer.start_as_current_span(name, attributes=attributes) as span:
                try:
                    yield span
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    raise
        else:
            # Fallback: just execute without tracing
            yield None
    
    def trace_function(self, span_name: Optional[str] = None, attributes: Optional[Dict[str, Any]] = None):
        """Decorator for function tracing"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                name = span_name or f"{func.__module__}.{func.__name__}"
                with self.trace_span(name, attributes):
                    start_time = time.time()
                    try:
                        result = func(*args, **kwargs)
                        duration = time.time() - start_time
                        self.record_metric("function.duration", duration, {"function": name})
                        return result
                    except Exception as e:
                        self.record_metric("function.errors", 1, {"function": name})
                        raise
            return wrapper
        return decorator
    
    def register_health_check(self, name: str, check_func: Callable[[], bool]):
        """Register a health check"""
        self.health_checks[name] = check_func
        self.logger.info(f"Registered health check: {name}")
    
    def register_readiness_check(self, name: str, check_func: Callable[[], bool]):
        """Register a readiness check"""
        self.readiness_checks[name] = check_func
        self.logger.info(f"Registered readiness check: {name}")
    
    def get_health_status(self) -> HealthStatus:
        """Get current health status"""
        checks = {}
        all_healthy = True
        
        for name, check_func in self.health_checks.items():
            try:
                checks[name] = check_func()
                if not checks[name]:
                    all_healthy = False
            except Exception as e:
                checks[name] = False
                all_healthy = False
                self.logger.error(f"Health check {name} failed: {e}")
        
        return HealthStatus(
            status="healthy" if all_healthy else "unhealthy",
            timestamp=time.time(),
            checks=checks,
            details={"service": "system-design-primer-ultra"}
        )
    
    def get_readiness_status(self) -> HealthStatus:
        """Get current readiness status"""
        checks = {}
        all_ready = True
        
        for name, check_func in self.readiness_checks.items():
            try:
                checks[name] = check_func()
                if not checks[name]:
                    all_ready = False
            except Exception as e:
                checks[name] = False
                all_ready = False
                self.logger.error(f"Readiness check {name} failed: {e}")
        
        return HealthStatus(
            status="ready" if all_ready else "not_ready",
            timestamp=time.time(),
            checks=checks,
            details={"service": "system-design-primer-ultra"}
        )
    
    def record_performance_data(self, operation: str, duration: float, metadata: Optional[Dict] = None):
        """Record performance data for dashboard"""
        data_point = {
            "operation": operation,
            "duration": duration,
            "timestamp": time.time(),
            "metadata": metadata or {}
        }
        self.performance_data.append(data_point)
        
        # Keep only last 10000 data points
        if len(self.performance_data) > 10000:
            self.performance_data = self.performance_data[-10000:]
        
        # Record as metric
        self.record_metric("performance.operation.duration", duration, {"operation": operation})
    
    def get_performance_summary(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get performance summary for dashboard"""
        if operation:
            filtered_data = [d for d in self.performance_data if d["operation"] == operation]
        else:
            filtered_data = self.performance_data
        
        if not filtered_data:
            return {}
        
        durations = [d["duration"] for d in filtered_data]
        return {
            "operation": operation or "all",
            "count": len(durations),
            "avg_duration": sum(durations) / len(durations),
            "min_duration": min(durations),
            "max_duration": max(durations),
            "p95_duration": sorted(durations)[int(len(durations) * 0.95)] if durations else 0,
            "p99_duration": sorted(durations)[int(len(durations) * 0.99)] if durations else 0
        }
    
    def get_grafana_dashboard(self) -> Dict[str, Any]:
        """Generate Grafana dashboard configuration"""
        dashboard = {
            "dashboard": {
                "title": "System Design Primer - Observability Dashboard",
                "tags": ["system-design", "observability", "python"],
                "timezone": "browser",
                "panels": [
                    {
                        "title": "Request Rate",
                        "type": "graph",
                        "targets": [{
                            "expr": "rate(system_requests_total[5m])",
                            "legendFormat": "{{method}} {{endpoint}}"
                        }]
                    },
                    {
                        "title": "Request Duration",
                        "type": "graph",
                        "targets": [{
                            "expr": "histogram_quantile(0.95, rate(system_request_duration_bucket[5m]))",
                            "legendFormat": "p95"
                        }, {
                            "expr": "histogram_quantile(0.99, rate(system_request_duration_bucket[5m]))",
                            "legendFormat": "p99"
                        }]
                    },
                    {
                        "title": "Error Rate",
                        "type": "graph",
                        "targets": [{
                            "expr": "rate(system_errors_total[5m])",
                            "legendFormat": "errors/sec"
                        }]
                    },
                    {
                        "title": "Cache Hit Rate",
                        "type": "singlestat",
                        "targets": [{
                            "expr": "business_cache_hits / (business_cache_hits + business_cache_misses)",
                            "legendFormat": "hit rate"
                        }],
                        "thresholds": "0.7,0.9"
                    },
                    {
                        "title": "Active Users",
                        "type": "singlestat",
                        "targets": [{
                            "expr": "business_users_active",
                            "legendFormat": "active users"
                        }]
                    },
                    {
                        "title": "Escalation Rate",
                        "type": "graph",
                        "targets": [{
                            "expr": "rate(business_escalations_total[5m])",
                            "legendFormat": "escalations/min"
                        }]
                    },
                    {
                        "title": "Transaction Volume",
                        "type": "graph",
                        "targets": [{
                            "expr": "rate(business_transactions_total[5m])",
                            "legendFormat": "transactions/sec"
                        }]
                    }
                ],
                "refresh": "10s",
                "time": {
                    "from": "now-1h",
                    "to": "now"
                }
            }
        }
        return dashboard

class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health and readiness probes"""
    
    def do_GET(self):
        metrics_collector = MetricsCollector()
        
        if self.path == '/healthz':
            status = metrics_collector.get_health_status()
            self.send_response(200 if status.status == "healthy" else 503)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(asdict(status)).encode())
            
        elif self.path == '/readyz':
            status = metrics_collector.get_readiness_status()
            self.send_response(200 if status.status == "ready" else 503)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(asdict(status)).encode())
            
        elif self.path == '/metrics':
            # Prometheus metrics endpoint
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            # In production, this would serve Prometheus metrics
            self.wfile.write(b"# Prometheus metrics would be served here\n")
            
        elif self.path == '/dashboard':
            # Serve Grafana dashboard JSON
            dashboard = metrics_collector.get_grafana_dashboard()
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(dashboard).encode())
            
        else:
            self.send_response(404)
            self.end_headers()

class HealthCheckServer:
    """Server for health checks and metrics endpoints"""
    
    def __init__(self, port: int = 8080):
        self.port = port
        self.server = None
        self.thread = None
        self.logger = StructuredLogger("health_check_server")
    
    def start(self):
        """Start the health check server in a separate thread"""
        def run_server():
            try:
                with socketserver.TCPServer(("", self.port), HealthCheckHandler) as httpd:
                    self.server = httpd
                    self.logger.info(f"Health check server started on port {self.port}")
                    httpd.serve_forever()
            except Exception as e:
                self.logger.error(f"Failed to start health check server: {e}")
        
        self.thread = threading.Thread(target=run_server, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop the health check server"""
        if self.server:
            self.server.shutdown()
            self.logger.info("Health check server stopped")

# Instrumentation decorators for existing modules
def instrument_lru_cache(cache_class):
    """Instrument LRU cache with observability"""
    original_get = cache_class.get
    original_put = cache_class.put
    
    @wraps(original_get)
    def instrumented_get(self, key):
        metrics = MetricsCollector()
        with metrics.trace_span("lru_cache.get", {"key": str(key)}):
            result = original_get(self, key)
            if result is not None:
                metrics.record_metric("cache.hit", 1, {"cache": "lru"})
                metrics.cache_hits.add(1)
            else:
                metrics.record_metric("cache.miss", 1, {"cache": "lru"})
                metrics.cache_misses.add(1)
            return result
    
    @wraps(original_put)
    def instrumented_put(self, key, value):
        metrics = MetricsCollector()
        with metrics.trace_span("lru_cache.put", {"key": str(key)}):
            result = original_put(self, key, value)
            metrics.record_metric("cache.put", 1, {"cache": "lru"})
            return result
    
    cache_class.get = instrumented_get
    cache_class.put = instrumented_put
    return cache_class

def instrument_call_center(call_center_class):
    """Instrument call center with observability"""
    original_dispatch = call_center_class.dispatch_call
    original_escalate = call_center_class.escalate_call
    
    @wraps(original_dispatch)
    def instrumented_dispatch(self, call):
        metrics = MetricsCollector()
        with metrics.trace_span("call_center.dispatch", {"call_id": str(call.id)}):
            start_time = time.time()
            result = original_dispatch(self, call)
            duration = time.time() - start_time
            metrics.record_performance_data("call_dispatch", duration)
            metrics.record_metric("call_center.dispatched", 1)
            return result
    
    @wraps(original_escalate)
    def instrumented_escalate(self, call, reason):
        metrics = MetricsCollector()
        with metrics.trace_span("call_center.escalate", {"call_id": str(call.id), "reason": reason}):
            result = original_escalate(self, call, reason)
            metrics.escalations.add(1, {"reason": reason})
            metrics.record_metric("call_center.escalated", 1, {"reason": reason})
            return result
    
    call_center_class.dispatch_call = instrumented_dispatch
    call_center_class.escalate_call = instrumented_escalate
    return call_center_class

def instrument_online_chat(chat_class):
    """Instrument online chat with observability"""
    original_send = chat_class.send_message
    original_join = chat_class.join_room
    original_leave = chat_class.leave_room
    
    @wraps(original_send)
    def instrumented_send(self, user, message):
        metrics = MetricsCollector()
        with metrics.trace_span("chat.send_message", {"user": user.name, "room": self.room_name}):
            result = original_send(self, user, message)
            metrics.record_metric("chat.messages.sent", 1, {"room": self.room_name})
            metrics.transactions.add(1, {"type": "chat_message"})
            return result
    
    @wraps(original_join)
    def instrumented_join(self, user):
        metrics = MetricsCollector()
        with metrics.trace_span("chat.join_room", {"user": user.name, "room": self.room_name}):
            result = original_join(self, user)
            metrics.active_users.add(1, {"room": self.room_name})
            metrics.record_metric("chat.users.joined", 1, {"room": self.room_name})
            return result
    
    @wraps(original_leave)
    def instrumented_leave(self, user):
        metrics = MetricsCollector()
        with metrics.trace_span("chat.leave_room", {"user": user.name, "room": self.room_name}):
            result = original_leave(self, user)
            metrics.active_users.add(-1, {"room": self.room_name})
            metrics.record_metric("chat.users.left", 1, {"room": self.room_name})
            return result
    
    chat_class.send_message = instrumented_send
    chat_class.join_room = instrumented_join
    chat_class.leave_room = instrumented_leave
    return chat_class

# Global instance for easy access
metrics_collector = MetricsCollector()

# Export key components
__all__ = [
    'MetricsCollector',
    'StructuredLogger',
    'HealthCheckServer',
    'HealthStatus',
    'MetricType',
    'MetricDefinition',
    'instrument_lru_cache',
    'instrument_call_center',
    'instrument_online_chat',
    'metrics_collector'
]