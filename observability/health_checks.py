import logging
import time
import threading
import json
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
import socketserver

# OpenTelemetry imports
try:
    from opentelemetry import trace, metrics
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    from opentelemetry.baggage.propagation import W3CBaggagePropagator
    from opentelemetry.propagators.composite import CompositeHTTPPropagator
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

# Import existing modules for instrumentation
try:
    from solutions.object_oriented_design.call_center.call_center import CallCenter
    from solutions.object_oriented_design.lru_cache.lru_cache import LRUCache
    from solutions.object_oriented_design.online_chat.online_chat import ChatServer
    EXISTING_MODULES_AVAILABLE = True
except ImportError:
    EXISTING_MODULES_AVAILABLE = False

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class HealthCheckResult:
    status: HealthStatus
    timestamp: str
    duration_ms: float
    details: Dict[str, Any]
    checks: Dict[str, Dict[str, Any]]
    version: str = "1.0.0"

class StructuredLogger:
    """Structured logging with OpenTelemetry context propagation"""
    
    def __init__(self, name: str, service_name: str = "system-design-primer-ultra"):
        self.logger = logging.getLogger(name)
        self.service_name = service_name
        self.tracer = None
        self.propagator = None
        
        if OPENTELEMETRY_AVAILABLE:
            self.tracer = trace.get_tracer(name)
            self.propagator = CompositeHTTPPropagator([
                TraceContextTextMapPropagator(),
                W3CBaggagePropagator()
            ])
        
        # Configure JSON formatter
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"logger": "%(name)s", "message": "%(message)s", '
            '"service": "' + service_name + '"}'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_with_context(self, level: str, message: str, **kwargs):
        """Log with OpenTelemetry context"""
        context = {}
        
        if OPENTELEMETRY_AVAILABLE and self.tracer:
            current_span = trace.get_current_span()
            if current_span:
                context["trace_id"] = format(current_span.get_span_context().trace_id, '032x')
                context["span_id"] = format(current_span.get_span_context().span_id, '016x')
        
        log_entry = {
            "message": message,
            "context": context,
            **kwargs
        }
        
        getattr(self.logger, level)(json.dumps(log_entry))

class MetricsCollector:
    """Collects and exports metrics with OpenTelemetry"""
    
    def __init__(self, service_name: str = "system-design-primer-ultra"):
        self.service_name = service_name
        self.meter = None
        self.custom_metrics = {}
        
        if OPENTELEMETRY_AVAILABLE:
            resource = Resource.create({"service.name": service_name})
            metric_reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint="localhost:4317"),
                export_interval_millis=5000
            )
            provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
            metrics.set_meter_provider(provider)
            self.meter = metrics.get_meter(service_name)
    
    def create_counter(self, name: str, description: str = "", unit: str = "1"):
        """Create a counter metric"""
        if self.meter:
            counter = self.meter.create_counter(
                name=name,
                description=description,
                unit=unit
            )
            self.custom_metrics[name] = counter
            return counter
        return None
    
    def create_histogram(self, name: str, description: str = "", unit: str = "ms"):
        """Create a histogram metric"""
        if self.meter:
            histogram = self.meter.create_histogram(
                name=name,
                description=description,
                unit=unit
            )
            self.custom_metrics[name] = histogram
            return histogram
        return None
    
    def create_gauge(self, name: str, description: str = "", unit: str = "1"):
        """Create a gauge metric"""
        if self.meter:
            gauge = self.meter.create_observable_gauge(
                name=name,
                callbacks=[self._gauge_callback],
                description=description,
                unit=unit
            )
            self.custom_metrics[name] = gauge
            return gauge
        return None
    
    def _gauge_callback(self, options):
        """Callback for gauge metrics"""
        # Implement custom gauge logic here
        pass
    
    def record_cache_hit(self, cache_name: str, hit: bool):
        """Record cache hit/miss"""
        if "cache_hits_total" not in self.custom_metrics:
            self.create_counter("cache_hits_total", "Total cache hits")
            self.create_counter("cache_misses_total", "Total cache misses")
        
        if hit:
            if self.custom_metrics.get("cache_hits_total"):
                self.custom_metrics["cache_hits_total"].add(1, {"cache": cache_name})
        else:
            if self.custom_metrics.get("cache_misses_total"):
                self.custom_metrics["cache_misses_total"].add(1, {"cache": cache_name})
    
    def record_escalation(self, from_level: str, to_level: str):
        """Record escalation path"""
        if "escalations_total" not in self.custom_metrics:
            self.create_counter("escalations_total", "Total escalations")
        
        if self.custom_metrics.get("escalations_total"):
            self.custom_metrics["escalations_total"].add(1, {
                "from_level": from_level,
                "to_level": to_level
            })
    
    def record_transaction(self, category: str, success: bool, duration_ms: float):
        """Record transaction metrics"""
        if "transactions_total" not in self.custom_metrics:
            self.create_counter("transactions_total", "Total transactions")
            self.create_histogram("transaction_duration_ms", "Transaction duration")
        
        if self.custom_metrics.get("transactions_total"):
            self.custom_metrics["transactions_total"].add(1, {
                "category": category,
                "success": str(success)
            })
        
        if self.custom_metrics.get("transaction_duration_ms"):
            self.custom_metrics["transaction_duration_ms"].record(duration_ms, {
                "category": category
            })

class DistributedTracer:
    """Distributed tracing with OpenTelemetry"""
    
    def __init__(self, service_name: str = "system-design-primer-ultra"):
        self.service_name = service_name
        self.tracer = None
        
        if OPENTELEMETRY_AVAILABLE:
            resource = Resource.create({"service.name": service_name})
            provider = TracerProvider(resource=resource)
            processor = BatchSpanProcessor(
                OTLPSpanExporter(endpoint="localhost:4317")
            )
            provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)
            self.tracer = trace.get_tracer(service_name)
    
    def start_span(self, name: str, attributes: Dict[str, Any] = None):
        """Start a new span"""
        if self.tracer:
            return self.tracer.start_as_current_span(name, attributes=attributes or {})
        return None
    
    def add_event(self, span, name: str, attributes: Dict[str, Any] = None):
        """Add event to span"""
        if span:
            span.add_event(name, attributes=attributes or {})

class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints"""
    
    def do_GET(self):
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/ready':
            self._handle_readiness()
        elif self.path == '/metrics':
            self._handle_metrics()
        elif self.path == '/dashboard':
            self._handle_dashboard()
        else:
            self.send_error(404, "Endpoint not found")
    
    def _handle_health(self):
        """Handle liveness probe"""
        result = self.server.health_checker.check_health()
        self._send_json_response(result)
    
    def _handle_readiness(self):
        """Handle readiness probe"""
        result = self.server.health_checker.check_readiness()
        self._send_json_response(result)
    
    def _handle_metrics(self):
        """Handle metrics endpoint"""
        metrics = self.server.health_checker.get_metrics()
        self._send_json_response(metrics)
    
    def _handle_dashboard(self):
        """Handle dashboard endpoint"""
        dashboard = self.server.health_checker.get_dashboard_data()
        self._send_json_response(dashboard)
    
    def _send_json_response(self, data: Dict):
        """Send JSON response"""
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to use structured logging"""
        pass

class HealthChecker:
    """Main health checker with observability integration"""
    
    def __init__(self, 
                 service_name: str = "system-design-primer-ultra",
                 port: int = 8080,
                 enable_observability: bool = True):
        self.service_name = service_name
        self.port = port
        self.checks = {}
        self.dependencies = {}
        self.start_time = datetime.now()
        self.server = None
        self.server_thread = None
        
        # Initialize observability
        self.logger = StructuredLogger("health_checker", service_name)
        self.metrics = MetricsCollector(service_name) if enable_observability else None
        self.tracer = DistributedTracer(service_name) if enable_observability else None
        
        # Register default checks
        self.register_check("uptime", self._check_uptime)
        self.register_check("memory", self._check_memory)
        self.register_check("threads", self._check_threads)
        
        # Instrument existing modules
        if EXISTING_MODULES_AVAILABLE:
            self._instrument_existing_modules()
    
    def _instrument_existing_modules(self):
        """Instrument existing modules with observability"""
        self.logger.log_with_context("info", "Instrumenting existing modules")
        
        # Example instrumentation for CallCenter
        if 'CallCenter' in globals():
            self.register_check("call_center", self._check_call_center)
        
        # Example instrumentation for LRUCache
        if 'LRUCache' in globals():
            self.register_check("lru_cache", self._check_lru_cache)
    
    def _check_call_center(self) -> Dict[str, Any]:
        """Health check for CallCenter module"""
        try:
            # This would be implemented based on actual CallCenter metrics
            return {
                "status": HealthStatus.HEALTHY.value,
                "queue_size": 0,
                "available_agents": 5,
                "avg_wait_time_ms": 150
            }
        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "error": str(e)
            }
    
    def _check_lru_cache(self) -> Dict[str, Any]:
        """Health check for LRUCache module"""
        try:
            # This would be implemented based on actual LRUCache metrics
            return {
                "status": HealthStatus.HEALTHY.value,
                "size": 100,
                "capacity": 1000,
                "hit_rate": 0.85
            }
        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "error": str(e)
            }
    
    def _check_uptime(self) -> Dict[str, Any]:
        """Check system uptime"""
        uptime_seconds = (datetime.now() - self.start_time).total_seconds()
        return {
            "status": HealthStatus.HEALTHY.value,
            "uptime_seconds": uptime_seconds,
            "start_time": self.start_time.isoformat()
        }
    
    def _check_memory(self) -> Dict[str, Any]:
        """Check memory usage"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            return {
                "status": HealthStatus.HEALTHY.value,
                "total_gb": round(memory.total / (1024**3), 2),
                "available_gb": round(memory.available / (1024**3), 2),
                "percent_used": memory.percent
            }
        except ImportError:
            return {
                "status": HealthStatus.UNKNOWN.value,
                "message": "psutil not installed"
            }
    
    def _check_threads(self) -> Dict[str, Any]:
        """Check thread count"""
        try:
            import threading
            return {
                "status": HealthStatus.HEALTHY.value,
                "active_threads": threading.active_count(),
                "main_thread_alive": threading.main_thread().is_alive()
            }
        except Exception as e:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "error": str(e)
            }
    
    def register_check(self, name: str, check_func: Callable[[], Dict[str, Any]]):
        """Register a health check"""
        self.checks[name] = check_func
        self.logger.log_with_context("info", f"Registered health check: {name}")
    
    def register_dependency(self, name: str, check_func: Callable[[], bool]):
        """Register a dependency check"""
        self.dependencies[name] = check_func
        self.logger.log_with_context("info", f"Registered dependency: {name}")
    
    def check_health(self) -> Dict[str, Any]:
        """Perform all health checks"""
        start_time = time.time()
        
        if self.tracer:
            with self.tracer.start_span("health_check") as span:
                span.set_attribute("service", self.service_name)
                return self._perform_checks(start_time)
        else:
            return self._perform_checks(start_time)
    
    def _perform_checks(self, start_time: float) -> Dict[str, Any]:
        """Perform the actual health checks"""
        results = {}
        overall_status = HealthStatus.HEALTHY
        
        for name, check_func in self.checks.items():
            try:
                check_start = time.time()
                result = check_func()
                check_duration = (time.time() - check_start) * 1000
                
                results[name] = {
                    **result,
                    "duration_ms": round(check_duration, 2)
                }
                
                if result.get("status") == HealthStatus.UNHEALTHY.value:
                    overall_status = HealthStatus.UNHEALTHY
                elif result.get("status") == HealthStatus.DEGRADED.value and overall_status == HealthStatus.HEALTHY:
                    overall_status = HealthStatus.DEGRADED
                
                # Record metrics
                if self.metrics:
                    self.metrics.record_transaction(
                        category=f"health_check_{name}",
                        success=result.get("status") == HealthStatus.HEALTHY.value,
                        duration_ms=check_duration
                    )
                    
            except Exception as e:
                results[name] = {
                    "status": HealthStatus.UNHEALTHY.value,
                    "error": str(e)
                }
                overall_status = HealthStatus.UNHEALTHY
        
        # Check dependencies
        dependency_results = {}
        for name, check_func in self.dependencies.items():
            try:
                dependency_results[name] = check_func()
                if not dependency_results[name]:
                    overall_status = HealthStatus.DEGRADED
            except Exception as e:
                dependency_results[name] = False
                overall_status = HealthStatus.UNHEALTHY
        
        duration_ms = (time.time() - start_time) * 1000
        
        result = HealthCheckResult(
            status=overall_status,
            timestamp=datetime.now().isoformat(),
            duration_ms=round(duration_ms, 2),
            details={
                "service": self.service_name,
                "version": "1.0.0",
                "dependencies": dependency_results
            },
            checks=results
        )
        
        # Log the result
        self.logger.log_with_context(
            "info" if overall_status == HealthStatus.HEALTHY else "warning",
            "Health check completed",
            status=overall_status.value,
            duration_ms=duration_ms,
            checks_count=len(results)
        )
        
        return asdict(result)
    
    def check_readiness(self) -> Dict[str, Any]:
        """Check if service is ready to accept traffic"""
        start_time = time.time()
        
        # Check dependencies first
        for name, check_func in self.dependencies.items():
            try:
                if not check_func():
                    return {
                        "status": HealthStatus.UNHEALTHY.value,
                        "timestamp": datetime.now().isoformat(),
                        "duration_ms": (time.time() - start_time) * 1000,
                        "message": f"Dependency {name} not ready"
                    }
            except Exception as e:
                return {
                    "status": HealthStatus.UNHEALTHY.value,
                    "timestamp": datetime.now().isoformat(),
                    "duration_ms": (time.time() - start_time) * 1000,
                    "message": f"Dependency check failed: {str(e)}"
                }
        
        # Perform readiness-specific checks
        readiness_checks = {}
        
        # Check if critical resources are available
        if "memory" in self.checks:
            memory_result = self.checks["memory"]()
            if memory_result.get("percent_used", 0) > 90:
                return {
                    "status": HealthStatus.DEGRADED.value,
                    "timestamp": datetime.now().isoformat(),
                    "duration_ms": (time.time() - start_time) * 1000,
                    "message": "Memory usage too high"
                }
        
        duration_ms = (time.time() - start_time) * 1000
        
        return {
            "status": HealthStatus.HEALTHY.value,
            "timestamp": datetime.now().isoformat(),
            "duration_ms": round(duration_ms, 2),
            "message": "Service is ready",
            "checks": readiness_checks
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        metrics_data = {
            "timestamp": datetime.now().isoformat(),
            "service": self.service_name,
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds(),
            "checks_registered": len(self.checks),
            "dependencies_registered": len(self.dependencies)
        }
        
        # Add custom metrics if available
        if self.metrics:
            metrics_data["custom_metrics"] = list(self.metrics.custom_metrics.keys())
        
        return metrics_data
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data for Grafana dashboard"""
        health_data = self.check_health()
        
        dashboard_data = {
            "health": health_data,
            "metrics": self.get_metrics(),
            "grafana_dashboard": self._generate_grafana_dashboard()
        }
        
        return dashboard_data
    
    def _generate_grafana_dashboard(self) -> Dict[str, Any]:
        """Generate Grafana dashboard configuration"""
        return {
            "title": f"{self.service_name} Health Dashboard",
            "panels": [
                {
                    "title": "Health Status",
                    "type": "stat",
                    "targets": [{
                        "expr": "up{service=\"" + self.service_name + "\"}",
                        "legendFormat": "Health Status"
                    }]
                },
                {
                    "title": "Request Duration",
                    "type": "graph",
                    "targets": [{
                        "expr": "rate(transaction_duration_ms_sum{service=\"" + self.service_name + "\"}[5m])",
                        "legendFormat": "Avg Duration"
                    }]
                },
                {
                    "title": "Cache Hit Rate",
                    "type": "gauge",
                    "targets": [{
                        "expr": "cache_hits_total{service=\"" + self.service_name + "\"} / (cache_hits_total{service=\"" + self.service_name + "\"} + cache_misses_total{service=\"" + self.service_name + "\"})",
                        "legendFormat": "Hit Rate"
                    }]
                },
                {
                    "title": "Escalation Paths",
                    "type": "bar",
                    "targets": [{
                        "expr": "escalations_total{service=\"" + self.service_name + "\"}",
                        "legendFormat": "{{from_level}} -> {{to_level}}"
                    }]
                }
            ],
            "refresh": "5s",
            "time": {
                "from": "now-1h",
                "to": "now"
            }
        }
    
    def start_server(self):
        """Start the health check server"""
        if self.server_thread and self.server_thread.is_alive():
            self.logger.log_with_context("warning", "Health check server already running")
            return
        
        class HealthCheckHTTPServer(HTTPServer):
            def __init__(self, server_address, RequestHandlerClass, health_checker):
                super().__init__(server_address, RequestHandlerClass)
                self.health_checker = health_checker
        
        self.server = HealthCheckHTTPServer(
            ('', self.port),
            HealthCheckHandler,
            self
        )
        
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True
        )
        self.server_thread.start()
        
        self.logger.log_with_context(
            "info",
            f"Health check server started on port {self.port}",
            endpoints=["/health", "/ready", "/metrics", "/dashboard"]
        )
    
    def stop_server(self):
        """Stop the health check server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.logger.log_with_context("info", "Health check server stopped")

# Decorator for instrumenting functions
def instrument_function(func_name: str = None):
    """Decorator to instrument functions with tracing and metrics"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Get the health checker instance if available
            health_checker = None
            for arg in args:
                if isinstance(arg, HealthChecker):
                    health_checker = arg
                    break
            
            start_time = time.time()
            span_name = func_name or func.__name__
            
            if health_checker and health_checker.tracer:
                with health_checker.tracer.start_span(span_name) as span:
                    span.set_attribute("function", func.__name__)
                    span.set_attribute("args_count", len(args))
                    span.set_attribute("kwargs_count", len(kwargs))
                    
                    try:
                        result = func(*args, **kwargs)
                        duration_ms = (time.time() - start_time) * 1000
                        
                        # Record success metric
                        if health_checker.metrics:
                            health_checker.metrics.record_transaction(
                                category=span_name,
                                success=True,
                                duration_ms=duration_ms
                            )
                        
                        span.set_attribute("success", True)
                        span.set_attribute("duration_ms", duration_ms)
                        
                        return result
                    except Exception as e:
                        duration_ms = (time.time() - start_time) * 1000
                        
                        # Record failure metric
                        if health_checker.metrics:
                            health_checker.metrics.record_transaction(
                                category=span_name,
                                success=False,
                                duration_ms=duration_ms
                            )
                        
                        span.set_attribute("success", False)
                        span.set_attribute("error", str(e))
                        span.set_attribute("duration_ms", duration_ms)
                        
                        raise
            else:
                return func(*args, **kwargs)
        
        return wrapper
    return decorator

# Example usage
if __name__ == "__main__":
    # Create health checker
    checker = HealthChecker(
        service_name="system-design-primer-ultra",
        port=8080,
        enable_observability=True
    )
    
    # Register custom checks
    @instrument_function("custom_business_check")
    def check_business_logic():
        """Example business logic check"""
        # Simulate some work
        time.sleep(0.1)
        return {
            "status": HealthStatus.HEALTHY.value,
            "processed_items": 42,
            "queue_depth": 5
        }
    
    checker.register_check("business_logic", check_business_logic)
    
    # Register dependency
    def check_database():
        """Example database dependency check"""
        # Simulate database check
        return True
    
    checker.register_dependency("database", check_database)
    
    # Start server
    checker.start_server()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        checker.stop_server()
        print("Health check server stopped")