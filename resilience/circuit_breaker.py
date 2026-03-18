# resilience/circuit_breaker.py

import time
import random
import threading
import functools
import logging
from enum import Enum
from typing import Callable, Any, Optional, Dict, Set, List
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"      # Normal operation, requests flow through
    OPEN = "OPEN"          # Circuit is open, requests fail fast
    HALF_OPEN = "HALF_OPEN"  # Testing if service has recovered


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5  # Number of failures before opening circuit
    recovery_timeout: float = 30.0  # Seconds before attempting recovery
    half_open_max_calls: int = 3  # Max concurrent calls in half-open state
    success_threshold: int = 2  # Successes needed to close circuit from half-open
    timeout: float = 10.0  # Request timeout in seconds
    excluded_exceptions: Set[type] = field(default_factory=set)
    included_exceptions: Set[type] = field(default_factory=lambda: {Exception})


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 30.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Base for exponential backoff
    jitter: bool = True  # Add randomness to delays
    retry_on_exceptions: Set[type] = field(default_factory=lambda: {Exception})


@dataclass
class BulkheadConfig:
    max_concurrent_calls: int = 10
    max_wait_time: float = 5.0  # Max time to wait for a slot
    pool_size: int = 20  # Thread pool size


@dataclass
class MonitoringEvent:
    timestamp: datetime
    event_type: str
    service_name: str
    details: Dict[str, Any]
    duration: Optional[float] = None
    exception: Optional[Exception] = None


class ResilienceMetrics:
    def __init__(self, window_size: int = 100):
        self.window_size = window_size
        self.success_count = 0
        self.failure_count = 0
        self.total_calls = 0
        self.latencies = deque(maxlen=window_size)
        self.state_transitions = []
        self.last_state_change = datetime.now()
        self._lock = threading.RLock()
    
    def record_success(self, latency: float):
        with self._lock:
            self.success_count += 1
            self.total_calls += 1
            self.latencies.append(latency)
    
    def record_failure(self, latency: float):
        with self._lock:
            self.failure_count += 1
            self.total_calls += 1
            self.latencies.append(latency)
    
    def record_state_transition(self, from_state: CircuitState, to_state: CircuitState):
        with self._lock:
            self.state_transitions.append({
                'timestamp': datetime.now(),
                'from': from_state.value,
                'to': to_state.value
            })
            self.last_state_change = datetime.now()
    
    def get_failure_rate(self) -> float:
        with self._lock:
            if self.total_calls == 0:
                return 0.0
            return self.failure_count / self.total_calls
    
    def get_avg_latency(self) -> float:
        with self._lock:
            if not self.latencies:
                return 0.0
            return sum(self.latencies) / len(self.latencies)
    
    def get_percentile_latency(self, percentile: float) -> float:
        with self._lock:
            if not self.latencies:
                return 0.0
            sorted_latencies = sorted(self.latencies)
            index = int(len(sorted_latencies) * percentile)
            return sorted_latencies[min(index, len(sorted_latencies) - 1)]
    
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'success_count': self.success_count,
                'failure_count': self.failure_count,
                'total_calls': self.total_calls,
                'failure_rate': self.get_failure_rate(),
                'avg_latency': self.get_avg_latency(),
                'p95_latency': self.get_percentile_latency(0.95),
                'last_state_change': self.last_state_change.isoformat(),
                'state_transitions': self.state_transitions[-10:]  # Last 10 transitions
            }


class CircuitBreaker:
    def __init__(
        self,
        service_name: str,
        config: Optional[CircuitBreakerConfig] = None,
        on_state_change: Optional[Callable] = None
    ):
        self.service_name = service_name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0
        self.metrics = ResilienceMetrics()
        self.on_state_change = on_state_change
        self._lock = threading.RLock()
        self._listeners: List[Callable] = []
        
        logger.info(f"CircuitBreaker initialized for {service_name}")
    
    def add_listener(self, listener: Callable):
        self._listeners.append(listener)
    
    def _notify_listeners(self, event: MonitoringEvent):
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Error in circuit breaker listener: {e}")
    
    def _transition_to(self, new_state: CircuitState):
        with self._lock:
            old_state = self.state
            if old_state == new_state:
                return
            
            self.state = new_state
            self.metrics.record_state_transition(old_state, new_state)
            
            if new_state == CircuitState.CLOSED:
                self.failure_count = 0
                self.success_count = 0
                self.half_open_calls = 0
            elif new_state == CircuitState.OPEN:
                self.last_failure_time = datetime.now()
            elif new_state == CircuitState.HALF_OPEN:
                self.half_open_calls = 0
            
            event = MonitoringEvent(
                timestamp=datetime.now(),
                event_type='STATE_TRANSITION',
                service_name=self.service_name,
                details={
                    'from_state': old_state.value,
                    'to_state': new_state.value,
                    'failure_count': self.failure_count,
                    'success_count': self.success_count
                }
            )
            self._notify_listeners(event)
            
            if self.on_state_change:
                self.on_state_change(old_state, new_state)
            
            logger.info(
                f"CircuitBreaker {self.service_name}: {old_state.value} -> {new_state.value}"
            )
    
    def is_call_allowed(self) -> bool:
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if (self.last_failure_time and 
                    (datetime.now() - self.last_failure_time).total_seconds() > 
                    self.config.recovery_timeout):
                    self._transition_to(CircuitState.HALF_OPEN)
                    return True
                return False
            elif self.state == CircuitState.HALF_OPEN:
                return self.half_open_calls < self.config.half_open_max_calls
            return False
    
    def record_success(self, latency: float):
        with self._lock:
            self.metrics.record_success(latency)
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            
            event = MonitoringEvent(
                timestamp=datetime.now(),
                event_type='SUCCESS',
                service_name=self.service_name,
                details={
                    'latency': latency,
                    'state': self.state.value
                },
                duration=latency
            )
            self._notify_listeners(event)
    
    def record_failure(self, latency: float, exception: Exception):
        with self._lock:
            self.metrics.record_failure(latency)
            self.failure_count += 1
            
            event = MonitoringEvent(
                timestamp=datetime.now(),
                event_type='FAILURE',
                service_name=self.service_name,
                details={
                    'latency': latency,
                    'exception_type': type(exception).__name__,
                    'exception_message': str(exception),
                    'failure_count': self.failure_count,
                    'state': self.state.value
                },
                duration=latency,
                exception=exception
            )
            self._notify_listeners(event)
            
            if self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
            elif self.state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
    
    def get_metrics(self) -> Dict[str, Any]:
        return {
            'service_name': self.service_name,
            'state': self.state.value,
            'failure_count': self.failure_count,
            'success_count': self.success_count,
            'half_open_calls': self.half_open_calls,
            'metrics': self.metrics.to_dict(),
            'config': {
                'failure_threshold': self.config.failure_threshold,
                'recovery_timeout': self.config.recovery_timeout,
                'half_open_max_calls': self.config.half_open_max_calls
            }
        }


class RetryPolicy:
    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        on_retry: Optional[Callable] = None
    ):
        self.config = config or RetryConfig()
        self.on_retry = on_retry
        self._listeners: List[Callable] = []
    
    def add_listener(self, listener: Callable):
        self._listeners.append(listener)
    
    def _notify_listeners(self, event: MonitoringEvent):
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Error in retry listener: {e}")
    
    def get_delay(self, attempt: int) -> float:
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        if self.config.jitter:
            delay = delay * (0.5 + random.random())
        
        return delay
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        if attempt >= self.config.max_retries:
            return False
        
        for exc_type in self.config.retry_on_exceptions:
            if isinstance(exception, exc_type):
                return True
        
        return False
    
    def execute_with_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                start_time = time.time()
                result = func(*args, **kwargs)
                latency = time.time() - start_time
                
                if attempt > 0:
                    event = MonitoringEvent(
                        timestamp=datetime.now(),
                        event_type='RETRY_SUCCESS',
                        service_name=func.__name__,
                        details={
                            'attempt': attempt,
                            'total_attempts': self.config.max_retries + 1,
                            'latency': latency
                        },
                        duration=latency
                    )
                    self._notify_listeners(event)
                
                return result
                
            except Exception as e:
                last_exception = e
                latency = time.time() - start_time
                
                if not self.should_retry(e, attempt):
                    event = MonitoringEvent(
                        timestamp=datetime.now(),
                        event_type='RETRY_EXHAUSTED',
                        service_name=func.__name__,
                        details={
                            'attempt': attempt,
                            'total_attempts': self.config.max_retries + 1,
                            'exception_type': type(e).__name__,
                            'latency': latency
                        },
                        duration=latency,
                        exception=e
                    )
                    self._notify_listeners(event)
                    raise
                
                if attempt < self.config.max_retries:
                    delay = self.get_delay(attempt)
                    
                    event = MonitoringEvent(
                        timestamp=datetime.now(),
                        event_type='RETRY_ATTEMPT',
                        service_name=func.__name__,
                        details={
                            'attempt': attempt + 1,
                            'total_attempts': self.config.max_retries + 1,
                            'delay': delay,
                            'exception_type': type(e).__name__,
                            'latency': latency
                        },
                        duration=latency,
                        exception=e
                    )
                    self._notify_listeners(event)
                    
                    if self.on_retry:
                        self.on_retry(attempt + 1, e, delay)
                    
                    time.sleep(delay)
        
        raise last_exception


class Bulkhead:
    def __init__(
        self,
        service_name: str,
        config: Optional[BulkheadConfig] = None,
        on_rejected: Optional[Callable] = None
    ):
        self.service_name = service_name
        self.config = config or BulkheadConfig()
        self.on_rejected = on_rejected
        self.semaphore = threading.Semaphore(self.config.max_concurrent_calls)
        self.executor = ThreadPoolExecutor(max_workers=self.config.pool_size)
        self.active_calls = 0
        self.rejected_calls = 0
        self._lock = threading.RLock()
        self._listeners: List[Callable] = []
        
        logger.info(
            f"Bulkhead initialized for {service_name} with "
            f"{self.config.max_concurrent_calls} max concurrent calls"
        )
    
    def add_listener(self, listener: Callable):
        self._listeners.append(listener)
    
    def _notify_listeners(self, event: MonitoringEvent):
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Error in bulkhead listener: {e}")
    
    def execute(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Future:
        acquired = self.semaphore.acquire(timeout=self.config.max_wait_time)
        
        if not acquired:
            with self._lock:
                self.rejected_calls += 1
            
            event = MonitoringEvent(
                timestamp=datetime.now(),
                event_type='BULKHEAD_REJECTED',
                service_name=self.service_name,
                details={
                    'active_calls': self.active_calls,
                    'max_concurrent': self.config.max_concurrent_calls,
                    'rejected_calls': self.rejected_calls
                }
            )
            self._notify_listeners(event)
            
            if self.on_rejected:
                self.on_rejected(self.service_name)
            
            raise RuntimeError(
                f"Bulkhead {self.service_name}: Maximum concurrent calls "
                f"({self.config.max_concurrent_calls}) exceeded"
            )
        
        def wrapped_func():
            try:
                with self._lock:
                    self.active_calls += 1
                
                start_time = time.time()
                result = func(*args, **kwargs)
                latency = time.time() - start_time
                
                event = MonitoringEvent(
                    timestamp=datetime.now(),
                    event_type='BULKHEAD_CALL_COMPLETE',
                    service_name=self.service_name,
                    details={
                        'active_calls': self.active_calls,
                        'latency': latency
                    },
                    duration=latency
                )
                self._notify_listeners(event)
                
                return result
            finally:
                with self._lock:
                    self.active_calls -= 1
                self.semaphore.release()
        
        return self.executor.submit(wrapped_func)
    
    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return {
                'service_name': self.service_name,
                'active_calls': self.active_calls,
                'max_concurrent_calls': self.config.max_concurrent_calls,
                'rejected_calls': self.rejected_calls,
                'available_slots': self.config.max_concurrent_calls - self.active_calls,
                'config': {
                    'max_concurrent_calls': self.config.max_concurrent_calls,
                    'max_wait_time': self.config.max_wait_time,
                    'pool_size': self.config.pool_size
                }
            }


class ResilienceLayer:
    def __init__(
        self,
        service_name: str,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        retry_config: Optional[RetryConfig] = None,
        bulkhead_config: Optional[BulkheadConfig] = None,
        enable_circuit_breaker: bool = True,
        enable_retry: bool = True,
        enable_bulkhead: bool = True,
        fallback_function: Optional[Callable] = None
    ):
        self.service_name = service_name
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_retry = enable_retry
        self.enable_bulkhead = enable_bulkhead
        self.fallback_function = fallback_function
        
        # Initialize components
        self.circuit_breaker = None
        self.retry_policy = None
        self.bulkhead = None
        
        if enable_circuit_breaker:
            self.circuit_breaker = CircuitBreaker(
                service_name=service_name,
                config=circuit_breaker_config
            )
        
        if enable_retry:
            self.retry_policy = RetryPolicy(config=retry_config)
        
        if enable_bulkhead:
            self.bulkhead = Bulkhead(
                service_name=service_name,
                config=bulkhead_config
            )
        
        self._listeners: List[Callable] = []
        self._setup_internal_listeners()
    
    def _setup_internal_listeners(self):
        # Connect listeners for monitoring
        if self.circuit_breaker:
            self.circuit_breaker.add_listener(self._handle_event)
        
        if self.retry_policy:
            self.retry_policy.add_listener(self._handle_event)
        
        if self.bulkhead:
            self.bulkhead.add_listener(self._handle_event)
    
    def add_listener(self, listener: Callable):
        self._listeners.append(listener)
    
    def _handle_event(self, event: MonitoringEvent):
        for listener in self._listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Error in resilience layer listener: {e}")
    
    def _execute_with_fallback(self, func: Callable, *args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if self.fallback_function:
                event = MonitoringEvent(
                    timestamp=datetime.now(),
                    event_type='FALLBACK_EXECUTED',
                    service_name=self.service_name,
                    details={
                        'original_exception': str(e),
                        'exception_type': type(e).__name__
                    },
                    exception=e
                )
                self._handle_event(event)
                return self.fallback_function(*args, **kwargs)
            raise
    
    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Apply resilience patterns in order: Bulkhead -> CircuitBreaker -> Retry
            
            # 1. Execute with bulkhead (thread isolation)
            if self.enable_bulkhead and self.bulkhead:
                try:
                    future = self.bulkhead.execute(
                        self._execute_with_circuit_breaker_and_retry,
                        func,
                        *args,
                        **kwargs
                    )
                    return future.result()
                except RuntimeError as e:
                    # Bulkhead rejected, try fallback
                    if self.fallback_function:
                        return self.fallback_function(*args, **kwargs)
                    raise
            else:
                return self._execute_with_circuit_breaker_and_retry(func, *args, **kwargs)
        
        return wrapper
    
    def _execute_with_circuit_breaker_and_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        # 2. Execute with circuit breaker
        if self.enable_circuit_breaker and self.circuit_breaker:
            if not self.circuit_breaker.is_call_allowed():
                event = MonitoringEvent(
                    timestamp=datetime.now(),
                    event_type='CIRCUIT_OPEN',
                    service_name=self.service_name,
                    details={
                        'state': self.circuit_breaker.state.value,
                        'failure_count': self.circuit_breaker.failure_count
                    }
                )
                self._handle_event(event)
                
                if self.fallback_function:
                    return self.fallback_function(*args, **kwargs)
                raise RuntimeError(
                    f"Circuit breaker {self.service_name} is "
                    f"{self.circuit_breaker.state.value}"
                )
        
        # 3. Execute with retry
        if self.enable_retry and self.retry_policy:
            try:
                start_time = time.time()
                result = self.retry_policy.execute_with_retry(func, *args, **kwargs)
                latency = time.time() - start_time
                
                if self.enable_circuit_breaker and self.circuit_breaker:
                    self.circuit_breaker.record_success(latency)
                
                return result
            except Exception as e:
                latency = time.time() - start_time
                
                if self.enable_circuit_breaker and self.circuit_breaker:
                    self.circuit_breaker.record_failure(latency, e)
                
                if self.fallback_function:
                    return self.fallback_function(*args, **kwargs)
                raise
        else:
            # No retry, execute directly
            try:
                start_time = time.time()
                result = func(*args, **kwargs)
                latency = time.time() - start_time
                
                if self.enable_circuit_breaker and self.circuit_breaker:
                    self.circuit_breaker.record_success(latency)
                
                return result
            except Exception as e:
                latency = time.time() - start_time
                
                if self.enable_circuit_breaker and self.circuit_breaker:
                    self.circuit_breaker.record_failure(latency, e)
                
                if self.fallback_function:
                    return self.fallback_function(*args, **kwargs)
                raise
    
    def get_metrics(self) -> Dict[str, Any]:
        metrics = {
            'service_name': self.service_name,
            'components': {
                'circuit_breaker_enabled': self.enable_circuit_breaker,
                'retry_enabled': self.enable_retry,
                'bulkhead_enabled': self.enable_bulkhead,
                'fallback_enabled': self.fallback_function is not None
            }
        }
        
        if self.circuit_breaker:
            metrics['circuit_breaker'] = self.circuit_breaker.get_metrics()
        
        if self.bulkhead:
            metrics['bulkhead'] = self.bulkhead.get_metrics()
        
        return metrics


# Decorator factory for easy application
def with_resilience(
    service_name: str,
    circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
    retry_config: Optional[RetryConfig] = None,
    bulkhead_config: Optional[BulkheadConfig] = None,
    enable_circuit_breaker: bool = True,
    enable_retry: bool = True,
    enable_bulkhead: bool = True,
    fallback_function: Optional[Callable] = None
) -> Callable:
    """
    Decorator factory for applying resilience patterns to a function.
    
    Usage:
        @with_resilience(
            service_name="payment_service",
            circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
            retry_config=RetryConfig(max_retries=2),
            bulkhead_config=BulkheadConfig(max_concurrent_calls=5),
            fallback_function=payment_fallback
        )
        def process_payment(amount, user_id):
            # Call payment service
            pass
    """
    def decorator(func: Callable) -> Callable:
        layer = ResilienceLayer(
            service_name=service_name,
            circuit_breaker_config=circuit_breaker_config,
            retry_config=retry_config,
            bulkhead_config=bulkhead_config,
            enable_circuit_breaker=enable_circuit_breaker,
            enable_retry=enable_retry,
            enable_bulkhead=enable_bulkhead,
            fallback_function=fallback_function
        )
        
        return layer(func)
    
    return decorator


# Monitoring and metrics collection
class ResilienceMonitor:
    def __init__(self):
        self.events: List[MonitoringEvent] = []
        self.metrics: Dict[str, ResilienceMetrics] = {}
        self._lock = threading.RLock()
    
    def record_event(self, event: MonitoringEvent):
        with self._lock:
            self.events.append(event)
            # Keep only last 1000 events
            if len(self.events) > 1000:
                self.events = self.events[-1000:]
    
    def get_events(
        self,
        service_name: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100
    ) -> List[MonitoringEvent]:
        with self._lock:
            filtered = self.events
            
            if service_name:
                filtered = [e for e in filtered if e.service_name == service_name]
            
            if event_type:
                filtered = [e for e in filtered if e.event_type == event_type]
            
            return filtered[-limit:]
    
    def get_service_metrics(self, service_name: str) -> Dict[str, Any]:
        with self._lock:
            service_events = [e for e in self.events if e.service_name == service_name]
            
            if not service_events:
                return {}
            
            success_count = len([e for e in service_events if e.event_type == 'SUCCESS'])
            failure_count = len([e for e in service_events if e.event_type == 'FAILURE'])
            total_calls = success_count + failure_count
            
            latencies = [e.duration for e in service_events if e.duration is not None]
            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            
            return {
                'service_name': service_name,
                'total_calls': total_calls,
                'success_count': success_count,
                'failure_count': failure_count,
                'success_rate': success_count / total_calls if total_calls > 0 else 0,
                'avg_latency': avg_latency,
                'last_event': service_events[-1].timestamp.isoformat() if service_events else None
            }
    
    def get_global_metrics(self) -> Dict[str, Any]:
        with self._lock:
            services = set(e.service_name for e in self.events)
            
            metrics = {
                'total_events': len(self.events),
                'services_monitored': len(services),
                'service_metrics': {}
            }
            
            for service in services:
                metrics['service_metrics'][service] = self.get_service_metrics(service)
            
            return metrics


# Global monitor instance
global_monitor = ResilienceMonitor()


def setup_global_monitoring():
    """Setup global monitoring for all resilience components"""
    def global_event_listener(event: MonitoringEvent):
        global_monitor.record_event(event)
    
    return global_event_listener


# Example usage with existing service patterns
if __name__ == "__main__":
    # Example 1: Using the decorator directly
    @with_resilience(
        service_name="example_service",
        circuit_breaker_config=CircuitBreakerConfig(
            failure_threshold=3,
            recovery_timeout=10.0
        ),
        retry_config=RetryConfig(max_retries=2, base_delay=0.5),
        bulkhead_config=BulkheadConfig(max_concurrent_calls=5),
        fallback_function=lambda x: f"Fallback: {x}"
    )
    def example_service_call(data: str) -> str:
        # Simulate service call that might fail
        if random.random() < 0.3:  # 30% failure rate
            raise ValueError("Service unavailable")
        return f"Processed: {data}"
    
    # Example 2: Using ResilienceLayer directly for more control
    def create_resilient_service(service_name: str):
        layer = ResilienceLayer(
            service_name=service_name,
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=30.0
            ),
            retry_config=RetryConfig(max_retries=3),
            bulkhead_config=BulkheadConfig(max_concurrent_calls=10),
            fallback_function=lambda: {"status": "degraded", "data": None}
        )
        
        # Add monitoring
        layer.add_listener(global_monitor.record_event)
        
        @layer
        def service_operation(operation_id: int) -> Dict[str, Any]:
            # Simulate operation
            time.sleep(0.1)
            if random.random() < 0.2:  # 20% failure rate
                raise RuntimeError(f"Operation {operation_id} failed")
            return {"operation_id": operation_id, "status": "success"}
        
        return service_operation
    
    # Test the implementation
    print("Testing Resilience Layer...")
    
    # Test example service
    for i in range(20):
        try:
            result = example_service_call(f"test_{i}")
            print(f"Call {i}: {result}")
        except Exception as e:
            print(f"Call {i} failed: {e}")
    
    # Test resilient service
    resilient_service = create_resilient_service("test_service")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for i in range(30):
            future = executor.submit(resilient_service, i)
            futures.append(future)
        
        for future in futures:
            try:
                result = future.result(timeout=5)
                print(f"Service result: {result}")
            except Exception as e:
                print(f"Service call failed: {e}")
    
    # Print metrics
    print("\nGlobal Metrics:")
    print(json.dumps(global_monitor.get_global_metrics(), indent=2, default=str))