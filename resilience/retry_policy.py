"""
resilience/retry_policy.py

Production-Ready Resilience Layer for System Design Primer
Implements circuit breakers, bulkheads, retry policies with exponential backoff,
and graceful degradation patterns via decorator-based approach.
"""

import time
import random
import logging
import threading
from enum import Enum
from typing import Callable, Any, Optional, Dict, List
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, Future
from functools import wraps
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing, reject requests
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5          # Failures before opening
    recovery_timeout: float = 30.0      # Seconds before half-open
    half_open_max_calls: int = 3        # Max calls in half-open state
    success_threshold: int = 2          # Successes to close from half-open


@dataclass
class RetryConfig:
    """Configuration for retry policy."""
    max_retries: int = 3
    base_delay: float = 0.1             # Base delay in seconds
    max_delay: float = 10.0             # Maximum delay cap
    exponential_base: float = 2.0       # Exponential backoff base
    jitter: bool = True                 # Add random jitter


@dataclass
class BulkheadConfig:
    """Configuration for bulkhead isolation."""
    max_concurrent_calls: int = 10      # Max concurrent executions
    max_wait_time: float = 1.0          # Max wait for available slot


class ResilienceEvent(Enum):
    """Types of resilience events for monitoring."""
    CIRCUIT_OPENED = "circuit_opened"
    CIRCUIT_CLOSED = "circuit_closed"
    CIRCUIT_HALF_OPENED = "circuit_half_opened"
    RETRY_ATTEMPTED = "retry_attempted"
    RETRY_EXHAUSTED = "retry_exhausted"
    BULKHEAD_REJECTED = "bulkhead_rejected"
    CALL_SUCCEEDED = "call_succeeded"
    CALL_FAILED = "call_failed"
    FALLBACK_EXECUTED = "fallback_executed"


class ResilienceMonitor:
    """Monitoring interface for resilience events."""
    
    def __init__(self):
        self._callbacks: Dict[ResilienceEvent, List[Callable]] = {
            event: [] for event in ResilienceEvent
        }
    
    def register_callback(self, event: ResilienceEvent, callback: Callable):
        """Register a callback for a specific resilience event."""
        self._callbacks[event].append(callback)
    
    def emit_event(self, event: ResilienceEvent, metadata: Optional[Dict] = None):
        """Emit an event to all registered callbacks."""
        metadata = metadata or {}
        metadata['timestamp'] = datetime.utcnow().isoformat()
        
        for callback in self._callbacks[event]:
            try:
                callback(event, metadata)
            except Exception as e:
                logger.warning(f"Resilience monitor callback failed: {e}")


# Global monitor instance
monitor = ResilienceMonitor()


class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    def __init__(self, config: CircuitBreakerConfig, name: str = "default"):
        self.config = config
        self.name = name
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0
        self._lock = threading.RLock()
    
    def allow_request(self) -> bool:
        """Check if request should be allowed based on circuit state."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            
            elif self.state == CircuitState.OPEN:
                # Check if recovery timeout has passed
                if self.last_failure_time:
                    time_since_failure = (datetime.utcnow() - self.last_failure_time).total_seconds()
                    if time_since_failure >= self.config.recovery_timeout:
                        self.state = CircuitState.HALF_OPEN
                        self.half_open_calls = 0
                        monitor.emit_event(ResilienceEvent.CIRCUIT_HALF_OPENED, {
                            'circuit': self.name
                        })
                        return True
                return False
            
            elif self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls < self.config.half_open_max_calls:
                    self.half_open_calls += 1
                    return True
                return False
            
            return False
    
    def record_success(self):
        """Record a successful call."""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.half_open_calls = 0
                    monitor.emit_event(ResilienceEvent.CIRCUIT_CLOSED, {
                        'circuit': self.name
                    })
            elif self.state == CircuitState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self):
        """Record a failed call."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            if self.state == CircuitState.HALF_OPEN:
                # Failure in half-open state immediately opens circuit
                self.state = CircuitState.OPEN
                monitor.emit_event(ResilienceEvent.CIRCUIT_OPENED, {
                    'circuit': self.name,
                    'reason': 'failure_in_half_open'
                })
            
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitState.OPEN
                    monitor.emit_event(ResilienceEvent.CIRCUIT_OPENED, {
                        'circuit': self.name,
                        'failure_count': self.failure_count
                    })


class Bulkhead:
    """Bulkhead pattern for thread pool isolation."""
    
    def __init__(self, config: BulkheadConfig, name: str = "default"):
        self.config = config
        self.name = name
        self.executor = ThreadPoolExecutor(
            max_workers=config.max_concurrent_calls,
            thread_name_prefix=f"bulkhead-{name}"
        )
        self.active_calls = 0
        self._lock = threading.Lock()
    
    def execute(self, func: Callable, *args, **kwargs) -> Future:
        """Execute function in isolated thread pool."""
        with self._lock:
            if self.active_calls >= self.config.max_concurrent_calls:
                monitor.emit_event(ResilienceEvent.BULKHEAD_REJECTED, {
                    'bulkhead': self.name,
                    'active_calls': self.active_calls
                })
                raise BulkheadFullError(f"Bulkhead {self.name} is full")
            
            self.active_calls += 1
        
        try:
            future = self.executor.submit(func, *args, **kwargs)
            future.add_done_callback(self._call_completed)
            return future
        except Exception as e:
            with self._lock:
                self.active_calls -= 1
            raise
    
    def _call_completed(self, future: Future):
        """Callback when call completes."""
        with self._lock:
            self.active_calls -= 1
    
    def shutdown(self, wait: bool = True):
        """Shutdown the thread pool."""
        self.executor.shutdown(wait=wait)


class RetryPolicy:
    """Retry policy with exponential backoff and jitter."""
    
    def __init__(self, config: RetryConfig, name: str = "default"):
        self.config = config
        self.name = name
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt with exponential backoff."""
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        if self.config.jitter:
            # Add jitter: random between 0 and delay
            delay = random.uniform(0, delay)
        
        return delay
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Determine if we should retry based on attempt and exception."""
        if attempt >= self.config.max_retries:
            return False
        
        # Don't retry on certain exceptions (configurable)
        non_retryable = (TypeError, ValueError, AttributeError)
        if isinstance(exception, non_retryable):
            return False
        
        return True


class BulkheadFullError(Exception):
    """Raised when bulkhead is at capacity."""
    pass


class ResilientDecorator:
    """
    Main decorator that combines all resilience patterns.
    
    Usage:
        @Resilient(
            circuit_breaker=CircuitBreakerConfig(),
            retry=RetryConfig(),
            bulkhead=BulkheadConfig(),
            fallback=my_fallback_function
        )
        def my_service_call():
            ...
    """
    
    def __init__(
        self,
        circuit_breaker: Optional[CircuitBreakerConfig] = None,
        retry: Optional[RetryConfig] = None,
        bulkhead: Optional[BulkheadConfig] = None,
        fallback: Optional[Callable] = None,
        name: Optional[str] = None
    ):
        self.circuit_breaker = CircuitBreaker(circuit_breaker, name) if circuit_breaker else None
        self.retry_policy = RetryPolicy(retry, name) if retry else None
        self.bulkhead = Bulkhead(bulkhead, name) if bulkhead else None
        self.fallback = fallback
        self.name = name or "unnamed"
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self._execute_with_resilience(func, *args, **kwargs)
        
        # Add resilience metadata to function
        wrapper.resilience_config = {
            'circuit_breaker': self.circuit_breaker,
            'retry_policy': self.retry_policy,
            'bulkhead': self.bulkhead,
            'name': self.name
        }
        
        return wrapper
    
    def _execute_with_resilience(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with all resilience patterns applied."""
        attempt = 0
        last_exception = None
        
        while True:
            try:
                # Check circuit breaker
                if self.circuit_breaker and not self.circuit_breaker.allow_request():
                    raise CircuitOpenError(f"Circuit {self.name} is open")
                
                # Execute with bulkhead isolation
                if self.bulkhead:
                    future = self.bulkhead.execute(func, *args, **kwargs)
                    result = future.result(timeout=self.bulkhead.config.max_wait_time)
                else:
                    result = func(*args, **kwargs)
                
                # Record success
                if self.circuit_breaker:
                    self.circuit_breaker.record_success()
                
                monitor.emit_event(ResilienceEvent.CALL_SUCCEEDED, {
                    'function': func.__name__,
                    'attempt': attempt
                })
                
                return result
                
            except Exception as e:
                last_exception = e
                
                # Record failure
                if self.circuit_breaker:
                    self.circuit_breaker.record_failure()
                
                monitor.emit_event(ResilienceEvent.CALL_FAILED, {
                    'function': func.__name__,
                    'attempt': attempt,
                    'error': str(e)
                })
                
                # Check if we should retry
                if self.retry_policy and self.retry_policy.should_retry(attempt, e):
                    delay = self.retry_policy.calculate_delay(attempt)
                    
                    monitor.emit_event(ResilienceEvent.RETRY_ATTEMPTED, {
                        'function': func.__name__,
                        'attempt': attempt,
                        'delay': delay
                    })
                    
                    time.sleep(delay)
                    attempt += 1
                    continue
                else:
                    # No more retries
                    if self.retry_policy:
                        monitor.emit_event(ResilienceEvent.RETRY_EXHAUSTED, {
                            'function': func.__name__,
                            'total_attempts': attempt + 1
                        })
                    
                    # Try fallback if available
                    if self.fallback:
                        try:
                            monitor.emit_event(ResilienceEvent.FALLBACK_EXECUTED, {
                                'function': func.__name__,
                                'original_error': str(e)
                            })
                            return self.fallback(*args, **kwargs)
                        except Exception as fallback_error:
                            logger.error(f"Fallback also failed: {fallback_error}")
                    
                    raise last_exception


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


# Convenience decorator factory
def resilient(
    circuit_breaker: Optional[CircuitBreakerConfig] = None,
    retry: Optional[RetryConfig] = None,
    bulkhead: Optional[BulkheadConfig] = None,
    fallback: Optional[Callable] = None,
    name: Optional[str] = None
) -> Callable:
    """
    Decorator factory for applying resilience patterns.
    
    Example:
        @resilient(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=3),
            retry=RetryConfig(max_retries=2),
            bulkhead=BulkheadConfig(max_concurrent_calls=5)
        )
        def call_external_service():
            ...
    """
    def decorator(func: Callable) -> Callable:
        resilience = ResilientDecorator(
            circuit_breaker=circuit_breaker,
            retry=retry,
            bulkhead=bulkhead,
            fallback=fallback,
            name=name or func.__name__
        )
        return resilience(func)
    
    return decorator


# Example usage with existing codebase
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Example monitoring callback
    def log_resilience_event(event: ResilienceEvent, metadata: Dict):
        print(f"[RESILIENCE] {event.value}: {metadata}")
    
    monitor.register_callback(ResilienceEvent.CIRCUIT_OPENED, log_resilience_event)
    monitor.register_callback(ResilienceEvent.RETRY_ATTEMPTED, log_resilience_event)
    
    # Example service with resilience
    @resilient(
        circuit_breaker=CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=5.0
        ),
        retry=RetryConfig(
            max_retries=2,
            base_delay=0.5
        ),
        bulkhead=BulkheadConfig(
            max_concurrent_calls=3
        ),
        fallback=lambda: "Fallback response"
    )
    def unreliable_service():
        """Simulates an unreliable external service."""
        if random.random() < 0.7:  # 70% failure rate
            raise ConnectionError("Service unavailable")
        return "Success!"
    
    # Test the resilient service
    for i in range(10):
        try:
            result = unreliable_service()
            print(f"Call {i}: {result}")
        except Exception as e:
            print(f"Call {i} failed: {e}")
        time.sleep(0.1)


# Integration with existing System Design Primer examples
def integrate_with_existing_examples():
    """
    Example of how to integrate resilience patterns with existing codebase.
    
    This shows how to wrap existing service calls from the system-design-primer-ultra
    with resilience patterns.
    """
    # Example 1: Wrap call center service
    try:
        from solutions.object_oriented_design.call_center.call_center import CallCenter
        
        # Create resilient version of call routing
        @resilient(
            circuit_breaker=CircuitBreakerConfig(failure_threshold=3),
            retry=RetryConfig(max_retries=2),
            name="call_center_routing"
        )
        def route_call_resilient(call_center: CallCenter, call):
            """Resilient version of call routing."""
            return call_center.route_call(call)
        
        # Example 2: Wrap LRU cache operations
        from solutions.object_oriented_design.lru_cache.lru_cache import LRUCache
        
        @resilient(
            retry=RetryConfig(max_retries=1),
            bulkhead=BulkheadConfig(max_concurrent_calls=10),
            name="lru_cache_get"
        )
        def lru_cache_get_resilient(cache: LRUCache, key):
            """Resilient version of LRU cache get."""
            return cache.get(key)
        
        logger.info("Resilience patterns integrated with existing examples")
        
    except ImportError as e:
        logger.warning(f"Could not import existing examples: {e}")