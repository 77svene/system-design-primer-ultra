# -*- coding: utf-8 -*-

import time
import logging
import threading
import random
from enum import Enum
from functools import wraps
from typing import Callable, Any, Optional, Dict
from concurrent.futures import ThreadPoolExecutor, Future
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DefaultCategories(Enum):
    HOUSING = 0
    FOOD = 1
    GAS = 2
    SHOPPING = 3


seller_category_map = {
    'Exxon': DefaultCategories.GAS,
    'Target': DefaultCategories.SHOPPING
}


# Resilience Layer Components
class CircuitState(Enum):
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing, reject requests
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker state machine to prevent cascading failures"""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self._lock = threading.RLock()
    
    def can_execute(self) -> bool:
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time >= self.reset_timeout:
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                    return True
                return False
            else:  # HALF_OPEN
                return True
    
    def record_success(self):
        with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                logger.info("Circuit breaker closed after successful test")
    
    def record_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitState.CLOSED and self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning("Circuit breaker reopened after failed test")


class Bulkhead:
    """Isolate failures using thread pool isolation"""
    
    def __init__(self, max_workers: int = 10, queue_size: int = 100):
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="bulkhead_worker"
        )
        self.semaphore = threading.Semaphore(queue_size)
        self.active_count = 0
        self.rejected_count = 0
        self._lock = threading.RLock()
    
    def execute(self, func: Callable, *args, **kwargs) -> Future:
        """Execute function in isolated thread pool"""
        if not self.semaphore.acquire(blocking=False):
            with self._lock:
                self.rejected_count += 1
            raise RuntimeError("Bulkhead queue full - request rejected")
        
        def wrapped():
            try:
                with self._lock:
                    self.active_count += 1
                return func(*args, **kwargs)
            finally:
                with self._lock:
                    self.active_count -= 1
                self.semaphore.release()
        
        return self.executor.submit(wrapped)
    
    def get_stats(self) -> Dict[str, int]:
        with self._lock:
            return {
                "active": self.active_count,
                "rejected": self.rejected_count,
                "available_queue": self.semaphore._value
            }


class RetryPolicy:
    """Configurable retry with exponential backoff and jitter"""
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
    
    def get_delay(self, attempt: int) -> float:
        """Calculate delay with exponential backoff and optional jitter"""
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        
        if self.jitter:
            # Add random jitter (±25%)
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)


class ResilienceMetrics:
    """Monitor resilience layer performance"""
    
    def __init__(self):
        self.circuit_breaker_trips = defaultdict(int)
        self.retry_attempts = defaultdict(int)
        self.bulkhead_rejections = defaultdict(int)
        self.fallback_invocations = defaultdict(int)
        self.successful_calls = defaultdict(int)
        self.failed_calls = defaultdict(int)
        self._lock = threading.RLock()
    
    def record_circuit_trip(self, service_name: str):
        with self._lock:
            self.circuit_breaker_trips[service_name] += 1
    
    def record_retry(self, service_name: str, attempt: int):
        with self._lock:
            self.retry_attempts[f"{service_name}_attempt_{attempt}"] += 1
    
    def record_bulkhead_rejection(self, service_name: str):
        with self._lock:
            self.bulkhead_rejections[service_name] += 1
    
    def record_fallback(self, service_name: str):
        with self._lock:
            self.fallback_invocations[service_name] += 1
    
    def record_success(self, service_name: str):
        with self._lock:
            self.successful_calls[service_name] += 1
    
    def record_failure(self, service_name: str):
        with self._lock:
            self.failed_calls[service_name] += 1
    
    def get_summary(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "circuit_breaker_trips": dict(self.circuit_breaker_trips),
                "retry_attempts": dict(self.retry_attempts),
                "bulkhead_rejections": dict(self.bulkhead_rejections),
                "fallback_invocations": dict(self.fallback_invocations),
                "successful_calls": dict(self.successful_calls),
                "failed_calls": dict(self.failed_calls)
            }


# Global resilience metrics
resilience_metrics = ResilienceMetrics()


def resilient_service(
    service_name: str,
    circuit_breaker: Optional[CircuitBreaker] = None,
    bulkhead: Optional[Bulkhead] = None,
    retry_policy: Optional[RetryPolicy] = None,
    fallback: Optional[Callable] = None
):
    """
    Decorator to add resilience patterns to any service call
    
    Args:
        service_name: Name for metrics and logging
        circuit_breaker: Optional circuit breaker instance
        bulkhead: Optional bulkhead for thread isolation
        retry_policy: Optional retry configuration
        fallback: Optional fallback function when all retries fail
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Use provided instances or create defaults
            cb = circuit_breaker or CircuitBreaker()
            bh = bulkhead or Bulkhead(max_workers=5)
            rp = retry_policy or RetryPolicy(max_retries=2)
            
            def execute_with_resilience():
                """Core execution logic with all resilience patterns"""
                
                # Check circuit breaker
                if not cb.can_execute():
                    resilience_metrics.record_circuit_trip(service_name)
                    logger.warning(f"{service_name}: Circuit breaker OPEN, rejecting request")
                    if fallback:
                        resilience_metrics.record_fallback(service_name)
                        return fallback(*args, **kwargs)
                    raise RuntimeError(f"{service_name}: Service unavailable (circuit open)")
                
                # Execute with retry logic
                last_exception = None
                for attempt in range(rp.max_retries + 1):
                    try:
                        # Execute in bulkhead
                        future = bh.execute(func, *args, **kwargs)
                        result = future.result(timeout=30)  # 30 second timeout
                        
                        # Record success
                        cb.record_success()
                        resilience_metrics.record_success(service_name)
                        return result
                    
                    except Exception as e:
                        last_exception = e
                        resilience_metrics.record_failure(service_name)
                        
                        if attempt < rp.max_retries:
                            # Record retry
                            resilience_metrics.record_retry(service_name, attempt + 1)
                            delay = rp.get_delay(attempt)
                            logger.warning(
                                f"{service_name}: Attempt {attempt + 1} failed, "
                                f"retrying in {delay:.2f}s: {str(e)}"
                            )
                            time.sleep(delay)
                        else:
                            # Final failure
                            cb.record_failure()
                            logger.error(f"{service_name}: All {rp.max_retries + 1} attempts failed")
                
                # All retries exhausted
                if fallback:
                    resilience_metrics.record_fallback(service_name)
                    logger.info(f"{service_name}: Using fallback after all retries failed")
                    return fallback(*args, **kwargs)
                
                raise last_exception
            
            try:
                return execute_with_resilience()
            except Exception as e:
                # Handle bulkhead rejection
                if "Bulkhead queue full" in str(e):
                    resilience_metrics.record_bulkhead_rejection(service_name)
                    logger.error(f"{service_name}: Bulkhead rejected request")
                    if fallback:
                        resilience_metrics.record_fallback(service_name)
                        return fallback(*args, **kwargs)
                raise
        
        return wrapper
    return decorator


class Categorizer(object):
    def __init__(self, seller_category_map, seller_category_overrides_map):
        self.seller_category_map = seller_category_map
        self.seller_category_overrides_map = seller_category_overrides_map
        self._resilience_circuit_breaker = CircuitBreaker(failure_threshold=3, reset_timeout=30)
        self._resilience_bulkhead = Bulkhead(max_workers=5, queue_size=20)
    
    @resilience_service(
        service_name="categorize_transaction",
        retry_policy=RetryPolicy(max_retries=2, base_delay=0.5),
        fallback=lambda self, transaction: DefaultCategories.SHOPPING  # Default fallback category
    )
    def categorize(self, transaction):
        """Categorize a transaction with resilience patterns"""
        # Simulate potential external service call that might fail
        # In a real implementation, this might call an ML model or external API
        
        if transaction.seller in self.seller_category_map:
            return self.seller_category_map[transaction.seller]
        
        if transaction.seller in self.seller_category_overrides_map:
            # Simulate occasional failures for demonstration
            if random.random() < 0.1:  # 10% failure rate
                raise RuntimeError(f"External categorization service failed for {transaction.seller}")
            
            category = self.seller_category_overrides_map[transaction.seller].peek_min()
            self.seller_category_map[transaction.seller] = category
            return category
        
        return None


class Transaction(object):
    def __init__(self, timestamp, seller, amount):
        self.timestamp = timestamp
        self.seller = seller
        self.amount = amount


class Budget(object):
    def __init__(self, template_categories_to_budget_map):
        self.categories_to_budget_map = template_categories_to_budget_map
    
    @resilience_service(
        service_name="override_budget",
        retry_policy=RetryPolicy(max_retries=1, base_delay=0.1)
    )
    def override_category_budget(self, category, amount):
        """Override budget with resilience patterns"""
        # Simulate validation that might fail
        if amount < 0:
            raise ValueError("Budget amount cannot be negative")
        
        self.categories_to_budget_map[category] = amount
        return True


# Example usage and monitoring
def get_resilience_metrics() -> Dict[str, Any]:
    """Get current resilience metrics for monitoring"""
    return resilience_metrics.get_summary()


def reset_resilience_metrics():
    """Reset all resilience metrics (useful for testing)"""
    global resilience_metrics
    resilience_metrics = ResilienceMetrics()