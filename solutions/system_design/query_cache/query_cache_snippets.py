# -*- coding: utf-8 -*-
import time
import threading
import logging
from enum import Enum
from typing import Callable, Any, Optional, Dict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from collections import defaultdict

# Configure logging for resilience events
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """Circuit breaker pattern implementation."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 30, 
                 expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.success_count = 0
        self._lock = threading.RLock()
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    logger.info(f"Circuit breaker transitioning to HALF_OPEN for {func.__name__}")
                else:
                    raise CircuitBreakerOpenError(f"Circuit breaker is OPEN for {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        return (self.last_failure_time is not None and 
                time.time() - self.last_failure_time >= self.recovery_timeout)
    
    def _on_success(self) -> None:
        with self._lock:
            self.failure_count = 0
            self.success_count += 1
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                logger.info("Circuit breaker reset to CLOSED")
    
    def _on_failure(self) -> None:
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            self.success_count = 0
            
            if (self.state == CircuitState.CLOSED and 
                self.failure_count >= self.failure_threshold):
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker OPENED after {self.failure_count} failures")
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                logger.warning("Circuit breaker returned to OPEN from HALF_OPEN")


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class Bulkhead:
    """Bulkhead pattern implementation using thread pools."""
    
    def __init__(self, max_concurrent: int = 10, max_waiting: int = 50):
        self.max_concurrent = max_concurrent
        self.max_waiting = max_waiting
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._semaphore = threading.Semaphore(max_waiting)
        self._active_count = 0
        self._lock = threading.RLock()
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        if not self._semaphore.acquire(blocking=False):
            raise BulkheadFullError(f"Bulkhead is full (max waiting: {self.max_waiting})")
        
        try:
            future = self.executor.submit(func, *args, **kwargs)
            return future.result()
        finally:
            self._semaphore.release()
    
    def shutdown(self) -> None:
        self.executor.shutdown(wait=True)


class BulkheadFullError(Exception):
    """Raised when bulkhead is at capacity."""
    pass


class RetryPolicy:
    """Retry policy with exponential backoff."""
    
    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0, 
                 max_delay: float = 60.0, exponential_base: float = 2.0):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        last_exception = None
        
        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self.max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay:.2f}s: {str(e)}")
                    time.sleep(delay)
                else:
                    logger.error(f"All {self.max_attempts} attempts failed: {str(e)}")
        
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        delay = self.base_delay * (self.exponential_base ** attempt)
        return min(delay, self.max_delay)


class GracefulDegradation:
    """Graceful degradation pattern with fallback functions."""
    
    def __init__(self, fallback_func: Optional[Callable] = None, 
                 default_value: Any = None):
        self.fallback_func = fallback_func
        self.default_value = default_value
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Primary function failed, using fallback: {str(e)}")
            if self.fallback_func:
                try:
                    return self.fallback_func(*args, **kwargs)
                except Exception as fallback_error:
                    logger.error(f"Fallback also failed: {str(fallback_error)}")
                    if self.default_value is not None:
                        return self.default_value
                    raise
            elif self.default_value is not None:
                return self.default_value
            raise


class ResilienceLayer:
    """Complete resilience layer combining all patterns."""
    
    def __init__(self, 
                 circuit_breaker: Optional[CircuitBreaker] = None,
                 bulkhead: Optional[Bulkhead] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 graceful_degradation: Optional[GracefulDegradation] = None):
        self.circuit_breaker = circuit_breaker
        self.bulkhead = bulkhead
        self.retry_policy = retry_policy
        self.graceful_degradation = graceful_degradation
        self._metrics = defaultdict(int)
        self._metrics_lock = threading.RLock()
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        self._record_metric('total_calls')
        
        # Build the call chain from innermost to outermost
        call_chain = func
        
        # Apply graceful degradation (innermost)
        if self.graceful_degradation:
            call_chain = self.graceful_degradation.call(call_chain)
        
        # Apply retry policy
        if self.retry_policy:
            call_chain = self.retry_policy.call(call_chain)
        
        # Apply bulkhead
        if self.bulkhead:
            call_chain = self.bulkhead.call(call_chain)
        
        # Apply circuit breaker (outermost)
        if self.circuit_breaker:
            call_chain = self.circuit_breaker.call(call_chain)
        
        try:
            result = call_chain(*args, **kwargs)
            self._record_metric('successful_calls')
            return result
        except CircuitBreakerOpenError:
            self._record_metric('circuit_breaker_rejections')
            raise
        except BulkheadFullError:
            self._record_metric('bulkhead_rejections')
            raise
        except Exception as e:
            self._record_metric('failed_calls')
            raise
    
    def _record_metric(self, metric_name: str) -> None:
        with self._metrics_lock:
            self._metrics[metric_name] += 1
    
    def get_metrics(self) -> Dict[str, int]:
        with self._metrics_lock:
            return dict(self._metrics)
    
    def reset_metrics(self) -> None:
        with self._metrics_lock:
            self._metrics.clear()


class QueryApi(object):

    def __init__(self, memory_cache, reverse_index_cluster):
        self.memory_cache = memory_cache
        self.reverse_index_cluster = reverse_index_cluster
        
        # Initialize resilience layer for the reverse index cluster
        self._resilience_layer = ResilienceLayer(
            circuit_breaker=CircuitBreaker(
                failure_threshold=5,
                recovery_timeout=30,
                expected_exception=Exception
            ),
            bulkhead=Bulkhead(
                max_concurrent=20,
                max_waiting=100
            ),
            retry_policy=RetryPolicy(
                max_attempts=3,
                base_delay=1.0,
                max_delay=10.0,
                exponential_base=2.0
            ),
            graceful_degradation=GracefulDegradation(
                fallback_func=self._fallback_search,
                default_value=[]
            )
        )
        
        # Wrap the cluster's process_search method with resilience
        self._resilient_process_search = self._resilience_layer(
            self.reverse_index_cluster.process_search
        )

    def _fallback_search(self, query):
        """Fallback search method when primary service fails."""
        logger.info(f"Using fallback search for query: {query}")
        # Return empty results or cached results if available
        return []

    def parse_query(self, query):
        """Remove markup, break text into terms, deal with typos,
        normalize capitalization, convert to use boolean operations.
        """
        # Implementation would go here
        return query.lower().strip()

    def process_query(self, query):
        query = self.parse_query(query)
        results = self.memory_cache.get(query)
        if results is None:
            try:
                results = self._resilient_process_search(query)
                self.memory_cache.set(query, results)
            except Exception as e:
                logger.error(f"Failed to process query '{query}': {str(e)}")
                # Return empty results on complete failure
                results = []
        return results


class Node(object):

    def __init__(self, query, results):
        self.query = query
        self.results = results


class LinkedList(object):

    def __init__(self):
        self.head = None
        self.tail = None

    def move_to_front(self, node):
        # Implementation would move node to front of list
        pass

    def append_to_front(self, node):
        # Implementation would append node to front
        pass

    def remove_from_tail(self):
        # Implementation would remove tail node
        pass


class Cache(object):

    def __init__(self, MAX_SIZE):
        self.MAX_SIZE = MAX_SIZE
        self.size = 0
        self.lookup = {}
        self.linked_list = LinkedList()

    def get(self, query):
        """Get the stored query result from the cache.

        Accessing a node updates its position to the front of the LRU list.
        """
        node = self.lookup.get(query)
        if node is None:
            return None
        self.linked_list.move_to_front(node)
        return node.results

    def set(self, query, results):
        """Set the result for the given query key in the cache.

        When updating an entry, updates its position to the front of the LRU list.
        If the entry is new and the cache is at capacity, removes the oldest entry
        before the new entry is added.
        """
        node = self.lookup.get(query)
        if node is not None:
            # Key exists in cache, update the value
            node.results = results
            self.linked_list.move_to_front(node)
        else:
            # Key does not exist in cache
            if self.size == self.MAX_SIZE:
                # Remove the oldest entry from the linked list and lookup
                if self.linked_list.tail:
                    self.lookup.pop(self.linked_list.tail.query, None)
                    self.linked_list.remove_from_tail()
            else:
                self.size += 1
            # Add the new key and value
            new_node = Node(query, results)
            self.linked_list.append_to_front(new_node)
            self.lookup[query] = new_node