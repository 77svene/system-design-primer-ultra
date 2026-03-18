from abc import ABCMeta, abstractmethod
from collections import deque
from enum import Enum
import time
import threading
import logging
from functools import wraps
from typing import Callable, Any, Optional
from concurrent.futures import ThreadPoolExecutor


# Resilience Layer Implementation
class CircuitState(Enum):
    CLOSED = 0
    OPEN = 1
    HALF_OPEN = 2


class CircuitBreaker:
    """Circuit breaker pattern implementation with state machine."""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0, 
                 half_open_max_calls: int = 1):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.half_open_calls = 0
        self._lock = threading.RLock()
        
        # Monitoring hooks
        self.on_state_change = None
        self.on_failure = None
        self.on_success = None
        
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
                    self.half_open_calls = 0
                    if self.on_state_change:
                        self.on_state_change(CircuitState.OPEN, CircuitState.HALF_OPEN)
                else:
                    raise CircuitOpenError(f"Circuit is OPEN for {func.__name__}")
            
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.half_open_max_calls:
                    raise CircuitOpenError(f"Circuit is HALF_OPEN but max calls reached for {func.__name__}")
                self.half_open_calls += 1
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _should_attempt_reset(self) -> bool:
        return (self.last_failure_time and 
                time.time() - self.last_failure_time >= self.reset_timeout)
    
    def _on_success(self):
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.CLOSED
                if self.on_state_change:
                    self.on_state_change(CircuitState.HALF_OPEN, CircuitState.CLOSED)
            self.failure_count = 0
            self.half_open_calls = 0
            if self.on_success:
                self.on_success()
    
    def _on_failure(self):
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.on_failure:
                self.on_failure(self.failure_count)
            
            if (self.state == CircuitState.CLOSED and 
                self.failure_count >= self.failure_threshold):
                self.state = CircuitState.OPEN
                if self.on_state_change:
                    self.on_state_change(CircuitState.CLOSED, CircuitState.OPEN)
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                if self.on_state_change:
                    self.on_state_change(CircuitState.HALF_OPEN, CircuitState.OPEN)


class RetryPolicy:
    """Retry policy with exponential backoff and jitter."""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0,
                 max_delay: float = 30.0, exponential_base: float = 2.0,
                 jitter: bool = True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        
        # Monitoring hooks
        self.on_retry = None
        self.on_failure = None
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(func, *args, **kwargs)
        return wrapper
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                if attempt == self.max_retries:
                    if self.on_failure:
                        self.on_failure(attempt + 1, e)
                    raise
                
                delay = self._calculate_delay(attempt)
                if self.on_retry:
                    self.on_retry(attempt + 1, delay, e)
                
                time.sleep(delay)
        
        raise last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )
        
        if self.jitter:
            import random
            delay = random.uniform(0, delay)
        
        return delay


class Bulkhead:
    """Bulkhead pattern for thread pool isolation."""
    
    def __init__(self, max_concurrent: int = 10, max_waiting: int = 100):
        self.max_concurrent = max_concurrent
        self.max_waiting = max_waiting
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._semaphore = threading.Semaphore(max_waiting)
        
        # Monitoring hooks
        self.on_rejected = None
        self.on_complete = None
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(func, *args, **kwargs)
        return wrapper
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        if not self._semaphore.acquire(blocking=False):
            if self.on_rejected:
                self.on_rejected(func.__name__)
            raise BulkheadFullError(f"Bulkhead full for {func.__name__}")
        
        try:
            future = self._executor.submit(func, *args, **kwargs)
            result = future.result()
            if self.on_complete:
                self.on_complete(func.__name__)
            return result
        finally:
            self._semaphore.release()
    
    def shutdown(self):
        self._executor.shutdown(wait=True)


class ResilientService:
    """Composite resilience decorator combining all patterns."""
    
    def __init__(self, 
                 circuit_breaker: Optional[CircuitBreaker] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 bulkhead: Optional[Bulkhead] = None,
                 fallback: Optional[Callable] = None):
        self.circuit_breaker = circuit_breaker
        self.retry_policy = retry_policy
        self.bulkhead = bulkhead
        self.fallback = fallback
        
        # Set up monitoring chain
        self._setup_monitoring()
    
    def _setup_monitoring(self):
        """Chain monitoring events from all components."""
        if self.circuit_breaker:
            self.circuit_breaker.on_state_change = self._on_circuit_state_change
            self.circuit_breaker.on_failure = self._on_circuit_failure
            self.circuit_breaker.on_success = self._on_circuit_success
        
        if self.retry_policy:
            self.retry_policy.on_retry = self._on_retry
            self.retry_policy.on_failure = self._on_retry_failure
        
        if self.bulkhead:
            self.bulkhead.on_rejected = self._on_bulkhead_rejected
            self.bulkhead.on_complete = self._on_bulkhead_complete
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute(func, *args, **kwargs)
        return wrapper
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        try:
            # Apply bulkhead first to limit concurrency
            if self.bulkhead:
                return self.bulkhead.execute(
                    self._wrap_with_circuit_and_retry(func),
                    *args, **kwargs
                )
            else:
                return self._wrap_with_circuit_and_retry(func)(*args, **kwargs)
        except (CircuitOpenError, BulkheadFullError) as e:
            if self.fallback:
                logging.warning(f"Using fallback for {func.__name__}: {e}")
                return self.fallback(*args, **kwargs)
            raise
    
    def _wrap_with_circuit_and_retry(self, func: Callable) -> Callable:
        """Apply circuit breaker and retry policy in correct order."""
        wrapped = func
        
        # Apply retry policy first (innermost)
        if self.retry_policy:
            wrapped = self.retry_policy(wrapped)
        
        # Then apply circuit breaker
        if self.circuit_breaker:
            wrapped = self.circuit_breaker(wrapped)
        
        return wrapped
    
    # Monitoring callbacks
    def _on_circuit_state_change(self, old_state: CircuitState, new_state: CircuitState):
        logging.info(f"Circuit state changed: {old_state} -> {new_state}")
    
    def _on_circuit_failure(self, failure_count: int):
        logging.warning(f"Circuit failure count: {failure_count}")
    
    def _on_circuit_success(self):
        logging.info("Circuit call succeeded")
    
    def _on_retry(self, attempt: int, delay: float, exception: Exception):
        logging.info(f"Retry attempt {attempt} after {delay:.2f}s: {exception}")
    
    def _on_retry_failure(self, attempts: int, exception: Exception):
        logging.error(f"Retry failed after {attempts} attempts: {exception}")
    
    def _on_bulkhead_rejected(self, func_name: str):
        logging.warning(f"Bulkhead rejected call to {func_name}")
    
    def _on_bulkhead_complete(self, func_name: str):
        logging.debug(f"Bulkhead completed call to {func_name}")


# Custom exceptions for resilience patterns
class CircuitOpenError(Exception):
    pass

class BulkheadFullError(Exception):
    pass


# Original Call Center Code with Resilience Layer
class Rank(Enum):
    OPERATOR = 0
    SUPERVISOR = 1
    DIRECTOR = 2


class Employee(metaclass=ABCMeta):

    def __init__(self, employee_id, name, rank, call_center):
        self.employee_id = employee_id
        self.name = name
        self.rank = rank
        self.call = None
        self.call_center = call_center
        
        # Resilience decorators for employee operations
        self._take_call_resilient = ResilientService(
            circuit_breaker=CircuitBreaker(failure_threshold=3, reset_timeout=60),
            retry_policy=RetryPolicy(max_retries=2, base_delay=0.5),
            fallback=self._take_call_fallback
        )(self._take_call_impl)
        
        self._complete_call_resilient = ResilientService(
            circuit_breaker=CircuitBreaker(failure_threshold=5, reset_timeout=30),
            retry_policy=RetryPolicy(max_retries=1, base_delay=1.0)
        )(self._complete_call_impl)

    def take_call(self, call):
        """Resilient call taking with circuit breaker and retry."""
        return self._take_call_resilient(call)
    
    def _take_call_impl(self, call):
        """Original take_call implementation."""
        self.call = call
        self.call.employee = self
        self.call.state = CallState.IN_PROGRESS
    
    def _take_call_fallback(self, call):
        """Fallback when take_call fails - mark call for manual handling."""
        logging.warning(f"Fallback: Employee {self.name} cannot take call, marking for manual handling")
        call.state = CallState.READY  # Return to queue
        return None

    def complete_call(self):
        """Resilient call completion."""
        return self._complete_call_resilient()
    
    def _complete_call_impl(self):
        """Original complete_call implementation."""
        self.call.state = CallState.COMPLETE
        self.call_center.notify_call_completed(self.call)

    @abstractmethod
    def escalate_call(self):
        pass

    def _escalate_call(self):
        self.call.state = CallState.READY
        call = self.call
        self.call = None
        self.call_center.notify_call_escalated(call)


class Operator(Employee):

    def __init__(self, employee_id, name, call_center):
        super().__init__(employee_id, name, Rank.OPERATOR, call_center)
        
        # Resilience for escalate_call
        self._escalate_resilient = ResilientService(
            circuit_breaker=CircuitBreaker(failure_threshold=2, reset_timeout=120),
            retry_policy=RetryPolicy(max_retries=1, base_delay=2.0),
            fallback=self._escalate_fallback
        )(self._escalate_call_impl)

    def escalate_call(self):
        return self._escalate_resilient()
    
    def _escalate_call_impl(self):
        self.call.level = Rank.SUPERVISOR
        self._escalate_call()
    
    def _escalate_fallback(self):
        """Fallback: If escalation fails, keep call with operator."""
        logging.warning(f"Fallback: Operator {self.name} cannot escalate, keeping call")
        # Don't escalate, just complete the call at current level
        self.complete_call()


class Supervisor(Employee):

    def __init__(self, employee_id, name, call_center):
        super().__init__(employee_id, name, Rank.SUPERVISOR, call_center)
        
        self._escalate_resilient = ResilientService(
            circuit_breaker=CircuitBreaker(failure_threshold=2, reset_timeout=120),
            retry_policy=RetryPolicy(max_retries=1, base_delay=2.0),
            fallback=self._escalate_fallback
        )(self._escalate_call_impl)

    def escalate_call(self):
        return self._escalate_resilient()
    
    def _escalate_call_impl(self):
        self.call.level = Rank.DIRECTOR
        self._escalate_call()
    
    def _escalate_fallback(self):
        logging.warning(f"Fallback: Supervisor {self.name} cannot escalate, keeping call")
        self.complete_call()


class Director(Employee):

    def __init__(self, employee_id, name, call_center):
        super().__init__(employee_id, name, Rank.DIRECTOR, call_center)

    def escalate_call(self):
        raise NotImplementedError('Directors must be able to handle any call')


class CallState(Enum):
    READY = 0
    IN_PROGRESS = 1
    COMPLETE = 2


class Call(object):

    def __init__(self, rank):
        self.state = CallState.READY
        self.rank = rank
        self.employee = None


class CallCenter(object):

    def __init__(self, operators, supervisors, directors):
        self.operators = operators
        self.supervisors = supervisors
        self.directors = directors
        self.queued_calls = deque()
        
        # Resilience for dispatch operations
        self._dispatch_resilient = ResilientService(
            circuit_breaker=CircuitBreaker(failure_threshold=10, reset_timeout=60),
            retry_policy=RetryPolicy(max_retries=2, base_delay=1.0),
            bulkhead=Bulkhead(max_concurrent=50, max_waiting=200),
            fallback=self._dispatch_fallback
        )(self._dispatch_call_impl)
        
        # Bulkhead for queued call processing
        self._queue_processor = Bulkhead(max_concurrent=10)
        
        # Start queue processor thread
        self._queue_thread = threading.Thread(target=self._process_queue, daemon=True)
        self._queue_thread.start()
    
    def dispatch_call(self, call):
        """Resilient call dispatch with bulkhead isolation."""
        return self._dispatch_resilient(call)
    
    def _dispatch_call_impl(self, call):
        """Original dispatch implementation."""
        if call.rank not in (Rank.OPERATOR, Rank.SUPERVISOR, Rank.DIRECTOR):
            raise ValueError('Invalid call rank: {}'.format(call.rank))
        
        employee = None
        if call.rank == Rank.OPERATOR:
            employee = self._dispatch_to_employee(call, self.operators)
        if call.rank == Rank.SUPERVISOR or employee is None:
            employee = self._dispatch_to_employee(call, self.supervisors)
        if call.rank == Rank.DIRECTOR or employee is None:
            employee = self._dispatch_to_employee(call, self.directors)
        
        if employee is None:
            self.queued_calls.append(call)
        
        return employee
    
    def _dispatch_to_employee(self, call, employees):
        """Dispatch to specific employee group with resilience."""
        for employee in employees:
            if employee.call is None:
                try:
                    employee.take_call(call)
                    return employee
                except (CircuitOpenError, BulkheadFullError):
                    continue  # Try next employee
        return None
    
    def _dispatch_fallback(self, call):
        """Fallback when dispatch fails - queue with high priority."""
        logging.warning(f"Fallback: Queuing call {call.rank} with high priority")
        self.queued_calls.appendleft(call)  # Add to front of queue
        return None
    
    def _process_queue(self):
        """Background thread to process queued calls with resilience."""
        while True:
            try:
                if self.queued_calls:
                    call = self.queued_calls[0]  # Peek without removing
                    
                    # Try to dispatch with bulkhead
                    self._queue_processor.execute(self._try_dispatch_queued, call)
                
                time.sleep(0.1)  # Small delay to prevent busy waiting
            except Exception as e:
                logging.error(f"Queue processor error: {e}")
                time.sleep(1)  # Back off on error
    
    def _try_dispatch_queued(self, call):
        """Attempt to dispatch a queued call."""
        # Try to find any available employee
        for employee_list in [self.operators, self.supervisors, self.directors]:
            for employee in employee_list:
                if employee.call is None:
                    try:
                        employee.take_call(call)
                        self.queued_calls.popleft()  # Remove from queue
                        logging.info(f"Dispatched queued call to {employee.name}")
                        return
                    except (CircuitOpenError, BulkheadFullError):
                        continue
    
    def notify_call_escalated(self, call):
        logging.info(f"Call escalated to {call.level}")
        # Re-dispatch the escalated call
        self.dispatch_call(call)
    
    def notify_call_completed(self, call):
        logging.info(f"Call completed by {call.employee.name if call.employee else 'unknown'}")
        # Process next queued call if any
        if self.queued_calls:
            self._try_dispatch_queued(self.queued_calls[0])
    
    def shutdown(self):
        """Graceful shutdown of resilience components."""
        self._queue_processor.shutdown()