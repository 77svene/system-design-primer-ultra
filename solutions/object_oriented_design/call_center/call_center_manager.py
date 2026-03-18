"""Thread-safe call center manager with priority escalation and load balancing.

This module implements a production-ready call center system featuring:
- Thread-safe call routing with priority queues
- Automatic escalation through employee ranks
- Load balancing across available employees
- Circuit breakers for failed escalations
- Real-time metrics collection
- Graceful shutdown handling

Integrates with existing call_center.py classes (Call, Employee, etc.)
"""

import threading
import time
import heapq
import random
from enum import IntEnum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import contextmanager
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CallPriority(IntEnum):
    """Priority levels for incoming calls."""
    LOW = 3
    MEDIUM = 2
    HIGH = 1
    CRITICAL = 0


class EmployeeRank(IntEnum):
    """Employee hierarchy levels."""
    RESPONDENT = 0
    MANAGER = 1
    DIRECTOR = 2


class CircuitState(IntEnum):
    """Circuit breaker states."""
    CLOSED = 0  # Normal operation
    OPEN = 1    # Failing, reject calls
    HALF_OPEN = 2  # Testing if service recovered


@dataclass(order=True)
class QueuedCall:
    """Wrapper for calls in priority queue with escalation tracking."""
    priority: int
    timestamp: float = field(compare=False)
    call: object = field(compare=False)
    escalation_level: int = field(default=0, compare=False)
    attempts: int = field(default=0, compare=False)
    
    def __post_init__(self):
        # Ensure priority is negative for max-heap behavior
        if self.priority >= 0:
            self.priority = -self.priority


@dataclass
class CallMetrics:
    """Metrics for call center performance tracking."""
    total_calls: int = 0
    completed_calls: int = 0
    abandoned_calls: int = 0
    escalated_calls: int = 0
    avg_wait_time: float = 0.0
    avg_handle_time: float = 0.0
    peak_queue_size: int = 0
    current_queue_size: int = 0
    calls_by_priority: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    calls_by_rank: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    def update_wait_time(self, wait_time: float):
        """Update average wait time with exponential moving average."""
        if self.completed_calls == 0:
            self.avg_wait_time = wait_time
        else:
            # Exponential moving average with alpha=0.1
            self.avg_wait_time = 0.1 * wait_time + 0.9 * self.avg_wait_time


class CircuitBreaker:
    """Circuit breaker pattern for handling escalation failures."""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure_time = 0
        self._lock = threading.RLock()
    
    def record_failure(self):
        """Record a failure and potentially open the circuit."""
        with self._lock:
            self.failures += 1
            self.last_failure_time = time.time()
            
            if self.failures >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(f"Circuit breaker opened after {self.failures} failures")
    
    def record_success(self):
        """Record a success and reset the circuit."""
        with self._lock:
            self.failures = 0
            self.state = CircuitState.CLOSED
    
    def can_execute(self) -> bool:
        """Check if operation can be executed based on circuit state."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                # Check if reset timeout has passed
                if time.time() - self.last_failure_time > self.reset_timeout:
                    self.state = CircuitState.HALF_OPEN
                    return True
                return False
            else:  # HALF_OPEN
                return True


class CallCenterManager:
    """Thread-safe call center manager with advanced routing and load balancing.
    
    Features:
    - Priority-based call routing with automatic escalation
    - Load balancing across employee ranks
    - Circuit breakers for failed escalations
    - Real-time metrics and monitoring
    - Graceful shutdown with call completion
    """
    
    def __init__(self, max_workers: int = 10):
        """Initialize the call center manager.
        
        Args:
            max_workers: Maximum number of concurrent call handling threads
        """
        # Thread-safe data structures
        self._call_queue: List[QueuedCall] = []
        self._queue_lock = threading.RLock()
        self._queue_not_empty = threading.Condition(self._queue_lock)
        
        # Employee management
        self._employees_by_rank: Dict[EmployeeRank, List[object]] = defaultdict(list)
        self._available_employees: Dict[EmployeeRank, deque] = defaultdict(deque)
        self._employee_locks: Dict[EmployeeRank, threading.RLock] = {
            rank: threading.RLock() for rank in EmployeeRank
        }
        self._employee_assignments: Dict[int, Optional[object]] = {}  # employee_id -> call
        
        # Circuit breakers for each escalation level
        self._circuit_breakers: Dict[int, CircuitBreaker] = {
            level: CircuitBreaker() for level in range(3)  # 3 escalation levels
        }
        
        # Thread pool for call handling
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="call_handler"
        )
        self._futures: Dict[int, Future] = {}  # call_id -> future
        
        # Metrics and monitoring
        self._metrics = CallMetrics()
        self._metrics_lock = threading.RLock()
        self._start_time = time.time()
        
        # Control flags
        self._running = True
        self._shutdown_event = threading.Event()
        
        # Start background threads
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_calls,
            name="call_dispatcher",
            daemon=True
        )
        self._dispatcher_thread.start()
        
        logger.info("Call center manager initialized")
    
    def add_employee(self, employee) -> None:
        """Add an employee to the call center.
        
        Args:
            employee: Employee object with id and rank attributes
        """
        rank = EmployeeRank(employee.rank)
        with self._employee_locks[rank]:
            self._employees_by_rank[rank].append(employee)
            self._available_employees[rank].append(employee)
            self._employee_assignments[employee.id] = None
        
        logger.info(f"Added employee {employee.id} with rank {rank.name}")
    
    def remove_employee(self, employee_id: int) -> bool:
        """Remove an employee from the call center.
        
        Args:
            employee_id: ID of the employee to remove
            
        Returns:
            True if employee was removed, False if not found or busy
        """
        for rank in EmployeeRank:
            with self._employee_locks[rank]:
                for i, emp in enumerate(self._employees_by_rank[rank]):
                    if emp.id == employee_id:
                        # Check if employee is currently handling a call
                        if self._employee_assignments.get(employee_id) is not None:
                            logger.warning(f"Cannot remove busy employee {employee_id}")
                            return False
                        
                        # Remove from all collections
                        self._employees_by_rank[rank].pop(i)
                        if emp in self._available_employees[rank]:
                            self._available_employees[rank].remove(emp)
                        del self._employee_assignments[employee_id]
                        
                        logger.info(f"Removed employee {employee_id}")
                        return True
        
        logger.warning(f"Employee {employee_id} not found")
        return False
    
    def add_call(self, call) -> None:
        """Add a new call to the queue.
        
        Args:
            call: Call object with id and priority attributes
        """
        if not self._running:
            logger.warning("Call center is shutting down, rejecting new calls")
            return
        
        with self._queue_lock:
            priority = CallPriority(call.priority)
            queued_call = QueuedCall(
                priority=priority.value,
                timestamp=time.time(),
                call=call
            )
            
            heapq.heappush(self._call_queue, queued_call)
            
            # Update metrics
            with self._metrics_lock:
                self._metrics.total_calls += 1
                self._metrics.current_queue_size = len(self._call_queue)
                self._metrics.peak_queue_size = max(
                    self._metrics.peak_queue_size,
                    self._metrics.current_queue_size
                )
                self._metrics.calls_by_priority[priority.name] += 1
            
            # Notify dispatcher
            self._queue_not_empty.notify()
        
        logger.info(f"Added call {call.id} with priority {priority.name}")
    
    def _dispatch_calls(self) -> None:
        """Background thread that dispatches calls to available employees."""
        logger.info("Call dispatcher started")
        
        while self._running:
            with self._queue_lock:
                # Wait for calls or shutdown
                while not self._call_queue and self._running:
                    self._queue_not_empty.wait(timeout=1.0)
                
                if not self._running:
                    break
                
                # Try to assign calls
                self._assign_calls()
        
        logger.info("Call dispatcher stopped")
    
    def _assign_calls(self) -> None:
        """Assign queued calls to available employees."""
        assigned_calls = []
        
        with self._queue_lock:
            # Process calls in priority order
            temp_queue = []
            while self._call_queue:
                queued_call = heapq.heappop(self._call_queue)
                
                # Try to assign this call
                assigned = self._try_assign_call(queued_call)
                if not assigned:
                    temp_queue.append(queued_call)
                else:
                    assigned_calls.append(queued_call)
            
            # Restore unassigned calls to queue
            for queued_call in temp_queue:
                heapq.heappush(self._call_queue, queued_call)
        
        # Submit assigned calls to thread pool
        for queued_call in assigned_calls:
            self._submit_call_for_processing(queued_call)
    
    def _try_assign_call(self, queued_call: QueuedCall) -> bool:
        """Try to assign a call to an available employee.
        
        Args:
            queued_call: The call to assign
            
        Returns:
            True if call was assigned, False otherwise
        """
        call = queued_call.call
        escalation_level = queued_call.escalation_level
        
        # Check circuit breaker for this escalation level
        if not self._circuit_breakers[escalation_level].can_execute():
            logger.warning(
                f"Circuit breaker open for escalation level {escalation_level}, "
                f"skipping call {call.id}"
            )
            return False
        
        # Determine target rank based on escalation level
        target_rank = self._get_target_rank(escalation_level)
        
        # Try to find an available employee of target rank or higher
        for rank in range(target_rank, len(EmployeeRank)):
            employee = self._get_available_employee(EmployeeRank(rank))
            if employee:
                # Assign call to employee
                self._assign_call_to_employee(queued_call, employee)
                return True
        
        return False
    
    def _get_target_rank(self, escalation_level: int) -> int:
        """Get target employee rank based on escalation level.
        
        Args:
            escalation_level: Current escalation level
            
        Returns:
            Target employee rank
        """
        # Escalation logic: each level moves up one rank
        return min(escalation_level, len(EmployeeRank) - 1)
    
    def _get_available_employee(self, rank: EmployeeRank) -> Optional[object]:
        """Get an available employee of the specified rank.
        
        Args:
            rank: Employee rank to look for
            
        Returns:
            Available employee or None
        """
        with self._employee_locks[rank]:
            if self._available_employees[rank]:
                employee = self._available_employees[rank].popleft()
                return employee
        return None
    
    def _assign_call_to_employee(self, queued_call: QueuedCall, employee) -> None:
        """Assign a call to a specific employee.
        
        Args:
            queued_call: Call to assign
            employee: Employee to assign to
        """
        call = queued_call.call
        rank = EmployeeRank(employee.rank)
        
        # Mark employee as busy
        self._employee_assignments[employee.id] = call
        
        # Update metrics
        with self._metrics_lock:
            self._metrics.calls_by_rank[rank.name] += 1
            if queued_call.escalation_level > 0:
                self._metrics.escalated_calls += 1
        
        logger.info(
            f"Assigned call {call.id} to employee {employee.id} "
            f"(rank: {rank.name}, escalation: {queued_call.escalation_level})"
        )
    
    def _submit_call_for_processing(self, queued_call: QueuedCall) -> None:
        """Submit a call for processing in the thread pool.
        
        Args:
            queued_call: Call to process
        """
        call = queued_call.call
        future = self._executor.submit(
            self._process_call,
            queued_call
        )
        self._futures[call.id] = future
        
        # Add completion callback
        future.add_done_callback(
            lambda f: self._call_processing_complete(call.id, f)
        )
    
    def _process_call(self, queued_call: QueuedCall) -> Tuple[float, float]:
        """Process a call (simulate call handling).
        
        Args:
            queued_call: Call to process
            
        Returns:
            Tuple of (wait_time, handle_time)
        """
        call = queued_call.call
        start_time = time.time()
        
        # Find which employee is handling this call
        employee = None
        for emp_id, assigned_call in self._employee_assignments.items():
            if assigned_call and assigned_call.id == call.id:
                # Get the employee object
                for rank in EmployeeRank:
                    with self._employee_locks[rank]:
                        for emp in self._employees_by_rank[rank]:
                            if emp.id == emp_id:
                                employee = emp
                                break
                        if employee:
                            break
                break
        
        if not employee:
            logger.error(f"No employee found for call {call.id}")
            return 0.0, 0.0
        
        try:
            # Simulate call handling with random duration
            handle_time = random.uniform(1.0, 10.0)
            time.sleep(handle_time)
            
            # Record success for circuit breaker
            self._circuit_breakers[queued_call.escalation_level].record_success()
            
            wait_time = start_time - queued_call.timestamp
            
            # Update metrics
            with self._metrics_lock:
                self._metrics.completed_calls += 1
                self._metrics.update_wait_time(wait_time)
                # Update average handle time
                if self._metrics.completed_calls == 1:
                    self._metrics.avg_handle_time = handle_time
                else:
                    self._metrics.avg_handle_time = (
                        0.1 * handle_time + 0.9 * self._metrics.avg_handle_time
                    )
            
            logger.info(
                f"Completed call {call.id} (wait: {wait_time:.2f}s, "
                f"handle: {handle_time:.2f}s)"
            )
            
            return wait_time, handle_time
            
        except Exception as e:
            logger.error(f"Error processing call {call.id}: {e}")
            
            # Record failure for circuit breaker
            self._circuit_breakers[queued_call.escalation_level].record_failure()
            
            # Escalate the call
            self._escalate_call(queued_call)
            
            return 0.0, 0.0
        
        finally:
            # Mark employee as available again
            self._release_employee(employee)
    
    def _release_employee(self, employee) -> None:
        """Release an employee back to the available pool.
        
        Args:
            employee: Employee to release
        """
        rank = EmployeeRank(employee.rank)
        with self._employee_locks[rank]:
            self._employee_assignments[employee.id] = None
            self._available_employees[rank].append(employee)
        
        # Notify dispatcher that an employee is available
        with self._queue_lock:
            self._queue_not_empty.notify()
    
    def _escalate_call(self, queued_call: QueuedCall) -> None:
        """Escalate a call to the next level.
        
        Args:
            queued_call: Call to escalate
        """
        call = queued_call.call
        
        # Check if we've exceeded max escalation attempts
        if queued_call.attempts >= 3:
            logger.warning(
                f"Call {call.id} exceeded max escalation attempts, abandoning"
            )
            with self._metrics_lock:
                self._metrics.abandoned_calls += 1
            return
        
        # Increase escalation level
        queued_call.escalation_level += 1
        queued_call.attempts += 1
        
        # Re-add to queue with updated priority
        with self._queue_lock:
            heapq.heappush(self._call_queue, queued_call)
            self._queue_not_empty.notify()
        
        logger.info(
            f"Escalated call {call.id} to level {queued_call.escalation_level}"
        )
    
    def _call_processing_complete(self, call_id: int, future: Future) -> None:
        """Handle completion of call processing.
        
        Args:
            call_id: ID of the completed call
            future: Future object for the call
        """
        if call_id in self._futures:
            del self._futures[call_id]
        
        try:
            future.result()  # Raise any exceptions
        except Exception as e:
            logger.error(f"Call {call_id} processing failed: {e}")
    
    def get_metrics(self) -> Dict:
        """Get current call center metrics.
        
        Returns:
            Dictionary of metrics
        """
        with self._metrics_lock:
            metrics_dict = {
                'total_calls': self._metrics.total_calls,
                'completed_calls': self._metrics.completed_calls,
                'abandoned_calls': self._metrics.abandoned_calls,
                'escalated_calls': self._metrics.escalated_calls,
                'avg_wait_time': self._metrics.avg_wait_time,
                'avg_handle_time': self._metrics.avg_handle_time,
                'peak_queue_size': self._metrics.peak_queue_size,
                'current_queue_size': self._metrics.current_queue_size,
                'calls_by_priority': dict(self._metrics.calls_by_priority),
                'calls_by_rank': dict(self._metrics.calls_by_rank),
                'uptime_seconds': time.time() - self._start_time,
                'active_calls': len(self._futures),
                'available_employees': {
                    rank.name: len(self._available_employees[rank])
                    for rank in EmployeeRank
                }
            }
        return metrics_dict
    
    def get_queue_status(self) -> List[Dict]:
        """Get current status of all calls in queue.
        
        Returns:
            List of call status dictionaries
        """
        with self._queue_lock:
            status = []
            for queued_call in self._call_queue:
                status.append({
                    'call_id': queued_call.call.id,
                    'priority': CallPriority(-queued_call.priority).name,
                    'wait_time': time.time() - queued_call.timestamp,
                    'escalation_level': queued_call.escalation_level,
                    'attempts': queued_call.attempts
                })
            return status
    
    def shutdown(self, wait: bool = True, timeout: float = 30.0) -> None:
        """Shutdown the call center gracefully.
        
        Args:
            wait: Whether to wait for active calls to complete
            timeout: Maximum time to wait for completion
        """
        logger.info("Initiating call center shutdown")
        
        self._running = False
        self._shutdown_event.set()
        
        # Notify dispatcher to wake up and exit
        with self._queue_lock:
            self._queue_not_empty.notify_all()
        
        # Wait for dispatcher thread
        self._dispatcher_thread.join(timeout=5.0)
        
        if wait:
            # Wait for active calls to complete
            start_time = time.time()
            while self._futures and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            # Cancel any remaining futures
            for future in self._futures.values():
                future.cancel()
        
        # Shutdown thread pool
        self._executor.shutdown(wait=False)
        
        logger.info("Call center shutdown complete")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()


# Integration with existing call_center.py classes
# This assumes the existing module has Call and Employee classes
try:
    from .call_center import Call, Employee
    
    # Example usage
    def example_usage():
        """Example demonstrating the call center manager."""
        with CallCenterManager(max_workers=5) as manager:
            # Add employees
            employees = [
                Employee(id=1, rank=EmployeeRank.RESPONDENT),
                Employee(id=2, rank=EmployeeRank.RESPONDENT),
                Employee(id=3, rank=EmployeeRank.MANAGER),
                Employee(id=4, rank=EmployeeRank.DIRECTOR)
            ]
            
            for emp in employees:
                manager.add_employee(emp)
            
            # Simulate incoming calls
            calls = [
                Call(id=i, priority=random.choice(list(CallPriority)))
                for i in range(1, 11)
            ]
            
            for call in calls:
                manager.add_call(call)
                time.sleep(random.uniform(0.1, 0.5))
            
            # Let calls process
            time.sleep(15)
            
            # Print metrics
            metrics = manager.get_metrics()
            print("\n=== Call Center Metrics ===")
            for key, value in metrics.items():
                print(f"{key}: {value}")
            
            print("\n=== Queue Status ===")
            for status in manager.get_queue_status():
                print(status)
    
    if __name__ == "__main__":
        example_usage()
        
except ImportError:
    # If call_center.py doesn't exist or doesn't have these classes,
    # define minimal versions for standalone operation
    logger.warning("Could not import from call_center.py, using minimal classes")
    
    class Call:
        """Minimal Call class for standalone operation."""
        def __init__(self, id: int, priority: int):
            self.id = id
            self.priority = priority
    
    class Employee:
        """Minimal Employee class for standalone operation."""
        def __init__(self, id: int, rank: int):
            self.id = id
            self.rank = rank