import threading
import time
import heapq
from abc import ABCMeta, abstractmethod
from collections import deque
from enum import Enum
from typing import Optional, List, Dict
import logging
from dataclasses import dataclass
from datetime import datetime
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Rank(Enum):
    OPERATOR = 0
    SUPERVISOR = 1
    DIRECTOR = 2


class CallState(Enum):
    READY = 0
    IN_PROGRESS = 1
    COMPLETE = 2
    ESCALATED = 3


@dataclass
class CallMetrics:
    call_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    escalations: int = 0
    wait_time: float = 0.0
    handle_time: float = 0.0


class Call:
    def __init__(self, rank: Rank, call_id: str = None):
        self.call_id = call_id or f"call_{int(time.time())}_{random.randint(1000, 9999)}"
        self.state = CallState.READY
        self.rank = rank
        self.employee = None
        self.created_at = datetime.now()
        self.assigned_at = None
        self.completed_at = None
        self.escalation_count = 0
        self.priority_score = self._calculate_priority()
        
    def _calculate_priority(self) -> int:
        """Calculate priority score for heap queue (lower is higher priority)"""
        base_priority = {
            Rank.DIRECTOR: 1,
            Rank.SUPERVISOR: 2,
            Rank.OPERATOR: 3
        }[self.rank]
        
        # Add escalation bonus (higher escalation = higher priority)
        escalation_bonus = -self.escalation_count * 0.1
        
        # Add time bonus (older calls get higher priority)
        time_bonus = -(time.time() - self.created_at.timestamp()) * 0.01
        
        return base_priority + escalation_bonus + time_bonus
    
    def __lt__(self, other):
        """For heap comparison"""
        return self.priority_score < other.priority_score
    
    def update_priority(self):
        """Update priority score based on current state"""
        self.priority_score = self._calculate_priority()


class Employee(metaclass=ABCMeta):
    def __init__(self, employee_id: str, name: str, rank: Rank, call_center: 'CallCenter'):
        self.employee_id = employee_id
        self.name = name
        self.rank = rank
        self.call: Optional[Call] = None
        self.call_center = call_center
        self.lock = threading.RLock()
        self.is_available = True
        self.circuit_breaker_failures = 0
        self.circuit_breaker_threshold = 3
        self.circuit_breaker_timeout = 30  # seconds
        self.last_failure_time = None
        
    def take_call(self, call: Call) -> bool:
        """Thread-safe call assignment with circuit breaker check"""
        with self.lock:
            if not self.is_available:
                return False
                
            if self._is_circuit_breaker_open():
                logger.warning(f"Circuit breaker open for {self.name}, rejecting call {call.call_id}")
                return False
                
            self.call = call
            self.call.employee = self
            self.call.state = CallState.IN_PROGRESS
            self.call.assigned_at = datetime.now()
            self.is_available = False
            
            # Update metrics
            if self.call.assigned_at and self.call.created_at:
                self.call.wait_time = (self.call.assigned_at - self.call.created_at).total_seconds()
                
            logger.info(f"{self.name} ({self.rank.name}) taking call {call.call_id}")
            return True
            
    def complete_call(self) -> None:
        """Complete current call and update metrics"""
        with self.lock:
            if not self.call:
                return
                
            self.call.state = CallState.COMPLETE
            self.call.completed_at = datetime.now()
            
            if self.call.completed_at and self.call.assigned_at:
                self.call.handle_time = (self.call.completed_at - self.call.assigned_at).total_seconds()
                
            logger.info(f"{self.name} completed call {self.call.call_id} "
                       f"(wait: {self.call.wait_time:.2f}s, handle: {self.call.handle_time:.2f}s)")
            
            # Reset circuit breaker on successful completion
            self.circuit_breaker_failures = 0
            self.last_failure_time = None
            
            completed_call = self.call
            self.call = None
            self.is_available = True
            
            self.call_center.notify_call_completed(completed_call)
            
    def escalate_call(self) -> bool:
        """Escalate call with circuit breaker protection"""
        with self.lock:
            if not self.call:
                return False
                
            try:
                if self._is_circuit_breaker_open():
                    logger.warning(f"Circuit breaker open for {self.name}, cannot escalate call {self.call.call_id}")
                    return False
                    
                self.call.state = CallState.ESCALATED
                self.call.escalation_count += 1
                self.call.update_priority()
                
                call = self.call
                self.call = None
                self.is_available = True
                
                logger.info(f"{self.name} escalating call {call.call_id} "
                          f"(escalation #{call.escalation_count})")
                
                self.call_center.notify_call_escalated(call)
                return True
                
            except Exception as e:
                self.circuit_breaker_failures += 1
                self.last_failure_time = datetime.now()
                logger.error(f"Escalation failed for {self.name}: {str(e)}")
                return False
                
    def _is_circuit_breaker_open(self) -> bool:
        """Check if circuit breaker is open"""
        if self.circuit_breaker_failures >= self.circuit_breaker_threshold:
            if self.last_failure_time:
                time_since_failure = (datetime.now() - self.last_failure_time).total_seconds()
                if time_since_failure < self.circuit_breaker_timeout:
                    return True
                else:
                    # Reset circuit breaker after timeout
                    self.circuit_breaker_failures = 0
                    self.last_failure_time = None
        return False
        
    @abstractmethod
    def _escalate_call(self):
        pass


class Operator(Employee):
    def __init__(self, employee_id: str, name: str, call_center: 'CallCenter'):
        super().__init__(employee_id, name, Rank.OPERATOR, call_center)
        
    def escalate_call(self) -> bool:
        if self.call:
            self.call.rank = Rank.SUPERVISOR
        return super().escalate_call()
        
    def _escalate_call(self):
        pass


class Supervisor(Employee):
    def __init__(self, employee_id: str, name: str, call_center: 'CallCenter'):
        super().__init__(employee_id, name, Rank.SUPERVISOR, call_center)
        
    def escalate_call(self) -> bool:
        if self.call:
            self.call.rank = Rank.DIRECTOR
        return super().escalate_call()
        
    def _escalate_call(self):
        pass


class Director(Employee):
    def __init__(self, employee_id: str, name: str, call_center: 'CallCenter'):
        super().__init__(employee_id, name, Rank.DIRECTOR, call_center)
        
    def escalate_call(self) -> bool:
        # Directors cannot escalate further
        logger.warning(f"Director {self.name} cannot escalate call {self.call.call_id if self.call else 'None'}")
        return False
        
    def _escalate_call(self):
        pass


class LoadBalancer:
    """Load balancer for distributing calls across employees"""
    
    def __init__(self, call_center: 'CallCenter'):
        self.call_center = call_center
        self.lock = threading.RLock()
        self.employee_loads: Dict[str, int] = {}
        
    def get_least_loaded_employee(self, rank: Rank) -> Optional[Employee]:
        """Get the least loaded employee of specified rank or higher"""
        with self.lock:
            available_employees = []
            
            # Check employees of the specified rank and higher ranks
            for check_rank in [rank, Rank.SUPERVISOR, Rank.DIRECTOR]:
                employees = self.call_center.get_available_employees(check_rank)
                for employee in employees:
                    load = self.employee_loads.get(employee.employee_id, 0)
                    available_employees.append((load, employee))
                    
            if not available_employees:
                return None
                
            # Sort by load (ascending) and return the least loaded
            available_employees.sort(key=lambda x: x[0])
            return available_employees[0][1]
            
    def update_load(self, employee: Employee, delta: int = 1) -> None:
        """Update load for an employee"""
        with self.lock:
            current_load = self.employee_loads.get(employee.employee_id, 0)
            self.employee_loads[employee.employee_id] = max(0, current_load + delta)


class MetricsCollector:
    """Collect and report system metrics"""
    
    def __init__(self):
        self.lock = threading.RLock()
        self.total_calls = 0
        self.completed_calls = 0
        self.escalated_calls = 0
        self.dropped_calls = 0
        self.total_wait_time = 0.0
        self.total_handle_time = 0.0
        self.call_metrics: List[CallMetrics] = []
        
    def record_call_start(self, call: Call) -> None:
        with self.lock:
            self.total_calls += 1
            metrics = CallMetrics(
                call_id=call.call_id,
                start_time=call.created_at
            )
            self.call_metrics.append(metrics)
            
    def record_call_completion(self, call: Call) -> None:
        with self.lock:
            self.completed_calls += 1
            if call.wait_time:
                self.total_wait_time += call.wait_time
            if call.handle_time:
                self.total_handle_time += call.handle_time
                
    def record_call_escalation(self, call: Call) -> None:
        with self.lock:
            self.escalated_calls += 1
            
    def record_call_drop(self, call: Call) -> None:
        with self.lock:
            self.dropped_calls += 1
            
    def get_metrics(self) -> Dict:
        with self.lock:
            avg_wait_time = (self.total_wait_time / self.completed_calls 
                           if self.completed_calls > 0 else 0)
            avg_handle_time = (self.total_handle_time / self.completed_calls 
                             if self.completed_calls > 0 else 0)
            
            return {
                "total_calls": self.total_calls,
                "completed_calls": self.completed_calls,
                "escalated_calls": self.escalated_calls,
                "dropped_calls": self.dropped_calls,
                "avg_wait_time": avg_wait_time,
                "avg_handle_time": avg_handle_time,
                "completion_rate": (self.completed_calls / self.total_calls * 100 
                                  if self.total_calls > 0 else 0)
            }
            
    def print_metrics(self) -> None:
        metrics = self.get_metrics()
        logger.info("=== Call Center Metrics ===")
        for key, value in metrics.items():
            if isinstance(value, float):
                logger.info(f"{key}: {value:.2f}")
            else:
                logger.info(f"{key}: {value}")


class CallCenter:
    def __init__(self, operators: List[Operator], supervisors: List[Supervisor], 
                 directors: List[Director]):
        self.operators = operators
        self.supervisors = supervisors
        self.directors = directors
        
        # Thread-safe data structures
        self.call_queue = []  # Priority queue (heap)
        self.queue_lock = threading.RLock()
        self.queue_condition = threading.Condition(self.queue_lock)
        
        # Employee availability tracking
        self.available_employees = {
            Rank.OPERATOR: set(operators),
            Rank.SUPERVISOR: set(supervisors),
            Rank.DIRECTOR: set(directors)
        }
        self.employee_lock = threading.RLock()
        
        # Components
        self.load_balancer = LoadBalancer(self)
        self.metrics = MetricsCollector()
        
        # Worker threads
        self.dispatcher_thread = threading.Thread(target=self._dispatcher_loop, daemon=True)
        self.dispatcher_running = True
        self.dispatcher_thread.start()
        
        # Start employee threads
        self._start_employee_threads()
        
        logger.info(f"CallCenter initialized with {len(operators)} operators, "
                   f"{len(supervisors)} supervisors, {len(directors)} directors")
        
    def _start_employee_threads(self) -> None:
        """Start worker threads for each employee"""
        all_employees = self.operators + self.supervisors + self.directors
        for employee in all_employees:
            thread = threading.Thread(target=self._employee_worker, args=(employee,), daemon=True)
            thread.start()
            
    def _employee_worker(self, employee: Employee) -> None:
        """Worker thread for processing employee calls"""
        while True:
            time.sleep(0.1)  # Small delay to prevent busy waiting
            
            # Check if employee has a call to process
            if employee.call and employee.call.state == CallState.IN_PROGRESS:
                # Simulate call processing
                processing_time = random.uniform(2, 8)
                time.sleep(processing_time)
                
                # Randomly decide to complete or escalate
                if random.random() < 0.8:  # 80% chance to complete
                    employee.complete_call()
                else:  # 20% chance to escalate
                    employee.escalate_call()
                    
    def _dispatcher_loop(self) -> None:
        """Main dispatcher loop for assigning calls to employees"""
        while self.dispatcher_running:
            with self.queue_condition:
                # Wait for calls in queue or timeout
                self.queue_condition.wait(timeout=1.0)
                
                while self.call_queue:
                    # Get highest priority call
                    call = heapq.heappop(self.call_queue)
                    
                    # Try to assign call
                    if not self._assign_call(call):
                        # If assignment failed, requeue with updated priority
                        call.update_priority()
                        heapq.heappush(self.call_queue, call)
                        time.sleep(0.1)  # Prevent busy waiting
                        
    def _assign_call(self, call: Call) -> bool:
        """Assign call to an available employee using load balancing"""
        employee = self.load_balancer.get_least_loaded_employee(call.rank)
        
        if employee and employee.take_call(call):
            self.load_balancer.update_load(employee, 1)
            self.metrics.record_call_start(call)
            return True
            
        return False
        
    def dispatch_call(self, call: Call) -> None:
        """Add call to priority queue"""
        with self.queue_lock:
            heapq.heappush(self.call_queue, call)
            self.queue_condition.notify()
            
        logger.info(f"Call {call.call_id} added to queue (rank: {call.rank.name})")
        
    def get_available_employees(self, rank: Rank) -> List[Employee]:
        """Get available employees of specified rank"""
        with self.employee_lock:
            return [emp for emp in self.available_employees[rank] 
                   if emp.is_available and not emp._is_circuit_breaker_open()]
            
    def notify_call_escalated(self, call: Call) -> None:
        """Handle call escalation"""
        with self.employee_lock:
            # Remove from previous employee's load
            if call.employee:
                self.load_balancer.update_load(call.employee, -1)
                
        # Requeue the escalated call
        call.update_priority()
        self.dispatch_call(call)
        self.metrics.record_call_escalation(call)
        
        logger.info(f"Call {call.call_id} requeued after escalation to {call.rank.name}")
        
    def notify_call_completed(self, call: Call) -> None:
        """Handle call completion"""
        with self.employee_lock:
            # Update load balancer
            if call.employee:
                self.load_balancer.update_load(call.employee, -1)
                
        self.metrics.record_call_completion(call)
        
        # Try to assign queued calls to newly available employee
        if call.employee:
            self._try_assign_queued_calls()
            
    def _try_assign_queued_calls(self) -> None:
        """Try to assign queued calls to available employees"""
        with self.queue_lock:
            if not self.call_queue:
                return
                
            # Make a copy of the queue to avoid modification during iteration
            temp_calls = self.call_queue.copy()
            self.call_queue.clear()
            
            for call in temp_calls:
                if not self._assign_call(call):
                    # If assignment failed, keep in queue
                    heapq.heappush(self.call_queue, call)
                    
    def get_queue_size(self) -> int:
        """Get current queue size"""
        with self.queue_lock:
            return len(self.call_queue)
            
    def get_employee_status(self) -> Dict:
        """Get status of all employees"""
        status = {
            Rank.OPERATOR.name: {"total": len(self.operators), "available": 0},
            Rank.SUPERVISOR.name: {"total": len(self.supervisors), "available": 0},
            Rank.DIRECTOR.name: {"total": len(self.directors), "available": 0}
        }
        
        with self.employee_lock:
            for rank in [Rank.OPERATOR, Rank.SUPERVISOR, Rank.DIRECTOR]:
                available = len([emp for emp in self.available_employees[rank] 
                               if emp.is_available])
                status[rank.name]["available"] = available
                
        return status
        
    def shutdown(self) -> None:
        """Shutdown the call center"""
        self.dispatcher_running = False
        with self.queue_condition:
            self.queue_condition.notify_all()
            
        logger.info("CallCenter shutdown initiated")
        self.metrics.print_metrics()


# Example usage and testing
if __name__ == "__main__":
    # Create employees
    call_center = CallCenter(
        operators=[Operator(f"op_{i}", f"Operator {i}", None) for i in range(3)],
        supervisors=[Supervisor(f"sup_{i}", f"Supervisor {i}", None) for i in range(2)],
        directors=[Director(f"dir_{i}", f"Director {i}", None) for i in range(1)]
    )
    
    # Set call center references for employees
    for emp in (call_center.operators + call_center.supervisors + call_center.directors):
        emp.call_center = call_center
    
    # Simulate incoming calls
    for i in range(10):
        rank = random.choice([Rank.OPERATOR, Rank.SUPERVISOR, Rank.DIRECTOR])
        call = Call(rank, f"test_call_{i}")
        call_center.dispatch_call(call)
        time.sleep(0.5)
    
    # Let system run for a bit
    time.sleep(30)
    
    # Print metrics
    call_center.metrics.print_metrics()
    print("\nEmployee Status:")
    print(call_center.get_employee_status())
    print(f"Queue Size: {call_center.get_queue_size()}")
    
    # Shutdown
    call_center.shutdown()