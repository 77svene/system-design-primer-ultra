"""
Chaos Engineering Toolkit for System Design Primer
==================================================

Implements fault injection, latency simulation, and resource exhaustion scenarios
to validate system resilience. Integrates with existing system design patterns.

Features:
- Network partition simulation
- Disk failure injection
- CPU/Memory exhaustion
- Latency injection
- Game day scenario runner
- CI/CD integration hooks
"""

import asyncio
import functools
import logging
import os
import random
import time
import threading
import psutil
import socket
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FaultType(Enum):
    """Types of faults that can be injected."""
    NETWORK_PARTITION = "network_partition"
    DISK_FAILURE = "disk_failure"
    CPU_SPIKE = "cpu_spike"
    MEMORY_EXHAUSTION = "memory_exhaustion"
    LATENCY = "latency"
    PACKET_LOSS = "packet_loss"
    SERVICE_CRASH = "service_crash"
    DEPENDENCY_FAILURE = "dependency_failure"


class ChaosMode(Enum):
    """Operation modes for chaos injection."""
    ACTIVE = "active"  # Real fault injection
    SIMULATION = "simulation"  # Simulate without actual damage
    OBSERVATION = "observation"  # Only observe and report


@dataclass
class ChaosScenario:
    """Configuration for a chaos engineering scenario."""
    name: str
    description: str
    fault_type: FaultType
    intensity: float = 0.5  # 0.0 to 1.0
    duration: float = 30.0  # seconds
    target_services: List[str] = None
    prerequisites: List[str] = None
    recovery_steps: List[str] = None
    success_criteria: Dict[str, Any] = None


class FaultInjector:
    """
    Core fault injection engine for chaos engineering.
    
    Can be used as decorator, context manager, or standalone injector.
    """
    
    def __init__(self, mode: ChaosMode = ChaosMode.SIMULATION):
        self.mode = mode
        self.active_faults = {}
        self._original_functions = {}
        self._monitoring = False
        self._monitor_thread = None
        
    def network_partition(self, 
                         service_a: str, 
                         service_b: str, 
                         duration: float = 30.0,
                         bidirectional: bool = True) -> Callable:
        """
        Simulate network partition between two services.
        
        Args:
            service_a: First service identifier
            service_b: Second service identifier
            duration: Duration of partition in seconds
            bidirectional: Whether partition is bidirectional
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                fault_id = f"net_partition_{service_a}_{service_b}_{int(time.time())}"
                
                if self.mode == ChaosMode.ACTIVE:
                    self._activate_network_partition(service_a, service_b, bidirectional)
                elif self.mode == ChaosMode.SIMULATION:
                    logger.info(f"[SIMULATION] Network partition between {service_a} and {service_b}")
                
                self.active_faults[fault_id] = {
                    'type': FaultType.NETWORK_PARTITION,
                    'services': [service_a, service_b],
                    'start_time': time.time(),
                    'duration': duration
                }
                
                try:
                    result = func(*args, **kwargs)
                finally:
                    self._deactivate_fault(fault_id)
                    
                return result
            return wrapper
        return decorator
    
    def disk_failure(self, 
                    path: str = "/tmp", 
                    failure_type: str = "full",
                    duration: float = 60.0) -> Callable:
        """
        Simulate disk failure scenarios.
        
        Args:
            path: Path to simulate failure on
            failure_type: Type of failure ('full', 'corrupt', 'slow')
            duration: Duration of failure in seconds
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                fault_id = f"disk_failure_{path}_{int(time.time())}"
                
                if self.mode == ChaosMode.ACTIVE:
                    self._activate_disk_failure(path, failure_type)
                elif self.mode == ChaosMode.SIMULATION:
                    logger.info(f"[SIMULATION] Disk failure ({failure_type}) on {path}")
                
                self.active_faults[fault_id] = {
                    'type': FaultType.DISK_FAILURE,
                    'path': path,
                    'failure_type': failure_type,
                    'start_time': time.time(),
                    'duration': duration
                }
                
                try:
                    result = func(*args, **kwargs)
                finally:
                    self._deactivate_fault(fault_id)
                    
                return result
            return wrapper
        return decorator
    
    def cpu_spike(self, 
                 intensity: float = 0.8, 
                 duration: float = 30.0,
                 cores: Optional[List[int]] = None) -> Callable:
        """
        Simulate CPU spike/load.
        
        Args:
            intensity: CPU load intensity (0.0 to 1.0)
            duration: Duration of spike in seconds
            cores: Specific CPU cores to target (None for all)
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                fault_id = f"cpu_spike_{int(time.time())}"
                
                if self.mode == ChaosMode.ACTIVE:
                    self._activate_cpu_spike(intensity, duration, cores)
                elif self.mode == ChaosMode.SIMULATION:
                    logger.info(f"[SIMULATION] CPU spike at {intensity*100}% for {duration}s")
                
                self.active_faults[fault_id] = {
                    'type': FaultType.CPU_SPIKE,
                    'intensity': intensity,
                    'start_time': time.time(),
                    'duration': duration
                }
                
                try:
                    result = func(*args, **kwargs)
                finally:
                    self._deactivate_fault(fault_id)
                    
                return result
            return wrapper
        return decorator
    
    def memory_exhaustion(self, 
                         target_mb: int = 1024,
                         duration: float = 60.0) -> Callable:
        """
        Simulate memory exhaustion.
        
        Args:
            target_mb: Target memory to exhaust in MB
            duration: Duration of exhaustion in seconds
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                fault_id = f"memory_exhaust_{int(time.time())}"
                
                if self.mode == ChaosMode.ACTIVE:
                    self._activate_memory_exhaustion(target_mb, duration)
                elif self.mode == ChaosMode.SIMULATION:
                    logger.info(f"[SIMULATION] Memory exhaustion targeting {target_mb}MB")
                
                self.active_faults[fault_id] = {
                    'type': FaultType.MEMORY_EXHAUSTION,
                    'target_mb': target_mb,
                    'start_time': time.time(),
                    'duration': duration
                }
                
                try:
                    result = func(*args, **kwargs)
                finally:
                    self._deactivate_fault(fault_id)
                    
                return result
            return wrapper
        return decorator
    
    def latency_injection(self, 
                         min_ms: int = 100,
                         max_ms: int = 1000,
                         distribution: str = "uniform") -> Callable:
        """
        Inject network latency.
        
        Args:
            min_ms: Minimum latency in milliseconds
            max_ms: Maximum latency in milliseconds
            distribution: Latency distribution ('uniform', 'normal', 'exponential')
            
        Returns:
            Decorator function
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if distribution == "uniform":
                    delay = random.uniform(min_ms, max_ms) / 1000.0
                elif distribution == "normal":
                    mean = (min_ms + max_ms) / 2
                    std = (max_ms - min_ms) / 4
                    delay = max(min_ms, min(max_ms, random.normalvariate(mean, std))) / 1000.0
                elif distribution == "exponential":
                    scale = (max_ms - min_ms) / 3
                    delay = min_ms + random.expovariate(1.0 / scale) / 1000.0
                else:
                    delay = random.uniform(min_ms, max_ms) / 1000.0
                
                if self.mode in [ChaosMode.ACTIVE, ChaosMode.SIMULATION]:
                    logger.info(f"[LATENCY] Injecting {delay*1000:.2f}ms delay")
                    time.sleep(delay)
                
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    @contextmanager
    def inject_fault(self, 
                    fault_type: FaultType,
                    **kwargs):
        """
        Context manager for fault injection.
        
        Example:
            with injector.inject_fault(FaultType.CPU_SPIKE, intensity=0.9):
                # Code running under CPU spike
                process_data()
        """
        fault_id = f"{fault_type.value}_{int(time.time())}"
        
        if fault_type == FaultType.NETWORK_PARTITION:
            self._activate_network_partition(
                kwargs.get('service_a', 'service_a'),
                kwargs.get('service_b', 'service_b'),
                kwargs.get('bidirectional', True)
            )
        elif fault_type == FaultType.DISK_FAILURE:
            self._activate_disk_failure(
                kwargs.get('path', '/tmp'),
                kwargs.get('failure_type', 'full')
            )
        elif fault_type == FaultType.CPU_SPIKE:
            self._activate_cpu_spike(
                kwargs.get('intensity', 0.8),
                kwargs.get('duration', 30.0),
                kwargs.get('cores', None)
            )
        elif fault_type == FaultType.MEMORY_EXHAUSTION:
            self._activate_memory_exhaustion(
                kwargs.get('target_mb', 1024),
                kwargs.get('duration', 60.0)
            )
        
        self.active_faults[fault_id] = {
            'type': fault_type,
            'start_time': time.time(),
            **kwargs
        }
        
        try:
            yield
        finally:
            self._deactivate_fault(fault_id)
    
    def _activate_network_partition(self, service_a: str, service_b: str, bidirectional: bool):
        """Activate network partition (simulation mode logs only)."""
        logger.info(f"Activating network partition: {service_a} <-> {service_b}")
        # In a real implementation, this would modify network rules
        # For simulation, we just log the action
    
    def _activate_disk_failure(self, path: str, failure_type: str):
        """Activate disk failure (simulation mode logs only)."""
        logger.info(f"Activating disk failure ({failure_type}) on {path}")
        # In a real implementation, this would fill disk or corrupt files
        # For simulation, we just log the action
    
    def _activate_cpu_spike(self, intensity: float, duration: float, cores: Optional[List[int]]):
        """Activate CPU spike."""
        if self.mode == ChaosMode.ACTIVE:
            # Create CPU load
            def cpu_load():
                end_time = time.time() + duration
                while time.time() < end_time:
                    # Busy wait to consume CPU
                    pass
            
            threads = []
            num_cores = psutil.cpu_count() if cores is None else len(cores)
            for _ in range(int(num_cores * intensity)):
                t = threading.Thread(target=cpu_load)
                t.daemon = True
                t.start()
                threads.append(t)
    
    def _activate_memory_exhaustion(self, target_mb: int, duration: float):
        """Activate memory exhaustion."""
        if self.mode == ChaosMode.ACTIVE:
            # Allocate memory
            memory_hog = []
            chunk_size = 1024 * 1024  # 1MB chunks
            chunks_needed = target_mb
            
            def allocate_memory():
                try:
                    for _ in range(chunks_needed):
                        memory_hog.append(b' ' * chunk_size)
                    time.sleep(duration)
                finally:
                    memory_hog.clear()
            
            t = threading.Thread(target=allocate_memory)
            t.daemon = True
            t.start()
    
    def _deactivate_fault(self, fault_id: str):
        """Deactivate a fault and clean up."""
        if fault_id in self.active_faults:
            fault = self.active_faults[fault_id]
            logger.info(f"Deactivating fault: {fault_id}")
            del self.active_faults[fault_id]
    
    def start_monitoring(self, interval: float = 5.0):
        """Start system monitoring for chaos experiments."""
        if self._monitoring:
            return
        
        self._monitoring = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_system,
            args=(interval,),
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("Started chaos monitoring")
    
    def stop_monitoring(self):
        """Stop system monitoring."""
        self._monitoring = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=1.0)
        logger.info("Stopped chaos monitoring")
    
    def _monitor_system(self, interval: float):
        """Monitor system metrics during chaos experiments."""
        while self._monitoring:
            try:
                # Collect system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                logger.info(
                    f"System Metrics - "
                    f"CPU: {cpu_percent}%, "
                    f"Memory: {memory.percent}%, "
                    f"Disk: {disk.percent}%"
                )
                
                # Check for anomalies
                if cpu_percent > 90:
                    logger.warning("High CPU usage detected!")
                if memory.percent > 90:
                    logger.warning("High memory usage detected!")
                if disk.percent > 90:
                    logger.warning("High disk usage detected!")
                    
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
            
            time.sleep(interval)


class GameDayRunner:
    """
    Runner for chaos engineering game day scenarios.
    
    Executes predefined chaos scenarios with runbooks and recovery procedures.
    """
    
    def __init__(self, 
                 scenarios: List[ChaosScenario],
                 injector: Optional[FaultInjector] = None):
        self.scenarios = scenarios
        self.injector = injector or FaultInjector(ChaosMode.SIMULATION)
        self.results = []
        self.current_scenario = None
    
    def run_scenario(self, scenario: ChaosScenario) -> Dict[str, Any]:
        """
        Run a single chaos scenario.
        
        Args:
            scenario: The chaos scenario to run
            
        Returns:
            Dictionary with scenario results
        """
        logger.info(f"Starting scenario: {scenario.name}")
        logger.info(f"Description: {scenario.description}")
        
        self.current_scenario = scenario
        start_time = time.time()
        
        # Check prerequisites
        if scenario.prerequisites:
            logger.info("Checking prerequisites...")
            for prereq in scenario.prerequisites:
                logger.info(f"  - {prereq}")
        
        # Execute fault injection
        result = {
            'scenario': scenario.name,
            'start_time': start_time,
            'success': False,
            'observations': [],
            'recovery_time': None
        }
        
        try:
            with self.injector.inject_fault(
                scenario.fault_type,
                intensity=scenario.intensity,
                duration=scenario.duration,
                **self._get_fault_kwargs(scenario)
            ):
                # Simulate system behavior under fault
                logger.info(f"System under {scenario.fault_type.value} fault...")
                time.sleep(scenario.duration)
                
                # Check success criteria
                if scenario.success_criteria:
                    result['success'] = self._check_success_criteria(scenario.success_criteria)
                else:
                    result['success'] = True
                
                result['observations'].append(
                    f"Fault injection completed after {scenario.duration} seconds"
                )
                
        except Exception as e:
            result['observations'].append(f"Error during scenario: {str(e)}")
            logger.error(f"Scenario failed: {e}")
        
        finally:
            # Execute recovery steps
            if scenario.recovery_steps:
                logger.info("Executing recovery steps...")
                recovery_start = time.time()
                for step in scenario.recovery_steps:
                    logger.info(f"  - {step}")
                    time.sleep(1)  # Simulate recovery time
                result['recovery_time'] = time.time() - recovery_start
            
            result['end_time'] = time.time()
            result['duration'] = result['end_time'] - start_time
            self.results.append(result)
            self.current_scenario = None
        
        logger.info(f"Scenario completed: {scenario.name} - Success: {result['success']}")
        return result
    
    def run_all_scenarios(self) -> List[Dict[str, Any]]:
        """Run all defined chaos scenarios."""
        logger.info(f"Starting game day with {len(self.scenarios)} scenarios")
        
        all_results = []
        for i, scenario in enumerate(self.scenarios, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Scenario {i}/{len(self.scenarios)}")
            logger.info(f"{'='*60}")
            
            result = self.run_scenario(scenario)
            all_results.append(result)
            
            # Brief pause between scenarios
            if i < len(self.scenarios):
                time.sleep(5)
        
        self._generate_report(all_results)
        return all_results
    
    def _get_fault_kwargs(self, scenario: ChaosScenario) -> Dict[str, Any]:
        """Get additional keyword arguments for fault injection."""
        kwargs = {}
        
        if scenario.fault_type == FaultType.NETWORK_PARTITION:
            kwargs.update({
                'service_a': scenario.target_services[0] if scenario.target_services else 'service_a',
                'service_b': scenario.target_services[1] if len(scenario.target_services) > 1 else 'service_b'
            })
        elif scenario.fault_type == FaultType.DISK_FAILURE:
            kwargs['path'] = '/tmp'
        elif scenario.fault_type == FaultType.CPU_SPIKE:
            kwargs['intensity'] = scenario.intensity
        
        return kwargs
    
    def _check_success_criteria(self, criteria: Dict[str, Any]) -> bool:
        """Check if success criteria are met."""
        # In a real implementation, this would check actual system metrics
        # For simulation, we assume success if no exceptions occurred
        return True
    
    def _generate_report(self, results: List[Dict[str, Any]]):
        """Generate game day report."""
        logger.info("\n" + "="*60)
        logger.info("GAME DAY REPORT")
        logger.info("="*60)
        
        total_scenarios = len(results)
        successful = sum(1 for r in results if r['success'])
        
        logger.info(f"Total Scenarios: {total_scenarios}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {total_scenarios - successful}")
        logger.info(f"Success Rate: {(successful/total_scenarios)*100:.1f}%")
        
        for result in results:
            status = "✓" if result['success'] else "✗"
            logger.info(f"{status} {result['scenario']} - {result['duration']:.1f}s")


class ChaosTestSuite:
    """
    Automated chaos test suite for CI/CD integration.
    
    Can be integrated into GitHub Actions, Jenkins, or other CI/CD pipelines.
    """
    
    def __init__(self, 
                 test_cases: List[Callable],
                 injector: Optional[FaultInjector] = None):
        self.test_cases = test_cases
        self.injector = injector or FaultInjector(ChaosMode.SIMULATION)
        self.test_results = []
    
    def run_tests(self) -> bool:
        """
        Run all chaos tests.
        
        Returns:
            True if all tests pass, False otherwise
        """
        logger.info(f"Running {len(self.test_cases)} chaos tests")
        
        all_passed = True
        for i, test_case in enumerate(self.test_cases, 1):
            logger.info(f"\nRunning test {i}/{len(self.test_cases)}: {test_case.__name__}")
            
            try:
                test_case(self.injector)
                self.test_results.append({
                    'test': test_case.__name__,
                    'status': 'PASSED',
                    'error': None
                })
                logger.info(f"✓ Test passed: {test_case.__name__}")
                
            except Exception as e:
                self.test_results.append({
                    'test': test_case.__name__,
                    'status': 'FAILED',
                    'error': str(e)
                })
                logger.error(f"✗ Test failed: {test_case.__name__} - {e}")
                all_passed = False
        
        self._generate_test_report()
        return all_passed
    
    def _generate_test_report(self):
        """Generate test report for CI/CD."""
        passed = sum(1 for r in self.test_results if r['status'] == 'PASSED')
        failed = len(self.test_results) - passed
        
        logger.info("\n" + "="*60)
        logger.info("CHAOS TEST REPORT")
        logger.info("="*60)
        logger.info(f"Total Tests: {len(self.test_results)}")
        logger.info(f"Passed: {passed}")
        logger.info(f"Failed: {failed}")
        
        if failed > 0:
            logger.info("\nFailed Tests:")
            for result in self.test_results:
                if result['status'] == 'FAILED':
                    logger.info(f"  - {result['test']}: {result['error']}")


# Integration with existing System Design Primer modules
class SystemDesignChaosIntegration:
    """
    Integration layer for chaos engineering with existing system design patterns.
    
    Provides chaos hooks for common system design components.
    """
    
    @staticmethod
    def inject_into_lru_cache(cache_class, injector: FaultInjector):
        """
        Add chaos hooks to LRU Cache implementation.
        
        Simulates cache failures, evictions, and latency.
        """
        original_get = cache_class.get
        original_put = cache_class.put
        
        @injector.latency_injection(min_ms=10, max_ms=100)
        def chaotic_get(self, key):
            # Simulate cache miss with probability
            if random.random() < 0.1:  # 10% miss rate
                raise KeyError(f"Simulated cache miss for key: {key}")
            return original_get(self, key)
        
        @injector.latency_injection(min_ms=5, max_ms=50)
        def chaotic_put(self, key, value):
            # Simulate cache full with probability
            if random.random() < 0.05:  # 5% full rate
                raise Exception("Simulated cache full")
            return original_put(self, key, value)
        
        cache_class.get = chaotic_get
        cache_class.put = chaotic_put
    
    @staticmethod
    def inject_into_hash_map(hash_map_class, injector: FaultInjector):
        """
        Add chaos hooks to Hash Map implementation.
        
        Simulates hash collisions, rehashing failures, and memory issues.
        """
        original_get = hash_map_class.get
        original_put = hash_map_class.put
        
        @injector.latency_injection(min_ms=5, max_ms=30)
        def chaotic_get(self, key):
            # Simulate hash collision delay
            if random.random() < 0.2:  # 20% collision simulation
                time.sleep(random.uniform(0.01, 0.05))
            return original_get(self, key)
        
        @injector.latency_injection(min_ms=10, max_ms=100)
        def chaotic_put(self, key, value):
            # Simulate rehashing delay
            if len(self) > self.capacity * 0.8 and random.random() < 0.3:
                logger.info("Simulating rehashing delay...")
                time.sleep(random.uniform(0.1, 0.5))
            return original_put(self, key, value)
        
        hash_map_class.get = chaotic_get
        hash_map_class.put = chaotic_put
    
    @staticmethod
    def inject_into_call_center(call_center_class, injector: FaultInjector):
        """
        Add chaos hooks to Call Center system.
        
        Simulates agent failures, call drops, and routing issues.
        """
        original_route_call = call_center_class.route_call
        
        @injector.latency_injection(min_ms=100, max_ms=2000)
        def chaotic_route_call(self, call):
            # Simulate agent unavailable
            if random.random() < 0.1:  # 10% agent unavailable
                raise Exception("Simulated agent unavailable")
            
            # Simulate call drop
            if random.random() < 0.05:  # 5% call drop rate
                logger.warning("Simulating call drop")
                return None
            
            return original_route_call(self, call)
        
        call_center_class.route_call = chaotic_route_call


# Predefined chaos scenarios for common system designs
PREDEFINED_SCENARIOS = [
    ChaosScenario(
        name="Network Partition During Peak Load",
        description="Simulate network partition between microservices during high traffic",
        fault_type=FaultType.NETWORK_PARTITION,
        intensity=0.7,
        duration=120.0,
        target_services=["api_gateway", "user_service"],
        recovery_steps=[
            "Check network connectivity between services",
            "Verify service discovery is working",
            "Monitor error rates and latency",
            "Gradually restore network connectivity"
        ],
        success_criteria={
            "max_error_rate": 0.05,
            "max_latency_increase": 2.0,
            "service_availability": 0.95
        }
    ),
    ChaosScenario(
        name="Database Disk Failure",
        description="Simulate disk failure on primary database node",
        fault_type=FaultType.DISK_FAILURE,
        intensity=0.9,
        duration=180.0,
        target_services=["primary_database"],
        prerequisites=[
            "Verify database backups are current",
            "Confirm replica is synchronized",
            "Check monitoring alerts are configured"
        ],
        recovery_steps=[
            "Failover to replica database",
            "Replace failed disk",
            "Restore from backup if necessary",
            "Verify data consistency"
        ],
        success_criteria={
            "max_downtime": 300.0,
            "data_loss": "none",
            "recovery_time_objective": 600.0
        }
    ),
    ChaosScenario(
        name="CPU Exhaustion on API Servers",
        description="Simulate CPU spike on API servers during traffic surge",
        fault_type=FaultType.CPU_SPIKE,
        intensity=0.95,
        duration=60.0,
        target_services=["api_server_1", "api_server_2"],
        recovery_steps=[
            "Auto-scaling should trigger",
            "Load balancer should redistribute traffic",
            "Monitor response times and error rates",
            "Scale down after load decreases"
        ],
        success_criteria={
            "auto_scaling_triggered": True,
            "max_response_time": 2.0,
            "error_rate": 0.01
        }
    ),
    ChaosScenario(
        name="Memory Leak Simulation",
        description="Simulate memory leak in caching service",
        fault_type=FaultType.MEMORY_EXHAUSTION,
        intensity=0.8,
        duration=300.0,
        target_services=["redis_cache"],
        recovery_steps=[
            "Monitor memory usage trends",
            "Trigger cache eviction policies",
            "Restart service if memory exceeds threshold",
            "Identify and fix memory leak"
        ],
        success_criteria={
            "service_restart_successful": True,
            "max_memory_usage": 0.9,
            "cache_hit_rate_maintained": 0.8
        }
    )
]


# Example chaos tests for CI/CD
def example_chaos_test_1(injector: FaultInjector):
    """Test system resilience under network latency."""
    @injector.latency_injection(min_ms=200, max_ms=1000)
    def test_function():
        # Simulate API call
        time.sleep(0.1)
        return "success"
    
    result = test_function()
    assert result == "success", "Function should complete despite latency"


def example_chaos_test_2(injector: FaultInjector):
    """Test system behavior during CPU spike."""
    with injector.inject_fault(FaultType.CPU_SPIKE, intensity=0.7, duration=5.0):
        # Simulate computation during CPU spike
        start = time.time()
        while time.time() - start < 2.0:
            pass  # Busy wait
        return True


def example_chaos_test_3(injector: FaultInjector):
    """Test cache resilience under fault conditions."""
    # This would integrate with actual cache implementation
    logger.info("Testing cache resilience...")
    time.sleep(1)
    return True


# GitHub Actions integration
def create_github_actions_chaos_job():
    """
    Generate GitHub Actions configuration for chaos testing.
    
    Returns:
        Dictionary with GitHub Actions job configuration
    """
    return {
        "chaos-testing": {
            "runs-on": "ubuntu-latest",
            "steps": [
                {
                    "name": "Checkout code",
                    "uses": "actions/checkout@v3"
                },
                {
                    "name": "Set up Python",
                    "uses": "actions/setup-python@v4",
                    "with": {
                        "python-version": "3.9"
                    }
                },
                {
                    "name": "Install dependencies",
                    "run": "pip install psutil"
                },
                {
                    "name": "Run chaos tests",
                    "run": "python -c \"from chaos.fault_injector import ChaosTestSuite, example_chaos_test_1, example_chaos_test_2, example_chaos_test_3; suite = ChaosTestSuite([example_chaos_test_1, example_chaos_test_2, example_chaos_test_3]); exit(0 if suite.run_tests() else 1)\""
                }
            ]
        }
    }


# Main execution for standalone testing
if __name__ == "__main__":
    # Example usage
    print("Chaos Engineering Toolkit for System Design Primer")
    print("="*60)
    
    # Create fault injector
    injector = FaultInjector(ChaosMode.SIMULATION)
    
    # Start monitoring
    injector.start_monitoring(interval=2.0)
    
    try:
        # Example 1: Use as decorator
        @injector.latency_injection(min_ms=100, max_ms=500)
        def sample_api_call():
            print("Making API call...")
            time.sleep(0.5)
            return {"status": "success"}
        
        result = sample_api_call()
        print(f"API call result: {result}")
        
        # Example 2: Use as context manager
        print("\nTesting CPU spike scenario...")
        with injector.inject_fault(FaultType.CPU_SPIKE, intensity=0.5, duration=3.0):
            print("Working under CPU spike...")
            time.sleep(2)
        
        # Example 3: Run game day scenarios
        print("\nRunning Game Day Scenarios...")
        game_day = GameDayRunner(PREDEFINED_SCENARIOS[:2], injector)  # Run first 2 scenarios
        results = game_day.run_all_scenarios()
        
        # Example 4: Run chaos tests
        print("\nRunning Chaos Tests...")
        test_suite = ChaosTestSuite([
            example_chaos_test_1,
            example_chaos_test_2,
            example_chaos_test_3
        ], injector)
        
        all_passed = test_suite.run_tests()
        print(f"\nAll tests passed: {all_passed}")
        
    finally:
        # Stop monitoring
        injector.stop_monitoring()
    
    print("\nChaos engineering toolkit demonstration complete!")