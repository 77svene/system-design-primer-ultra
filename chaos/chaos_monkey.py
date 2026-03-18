"""
chaos/chaos_monkey.py — Chaos Engineering Toolkit for System Design Primer

This module provides chaos engineering capabilities to test system resilience.
It integrates with existing system design solutions to inject failures, simulate
latency, exhaust resources, and validate system behavior under adverse conditions.
"""

import asyncio
import functools
import logging
import random
import time
import threading
import psutil
import os
import signal
import sys
from typing import Callable, Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
from contextlib import contextmanager
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("chaos_monkey")


class FaultType(Enum):
    """Types of faults that can be injected"""
    NETWORK_PARTITION = "network_partition"
    DISK_FAILURE = "disk_failure"
    CPU_SPIKE = "cpu_spike"
    MEMORY_LEAK = "memory_leak"
    LATENCY = "latency"
    EXCEPTION = "exception"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    SERVICE_UNAVAILABLE = "service_unavailable"


@dataclass
class ChaosScenario:
    """Configuration for a chaos engineering scenario"""
    name: str
    fault_type: FaultType
    duration: float = 30.0  # seconds
    intensity: float = 0.5  # 0.0 to 1.0
    target_services: List[str] = None
    probability: float = 1.0  # probability of injection
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.target_services is None:
            self.target_services = []
        if self.metadata is None:
            self.metadata = {}


class ChaosMonkey:
    """
    Main chaos engineering toolkit for injecting failures and testing system resilience.
    
    Usage:
        chaos = ChaosMonkey()
        
        # Inject latency into a function
        @chaos.inject_latency(min_ms=100, max_ms=500)
        def my_function():
            pass
            
        # Run a chaos scenario
        scenario = ChaosScenario(
            name="network_partition_test",
            fault_type=FaultType.NETWORK_PARTITION,
            duration=60.0
        )
        chaos.run_scenario(scenario)
    """
    
    def __init__(self, enabled: bool = True, seed: Optional[int] = None):
        self.enabled = enabled
        self.scenarios: Dict[str, ChaosScenario] = {}
        self.active_faults: Dict[str, threading.Thread] = {}
        self.metrics = {
            "injections": 0,
            "recoveries": 0,
            "failures": 0,
            "start_time": datetime.now()
        }
        
        if seed is not None:
            random.seed(seed)
            
        logger.info(f"ChaosMonkey initialized (enabled={enabled})")
    
    def enable(self):
        """Enable chaos injections"""
        self.enabled = True
        logger.info("ChaosMonkey enabled")
    
    def disable(self):
        """Disable chaos injections"""
        self.enabled = False
        logger.info("ChaosMonkey disabled")
    
    def inject_latency(self, min_ms: int = 100, max_ms: int = 1000, 
                      probability: float = 1.0):
        """
        Decorator to inject random latency into function calls.
        
        Args:
            min_ms: Minimum latency in milliseconds
            max_ms: Maximum latency in milliseconds
            probability: Probability of injection (0.0 to 1.0)
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if self.enabled and random.random() < probability:
                    delay = random.uniform(min_ms, max_ms) / 1000.0
                    logger.debug(f"Injecting {delay*1000:.0f}ms latency into {func.__name__}")
                    time.sleep(delay)
                    self.metrics["injections"] += 1
                return func(*args, **kwargs)
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                if self.enabled and random.random() < probability:
                    delay = random.uniform(min_ms, max_ms) / 1000.0
                    logger.debug(f"Injecting {delay*1000:.0f}ms latency into {func.__name__}")
                    await asyncio.sleep(delay)
                    self.metrics["injections"] += 1
                return await func(*args, **kwargs)
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return wrapper
        return decorator
    
    def inject_exception(self, exception_type: type = Exception, 
                        message: str = "Chaos-induced failure",
                        probability: float = 0.1):
        """
        Decorator to inject exceptions into function calls.
        
        Args:
            exception_type: Type of exception to raise
            message: Exception message
            probability: Probability of injection (0.0 to 1.0)
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if self.enabled and random.random() < probability:
                    logger.debug(f"Injecting {exception_type.__name__} into {func.__name__}")
                    self.metrics["injections"] += 1
                    raise exception_type(message)
                return func(*args, **kwargs)
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                if self.enabled and random.random() < probability:
                    logger.debug(f"Injecting {exception_type.__name__} into {func.__name__}")
                    self.metrics["injections"] += 1
                    raise exception_type(message)
                return await func(*args, **kwargs)
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return wrapper
        return decorator
    
    def inject_resource_exhaustion(self, resource_type: str = "memory",
                                  intensity: float = 0.5):
        """
        Context manager to simulate resource exhaustion.
        
        Args:
            resource_type: Type of resource ('memory', 'cpu', 'disk', 'connections')
            intensity: Intensity of exhaustion (0.0 to 1.0)
        """
        @contextmanager
        def _resource_exhaustion():
            if not self.enabled:
                yield
                return
                
            logger.info(f"Starting {resource_type} exhaustion (intensity={intensity})")
            self.metrics["injections"] += 1
            
            try:
                if resource_type == "memory":
                    # Allocate memory
                    size_mb = int(psutil.virtual_memory().total / (1024 * 1024) * intensity)
                    _memory_hog = []
                    for _ in range(size_mb):
                        _memory_hog.append(b' ' * 1024 * 1024)  # 1MB chunks
                    logger.info(f"Allocated {size_mb}MB of memory")
                    
                elif resource_type == "cpu":
                    # Start CPU-intensive threads
                    def cpu_stress():
                        while True:
                            _ = [i * i for i in range(10000)]
                    
                    threads = []
                    cpu_count = psutil.cpu_count()
                    for _ in range(int(cpu_count * intensity)):
                        t = threading.Thread(target=cpu_stress, daemon=True)
                        t.start()
                        threads.append(t)
                    logger.info(f"Started {len(threads)} CPU stress threads")
                
                elif resource_type == "disk":
                    # Fill disk space
                    temp_file = "/tmp/chaos_disk_fill"
                    size_gb = intensity * 10  # Up to 10GB
                    with open(temp_file, 'wb') as f:
                        f.write(os.urandom(int(size_gb * 1024 * 1024 * 1024)))
                    logger.info(f"Created {size_gb}GB disk file")
                
                yield
                
            finally:
                # Cleanup
                if resource_type == "memory":
                    del _memory_hog
                elif resource_type == "disk":
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                logger.info(f"Cleaned up {resource_type} exhaustion")
                self.metrics["recoveries"] += 1
        
        return _resource_exhaustion()
    
    def network_partition(self, duration: float = 30.0, 
                         target_ips: List[str] = None):
        """
        Simulate network partition by blocking traffic to target IPs.
        
        Args:
            duration: Duration of partition in seconds
            target_ips: List of IPs to block (simulates partition)
        """
        if not self.enabled:
            return
            
        if target_ips is None:
            target_ips = ["127.0.0.1"]  # Default to localhost for testing
            
        logger.info(f"Starting network partition for {duration}s")
        self.metrics["injections"] += 1
        
        def _block_traffic():
            # This is a simulation - in production, you'd use iptables or similar
            logger.warning(f"SIMULATED: Blocking traffic to {target_ips}")
            time.sleep(duration)
            logger.warning(f"SIMULATED: Restoring traffic to {target_ips}")
            self.metrics["recoveries"] += 1
        
        thread = threading.Thread(target=_block_traffic, daemon=True)
        thread.start()
        self.active_faults[f"network_partition_{len(self.active_faults)}"] = thread
    
    def disk_failure(self, duration: float = 30.0, 
                    failure_type: str = "read_only"):
        """
        Simulate disk failure.
        
        Args:
            duration: Duration of failure in seconds
            failure_type: Type of failure ('read_only', 'slow', 'corrupt')
        """
        if not self.enabled:
            return
            
        logger.info(f"Starting disk failure simulation ({failure_type}) for {duration}s")
        self.metrics["injections"] += 1
        
        def _simulate_disk_failure():
            # This is a simulation - in production, you'd use filesystem hooks
            logger.warning(f"SIMULATED: Disk failure ({failure_type})")
            time.sleep(duration)
            logger.warning(f"SIMULATED: Disk recovered")
            self.metrics["recoveries"] += 1
        
        thread = threading.Thread(target=_simulate_disk_failure, daemon=True)
        thread.start()
        self.active_faults[f"disk_failure_{len(self.active_faults)}"] = thread
    
    def cpu_spike(self, duration: float = 30.0, intensity: float = 0.8):
        """
        Simulate CPU spike.
        
        Args:
            duration: Duration of spike in seconds
            intensity: Intensity of spike (0.0 to 1.0)
        """
        if not self.enabled:
            return
            
        logger.info(f"Starting CPU spike (intensity={intensity}) for {duration}s")
        self.metrics["injections"] += 1
        
        def _cpu_spike():
            end_time = time.time() + duration
            while time.time() < end_time:
                # CPU-intensive calculation
                _ = [i * i for i in range(int(100000 * intensity))]
            self.metrics["recoveries"] += 1
        
        thread = threading.Thread(target=_cpu_spike, daemon=True)
        thread.start()
        self.active_faults[f"cpu_spike_{len(self.active_faults)}"] = thread
    
    def run_scenario(self, scenario: ChaosScenario):
        """
        Run a predefined chaos scenario.
        
        Args:
            scenario: ChaosScenario configuration
        """
        if not self.enabled:
            logger.info(f"ChaosMonkey disabled, skipping scenario: {scenario.name}")
            return
            
        logger.info(f"Running chaos scenario: {scenario.name}")
        self.scenarios[scenario.name] = scenario
        
        if scenario.fault_type == FaultType.NETWORK_PARTITION:
            self.network_partition(
                duration=scenario.duration,
                target_ips=scenario.metadata.get("target_ips")
            )
            
        elif scenario.fault_type == FaultType.DISK_FAILURE:
            self.disk_failure(
                duration=scenario.duration,
                failure_type=scenario.metadata.get("failure_type", "read_only")
            )
            
        elif scenario.fault_type == FaultType.CPU_SPIKE:
            self.cpu_spike(
                duration=scenario.duration,
                intensity=scenario.intensity
            )
            
        elif scenario.fault_type == FaultType.MEMORY_LEAK:
            with self.inject_resource_exhaustion("memory", scenario.intensity):
                time.sleep(scenario.duration)
                
        elif scenario.fault_type == FaultType.LATENCY:
            # This would be applied via decorators
            logger.info(f"Latency scenario configured: {scenario.intensity*1000:.0f}ms average")
            
        elif scenario.fault_type == FaultType.EXCEPTION:
            logger.info(f"Exception injection scenario configured: {scenario.probability*100:.0f}% probability")
    
    def run_game_day(self, scenarios: List[ChaosScenario], 
                    recovery_validation: Callable = None):
        """
        Run a game day with multiple scenarios.
        
        Args:
            scenarios: List of scenarios to run
            recovery_validation: Function to validate recovery after each scenario
        """
        logger.info(f"Starting game day with {len(scenarios)} scenarios")
        
        results = []
        for i, scenario in enumerate(scenarios, 1):
            logger.info(f"Game Day - Scenario {i}/{len(scenarios)}: {scenario.name}")
            
            try:
                self.run_scenario(scenario)
                time.sleep(scenario.duration + 5)  # Wait for scenario to complete
                
                # Validate recovery
                if recovery_validation:
                    recovery_ok = recovery_validation()
                    results.append({
                        "scenario": scenario.name,
                        "success": recovery_ok,
                        "timestamp": datetime.now()
                    })
                    
                    if not recovery_ok:
                        logger.error(f"Recovery validation failed for {scenario.name}")
                        
            except Exception as e:
                logger.error(f"Scenario {scenario.name} failed: {e}")
                self.metrics["failures"] += 1
                results.append({
                    "scenario": scenario.name,
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.now()
                })
        
        # Generate game day report
        self._generate_game_day_report(results)
        return results
    
    def _generate_game_day_report(self, results: List[Dict]):
        """Generate a game day report"""
        total = len(results)
        successful = sum(1 for r in results if r.get("success", False))
        
        report = f"""
        ============================================
        CHAOS GAME DAY REPORT
        ============================================
        Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        Total Scenarios: {total}
        Successful: {successful}
        Failed: {total - successful}
        Success Rate: {(successful/total*100):.1f}%
        
        Metrics:
        - Total Injections: {self.metrics['injections']}
        - Total Recoveries: {self.metrics['recoveries']}
        - Total Failures: {self.metrics['failures']}
        
        Scenario Results:
        """
        
        for result in results:
            status = "✓" if result.get("success", False) else "✗"
            report += f"  {status} {result['scenario']}\n"
            if "error" in result:
                report += f"      Error: {result['error']}\n"
        
        report += "============================================"
        logger.info(report)
        
        # Save report to file
        report_file = f"chaos_game_day_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(report_file, "w") as f:
            f.write(report)
        
        return report
    
    def get_metrics(self) -> Dict:
        """Get chaos engineering metrics"""
        return {
            **self.metrics,
            "uptime": (datetime.now() - self.metrics["start_time"]).total_seconds(),
            "active_faults": len(self.active_faults),
            "scenarios_run": len(self.scenarios)
        }
    
    def stop_all_faults(self):
        """Stop all active faults"""
        logger.info("Stopping all active faults")
        for fault_id, thread in self.active_faults.items():
            # Note: In a real implementation, you'd need a way to stop threads gracefully
            logger.debug(f"Stopping fault: {fault_id}")
        
        self.active_faults.clear()
        logger.info("All faults stopped")


class ChaosTest:
    """
    Base class for automated chaos tests that can be run in CI/CD.
    
    Usage:
        class MySystemChaosTest(ChaosTest):
            def setup(self):
                # Initialize your system
                self.system = MySystem()
                
            def test_network_partition(self):
                # Test system behavior during network partition
                with self.chaos.network_partition(duration=10):
                    result = self.system.make_request()
                    self.assert_resilient(result)
                    
            def validate_recovery(self):
                # Validate system recovers after chaos
                return self.system.is_healthy()
    """
    
    def __init__(self, chaos_monkey: Optional[ChaosMonkey] = None):
        self.chaos = chaos_monkey or ChaosMonkey()
        self.test_results = []
    
    def setup(self):
        """Setup test environment"""
        pass
    
    def teardown(self):
        """Cleanup after tests"""
        self.chaos.stop_all_faults()
    
    def run_all_tests(self) -> bool:
        """Run all chaos tests"""
        logger.info("Running chaos tests")
        self.setup()
        
        try:
            # Get all test methods
            test_methods = [
                getattr(self, method) for method in dir(self)
                if method.startswith("test_") and callable(getattr(self, method))
            ]
            
            for test_method in test_methods:
                test_name = test_method.__name__
                logger.info(f"Running test: {test_name}")
                
                try:
                    test_method()
                    self.test_results.append({
                        "test": test_name,
                        "success": True
                    })
                    logger.info(f"✓ {test_name} passed")
                    
                except Exception as e:
                    self.test_results.append({
                        "test": test_name,
                        "success": False,
                        "error": str(e)
                    })
                    logger.error(f"✗ {test_name} failed: {e}")
            
            # Validate overall recovery
            recovery_ok = self.validate_recovery()
            if not recovery_ok:
                logger.error("System failed to recover after chaos tests")
            
            return all(r["success"] for r in self.test_results) and recovery_ok
            
        finally:
            self.teardown()
    
    def validate_recovery(self) -> bool:
        """Validate system recovery after chaos tests"""
        return True
    
    def assert_resilient(self, result: Any, message: str = ""):
        """Assert that system remained resilient during chaos"""
        if result is None or (isinstance(result, bool) and not result):
            raise AssertionError(f"System failed during chaos: {message}")


# Predefined game day scenarios
GAME_DAY_SCENARIOS = {
    "basic_resilience": [
        ChaosScenario(
            name="network_latency",
            fault_type=FaultType.LATENCY,
            duration=60.0,
            intensity=0.3,
            metadata={"min_ms": 100, "max_ms": 500}
        ),
        ChaosScenario(
            name="service_unavailable",
            fault_type=FaultType.SERVICE_UNAVAILABLE,
            duration=30.0,
            probability=0.2
        ),
        ChaosScenario(
            name="cpu_stress",
            fault_type=FaultType.CPU_SPIKE,
            duration=45.0,
            intensity=0.7
        )
    ],
    "advanced_failure": [
        ChaosScenario(
            name="network_partition",
            fault_type=FaultType.NETWORK_PARTITION,
            duration=120.0,
            metadata={"target_ips": ["10.0.0.1", "10.0.0.2"]}
        ),
        ChaosScenario(
            name="disk_failure",
            fault_type=FaultType.DISK_FAILURE,
            duration=90.0,
            metadata={"failure_type": "slow"}
        ),
        ChaosScenario(
            name="memory_exhaustion",
            fault_type=FaultType.MEMORY_LEAK,
            duration=60.0,
            intensity=0.8
        )
    ]
}


# Integration with existing system designs
def integrate_with_system_design(system_module: str, chaos_config: Dict = None):
    """
    Decorator to integrate chaos engineering with existing system designs.
    
    Args:
        system_module: Name of the system module to protect
        chaos_config: Chaos configuration dictionary
    """
    if chaos_config is None:
        chaos_config = {}
    
    def decorator(cls):
        # Add chaos monkey to the class
        cls.chaos = ChaosMonkey(
            enabled=chaos_config.get("enabled", True),
            seed=chaos_config.get("seed")
        )
        
        # Wrap methods with chaos injections
        for attr_name in dir(cls):
            if not attr_name.startswith("_"):
                attr = getattr(cls, attr_name)
                if callable(attr) and not isinstance(attr, type):
                    # Apply latency injection to public methods
                    wrapped = cls.chaos.inject_latency(
                        min_ms=chaos_config.get("min_latency_ms", 50),
                        max_ms=chaos_config.get("max_latency_ms", 200),
                        probability=chaos_config.get("latency_probability", 0.1)
                    )(attr)
                    setattr(cls, attr_name, wrapped)
        
        return cls
    
    return decorator


# Example usage with existing system designs
if __name__ == "__main__":
    # Example 1: Basic chaos injection
    chaos = ChaosMonkey(enabled=True)
    
    @chaos.inject_latency(min_ms=100, max_ms=300, probability=0.3)
    def example_function():
        print("Function executing")
        return "success"
    
    # Example 2: Resource exhaustion
    with chaos.inject_resource_exhaustion("cpu", intensity=0.5):
        print("Running with CPU exhaustion")
        time.sleep(5)
    
    # Example 3: Game day
    game_day_scenarios = GAME_DAY_SCENARIOS["basic_resilience"]
    chaos.run_game_day(game_day_scenarios)
    
    # Example 4: Chaos test
    class ExampleChaosTest(ChaosTest):
        def test_latency_injection(self):
            @self.chaos.inject_latency(min_ms=200, max_ms=500)
            def test_operation():
                return True
            
            result = test_operation()
            self.assert_resilient(result, "Operation should complete despite latency")
    
    test = ExampleChaosTest()
    success = test.run_all_tests()
    print(f"Chaos tests {'passed' if success else 'failed'}")