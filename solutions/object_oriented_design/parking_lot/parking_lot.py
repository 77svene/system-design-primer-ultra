from abc import ABCMeta, abstractmethod
from enum import Enum
import time
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ChaosToolkit:
    """Chaos engineering toolkit for injecting failures into the parking lot system."""
    
    def __init__(self, enabled=False):
        self.enabled = enabled
        self.network_partition_active = False
        self.disk_failure_active = False
        self.cpu_spike_active = False
        self.latency_ms = 0
        self.resource_exhaustion_level = 0  # 0-100 percentage
        
    def inject_network_partition(self, duration_seconds=30):
        """Simulate network partition making parking levels unreachable."""
        if not self.enabled:
            return
        logger.warning("CHAOS: Injecting network partition")
        self.network_partition_active = True
        # In a real system, this would be scheduled to clear after duration
        # For simplicity, we'll simulate with a flag
        
    def inject_disk_failure(self, duration_seconds=60):
        """Simulate disk failure preventing spot read/write operations."""
        if not self.enabled:
            return
        logger.warning("CHAOS: Injecting disk failure")
        self.disk_failure_active = True
        
    def inject_cpu_spike(self, duration_seconds=45):
        """Simulate CPU spike causing processing delays."""
        if not self.enabled:
            return
        logger.warning("CHAOS: Injecting CPU spike")
        self.cpu_spike_active = True
        
    def inject_latency(self, latency_ms=500):
        """Inject artificial latency into operations."""
        if not self.enabled:
            return
        self.latency_ms = latency_ms
        
    def inject_resource_exhaustion(self, level=80):
        """Simulate resource exhaustion (memory/disk space)."""
        if not self.enabled:
            return
        self.resource_exhaustion_level = level
        logger.warning(f"CHAOS: Resource exhaustion at {level}%")
        
    def clear_all_faults(self):
        """Clear all injected faults."""
        self.network_partition_active = False
        self.disk_failure_active = False
        self.cpu_spike_active = False
        self.latency_ms = 0
        self.resource_exhaustion_level = 0
        logger.info("CHAOS: All faults cleared")
        
    def simulate_chaos(self):
        """Apply chaos effects based on current configuration."""
        if not self.enabled:
            return
            
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)
            
        if self.cpu_spike_active:
            # Simulate CPU spike with busy wait
            start = time.time()
            while time.time() - start < 0.1:  # 100ms busy wait
                pass
                
        if random.random() < (self.resource_exhaustion_level / 100.0):
            raise ResourceExhaustionError("System resources exhausted")
            
    def check_network_partition(self):
        """Check if network partition should block operation."""
        if self.enabled and self.network_partition_active:
            raise NetworkPartitionError("Network partition detected")
            
    def check_disk_failure(self):
        """Check if disk failure should block operation."""
        if self.enabled and self.disk_failure_active:
            raise DiskFailureError("Disk failure detected")


class ChaosError(Exception):
    """Base class for chaos engineering errors."""
    pass


class NetworkPartitionError(ChaosError):
    """Raised when network partition is simulated."""
    pass


class DiskFailureError(ChaosError):
    """Raised when disk failure is simulated."""
    pass


class ResourceExhaustionError(ChaosError):
    """Raised when resource exhaustion is simulated."""
    pass


# Global chaos toolkit instance
chaos_toolkit = ChaosToolkit()


class VehicleSize(Enum):
    MOTORCYCLE = 0
    COMPACT = 1
    LARGE = 2


class Vehicle(metaclass=ABCMeta):
    def __init__(self, vehicle_size, license_plate, spot_size):
        self.vehicle_size = vehicle_size
        self.license_plate = license_plate
        self.spot_size = spot_size
        self.spots_taken = []

    def clear_spots(self):
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_network_partition()
        
        for spot in self.spots_taken:
            spot.remove_vehicle(self)
        self.spots_taken = []

    def take_spot(self, spot):
        chaos_toolkit.simulate_chaos()
        self.spots_taken.append(spot)

    @abstractmethod
    def can_fit_in_spot(self, spot):
        pass


class Motorcycle(Vehicle):
    def __init__(self, license_plate):
        super(Motorcycle, self).__init__(VehicleSize.MOTORCYCLE, license_plate, spot_size=1)

    def can_fit_in_spot(self, spot):
        chaos_toolkit.simulate_chaos()
        return True


class Car(Vehicle):
    def __init__(self, license_plate):
        super(Car, self).__init__(VehicleSize.COMPACT, license_plate, spot_size=1)

    def can_fit_in_spot(self, spot):
        chaos_toolkit.simulate_chaos()
        return spot.size in (VehicleSize.LARGE, VehicleSize.COMPACT)


class Bus(Vehicle):
    def __init__(self, license_plate):
        super(Bus, self).__init__(VehicleSize.LARGE, license_plate, spot_size=5)

    def can_fit_in_spot(self, spot):
        chaos_toolkit.simulate_chaos()
        return spot.size == VehicleSize.LARGE


class ParkingLot(object):
    def __init__(self, num_levels):
        self.num_levels = num_levels
        self.levels = []  # List of Levels
        self.chaos_test_results = []

    def park_vehicle(self, vehicle):
        """Park a vehicle with chaos engineering hooks."""
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_network_partition()
        
        try:
            for level in self.levels:
                if level.park_vehicle(vehicle):
                    logger.info(f"Vehicle {vehicle.license_plate} parked successfully")
                    return True
            logger.warning(f"Failed to park vehicle {vehicle.license_plate}")
            return False
        except ChaosError as e:
            logger.error(f"Chaos error during parking: {e}")
            return False

    def run_chaos_test_scenario(self, scenario_name, vehicle_count=10):
        """Run automated chaos test scenario with validation."""
        logger.info(f"Starting chaos test scenario: {scenario_name}")
        
        test_vehicles = []
        for i in range(vehicle_count):
            if i % 3 == 0:
                test_vehicles.append(Motorcycle(f"TEST-{i}"))
            elif i % 3 == 1:
                test_vehicles.append(Car(f"TEST-{i}"))
            else:
                test_vehicles.append(Bus(f"TEST-{i}"))
        
        results = {
            'scenario': scenario_name,
            'total_vehicles': vehicle_count,
            'successful_parks': 0,
            'failed_parks': 0,
            'chaos_errors': []
        }
        
        for vehicle in test_vehicles:
            try:
                if self.park_vehicle(vehicle):
                    results['successful_parks'] += 1
                else:
                    results['failed_parks'] += 1
            except ChaosError as e:
                results['failed_parks'] += 1
                results['chaos_errors'].append(str(e))
        
        self.chaos_test_results.append(results)
        logger.info(f"Chaos test completed: {results['successful_parks']}/{vehicle_count} successful")
        return results

    def get_chaos_test_runbook(self):
        """Generate runbook for chaos test scenarios."""
        runbook = """
        PARKING LOT CHAOS ENGINEERING RUNBOOK
        =====================================
        
        Scenario 1: Network Partition
        - Inject network partition for 30 seconds
        - Attempt to park 20 vehicles
        - Expected: 50% failure rate due to partitioned levels
        
        Scenario 2: Disk Failure
        - Inject disk failure for 60 seconds
        - Attempt to park 15 vehicles
        - Expected: All parking operations fail
        
        Scenario 3: CPU Spike
        - Inject CPU spike for 45 seconds
        - Attempt to park 25 vehicles
        - Expected: Increased latency, some timeouts
        
        Scenario 4: Resource Exhaustion
        - Set resource exhaustion to 90%
        - Attempt to park 30 vehicles
        - Expected: Random failures due to resource constraints
        
        Scenario 5: Combined Chaos
        - Enable all chaos modes simultaneously
        - Run for 2 minutes
        - Monitor system recovery
        
        Recovery Procedures:
        1. Clear all faults using chaos_toolkit.clear_all_faults()
        2. Verify system returns to normal operation
        3. Check logs for error patterns
        4. Validate data consistency
        """
        return runbook


class Level(object):
    SPOTS_PER_ROW = 10

    def __init__(self, floor, total_spots):
        self.floor = floor
        self.num_spots = total_spots
        self.available_spots = 0
        self.spots = []  # List of ParkingSpots

    def spot_freed(self):
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_disk_failure()
        self.available_spots += 1

    def park_vehicle(self, vehicle):
        """Park vehicle on this level with chaos hooks."""
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_network_partition()
        
        try:
            spot = self._find_available_spot(vehicle)
            if spot is None:
                return None
            else:
                spot.park_vehicle(vehicle)
                return spot
        except DiskFailureError:
            logger.error(f"Disk failure on level {self.floor}")
            return None

    def _find_available_spot(self, vehicle):
        """Find an available spot where vehicle can fit, or return None."""
        chaos_toolkit.simulate_chaos()
        
        # Simulate network partition for this level
        if chaos_toolkit.enabled and random.random() < 0.3:
            raise NetworkPartitionError(f"Level {self.floor} unreachable")
        
        for spot in self.spots:
            if spot.is_available() and vehicle.can_fit_in_spot(spot):
                return spot
        return None

    def _park_starting_at_spot(self, spot, vehicle):
        """Occupy starting at spot.spot_number to vehicle.spot_size."""
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_disk_failure()
        # Implementation would go here


class ParkingSpot(object):
    def __init__(self, level, row, spot_number, spot_size, vehicle_size):
        self.level = level
        self.row = row
        self.spot_number = spot_number
        self.spot_size = spot_size
        self.vehicle_size = vehicle_size
        self.vehicle = None

    def is_available(self):
        chaos_toolkit.simulate_chaos()
        return True if self.vehicle is None else False

    def can_fit_vehicle(self, vehicle):
        chaos_toolkit.simulate_chaos()
        if self.vehicle is not None:
            return False
        return vehicle.can_fit_in_spot(self)

    def park_vehicle(self, vehicle):
        """Park vehicle in spot with chaos hooks."""
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_disk_failure()
        
        if chaos_toolkit.enabled and random.random() < 0.1:
            raise DiskFailureError(f"Failed to write to spot {self.spot_number}")
        
        self.vehicle = vehicle
        vehicle.take_spot(self)
        self.level.available_spots -= 1

    def remove_vehicle(self):
        """Remove vehicle from spot with chaos hooks."""
        chaos_toolkit.simulate_chaos()
        chaos_toolkit.check_disk_failure()
        
        if self.vehicle:
            self.vehicle = None
            self.level.spot_freed()


# Game Day Scenario Runner
class GameDayRunner:
    """Run game day scenarios for chaos engineering."""
    
    @staticmethod
    def run_game_day(parking_lot):
        """Execute a full game day with multiple chaos scenarios."""
        logger.info("=== STARTING GAME DAY ===")
        
        scenarios = [
            ("Network Partition Test", lambda: chaos_toolkit.inject_network_partition(30)),
            ("Disk Failure Test", lambda: chaos_toolkit.inject_disk_failure(60)),
            ("CPU Spike Test", lambda: chaos_toolkit.inject_cpu_spike(45)),
            ("Resource Exhaustion Test", lambda: chaos_toolkit.inject_resource_exhaustion(85)),
            ("Combined Chaos Test", lambda: (
                chaos_toolkit.inject_network_partition(20),
                chaos_toolkit.inject_cpu_spike(30),
                chaos_toolkit.inject_resource_exhaustion(70)
            ))
        ]
        
        results = []
        for scenario_name, setup_func in scenarios:
            logger.info(f"\n--- Running Scenario: {scenario_name} ---")
            chaos_toolkit.clear_all_faults()
            setup_func()
            
            result = parking_lot.run_chaos_test_scenario(scenario_name, vehicle_count=15)
            results.append(result)
            
            # Clear faults after each scenario
            chaos_toolkit.clear_all_faults()
            time.sleep(2)  # Recovery period
        
        logger.info("\n=== GAME DAY COMPLETE ===")
        return results


# CI/CD Integration Hook
def run_chaos_tests_in_ci():
    """Hook for CI/CD pipeline to run chaos tests."""
    logger.info("Running chaos tests in CI/CD pipeline")
    
    # Enable chaos toolkit for testing
    chaos_toolkit.enabled = True
    
    parking_lot = ParkingLot(num_levels=3)
    
    # Run automated chaos tests
    test_results = []
    test_results.append(parking_lot.run_chaos_test_scenario("CI_Network_Partition", 10))
    test_results.append(parking_lot.run_chaos_test_scenario("CI_Disk_Failure", 10))
    test_results.append(parking_lot.run_chaos_test_scenario("CI_Resource_Exhaustion", 10))
    
    # Validate resilience
    total_success = sum(r['successful_parks'] for r in test_results)
    total_attempts = sum(r['total_vehicles'] for r in test_results)
    
    resilience_score = (total_success / total_attempts) * 100
    logger.info(f"System resilience score: {resilience_score:.1f}%")
    
    # Disable chaos after tests
    chaos_toolkit.enabled = False
    chaos_toolkit.clear_all_faults()
    
    return resilience_score >= 70  # Pass if 70% or better resilience


if __name__ == "__main__":
    # Example usage with chaos engineering
    chaos_toolkit.enabled = True
    
    parking_lot = ParkingLot(num_levels=3)
    
    # Run a game day scenario
    game_day = GameDayRunner()
    results = game_day.run_game_day(parking_lot)
    
    # Print runbook
    print(parking_lot.get_chaos_test_runbook())