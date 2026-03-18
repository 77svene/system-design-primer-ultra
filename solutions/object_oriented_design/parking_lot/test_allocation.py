"""
Parking Lot System with Real-Time Allocation

Complete implementation featuring:
- Dynamic spot allocation algorithms (first-fit, best-fit)
- Real-time availability tracking
- Dynamic pricing based on demand
- Reservation system
- Multi-level parking support
- Vehicle type optimization
"""

from enum import Enum
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import heapq
import threading
from collections import defaultdict


class VehicleType(Enum):
    MOTORCYCLE = 1
    CAR = 2
    TRUCK = 3
    ELECTRIC = 4


class SpotSize(Enum):
    SMALL = 1
    MEDIUM = 2
    LARGE = 3
    ELECTRIC = 4


class AllocationStrategy(Enum):
    FIRST_FIT = 1
    BEST_FIT = 2
    NEAREST = 3


@dataclass
class Vehicle:
    license_plate: str
    vehicle_type: VehicleType
    spot_size: SpotSize
    entry_time: Optional[datetime] = None
    
    def __post_init__(self):
        if not self.spot_size:
            self.spot_size = self._get_default_spot_size()
    
    def _get_default_spot_size(self) -> SpotSize:
        mapping = {
            VehicleType.MOTORCYCLE: SpotSize.SMALL,
            VehicleType.CAR: SpotSize.MEDIUM,
            VehicleType.TRUCK: SpotSize.LARGE,
            VehicleType.ELECTRIC: SpotSize.ELECTRIC
        }
        return mapping[self.vehicle_type]


@dataclass
class ParkingSpot:
    spot_id: str
    floor: int
    spot_size: SpotSize
    is_occupied: bool = False
    vehicle: Optional[Vehicle] = None
    reserved_until: Optional[datetime] = None
    is_electric: bool = False
    
    def can_fit_vehicle(self, vehicle: Vehicle) -> bool:
        if self.is_occupied or self.is_reserved():
            return False
        
        if vehicle.vehicle_type == VehicleType.ELECTRIC:
            return self.spot_size == SpotSize.ELECTRIC
        
        size_hierarchy = {
            SpotSize.SMALL: [SpotSize.SMALL],
            SpotSize.MEDIUM: [SpotSize.MEDIUM, SpotSize.LARGE],
            SpotSize.LARGE: [SpotSize.LARGE],
            SpotSize.ELECTRIC: [SpotSize.ELECTRIC]
        }
        
        return self.spot_size in size_hierarchy.get(vehicle.spot_size, [])
    
    def is_reserved(self) -> bool:
        if not self.reserved_until:
            return False
        return datetime.now() < self.reserved_until


class ParkingFloor:
    def __init__(self, floor_number: int, spots_config: Dict[SpotSize, int]):
        self.floor_number = floor_number
        self.spots: List[ParkingSpot] = []
        self._initialize_spots(spots_config)
    
    def _initialize_spots(self, spots_config: Dict[SpotSize, int]):
        spot_counter = 0
        for spot_size, count in spots_config.items():
            for _ in range(count):
                spot_id = f"F{self.floor_number}-S{spot_counter}"
                is_electric = spot_size == SpotSize.ELECTRIC
                self.spots.append(ParkingSpot(
                    spot_id=spot_id,
                    floor=self.floor_number,
                    spot_size=spot_size,
                    is_electric=is_electric
                ))
                spot_counter += 1
    
    def get_available_spots(self, vehicle_type: Optional[VehicleType] = None) -> List[ParkingSpot]:
        available = []
        for spot in self.spots:
            if spot.is_occupied or spot.is_reserved():
                continue
            if vehicle_type and vehicle_type == VehicleType.ELECTRIC:
                if spot.spot_size == SpotSize.ELECTRIC:
                    available.append(spot)
            else:
                if spot.spot_size != SpotSize.ELECTRIC:
                    available.append(spot)
        return available


class DynamicPricing:
    def __init__(self, base_rate: float = 2.0):
        self.base_rate = base_rate
        self.demand_multiplier = 1.0
        self.occupancy_history = []
        self.lock = threading.Lock()
    
    def update_demand(self, occupancy_rate: float):
        with self.lock:
            self.occupancy_history.append(occupancy_rate)
            if len(self.occupancy_history) > 24:  # Keep last 24 hours
                self.occupancy_history.pop(0)
            
            # Calculate demand multiplier based on recent occupancy
            avg_occupancy = sum(self.occupancy_history) / len(self.occupancy_history)
            if avg_occupancy > 0.9:
                self.demand_multiplier = 2.0
            elif avg_occupancy > 0.7:
                self.demand_multiplier = 1.5
            elif avg_occupancy > 0.5:
                self.demand_multiplier = 1.2
            else:
                self.demand_multiplier = 1.0
    
    def calculate_price(self, spot_size: SpotSize, hours: float) -> float:
        size_multiplier = {
            SpotSize.SMALL: 0.8,
            SpotSize.MEDIUM: 1.0,
            SpotSize.LARGE: 1.5,
            SpotSize.ELECTRIC: 1.2
        }
        
        base_cost = self.base_rate * hours * size_multiplier[spot_size]
        return base_cost * self.demand_multiplier


class Reservation:
    def __init__(self, reservation_id: str, vehicle: Vehicle, spot: ParkingSpot, 
                 start_time: datetime, end_time: datetime):
        self.reservation_id = reservation_id
        self.vehicle = vehicle
        self.spot = spot
        self.start_time = start_time
        self.end_time = end_time
        self.is_active = False
    
    def is_valid(self) -> bool:
        now = datetime.now()
        return self.start_time <= now <= self.end_time


class ParkingLot:
    def __init__(self, name: str, floors_config: List[Dict[SpotSize, int]], 
                 allocation_strategy: AllocationStrategy = AllocationStrategy.FIRST_FIT):
        self.name = name
        self.floors: List[ParkingFloor] = []
        self.allocation_strategy = allocation_strategy
        self.pricing = DynamicPricing()
        self.reservations: Dict[str, Reservation] = {}
        self.vehicle_to_spot: Dict[str, ParkingSpot] = {}
        self.lock = threading.RLock()
        
        self._initialize_floors(floors_config)
    
    def _initialize_floors(self, floors_config: List[Dict[SpotSize, int]]):
        for floor_num, config in enumerate(floors_config, 1):
            self.floors.append(ParkingFloor(floor_num, config))
    
    def _find_spot_first_fit(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        for floor in self.floors:
            for spot in floor.spots:
                if spot.can_fit_vehicle(vehicle):
                    return spot
        return None
    
    def _find_spot_best_fit(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        best_spot = None
        best_size_diff = float('inf')
        
        for floor in self.floors:
            for spot in floor.spots:
                if spot.can_fit_vehicle(vehicle):
                    # Calculate size difference (prefer smaller spots that fit)
                    size_values = {
                        SpotSize.SMALL: 1,
                        SpotSize.MEDIUM: 2,
                        SpotSize.LARGE: 3,
                        SpotSize.ELECTRIC: 4
                    }
                    size_diff = size_values[spot.spot_size] - size_values[vehicle.spot_size]
                    
                    if size_diff >= 0 and size_diff < best_size_diff:
                        best_size_diff = size_diff
                        best_spot = spot
        
        return best_spot
    
    def _find_nearest_spot(self, vehicle: Vehicle, entry_floor: int = 1) -> Optional[ParkingSpot]:
        # Find nearest available spot to entry point
        nearest_spot = None
        min_distance = float('inf')
        
        for floor in self.floors:
            for spot in floor.spots:
                if spot.can_fit_vehicle(vehicle):
                    distance = abs(floor.floor_number - entry_floor)
                    if distance < min_distance:
                        min_distance = distance
                        nearest_spot = spot
        
        return nearest_spot
    
    def find_available_spot(self, vehicle: Vehicle, entry_floor: int = 1) -> Optional[ParkingSpot]:
        with self.lock:
            if self.allocation_strategy == AllocationStrategy.FIRST_FIT:
                return self._find_spot_first_fit(vehicle)
            elif self.allocation_strategy == AllocationStrategy.BEST_FIT:
                return self._find_spot_best_fit(vehicle)
            else:  # NEAREST
                return self._find_nearest_spot(vehicle, entry_floor)
    
    def park_vehicle(self, vehicle: Vehicle, entry_time: Optional[datetime] = None) -> Tuple[bool, str]:
        with self.lock:
            if vehicle.license_plate in self.vehicle_to_spot:
                return False, f"Vehicle {vehicle.license_plate} is already parked"
            
            spot = self.find_available_spot(vehicle)
            if not spot:
                return False, "No available spot for this vehicle type"
            
            spot.is_occupied = True
            spot.vehicle = vehicle
            vehicle.entry_time = entry_time or datetime.now()
            self.vehicle_to_spot[vehicle.license_plate] = spot
            
            self._update_pricing()
            return True, f"Parked at {spot.spot_id}"
    
    def remove_vehicle(self, license_plate: str, exit_time: Optional[datetime] = None) -> Tuple[bool, float]:
        with self.lock:
            if license_plate not in self.vehicle_to_spot:
                return False, 0.0
            
            spot = self.vehicle_to_spot[license_plate]
            vehicle = spot.vehicle
            exit_time = exit_time or datetime.now()
            
            # Calculate parking duration and cost
            duration = exit_time - vehicle.entry_time
            hours = max(1, duration.total_seconds() / 3600)  # Minimum 1 hour
            cost = self.pricing.calculate_price(spot.spot_size, hours)
            
            # Clear the spot
            spot.is_occupied = False
            spot.vehicle = None
            del self.vehicle_to_spot[license_plate]
            
            self._update_pricing()
            return True, cost
    
    def make_reservation(self, vehicle: Vehicle, start_time: datetime, 
                        duration_hours: float) -> Tuple[bool, str]:
        with self.lock:
            end_time = start_time + timedelta(hours=duration_hours)
            
            # Find an available spot for reservation
            spot = self.find_available_spot(vehicle)
            if not spot:
                return False, "No available spot for reservation"
            
            reservation_id = f"RES-{len(self.reservations) + 1:06d}"
            reservation = Reservation(reservation_id, vehicle, spot, start_time, end_time)
            
            spot.reserved_until = end_time
            self.reservations[reservation_id] = reservation
            
            return True, reservation_id
    
    def cancel_reservation(self, reservation_id: str) -> bool:
        with self.lock:
            if reservation_id not in self.reservations:
                return False
            
            reservation = self.reservations[reservation_id]
            reservation.spot.reserved_until = None
            del self.reservations[reservation_id]
            
            return True
    
    def get_availability_report(self) -> Dict:
        with self.lock:
            report = {
                'total_spots': 0,
                'available_spots': 0,
                'occupied_spots': 0,
                'reserved_spots': 0,
                'by_floor': {},
                'by_type': defaultdict(int)
            }
            
            for floor in self.floors:
                floor_report = {
                    'total': len(floor.spots),
                    'available': 0,
                    'occupied': 0,
                    'reserved': 0
                }
                
                for spot in floor.spots:
                    report['total_spots'] += 1
                    report['by_type'][spot.spot_size.name] += 1
                    
                    if spot.is_occupied:
                        report['occupied_spots'] += 1
                        floor_report['occupied'] += 1
                    elif spot.is_reserved():
                        report['reserved_spots'] += 1
                        floor_report['reserved'] += 1
                    else:
                        report['available_spots'] += 1
                        floor_report['available'] += 1
                
                report['by_floor'][floor.floor_number] = floor_report
            
            return report
    
    def _update_pricing(self):
        report = self.get_availability_report()
        occupancy_rate = report['occupied_spots'] / report['total_spots']
        self.pricing.update_demand(occupancy_rate)
    
    def optimize_layout(self) -> Dict:
        """Analyze usage patterns and suggest layout optimizations"""
        report = self.get_availability_report()
        suggestions = []
        
        # Analyze spot type utilization
        for spot_type, count in report['by_type'].items():
            utilization = report['occupied_spots'] / count if count > 0 else 0
            if utilization > 0.9:
                suggestions.append(f"Consider adding more {spot_type} spots (high demand)")
            elif utilization < 0.3:
                suggestions.append(f"Consider reducing {spot_type} spots (low demand)")
        
        return {
            'current_utilization': report['occupied_spots'] / report['total_spots'],
            'suggestions': suggestions,
            'peak_hours': self._analyze_peak_hours()
        }
    
    def _analyze_peak_hours(self) -> List[int]:
        """Simple peak hour analysis based on entry times"""
        hour_counts = defaultdict(int)
        
        for vehicle in self.vehicle_to_spot.values():
            if vehicle.entry_time:
                hour_counts[vehicle.entry_time.hour] += 1
        
        # Return top 3 peak hours
        peak_hours = heapq.nlargest(3, hour_counts.items(), key=lambda x: x[1])
        return [hour for hour, _ in peak_hours]


class ParkingLotManager:
    """Singleton manager for multiple parking lots"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance.parking_lots: Dict[str, ParkingLot] = {}
            return cls._instance
    
    def add_parking_lot(self, parking_lot: ParkingLot):
        self.parking_lots[parking_lot.name] = parking_lot
    
    def find_nearest_parking_lot(self, location: Tuple[float, float]) -> Optional[ParkingLot]:
        # In a real implementation, this would use actual coordinates
        # For now, return the first available lot with space
        for lot in self.parking_lots.values():
            report = lot.get_availability_report()
            if report['available_spots'] > 0:
                return lot
        return None
    
    def get_system_status(self) -> Dict:
        status = {
            'total_lots': len(self.parking_lots),
            'total_spots': 0,
            'total_available': 0,
            'lots': {}
        }
        
        for name, lot in self.parking_lots.items():
            report = lot.get_availability_report()
            status['total_spots'] += report['total_spots']
            status['total_available'] += report['available_spots']
            status['lots'][name] = {
                'available': report['available_spots'],
                'total': report['total_spots'],
                'utilization': report['occupied_spots'] / report['total_spots']
            }
        
        return status


# Example usage and testing
def create_sample_parking_lot() -> ParkingLot:
    """Create a sample parking lot for demonstration"""
    floors_config = [
        {SpotSize.SMALL: 10, SpotSize.MEDIUM: 20, SpotSize.LARGE: 5, SpotSize.ELECTRIC: 3},
        {SpotSize.SMALL: 15, SpotSize.MEDIUM: 25, SpotSize.LARGE: 8, SpotSize.ELECTric: 5},
        {SpotSize.MEDIUM: 30, SpotSize.LARGE: 10, SpotSize.ELECTRIC: 8}
    ]
    
    return ParkingLot("Downtown Parking", floors_config, AllocationStrategy.BEST_FIT)


if __name__ == "__main__":
    # Demonstration of the parking lot system
    parking_lot = create_sample_parking_lot()
    
    # Create some vehicles
    car = Vehicle("ABC123", VehicleType.CAR, SpotSize.MEDIUM)
    truck = Vehicle("XYZ789", VehicleType.TRUCK, SpotSize.LARGE)
    motorcycle = Vehicle("MOTO01", VehicleType.MOTORCYCLE, SpotSize.SMALL)
    electric_car = Vehicle("ELEC01", VehicleType.ELECTRIC, SpotSize.ELECTRIC)
    
    # Park vehicles
    success, message = parking_lot.park_vehicle(car)
    print(f"Car: {message}")
    
    success, message = parking_lot.park_vehicle(truck)
    print(f"Truck: {message}")
    
    success, message = parking_lot.park_vehicle(motorcycle)
    print(f"Motorcycle: {message}")
    
    success, message = parking_lot.park_vehicle(electric_car)
    print(f"Electric Car: {message}")
    
    # Get availability report
    report = parking_lot.get_availability_report()
    print(f"\nAvailability Report:")
    print(f"Total spots: {report['total_spots']}")
    print(f"Available: {report['available_spots']}")
    print(f"Occupied: {report['occupied_spots']}")
    
    # Make a reservation
    future_time = datetime.now() + timedelta(hours=2)
    success, reservation_id = parking_lot.make_reservation(
        Vehicle("RES001", VehicleType.CAR, SpotSize.MEDIUM),
        future_time,
        3.0
    )
    print(f"\nReservation: {reservation_id}")
    
    # Get optimization suggestions
    optimization = parking_lot.optimize_layout()
    print(f"\nOptimization suggestions: {optimization['suggestions']}")
    
    # Remove a vehicle
    success, cost = parking_lot.remove_vehicle("ABC123")
    if success:
        print(f"\nCar removed. Cost: ${cost:.2f}")
    
    # Final report
    final_report = parking_lot.get_availability_report()
    print(f"\nFinal availability: {final_report['available_spots']} spots")