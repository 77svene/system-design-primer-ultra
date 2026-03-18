"""
Parking Lot System with Real-Time Allocation and Optimization
"""

from enum import Enum, auto
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import heapq
from dataclasses import dataclass
from collections import defaultdict
import threading
import time


class VehicleType(Enum):
    MOTORCYCLE = auto()
    CAR = auto()
    TRUCK = auto()
    ELECTRIC = auto()


class SpotSize(Enum):
    SMALL = 1      # Motorcycle
    MEDIUM = 2     # Car
    LARGE = 3      # Truck
    CHARGING = 2   # Electric vehicle charging spot


class AllocationStrategy(Enum):
    FIRST_FIT = auto()
    BEST_FIT = auto()
    WORST_FIT = auto()


@dataclass
class Vehicle:
    license_plate: str
    vehicle_type: VehicleType
    spot_size: SpotSize = None
    
    def __post_init__(self):
        if self.spot_size is None:
            self.spot_size = self._get_required_spot_size()
    
    def _get_required_spot_size(self) -> SpotSize:
        mapping = {
            VehicleType.MOTORCYCLE: SpotSize.SMALL,
            VehicleType.CAR: SpotSize.MEDIUM,
            VehicleType.TRUCK: SpotSize.LARGE,
            VehicleType.ELECTRIC: SpotSize.CHARGING
        }
        return mapping[self.vehicle_type]


@dataclass
class ParkingSpot:
    spot_id: str
    spot_size: SpotSize
    floor: int
    is_occupied: bool = False
    vehicle: Optional[Vehicle] = None
    reserved_until: Optional[datetime] = None
    hourly_rate: float = 0.0
    
    def is_available(self, current_time: datetime = None) -> bool:
        if current_time is None:
            current_time = datetime.now()
        
        if self.is_occupied:
            return False
        
        if self.reserved_until and current_time < self.reserved_until:
            return False
        
        return True
    
    def can_accommodate(self, vehicle: Vehicle) -> bool:
        return self.spot_size.value >= vehicle.spot_size.value


class DynamicPricingEngine:
    def __init__(self, base_rates: Dict[SpotSize, float]):
        self.base_rates = base_rates
        self.occupancy_history = []
        self.demand_multiplier = 1.0
    
    def calculate_price(self, spot: ParkingSpot, duration_hours: float, 
                       current_occupancy: float) -> float:
        base_rate = self.base_rates.get(spot.spot_size, 10.0)
        
        # Dynamic pricing based on occupancy
        if current_occupancy > 0.9:
            multiplier = 2.0  # Peak pricing
        elif current_occupancy > 0.7:
            multiplier = 1.5
        elif current_occupancy < 0.3:
            multiplier = 0.8  # Discount for low occupancy
        else:
            multiplier = 1.0
        
        # Time-based pricing (higher during peak hours)
        hour = datetime.now().hour
        if 8 <= hour <= 10 or 17 <= hour <= 19:  # Rush hours
            multiplier *= 1.2
        
        return base_rate * multiplier * duration_hours
    
    def update_occupancy(self, occupancy_rate: float):
        self.occupancy_history.append((datetime.now(), occupancy_rate))
        # Keep only last 24 hours
        cutoff = datetime.now() - timedelta(hours=24)
        self.occupancy_history = [
            (t, r) for t, r in self.occupancy_history if t > cutoff
        ]


class ParkingLot:
    def __init__(self, lot_id: str, floors: int, spots_per_floor: Dict[SpotSize, int]):
        self.lot_id = lot_id
        self.floors = floors
        self.spots: List[ParkingSpot] = []
        self.available_spots: Dict[SpotSize, List[ParkingSpot]] = defaultdict(list)
        self.occupied_spots: Dict[str, ParkingSpot] = {}  # license_plate -> spot
        self.reservations: Dict[str, List[ParkingSpot]] = defaultdict(list)
        self._initialize_spots(spots_per_floor)
        self.lock = threading.RLock()
    
    def _initialize_spots(self, spots_per_floor: Dict[SpotSize, int]):
        spot_counter = 0
        for floor in range(1, self.floors + 1):
            for spot_size, count in spots_per_floor.items():
                for _ in range(count):
                    spot_id = f"{self.lot_id}-{floor}-{spot_counter}"
                    spot = ParkingSpot(
                        spot_id=spot_id,
                        spot_size=spot_size,
                        floor=floor,
                        hourly_rate=self._get_base_rate(spot_size)
                    )
                    self.spots.append(spot)
                    self.available_spots[spot_size].append(spot)
                    spot_counter += 1
    
    def _get_base_rate(self, spot_size: SpotSize) -> float:
        rates = {
            SpotSize.SMALL: 5.0,
            SpotSize.MEDIUM: 10.0,
            SpotSize.LARGE: 20.0,
            SpotSize.CHARGING: 15.0
        }
        return rates.get(spot_size, 10.0)
    
    def get_occupancy_rate(self) -> float:
        with self.lock:
            total_spots = len(self.spots)
            occupied = len(self.occupied_spots)
            return occupied / total_spots if total_spots > 0 else 0
    
    def find_available_spot(self, vehicle: Vehicle, 
                           strategy: AllocationStrategy = AllocationStrategy.BEST_FIT,
                           preferred_floor: Optional[int] = None) -> Optional[ParkingSpot]:
        with self.lock:
            required_size = vehicle.spot_size
            
            # Get all available spots that can accommodate the vehicle
            candidate_spots = []
            for spot in self.spots:
                if (spot.is_available() and 
                    spot.can_accommodate(vehicle) and
                    (preferred_floor is None or spot.floor == preferred_floor)):
                    candidate_spots.append(spot)
            
            if not candidate_spots:
                return None
            
            # Apply allocation strategy
            if strategy == AllocationStrategy.FIRST_FIT:
                return candidate_spots[0]
            
            elif strategy == AllocationStrategy.BEST_FIT:
                # Find smallest spot that fits the vehicle
                candidate_spots.sort(key=lambda s: s.spot_size.value)
                return candidate_spots[0]
            
            elif strategy == AllocationStrategy.WORST_FIT:
                # Find largest available spot
                candidate_spots.sort(key=lambda s: s.spot_size.value, reverse=True)
                return candidate_spots[0]
            
            return candidate_spots[0]
    
    def park_vehicle(self, vehicle: Vehicle, spot: ParkingSpot) -> bool:
        with self.lock:
            if not spot.is_available() or not spot.can_accommodate(vehicle):
                return False
            
            spot.is_occupied = True
            spot.vehicle = vehicle
            self.occupied_spots[vehicle.license_plate] = spot
            
            # Remove from available spots
            if spot in self.available_spots[spot.spot_size]:
                self.available_spots[spot.spot_size].remove(spot)
            
            return True
    
    def remove_vehicle(self, license_plate: str) -> Optional[Tuple[Vehicle, ParkingSpot]]:
        with self.lock:
            if license_plate not in self.occupied_spots:
                return None
            
            spot = self.occupied_spots[license_plate]
            vehicle = spot.vehicle
            
            spot.is_occupied = False
            spot.vehicle = None
            spot.reserved_until = None
            
            del self.occupied_spots[license_plate]
            self.available_spots[spot.spot_size].append(spot)
            
            return vehicle, spot
    
    def reserve_spot(self, spot: ParkingSpot, vehicle: Vehicle, 
                    duration_hours: float) -> bool:
        with self.lock:
            if spot.is_occupied:
                return False
            
            reservation_end = datetime.now() + timedelta(hours=duration_hours)
            spot.reserved_until = reservation_end
            self.reservations[vehicle.license_plate].append(spot)
            
            return True
    
    def get_available_count(self, spot_size: SpotSize = None) -> int:
        with self.lock:
            if spot_size:
                return len(self.available_spots[spot_size])
            return sum(len(spots) for spots in self.available_spots.values())
    
    def get_spot_by_id(self, spot_id: str) -> Optional[ParkingSpot]:
        for spot in self.spots:
            if spot.spot_id == spot_id:
                return spot
        return None


class ParkingManager:
    def __init__(self):
        self.parking_lots: Dict[str, ParkingLot] = {}
        self.pricing_engine = DynamicPricingEngine({
            SpotSize.SMALL: 5.0,
            SpotSize.MEDIUM: 10.0,
            SpotSize.LARGE: 20.0,
            SpotSize.CHARGING: 15.0
        })
        self.allocation_strategy = AllocationStrategy.BEST_FIT
        self.vehicle_history: Dict[str, List[Tuple[ParkingSpot, datetime, datetime]]] = defaultdict(list)
        self.lock = threading.RLock()
    
    def add_parking_lot(self, lot: ParkingLot):
        with self.lock:
            self.parking_lots[lot.lot_id] = lot
    
    def set_allocation_strategy(self, strategy: AllocationStrategy):
        self.allocation_strategy = strategy
    
    def find_and_park_vehicle(self, vehicle: Vehicle, 
                             lot_id: str = None,
                             preferred_floor: Optional[int] = None) -> Optional[Tuple[ParkingLot, ParkingSpot]]:
        with self.lock:
            # If specific lot requested, use only that lot
            if lot_id:
                lots = [self.parking_lots.get(lot_id)]
            else:
                lots = list(self.parking_lots.values())
            
            for lot in lots:
                if lot is None:
                    continue
                
                spot = lot.find_available_spot(
                    vehicle, 
                    self.allocation_strategy,
                    preferred_floor
                )
                
                if spot:
                    if lot.park_vehicle(vehicle, spot):
                        return lot, spot
            
            return None
    
    def park_vehicle_with_payment(self, vehicle: Vehicle, 
                                 duration_hours: float,
                                 lot_id: str = None) -> Optional[Dict]:
        with self.lock:
            result = self.find_and_park_vehicle(vehicle, lot_id)
            if not result:
                return None
            
            lot, spot = result
            occupancy = lot.get_occupancy_rate()
            price = self.pricing_engine.calculate_price(spot, duration_hours, occupancy)
            
            # Record in history
            self.vehicle_history[vehicle.license_plate].append(
                (spot, datetime.now(), None)
            )
            
            return {
                'lot_id': lot.lot_id,
                'spot_id': spot.spot_id,
                'floor': spot.floor,
                'price': price,
                'duration_hours': duration_hours,
                'vehicle': vehicle
            }
    
    def remove_vehicle(self, license_plate: str) -> Optional[Dict]:
        with self.lock:
            for lot in self.parking_lots.values():
                result = lot.remove_vehicle(license_plate)
                if result:
                    vehicle, spot = result
                    
                    # Update history
                    if license_plate in self.vehicle_history:
                        last_record = self.vehicle_history[license_plate][-1]
                        if last_record[2] is None:  # End time not set
                            self.vehicle_history[license_plate][-1] = (
                                last_record[0], last_record[1], datetime.now()
                            )
                    
                    return {
                        'vehicle': vehicle,
                        'spot': spot,
                        'lot_id': lot.lot_id
                    }
            
            return None
    
    def reserve_spot(self, vehicle: Vehicle, spot_id: str, 
                    duration_hours: float) -> bool:
        with self.lock:
            for lot in self.parking_lots.values():
                spot = lot.get_spot_by_id(spot_id)
                if spot:
                    return lot.reserve_spot(spot, vehicle, duration_hours)
            return False
    
    def get_parking_status(self) -> Dict:
        with self.lock:
            status = {
                'total_spots': 0,
                'available_spots': 0,
                'occupancy_rate': 0.0,
                'lots': {}
            }
            
            for lot_id, lot in self.parking_lots.items():
                lot_status = {
                    'total_spots': len(lot.spots),
                    'available_spots': lot.get_available_count(),
                    'occupancy_rate': lot.get_occupancy_rate(),
                    'floors': lot.floors,
                    'spots_by_size': {}
                }
                
                for spot_size in SpotSize:
                    available = lot.get_available_count(spot_size)
                    total = sum(1 for s in lot.spots if s.spot_size == spot_size)
                    lot_status['spots_by_size'][spot_size.name] = {
                        'available': available,
                        'total': total
                    }
                
                status['lots'][lot_id] = lot_status
                status['total_spots'] += lot_status['total_spots']
                status['available_spots'] += lot_status['available_spots']
            
            if status['total_spots'] > 0:
                status['occupancy_rate'] = 1 - (status['available_spots'] / status['total_spots'])
            
            return status
    
    def get_vehicle_history(self, license_plate: str) -> List[Dict]:
        with self.lock:
            if license_plate not in self.vehicle_history:
                return []
            
            history = []
            for spot, start_time, end_time in self.vehicle_history[license_plate]:
                history.append({
                    'spot_id': spot.spot_id,
                    'floor': spot.floor,
                    'spot_size': spot.spot_size.name,
                    'start_time': start_time.isoformat() if start_time else None,
                    'end_time': end_time.isoformat() if end_time else None
                })
            
            return history
    
    def optimize_spot_allocation(self):
        """
        Advanced optimization: Reorganize vehicles to minimize fragmentation
        and maximize availability for different vehicle types.
        """
        with self.lock:
            for lot in self.parking_lots.values():
                # Simple optimization: move vehicles to better fitting spots
                # when possible to free up larger spots for larger vehicles
                self._compact_vehicles(lot)
    
    def _compact_vehicles(self, lot: ParkingLot):
        """Move vehicles to more appropriately sized spots when possible"""
        # This is a simplified version - in production, this would be more sophisticated
        vehicles_to_move = []
        
        for spot in lot.spots:
            if spot.is_occupied and spot.vehicle:
                # Check if there's a better fitting spot available
                required_size = spot.vehicle.spot_size
                if spot.spot_size.value > required_size.value + 1:  # Spot is much larger than needed
                    # Look for a smaller available spot
                    for candidate in lot.spots:
                        if (not candidate.is_occupied and 
                            candidate.spot_size == required_size and
                            candidate.floor == spot.floor):
                            vehicles_to_move.append((spot, candidate))
                            break
        
        # Execute moves
        for from_spot, to_spot in vehicles_to_move:
            vehicle = from_spot.vehicle
            lot.remove_vehicle(vehicle.license_plate)
            lot.park_vehicle(vehicle, to_spot)


# Singleton instance for global access
parking_manager = ParkingManager()


def create_sample_parking_lot():
    """Create a sample parking lot for demonstration"""
    spots_config = {
        SpotSize.SMALL: 20,    # Motorcycles
        SpotSize.MEDIUM: 100,  # Cars
        SpotSize.LARGE: 30,    # Trucks
        SpotSize.CHARGING: 20  # Electric vehicles
    }
    
    lot = ParkingLot("MAIN-LOT", floors=3, spots_per_floor=spots_config)
    parking_manager.add_parking_lot(lot)
    
    # Add a second lot
    lot2 = ParkingLot("ANNEX-LOT", floors=2, spots_per_floor={
        SpotSize.SMALL: 10,
        SpotSize.MEDIUM: 50,
        SpotSize.LARGE: 10,
        SpotSize.CHARGING: 10
    })
    parking_manager.add_parking_lot(lot2)
    
    return parking_manager


if __name__ == "__main__":
    # Example usage
    manager = create_sample_parking_lot()
    
    # Create vehicles
    car = Vehicle("ABC123", VehicleType.CAR)
    truck = Vehicle("XYZ789", VehicleType.TRUCK)
    motorcycle = Vehicle("MOTO01", VehicleType.MOTORCYCLE)
    electric = Vehicle("EV2024", VehicleType.ELECTRIC)
    
    # Park vehicles
    result1 = manager.park_vehicle_with_payment(car, duration_hours=2.0)
    print(f"Car parked: {result1}")
    
    result2 = manager.park_vehicle_with_payment(truck, duration_hours=4.0)
    print(f"Truck parked: {result2}")
    
    # Check status
    status = manager.get_parking_status()
    print(f"Parking status: {status['occupancy_rate']:.1%} occupied")
    
    # Remove a vehicle
    removal = manager.remove_vehicle("ABC123")
    print(f"Vehicle removed: {removal}")
    
    # Get updated status
    status = manager.get_parking_status()
    print(f"Updated status: {status['occupancy_rate']:.1%} occupied")