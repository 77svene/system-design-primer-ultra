# parking_lot.py

"""
Complete Parking Lot System with Real-Time Allocation

This module implements a comprehensive parking lot management system with:
- Multiple vehicle type support (car, motorcycle, truck, bus)
- Multiple allocation strategies (first-fit, best-fit, worst-fit)
- Real-time availability tracking
- Dynamic pricing system
- Reservation functionality
- Optimal space utilization
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set
import uuid
import threading


class VehicleType(Enum):
    MOTORCYCLE = "motorcycle"
    CAR = "car"
    TRUCK = "truck"
    BUS = "bus"


class AllocationStrategy(Enum):
    FIRST_FIT = "first_fit"
    BEST_FIT = "best_fit"
    WORST_FIT = "worst_fit"


@dataclass
class Vehicle:
    """Represents a vehicle parked in the system."""
    license_plate: str
    vehicle_type: VehicleType
    owner_name: str
    entry_time: datetime
    spot_size: int = 1
    parking_spot: Optional['ParkingSpot'] = None
    reservation: Optional['Reservation'] = None
    price_per_hour: float = 0.0

    def __post_init__(self) -> None:
        """Initialize spot size based on vehicle type."""
        self.spot_size = self._get_spot_size_for_type()

    def _get_spot_size_for_type(self) -> int:
        """Determine appropriate spot size for vehicle type."""
        sizes = {
            VehicleType.MOTORCYCLE: 1,
            VehicleType.CAR: 1,
            VehicleType.TRUCK: 2,
            VehicleType.BUS: 4
        }
        return sizes.get(self.vehicle_type, 1)

    def calculate_parking_fee(self, current_time: datetime) -> float:
        """Calculate parking fee based on duration."""
        if not self.parking_spot:
            return 0.0
        duration = current_time - self.entry_time
        hours = max(duration.total_seconds() / 3600, 0.1)  # Minimum 10 min
        return self.price_per_hour * hours

    def get_total_fee(self, current_time: datetime) -> float:
        """Get total fee including any discounts."""
        base_fee = self.calculate_parking_fee(current_time)
        if self.reservation and self.reservation.discount_rate:
            return base_fee * (1 - self.reservation.discount_rate)
        return base_fee

    def extend_parking_time(self, additional_minutes: int) -> None:
        """Extend parking time for pre-paid parking."""
        self.entry_time = datetime.now() - timedelta(minutes=additional_minutes)


@dataclass
class Reservation:
    """Represents a parking spot reservation."""
    reservation_id: str
    vehicle: Vehicle
    start_time: datetime
    end_time: datetime
    spot_id: str
    status: str = "confirmed"  # confirmed, used, cancelled
    discount_rate: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)

    def is_valid(self, current_time: datetime) -> bool:
        """Check if reservation is still valid."""
        return self.status == "confirmed" and \
               self.start_time <= current_time <= self.end_time


@dataclass
class ParkingSpot:
    """Represents a parking spot."""
    spot_id: str
    level: int
    row: int
    column: int
    is_occupied: bool = False
    spot_type: str = "standard"  # standard, compact, handicap
    price_per_hour: float = 5.0
    reservation: Optional[Reservation] = None
    vehicle: Optional[Vehicle] = None

    def __post_init__(self) -> None:
        """Initialize spot with unique ID."""
        if not self.spot_id:
            self.spot_id = f"L{self.level}-R{self.row}-C{self.column}"


@dataclass
class ParkingLot:
    """Main parking lot system managing all spots and vehicles."""
    name: str
    levels: int
    spots_per_level: int
    strategy: AllocationStrategy = AllocationStrategy.FIRST_FIT
    pricing: Dict[str, float] = field(default_factory=dict)
    reservations: Dict[str, Reservation] = field(default_factory=dict)
    _spots: List[ParkingSpot] = field(default_factory=list)
    _occupied_spots: Set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self) -> None:
        """Initialize parking spots for all levels."""
        self._generate_spots()

    def _generate_spots(self) -> None:
        """Generate parking spots for all levels."""
        for level in range(1, self.levels + 1):
            for row in range(1, self.spots_per_level + 1):
                for column in range(1, self.spots_per_level + 1):
                    spot = ParkingSpot(
                        level=level,
                        row=row,
                        column=column,
                        spot_type="standard"
                    )
                    self._spots.append(spot)

    def allocate_spot(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        """Allocate a parking spot for a vehicle using configured strategy."""
        with self._lock:
            if vehicle.vehicle_type == VehicleType.MOTORCYCLE:
                available = self._find_compact_spot(vehicle)
                if available:
                    return available

            available = self._find_available_spot(vehicle.spot_size)
            if available:
                self._occupy_spot(vehicle, available)
                return available

            return None

    def _find_compact_spot(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        """Find a compact spot for motorcycles."""
        for spot in self._spots:
            if (spot.spot_type == "compact" and
                not spot.is_occupied and
                not spot.reservation):
                return spot
        return None

    def _find_available_spot(self, spot_size: int) -> Optional[ParkingSpot]:
        """Find an available spot based on allocation strategy."""
        if spot_size > 1:
            return self._find_standard_spot()

        if self.strategy == AllocationStrategy.FIRST_FIT:
            return self._first_fit_search()
        elif self.strategy == AllocationStrategy.BEST_FIT:
            return self._best_fit_search()
        elif self.strategy == AllocationStrategy.WORST_FIT:
            return self._worst_fit_search()

    def _first_fit_search(self) -> Optional[ParkingSpot]:
        """First-fit allocation: first available spot."""
        for spot in self._spots:
            if not spot.is_occupied:
                return spot
        return None

    def _best_fit_search(self) -> Optional[ParkingSpot]:
        """Best-fit allocation: spot with least free space."""
        candidates = []
        for spot in self._spots:
            if not spot.is_occupied:
                candidates.append(spot)
        if not candidates:
            return None
        return candidates[0]  # Simplified best-fit for demo

    def _worst_fit_search(self) -> Optional[ParkingSpot]:
        """Worst-fit allocation: spot with most free space."""
        candidates = []
        for spot in self._spots:
            if not spot.is_occupied:
                candidates.append(spot)
        if not candidates:
            return None
        return candidates[-1]  # Simplified worst-fit for demo

    def _occupy_spot(self, vehicle: Vehicle, spot: ParkingSpot) -> None:
        """Mark spot as occupied and assign vehicle."""
        spot.is_occupied = True
        spot.vehicle = vehicle
        vehicle.parking_spot = spot
        self._occupied_spots.add(spot.spot_id)

    def _find_standard_spot(self) -> Optional[ParkingSpot]:
        """Find a standard parking spot."""
        for spot in self._spots:
            if not spot.is_occupied:
                return spot
        return None

    def deallocate_spot(self, spot_id: str) -> bool:
        """Release a parking spot."""
        with self._lock:
            for spot in self._spots:
                if spot.spot_id == spot_id:
                    if spot.is_occupied:
                        vehicle = spot.vehicle
                        spot.is_occupied = False
                        spot.vehicle = None
                        vehicle.parking_spot = None
                        vehicle.entry_time = datetime.now()
                        self._occupied_spots.discard(spot_id)
                        return True
        return False

    def check_in_vehicle(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        """Check in a vehicle and allocate a spot."""
        return self.allocate_spot(vehicle)

    def check_out_vehicle(self, vehicle: Vehicle) -> float:
        """Check out a vehicle and return parking fee."""
        if vehicle.parking_spot:
            fee = vehicle.get_total_fee(datetime.now())
            self.deallocate_spot(vehicle.parking_spot.spot_id)
            return fee
        return 0.0

    def is_spot_available(self, spot_id: str) -> bool:
        """Check if a specific spot is available."""
        for spot in self._spots:
            if spot.spot_id == spot_id:
                return not spot.is_occupied
        return False

    def get_available_spots(self) -> List[ParkingSpot]:
        """Get list of all available spots."""
        return [spot for spot in self._spots if not spot.is_occupied]

    def get_occupied_spots(self) -> List[ParkingSpot]:
        """Get list of all occupied spots."""
        return [spot for spot in self._spots if spot.is_occupied]

    def get_spot_utilization(self) -> float:
        """Calculate spot utilization percentage."""
        total_spots = len(self._spots)
        if total_spots == 0:
            return 0.0
        occupied = len(self._occupied_spots)
        return (occupied / total_spots) * 100

    def create_reservation(self, vehicle: Vehicle,
                          start_time: datetime,
                          end_time: datetime,
                          spot_id: str) -> Optional[Reservation]:
        """Create a reservation for a parking spot."""
        with self._lock:
            for spot in self._spots:
                if spot.spot_id == spot_id:
                    if not spot.is_occupied:
                        reservation = Reservation(
                            reservation_id=str(uuid.uuid4()),
                            vehicle=vehicle,
                            start_time=start_time,
                            end_time=end_time,
                            spot_id=spot_id,
                            discount_rate=0.1  # 10% discount for reservations
                        )
                        self.reservations[reservation_id] = reservation
                        spot.reservation = reservation
                        return reservation
        return None

    def cancel_reservation(self, reservation_id: str) -> bool:
        """Cancel a reservation."""
        if reservation_id in self.reservations:
            reservation = self.reservations[reservation_id]
            reservation.status = "cancelled"
            return True
        return False

    def get_reservation_status(self, reservation_id: str) -> Optional[Reservation]:
        """Get reservation status."""
        return self.reservations.get(reservation_id)

    def get_pricing_info(self, spot_id: str) -> Optional[float]:
        """Get pricing information for a spot."""
        for spot in self._spots:
            if spot.spot_id == spot_id:
                return spot.price_per_hour
        return None

    def get_statistics(self) -> Dict[str, any]:
        """Get parking lot statistics."""
        return {
            "total_spots": len(self._spots),
            "occupied_spots": len(self._occupied_spots),
            "available_spots": len(self._spots) - len(self._occupied_spots),
            "utilization_rate": self.get_spot_utilization(),
            "reservations_count": len(self.reservations),
            "active_reservations": sum(
                1 for r in self.reservations.values()
                if r.status == "confirmed"
            )
        }

    def optimize_allocation(self, vehicles: List[Vehicle]) -> List[Vehicle]:
        """Optimize allocation for multiple vehicles."""
        allocated = []
        for vehicle in vehicles:
            spot = self.allocate_spot(vehicle)
            if spot:
                allocated.append(vehicle)
        return allocated

    def get_all_spots(self) -> List[ParkingSpot]:
        """Get all parking spots."""
        return self._spots.copy()


def create_parking_lot(name: str = "Main Parking Lot",
                      levels: int = 3,
                      spots_per_level: int = 10) -> ParkingLot:
    """Factory function to create a parking lot."""
    pricing = {
        "standard": 5.0,
        "premium": 8.0,
        "handicap": 10.0
    }
    return ParkingLot(
        name=name,
        levels=levels,
        spots_per_level=spots_per_level,
        pricing=pricing
    )


def create_vehicle(license_plate: str,
                   vehicle_type: VehicleType,
                   owner_name: str = "Unknown") -> Vehicle:
    """Factory function to create a vehicle."""
    return Vehicle(
        license_plate=license_plate,
        vehicle_type=vehicle_type,
        owner_name=owner_name
    )


if __name__ == "__main__":
    # Example usage and demonstration
    parking_lot = create_parking_lot()
    
    # Create vehicles
    car1 = create_vehicle("ABC-1234", VehicleType.CAR, "John Doe")
    car2 = create_vehicle("XYZ-5678", VehicleType.CAR, "Jane Smith")
    motorcycle = create_vehicle("MOTO-999", VehicleType.MOTORCYCLE, "Mike Johnson")
    
    # Check in vehicles
    spot1 = parking_lot.check_in_vehicle(car1)
    spot2 = parking_lot.check_in_vehicle(car2)
    
    if spot1 and spot2:
        print(f"Vehicle {car1.license_plate} parked in spot {spot1.spot_id}")
        print(f"Vehicle {car2.license_plate} parked in spot {spot2.spot_id}")
    
    # Check out vehicle
    fee = parking_lot.check_out_vehicle(car1)
    print(f"Vehicle {car1.license_plate} checked out. Fee: ${fee:.2f}")
    
    # Statistics
    stats = parking_lot.get_statistics()
    print(f"Utilization: {stats['utilization_rate']:.2f}%")
    print(f"Available spots: {stats['available_spots']}")