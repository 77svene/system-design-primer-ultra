from abc import ABCMeta, abstractmethod
from enum import Enum
from datetime import datetime, timedelta
import heapq
from collections import defaultdict


class VehicleSize(Enum):
    MOTORCYCLE = 0
    COMPACT = 1
    LARGE = 2


class Vehicle(metaclass=ABCMeta):
    def __init__(self, vehicle_size, license_plate, spot_size):
        self.vehicle_size = vehicle_size
        self.license_plate = license_plate
        self.spot_size = spot_size  # Fixed: was missing assignment
        self.spots_taken = []

    def clear_spots(self):
        for spot in self.spots_taken:
            spot.remove_vehicle(self)
        self.spots_taken = []

    def take_spot(self, spot):
        self.spots_taken.append(spot)

    @abstractmethod
    def can_fit_in_spot(self, spot):
        pass


class Motorcycle(Vehicle):
    def __init__(self, license_plate):
        super(Motorcycle, self).__init__(VehicleSize.MOTORCYCLE, license_plate, spot_size=1)

    def can_fit_in_spot(self, spot):
        return True


class Car(Vehicle):
    def __init__(self, license_plate):
        super(Car, self).__init__(VehicleSize.COMPACT, license_plate, spot_size=1)

    def can_fit_in_spot(self, spot):
        return spot.size in (VehicleSize.LARGE, VehicleSize.COMPACT)


class Bus(Vehicle):
    def __init__(self, license_plate):
        super(Bus, self).__init__(VehicleSize.LARGE, license_plate, spot_size=5)

    def can_fit_in_spot(self, spot):
        return spot.size == VehicleSize.LARGE


class ParkingTicket:
    """Represents a parking ticket with entry/exit times and pricing"""
    def __init__(self, vehicle, spot, entry_time=None):
        self.vehicle = vehicle
        self.spot = spot
        self.entry_time = entry_time or datetime.now()
        self.exit_time = None
        self.amount_due = 0.0

    def calculate_fee(self, pricing_strategy):
        """Calculate parking fee based on pricing strategy"""
        if not self.exit_time:
            self.exit_time = datetime.now()
        duration_hours = (self.exit_time - self.entry_time).total_seconds() / 3600
        return pricing_strategy.calculate_price(self.vehicle.vehicle_size, duration_hours)

    def close_ticket(self, pricing_strategy):
        """Close ticket and calculate final amount"""
        self.amount_due = self.calculate_fee(pricing_strategy)
        return self.amount_due


class PricingStrategy(metaclass=ABCMeta):
    """Abstract base class for pricing strategies"""
    @abstractmethod
    def calculate_price(self, vehicle_size, duration_hours):
        pass


class DynamicPricingStrategy(PricingStrategy):
    """Dynamic pricing based on time of day and demand"""
    def __init__(self):
        self.base_rates = {
            VehicleSize.MOTORCYCLE: 2.0,
            VehicleSize.COMPACT: 5.0,
            VehicleSize.LARGE: 10.0
        }
        self.peak_hours = [(7, 9), (17, 19)]  # Morning and evening rush hours
        self.peak_multiplier = 1.5
        self.weekend_multiplier = 0.8

    def calculate_price(self, vehicle_size, duration_hours):
        base_rate = self.base_rates[vehicle_size]
        now = datetime.now()
        
        # Check if it's weekend
        if now.weekday() >= 5:  # Saturday or Sunday
            base_rate *= self.weekend_multiplier
        
        # Check if it's peak hour
        current_hour = now.hour
        is_peak = any(start <= current_hour < end for start, end in self.peak_hours)
        if is_peak:
            base_rate *= self.peak_multiplier
        
        return base_rate * duration_hours


class Reservation:
    """Represents a parking reservation"""
    def __init__(self, vehicle, start_time, duration_hours, reservation_id):
        self.vehicle = vehicle
        self.start_time = start_time
        self.end_time = start_time + timedelta(hours=duration_hours)
        self.reservation_id = reservation_id
        self.assigned_spot = None
        self.is_active = True

    def is_valid(self):
        """Check if reservation is currently valid"""
        now = datetime.now()
        return self.is_active and self.start_time <= now <= self.end_time

    def cancel(self):
        """Cancel the reservation"""
        self.is_active = False


class ParkingLot(object):
    def __init__(self, num_levels, pricing_strategy=None):
        self.num_levels = num_levels
        self.levels = []  # List of Levels
        self.reservations = {}  # reservation_id -> Reservation
        self.active_tickets = {}  # license_plate -> ParkingTicket
        self.pricing_strategy = pricing_strategy or DynamicPricingStrategy()
        self.reservation_counter = 0
        
        # Real-time tracking
        self.available_spots_by_type = defaultdict(int)
        self.total_spots_by_type = defaultdict(int)
        
        # Optimization: priority queues for different vehicle types
        self.available_spots_heap = {
            VehicleSize.MOTORCYCLE: [],
            VehicleSize.COMPACT: [],
            VehicleSize.LARGE: []
        }

    def add_level(self, level):
        """Add a level to the parking lot"""
        self.levels.append(level)
        # Update total spots count
        for spot in level.spots:
            self.total_spots_by_type[spot.size] += 1
            if spot.is_available():
                self.available_spots_by_type[spot.size] += 1
                heapq.heappush(self.available_spots_heap[spot.size], 
                              (spot.spot_number, spot))

    def park_vehicle(self, vehicle, reservation_id=None):
        """Park a vehicle with optional reservation"""
        # Check if vehicle already parked
        if vehicle.license_plate in self.active_tickets:
            return None, "Vehicle already parked"
        
        # Check reservation if provided
        reservation = None
        if reservation_id:
            reservation = self.reservations.get(reservation_id)
            if not reservation or not reservation.is_valid():
                return None, "Invalid reservation"
            if reservation.vehicle.license_plate != vehicle.license_plate:
                return None, "Reservation vehicle mismatch"
        
        # Try to find a spot using allocation algorithm
        spot = self._allocate_spot(vehicle, reservation)
        if not spot:
            return None, "No available spot"
        
        # Park the vehicle
        spot.park_vehicle(vehicle)
        vehicle.take_spot(spot)
        
        # Create parking ticket
        ticket = ParkingTicket(vehicle, spot)
        self.active_tickets[vehicle.license_plate] = ticket
        
        # Update real-time tracking
        self.available_spots_by_type[spot.size] -= 1
        self._update_spot_availability(spot)
        
        # Mark reservation as used
        if reservation:
            reservation.assigned_spot = spot
            reservation.is_active = False
        
        return ticket, "Vehicle parked successfully"

    def _allocate_spot(self, vehicle, reservation=None):
        """Allocate spot using first-fit or best-fit algorithm"""
        if reservation and reservation.assigned_spot:
            # Use reserved spot if available
            spot = reservation.assigned_spot
            if spot.is_available() and vehicle.can_fit_in_spot(spot):
                return spot
        
        # First-fit allocation: find first available spot that fits
        for level in self.levels:
            spot = level.find_available_spot(vehicle)
            if spot:
                return spot
        
        # If no spot found with first-fit, try best-fit for large vehicles
        if vehicle.vehicle_size == VehicleSize.LARGE:
            return self._best_fit_allocation(vehicle)
        
        return None

    def _best_fit_allocation(self, vehicle):
        """Best-fit allocation for large vehicles (minimize fragmentation)"""
        best_spot = None
        min_waste = float('inf')
        
        for level in self.levels:
            for i, spot in enumerate(level.spots):
                if spot.is_available() and vehicle.can_fit_in_spot(spot):
                    # Calculate waste (how many spots would be left unused)
                    consecutive_available = self._count_consecutive_available(level, i)
                    if consecutive_available >= vehicle.spot_size:
                        waste = consecutive_available - vehicle.spot_size
                        if waste < min_waste:
                            min_waste = waste
                            best_spot = spot
        
        return best_spot

    def _count_consecutive_available(self, level, start_index):
        """Count consecutive available spots from start_index"""
        count = 0
        for i in range(start_index, len(level.spots)):
            if level.spots[i].is_available():
                count += 1
            else:
                break
        return count

    def _update_spot_availability(self, spot):
        """Update spot availability in priority queue"""
        if spot.is_available():
            heapq.heappush(self.available_spots_heap[spot.size], 
                          (spot.spot_number, spot))

    def remove_vehicle(self, license_plate):
        """Remove vehicle and calculate fee"""
        if license_plate not in self.active_tickets:
            return None, "Vehicle not found"
        
        ticket = self.active_tickets[license_plate]
        vehicle = ticket.vehicle
        
        # Calculate fee
        amount_due = ticket.close_ticket(self.pricing_strategy)
        
        # Free spots
        vehicle.clear_spots()
        
        # Update real-time tracking
        for spot in vehicle.spots_taken:
            self.available_spots_by_type[spot.size] += 1
            self._update_spot_availability(spot)
        
        # Remove ticket
        del self.active_tickets[license_plate]
        
        return amount_due, "Vehicle removed successfully"

    def make_reservation(self, vehicle, start_time, duration_hours):
        """Make a reservation for a vehicle"""
        self.reservation_counter += 1
        reservation_id = f"RES{self.reservation_counter:06d}"
        
        reservation = Reservation(vehicle, start_time, duration_hours, reservation_id)
        self.reservations[reservation_id] = reservation
        
        return reservation_id

    def cancel_reservation(self, reservation_id):
        """Cancel a reservation"""
        if reservation_id in self.reservations:
            self.reservations[reservation_id].cancel()
            return True
        return False

    def get_availability(self):
        """Get real-time availability by vehicle type"""
        return dict(self.available_spots_by_type)

    def get_occupancy_rate(self):
        """Calculate current occupancy rate"""
        total_spots = sum(self.total_spots_by_type.values())
        available_spots = sum(self.available_spots_by_type.values())
        return (total_spots - available_spots) / total_spots if total_spots > 0 else 0


class Level(object):
    SPOTS_PER_ROW = 10

    def __init__(self, floor, total_spots):
        self.floor = floor
        self.num_spots = total_spots
        self.available_spots = 0
        self.spots = []  # List of ParkingSpots

    def spot_freed(self):
        self.available_spots += 1

    def park_vehicle(self, vehicle):
        spot = self.find_available_spot(vehicle)
        if spot is None:
            return None
        else:
            spot.park_vehicle(vehicle)
            return spot

    def find_available_spot(self, vehicle):
        """Find an available spot where vehicle can fit, or return None"""
        for spot in self.spots:
            if spot.is_available() and vehicle.can_fit_in_spot(spot):
                # For vehicles requiring multiple spots, check consecutive availability
                if vehicle.spot_size > 1:
                    if self._check_consecutive_spots(spot, vehicle.spot_size):
                        return spot
                else:
                    return spot
        return None

    def _check_consecutive_spots(self, start_spot, num_spots):
        """Check if there are enough consecutive available spots"""
        start_index = self.spots.index(start_spot)
        for i in range(start_index, start_index + num_spots):
            if i >= len(self.spots) or not self.spots[i].is_available():
                return False
        return True

    def _park_starting_at_spot(self, spot, vehicle):
        """Occupy starting at spot.spot_number to vehicle.spot_size."""
        start_index = self.spots.index(spot)
        for i in range(start_index, start_index + vehicle.spot_size):
            self.spots[i].park_vehicle(vehicle)
            vehicle.take_spot(self.spots[i])
        self.available_spots -= vehicle.spot_size


class ParkingSpot(object):
    def __init__(self, level, row, spot_number, spot_size, vehicle_size):
        self.level = level
        self.row = row
        self.spot_number = spot_number
        self.size = spot_size  # Fixed: was spot_size, now consistent with usage
        self.vehicle_size = vehicle_size
        self.vehicle = None

    def is_available(self):
        return self.vehicle is None

    def can_fit_vehicle(self, vehicle):
        if not self.is_available():
            return False
        return vehicle.can_fit_in_spot(self)

    def park_vehicle(self, vehicle):
        """Park a vehicle in this spot"""
        if self.can_fit_vehicle(vehicle):
            self.vehicle = vehicle
            self.level.available_spots -= 1
            return True
        return False

    def remove_vehicle(self):
        """Remove vehicle from this spot"""
        if self.vehicle:
            self.vehicle = None
            self.level.spot_freed()
            return True
        return False

    def __repr__(self):
        return f"ParkingSpot(floor={self.level.floor}, row={self.row}, number={self.spot_number}, size={self.size.name})"