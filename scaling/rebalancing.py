"""
Scalable Data Partitioning Strategies
======================================

This module implements pluggable partitioning strategies with rebalancing algorithms
for distributed systems. It demonstrates virtual nodes, partition migration, and
hotspot mitigation techniques used in production systems.

Real-world examples included:
- Social graph partitioning (consistent hashing)
- Time-series data partitioning (range partitioning)
- User session partitioning (geographic partitioning)
"""

import bisect
import hashlib
import bisect
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple, Callable
from collections import defaultdict, deque
import random
import math
from enum import Enum


class PartitionStrategy(Enum):
    """Available partitioning strategies."""
    CONSISTENT_HASHING = "consistent_hashing"
    RANGE_PARTITIONING = "range_partitioning"
    GEOGRAPHIC = "geographic"


@dataclass
class Node:
    """Represents a storage node in the distributed system."""
    node_id: str
    capacity: int = 1000  # Maximum number of partitions
    load: int = 0  # Current number of partitions
    region: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def utilization(self) -> float:
        """Calculate node utilization percentage."""
        return (self.load / self.capacity) * 100 if self.capacity > 0 else 0
    
    def can_accept_partition(self) -> bool:
        """Check if node can accept another partition."""
        return self.load < self.capacity


@dataclass
class Partition:
    """Represents a data partition."""
    partition_id: str
    key_range: Optional[Tuple[Any, Any]] = None  # For range partitioning
    node_id: Optional[str] = None
    size: int = 0  # Size in bytes/records
    access_frequency: int = 0  # For hotspot detection
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_hotspot(self) -> bool:
        """Determine if partition is a hotspot (access frequency > threshold)."""
        # Simple heuristic: 10x average access frequency
        return self.access_frequency > 1000  # Adjustable threshold


class PartitioningStrategy(ABC):
    """Abstract base class for partitioning strategies."""
    
    @abstractmethod
    def add_node(self, node: Node) -> None:
        """Add a new node to the cluster."""
        pass
    
    @abstractmethod
    def remove_node(self, node_id: str) -> List[Partition]:
        """Remove a node and return partitions that need migration."""
        pass
    
    @abstractmethod
    def get_partition_for_key(self, key: Any) -> Optional[Partition]:
        """Get the partition responsible for a given key."""
        pass
    
    @abstractmethod
    def rebalance(self, migration_budget: int = 10) -> Dict[str, List[Partition]]:
        """
        Rebalance partitions across nodes.
        
        Args:
            migration_budget: Maximum number of partitions to migrate in one rebalance
            
        Returns:
            Dictionary mapping node_id to list of partitions to migrate to that node
        """
        pass
    
    @abstractmethod
    def add_partition(self, partition: Partition) -> str:
        """Add a new partition and return assigned node_id."""
        pass


class ConsistentHashingStrategy(PartitioningStrategy):
    """
    Consistent Hashing with virtual nodes for even distribution.
    
    Used for: Social graph partitioning, cache systems, distributed hash tables.
    
    Features:
    - Virtual nodes for better load distribution
    - Minimal data movement when nodes are added/removed
    - Hotspot detection and mitigation
    """
    
    def __init__(self, virtual_nodes_per_physical: int = 150):
        self.virtual_nodes_per_physical = virtual_nodes_per_physical
        self.ring: List[Tuple[int, str]] = []  # (hash, virtual_node_id)
        self.virtual_to_physical: Dict[str, str] = {}  # virtual_node_id -> physical_node_id
        self.nodes: Dict[str, Node] = {}
        self.partitions: Dict[str, Partition] = {}
        self.partition_assignments: Dict[str, str] = {}  # partition_id -> node_id
        
        # For hotspot mitigation
        self.hotspot_replicas: Dict[str, Set[str]] = defaultdict(set)  # partition_id -> set of replica node_ids
    
    def _hash(self, key: str) -> int:
        """Generate a consistent hash for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _add_virtual_node(self, physical_node_id: str) -> None:
        """Add virtual nodes for a physical node to the ring."""
        for i in range(self.virtual_nodes_per_physical):
            virtual_node_id = f"{physical_node_id}:vn{i}"
            hash_val = self._hash(virtual_node_id)
            bisect.insort(self.ring, (hash_val, virtual_node_id))
            self.virtual_to_physical[virtual_node_id] = physical_node_id
    
    def _remove_virtual_nodes(self, physical_node_id: str) -> None:
        """Remove all virtual nodes for a physical node from the ring."""
        virtual_nodes_to_remove = [
            vn for vn, pn in self.virtual_to_physical.items() 
            if pn == physical_node_id
        ]
        
        for virtual_node_id in virtual_nodes_to_remove:
            hash_val = self._hash(virtual_node_id)
            # Find and remove from ring
            idx = bisect.bisect_left(self.ring, (hash_val, virtual_node_id))
            if idx < len(self.ring) and self.ring[idx] == (hash_val, virtual_node_id):
                self.ring.pop(idx)
            del self.virtual_to_physical[virtual_node_id]
    
    def add_node(self, node: Node) -> None:
        """Add a new node to the consistent hash ring."""
        if node.node_id in self.nodes:
            raise ValueError(f"Node {node.node_id} already exists")
        
        self.nodes[node.node_id] = node
        self._add_virtual_node(node.node_id)
        
        # Reassign some partitions to the new node
        self._reassign_partitions_to_new_node(node.node_id)
    
    def _reassign_partitions_to_new_node(self, new_node_id: str) -> None:
        """Reassign some partitions to a newly added node."""
        # Simple strategy: take partitions from most loaded nodes
        overloaded_nodes = [
            (nid, n.load) for nid, n in self.nodes.items() 
            if n.load > 0 and nid != new_node_id
        ]
        overloaded_nodes.sort(key=lambda x: x[1], reverse=True)
        
        partitions_to_move = []
        for node_id, load in overloaded_nodes:
            if load > 1:  # Only move if node has multiple partitions
                node_partitions = [
                    pid for pid, nid in self.partition_assignments.items() 
                    if nid == node_id
                ]
                if node_partitions:
                    # Move one partition from overloaded node
                    partitions_to_move.append((node_partitions[0], node_id))
                    if len(partitions_to_move) >= 3:  # Limit migrations
                        break
        
        # Perform migrations
        for partition_id, from_node_id in partitions_to_move:
            self._migrate_partition(partition_id, from_node_id, new_node_id)
    
    def remove_node(self, node_id: str) -> List[Partition]:
        """Remove a node and return partitions that need migration."""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} does not exist")
        
        # Get partitions on this node
        partitions_to_migrate = [
            self.partitions[pid] 
            for pid, nid in self.partition_assignments.items() 
            if nid == node_id
        ]
        
        # Remove virtual nodes from ring
        self._remove_virtual_nodes(node_id)
        
        # Remove node
        del self.nodes[node_id]
        
        # Reassign partitions
        for partition in partitions_to_migrate:
            new_node_id = self._find_node_for_partition(partition)
            if new_node_id:
                self.partition_assignments[partition.partition_id] = new_node_id
                self.nodes[new_node_id].load += 1
                partition.node_id = new_node_id
        
        return partitions_to_migrate
    
    def _find_node_for_partition(self, partition: Partition) -> Optional[str]:
        """Find the appropriate node for a partition using consistent hashing."""
        if not self.ring:
            return None
        
        # Hash the partition ID to find its position on the ring
        partition_hash = self._hash(partition.partition_id)
        
        # Find the first virtual node with hash >= partition_hash
        idx = bisect.bisect_left(self.ring, (partition_hash, ""))
        if idx == len(self.ring):
            idx = 0  # Wrap around
        
        virtual_node_id = self.ring[idx][1]
        physical_node_id = self.virtual_to_physical[virtual_node_id]
        
        # Check if physical node has capacity
        if self.nodes[physical_node_id].can_accept_partition():
            return physical_node_id
        
        # Try next nodes on the ring
        for i in range(1, len(self.ring)):
            next_idx = (idx + i) % len(self.ring)
            virtual_node_id = self.ring[next_idx][1]
            physical_node_id = self.virtual_to_physical[virtual_node_id]
            if self.nodes[physical_node_id].can_accept_partition():
                return physical_node_id
        
        return None
    
    def get_partition_for_key(self, key: Any) -> Optional[Partition]:
        """Get the partition responsible for a given key."""
        # For social graphs, keys could be user IDs
        key_str = str(key)
        key_hash = self._hash(key_str)
        
        # Find which partition this key belongs to
        # In practice, you'd have a mapping from key ranges to partitions
        # Here we simulate by finding the closest partition
        for partition_id, partition in self.partitions.items():
            # Simple check: if partition has a key range and key is in it
            if partition.key_range:
                start, end = partition.key_range
                if start <= key <= end:
                    partition.access_frequency += 1
                    return partition
        
        # If no specific partition found, assign to a partition based on hash
        if self.partitions:
            partition_ids = list(self.partitions.keys())
            idx = key_hash % len(partition_ids)
            partition = self.partitions[partition_ids[idx]]
            partition.access_frequency += 1
            return partition
        
        return None
    
    def rebalance(self, migration_budget: int = 10) -> Dict[str, List[Partition]]:
        """Rebalance partitions across nodes using consistent hashing."""
        migrations = defaultdict(list)
        
        # Check for hotspots and create replicas
        self._mitigate_hotspots()
        
        # Check node utilization
        avg_utilization = sum(n.utilization for n in self.nodes.values()) / len(self.nodes) if self.nodes else 0
        
        for node_id, node in self.nodes.items():
            if node.utilization > avg_utilization * 1.5:  # 50% above average
                # This node is overloaded, move some partitions
                node_partitions = [
                    pid for pid, nid in self.partition_assignments.items() 
                    if nid == node_id
                ]
                
                # Sort by access frequency (move less accessed partitions first)
                partitions_with_freq = [
                    (pid, self.partitions[pid].access_frequency) 
                    for pid in node_partitions
                ]
                partitions_with_freq.sort(key=lambda x: x[1])
                
                # Move up to migration_budget partitions
                for partition_id, _ in partitions_with_freq[:migration_budget]:
                    new_node_id = self._find_least_loaded_node(exclude=node_id)
                    if new_node_id:
                        self._migrate_partition(partition_id, node_id, new_node_id)
                        migrations[new_node_id].append(self.partitions[partition_id])
        
        return dict(migrations)
    
    def _mitigate_hotspots(self) -> None:
        """Mitigate hotspots by creating additional replicas."""
        for partition_id, partition in self.partitions.items():
            if partition.is_hotspot and partition_id not in self.hotspot_replicas:
                # Create 2 additional replicas for hot partitions
                for i in range(2):
                    replica_node = self._find_least_loaded_node()
                    if replica_node:
                        self.hotspot_replicas[partition_id].add(replica_node)
                        self.nodes[replica_node].load += 1
    
    def _find_least_loaded_node(self, exclude: Optional[str] = None) -> Optional[str]:
        """Find the node with the lowest utilization."""
        candidates = [
            (nid, n.utilization) for nid, n in self.nodes.items() 
            if nid != exclude and n.can_accept_partition()
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[1])
        return candidates[0][0]
    
    def _migrate_partition(self, partition_id: str, from_node: str, to_node: str) -> None:
        """Migrate a partition from one node to another."""
        if partition_id in self.partition_assignments:
            self.partition_assignments[partition_id] = to_node
            self.nodes[from_node].load -= 1
            self.nodes[to_node].load += 1
            self.partitions[partition_id].node_id = to_node
    
    def add_partition(self, partition: Partition) -> str:
        """Add a new partition and return assigned node_id."""
        if partition.partition_id in self.partitions:
            raise ValueError(f"Partition {partition.partition_id} already exists")
        
        self.partitions[partition.partition_id] = partition
        
        # Find a node for this partition
        node_id = self._find_node_for_partition(partition)
        if not node_id:
            raise RuntimeError("No available nodes with capacity")
        
        self.partition_assignments[partition.partition_id] = node_id
        self.nodes[node_id].load += 1
        partition.node_id = node_id
        
        return node_id
    
    def get_social_graph_partition(self, user_id: str) -> Partition:
        """
        Example: Partition a social graph by user ID.
        
        In a social network, user data and their connections need to be partitioned.
        Consistent hashing ensures that a user's data stays on the same node even
        when the cluster size changes, minimizing data movement.
        """
        # Create a partition ID based on user ID
        partition_id = f"social_{hash(user_id) % 1000}"
        
        if partition_id not in self.partitions:
            # Create new partition
            partition = Partition(
                partition_id=partition_id,
                metadata={"type": "social_graph", "user_range": f"{user_id[:3]}xxx"}
            )
            self.add_partition(partition)
        
        return self.partitions[partition_id]


class RangePartitioningStrategy(PartitioningStrategy):
    """
    Range-based partitioning for ordered data.
    
    Used for: Time-series data, sequential access patterns, range queries.
    
    Features:
    - Efficient range queries
    - Natural data ordering
    - Dynamic range splitting for hotspots
    """
    
    def __init__(self, initial_ranges: Optional[List[Tuple[Any, Any]]] = None):
        self.nodes: Dict[str, Node] = {}
        self.partitions: Dict[str, Partition] = {}
        self.partition_assignments: Dict[str, str] = {}
        
        # Range management
        self.ranges: List[Tuple[Any, Any, str]] = []  # (start, end, partition_id)
        if initial_ranges:
            for i, (start, end) in enumerate(initial_ranges):
                partition_id = f"range_{i}"
                self.ranges.append((start, end, partition_id))
                self.partitions[partition_id] = Partition(
                    partition_id=partition_id,
                    key_range=(start, end)
                )
        
        # For dynamic range splitting
        self.split_threshold = 1000  # Records per partition before split
    
    def add_node(self, node: Node) -> None:
        """Add a new node and assign some ranges to it."""
        if node.node_id in self.nodes:
            raise ValueError(f"Node {node.node_id} already exists")
        
        self.nodes[node.node_id] = node
        
        # If we have unassigned ranges, assign them
        unassigned_ranges = [
            (start, end, pid) for start, end, pid in self.ranges 
            if pid not in self.partition_assignments
        ]
        
        if unassigned_ranges:
            # Assign up to 3 ranges to new node
            for start, end, partition_id in unassigned_ranges[:3]:
                self.partition_assignments[partition_id] = node.node_id
                node.load += 1
                self.partitions[partition_id].node_id = node.node_id
    
    def remove_node(self, node_id: str) -> List[Partition]:
        """Remove a node and reassign its ranges."""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} does not exist")
        
        # Get partitions on this node
        partitions_to_reassign = [
            self.partitions[pid] 
            for pid, nid in self.partition_assignments.items() 
            if nid == node_id
        ]
        
        # Remove node
        del self.nodes[node_id]
        
        # Reassign partitions
        for partition in partitions_to_reassign:
            new_node_id = self._find_node_for_range(partition.key_range)
            if new_node_id:
                self.partition_assignments[partition.partition_id] = new_node_id
                self.nodes[new_node_id].load += 1
                partition.node_id = new_node_id
        
        return partitions_to_reassign
    
    def _find_node_for_range(self, key_range: Tuple[Any, Any]) -> Optional[str]:
        """Find the best node for a given key range."""
        # Simple strategy: find node with least load
        available_nodes = [
            (nid, n.load) for nid, n in self.nodes.items() 
            if n.can_accept_partition()
        ]
        
        if not available_nodes:
            return None
        
        available_nodes.sort(key=lambda x: x[1])
        return available_nodes[0][0]
    
    def get_partition_for_key(self, key: Any) -> Optional[Partition]:
        """Get the partition responsible for a given key."""
        # Find the range containing this key
        for start, end, partition_id in self.ranges:
            if start <= key <= end:
                partition = self.partitions[partition_id]
                partition.access_frequency += 1
                
                # Check if partition needs splitting (for time-series hotspots)
                if partition.size > self.split_threshold:
                    self._split_range(partition_id)
                
                return partition
        
        return None
    
    def _split_range(self, partition_id: str) -> None:
        """Split a range partition into two when it gets too large."""
        partition = self.partitions[partition_id]
        start, end = partition.key_range
        
        # Find the split point (midpoint for simplicity)
        if isinstance(start, (int, float)) and isinstance(end, (int, float)):
            mid = (start + end) / 2
        elif isinstance(start, str) and isinstance(end, str):
            # For string ranges, use a simple approach
            mid = start[:len(start)//2] + "m" + start[len(start)//2:]
        else:
            # For other types, create a new range
            mid = f"{start}_split"
        
        # Create two new partitions
        left_partition_id = f"{partition_id}_left"
        right_partition_id = f"{partition_id}_right"
        
        left_partition = Partition(
            partition_id=left_partition_id,
            key_range=(start, mid),
            size=partition.size // 2
        )
        
        right_partition = Partition(
            partition_id=right_partition_id,
            key_range=(mid, end),
            size=partition.size // 2
        )
        
        # Update ranges
        self.ranges = [
            (s, e, pid) for s, e, pid in self.ranges 
            if pid != partition_id
        ]
        self.ranges.append((start, mid, left_partition_id))
        self.ranges.append((mid, end, right_partition_id))
        
        # Assign to nodes
        self.add_partition(left_partition)
        self.add_partition(right_partition)
        
        # Remove old partition
        del self.partitions[partition_id]
        if partition_id in self.partition_assignments:
            node_id = self.partition_assignments[partition_id]
            self.nodes[node_id].load -= 1
            del self.partition_assignments[partition_id]
    
    def rebalance(self, migration_budget: int = 10) -> Dict[str, List[Partition]]:
        """Rebalance range partitions based on load and access patterns."""
        migrations = defaultdict(list)
        
        # Sort partitions by access frequency (descending)
        partitions_by_freq = sorted(
            self.partitions.values(),
            key=lambda p: p.access_frequency,
            reverse=True
        )
        
        # Move high-frequency partitions to less loaded nodes
        for partition in partitions_by_freq[:migration_budget]:
            if partition.node_id:
                current_node = self.nodes[partition.node_id]
                if current_node.utilization > 70:  # If current node is busy
                    new_node_id = self._find_least_loaded_node()
                    if new_node_id and new_node_id != partition.node_id:
                        self._migrate_partition(
                            partition.partition_id,
                            partition.node_id,
                            new_node_id
                        )
                        migrations[new_node_id].append(partition)
        
        return dict(migrations)
    
    def _find_least_loaded_node(self) -> Optional[str]:
        """Find the node with the lowest load."""
        if not self.nodes:
            return None
        
        available_nodes = [
            (nid, n.load) for nid, n in self.nodes.items() 
            if n.can_accept_partition()
        ]
        
        if not available_nodes:
            return None
        
        available_nodes.sort(key=lambda x: x[1])
        return available_nodes[0][0]
    
    def _migrate_partition(self, partition_id: str, from_node: str, to_node: str) -> None:
        """Migrate a partition from one node to another."""
        if partition_id in self.partition_assignments:
            self.partition_assignments[partition_id] = to_node
            self.nodes[from_node].load -= 1
            self.nodes[to_node].load += 1
            self.partitions[partition_id].node_id = to_node
    
    def add_partition(self, partition: Partition) -> str:
        """Add a new partition and return assigned node_id."""
        if partition.partition_id in self.partitions:
            raise ValueError(f"Partition {partition.partition_id} already exists")
        
        self.partitions[partition.partition_id] = partition
        
        # Add to ranges if it has a key range
        if partition.key_range:
            self.ranges.append((*partition.key_range, partition.partition_id))
        
        # Find a node for this partition
        node_id = self._find_node_for_range(partition.key_range)
        if not node_id:
            raise RuntimeError("No available nodes with capacity")
        
        self.partition_assignments[partition.partition_id] = node_id
        self.nodes[node_id].load += 1
        partition.node_id = node_id
        
        return node_id
    
    def get_time_series_partition(self, timestamp: Any) -> Partition:
        """
        Example: Partition time-series data by timestamp.
        
        Time-series data (metrics, logs, events) is naturally ordered by time.
        Range partitioning allows efficient time-range queries and easy data
        retention policies (drop old partitions).
        """
        # Create hourly partitions for demonstration
        if isinstance(timestamp, (int, float)):
            # Assuming timestamp is Unix timestamp
            hour = timestamp // 3600 * 3600
            partition_id = f"ts_{hour}"
            
            if partition_id not in self.partitions:
                # Create new hourly partition
                partition = Partition(
                    partition_id=partition_id,
                    key_range=(hour, hour + 3600),
                    metadata={"type": "time_series", "interval": "hourly"}
                )
                self.add_partition(partition)
            
            return self.partitions[partition_id]
        
        # Fallback for other timestamp formats
        partition_id = f"ts_{hash(str(timestamp)) % 1000}"
        if partition_id not in self.partitions:
            partition = Partition(
                partition_id=partition_id,
                metadata={"type": "time_series"}
            )
            self.add_partition(partition)
        
        return self.partitions[partition_id]


class GeographicPartitioningStrategy(PartitioningStrategy):
    """
    Geographic-based partitioning for locality-aware data placement.
    
    Used for: User sessions, CDN, compliance requirements (data sovereignty).
    
    Features:
    - Data locality for reduced latency
    - Compliance with data residency laws
    - Region-aware replication
    """
    
    def __init__(self, regions: List[str]):
        self.regions = regions
        self.nodes: Dict[str, Node] = {}  # node_id -> Node
        self.partitions: Dict[str, Partition] = {}
        self.partition_assignments: Dict[str, str] = {}
        
        # Region to nodes mapping
        self.region_nodes: Dict[str, Set[str]] = defaultdict(set)
        
        # For cross-region replication
        self.replication_factor = 2
        self.partition_replicas: Dict[str, Set[str]] = defaultdict(set)
    
    def add_node(self, node: Node) -> None:
        """Add a new node in a specific region."""
        if node.node_id in self.nodes:
            raise ValueError(f"Node {node.node_id} already exists")
        
        if not node.region:
            raise ValueError("Geographic partitioning requires node.region to be set")
        
        if node.region not in self.regions:
            raise ValueError(f"Region {node.region} not in configured regions: {self.regions}")
        
        self.nodes[node.node_id] = node
        self.region_nodes[node.region].add(node.node_id)
        
        # Assign some partitions in this region to the new node
        self._assign_partitions_to_new_node(node.node_id, node.region)
    
    def _assign_partitions_to_new_node(self, node_id: str, region: str) -> None:
        """Assign some partitions in the region to a new node."""
        # Find partitions in this region that could be moved
        region_partitions = [
            pid for pid, nid in self.partition_assignments.items()
            if nid in self.region_nodes[region] and pid in self.partitions
        ]
        
        # Sort by load (move from most loaded nodes)
        node_loads = defaultdict(int)
        for pid in region_partitions:
            node_loads[self.partition_assignments[pid]] += 1
        
        overloaded_nodes = [
            (nid, load) for nid, load in node_loads.items()
            if load > 1 and nid != node_id
        ]
        overloaded_nodes.sort(key=lambda x: x[1], reverse=True)
        
        # Move up to 2 partitions
        for overloaded_node_id, _ in overloaded_nodes[:2]:
            partitions_on_node = [
                pid for pid, nid in self.partition_assignments.items()
                if nid == overloaded_node_id
            ]
            if partitions_on_node:
                partition_id = partitions_on_node[0]
                self._migrate_partition(partition_id, overloaded_node_id, node_id)
    
    def remove_node(self, node_id: str) -> List[Partition]:
        """Remove a node and reassign its partitions within the same region."""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} does not exist")
        
        node = self.nodes[node_id]
        region = node.region
        
        # Get partitions on this node
        partitions_to_reassign = [
            self.partitions[pid] 
            for pid, nid in self.partition_assignments.items() 
            if nid == node_id
        ]
        
        # Remove node
        del self.nodes[node_id]
        self.region_nodes[region].discard(node_id)
        
        # Reassign partitions within the same region
        for partition in partitions_to_reassign:
            new_node_id = self._find_node_in_region(region)
            if new_node_id:
                self.partition_assignments[partition.partition_id] = new_node_id
                self.nodes[new_node_id].load += 1
                partition.node_id = new_node_id
        
        return partitions_to_reassign
    
    def _find_node_in_region(self, region: str) -> Optional[str]:
        """Find an available node in a specific region."""
        region_node_ids = self.region_nodes.get(region, set())
        
        available_nodes = [
            (nid, self.nodes[nid].load) 
            for nid in region_node_ids 
            if nid in self.nodes and self.nodes[nid].can_accept_partition()
        ]
        
        if not available_nodes:
            return None
        
        available_nodes.sort(key=lambda x: x[1])
        return available_nodes[0][0]
    
    def get_partition_for_key(self, key: Any) -> Optional[Partition]:
        """Get the partition for a key, considering geographic locality."""
        # Extract region from key if possible
        # For user sessions, key might be "user_id:region"
        key_str = str(key)
        
        # Try to extract region from key
        region = None
        if ":" in key_str:
            parts = key_str.split(":")
            if len(parts) > 1 and parts[-1] in self.regions:
                region = parts[-1]
        
        if not region:
            # Default to first region or hash-based assignment
            region = self.regions[hash(key_str) % len(self.regions)]
        
        # Find or create partition for this region
        partition_id = f"geo_{region}_{hash(key_str) % 100}"
        
        if partition_id not in self.partitions:
            # Create partition in the specified region
            partition = Partition(
                partition_id=partition_id,
                metadata={"region": region, "type": "geographic"}
            )
            self._add_partition_in_region(partition, region)
        
        partition = self.partitions[partition_id]
        partition.access_frequency += 1
        
        return partition
    
    def _add_partition_in_region(self, partition: Partition, region: str) -> str:
        """Add a partition in a specific region."""
        if partition.partition_id in self.partitions:
            raise ValueError(f"Partition {partition.partition_id} already exists")
        
        self.partitions[partition.partition_id] = partition
        
        # Find a node in the region
        node_id = self._find_node_in_region(region)
        if not node_id:
            # Try to find any node (fallback)
            available_nodes = [
                (nid, n.load) for nid, n in self.nodes.items() 
                if n.can_accept_partition()
            ]
            if available_nodes:
                available_nodes.sort(key=lambda x: x[1])
                node_id = available_nodes[0][0]
            else:
                raise RuntimeError(f"No available nodes in region {region}")
        
        self.partition_assignments[partition.partition_id] = node_id
        self.nodes[node_id].load += 1
        partition.node_id = node_id
        
        # Create replicas in other regions for fault tolerance
        self._create_cross_region_replicas(partition.partition_id, region)
        
        return node_id
    
    def _create_cross_region_replicas(self, partition_id: str, primary_region: str) -> None:
        """Create replicas of a partition in other regions."""
        other_regions = [r for r in self.regions if r != primary_region]
        
        for i in range(min(self.replication_factor, len(other_regions))):
            replica_region = other_regions[i]
            replica_node = self._find_node_in_region(replica_region)
            
            if replica_node:
                self.partition_replicas[partition_id].add(replica_node)
                self.nodes[replica_node].load += 1
    
    def rebalance(self, migration_budget: int = 10) -> Dict[str, List[Partition]]:
        """Rebalance partitions considering geographic constraints."""
        migrations = defaultdict(list)
        
        # Check each region for imbalances
        for region in self.regions:
            region_node_ids = self.region_nodes.get(region, set())
            
            if len(region_node_ids) < 2:
                continue  # Need at least 2 nodes to rebalance
            
            # Calculate average load in this region
            region_loads = [
                self.nodes[nid].load for nid in region_node_ids 
                if nid in self.nodes
            ]
            
            if not region_loads:
                continue
            
            avg_load = sum(region_loads) / len(region_loads)
            
            # Find overloaded and underloaded nodes
            overloaded = []
            underloaded = []
            
            for node_id in region_node_ids:
                if node_id not in self.nodes:
                    continue
                    
                node = self.nodes[node_id]
                if node.load > avg_load * 1.3:  # 30% above average
                    overloaded.append((node_id, node.load))
                elif node.load < avg_load * 0.7:  # 30% below average
                    underloaded.append((node_id, node.load))
            
            # Move partitions from overloaded to underloaded
            for overloaded_id, _ in overloaded:
                if not underloaded:
                    break
                    
                partitions_on_node = [
                    pid for pid, nid in self.partition_assignments.items()
                    if nid == overloaded_id
                ]
                
                # Move up to 2 partitions per overloaded node
                for partition_id in partitions_on_node[:2]:
                    if underloaded:
                        target_id, _ = underloaded.pop(0)
                        self._migrate_partition(partition_id, overloaded_id, target_id)
                        migrations[target_id].append(self.partitions[partition_id])
        
        return dict(migrations)
    
    def _migrate_partition(self, partition_id: str, from_node: str, to_node: str) -> None:
        """Migrate a partition from one node to another."""
        if partition_id in self.partition_assignments:
            self.partition_assignments[partition_id] = to_node
            self.nodes[from_node].load -= 1
            self.nodes[to_node].load += 1
            self.partitions[partition_id].node_id = to_node
    
    def add_partition(self, partition: Partition) -> str:
        """Add a new partition (region must be specified in metadata)."""
        region = partition.metadata.get("region")
        if not region:
            raise ValueError("Geographic partitioning requires 'region' in partition metadata")
        
        return self._add_partition_in_region(partition, region)
    
    def get_user_session_partition(self, user_id: str, user_region: str) -> Partition:
        """
        Example: Partition user sessions by user location.
        
        User sessions need to be stored close to the user for low latency.
        Geographic partitioning ensures session data is stored in the user's
        region, with cross-region replication for failover.
        """
        partition_id = f"session_{user_region}_{hash(user_id) % 1000}"
        
        if partition_id not in self.partitions:
            partition = Partition(
                partition_id=partition_id,
                metadata={
                    "type": "user_session",
                    "region": user_region,
                    "user_id_prefix": user_id[:3]
                }
            )
            self._add_partition_in_region(partition, user_region)
        
        return self.partitions[partition_id]


class PartitionManager:
    """
    Main manager that coordinates partitioning strategies and provides
    a unified interface for the application.
    """
    
    def __init__(self, strategy: PartitioningStrategy):
        self.strategy = strategy
        self.metrics = {
            "total_partitions": 0,
            "total_nodes": 0,
            "migrations_performed": 0,
            "hotspots_detected": 0
        }
    
    def add_storage_node(self, node_id: str, capacity: int = 1000, region: Optional[str] = None) -> None:
        """Add a new storage node to the cluster."""
        node = Node(node_id=node_id, capacity=capacity, region=region)
        self.strategy.add_node(node)
        self.metrics["total_nodes"] += 1
    
    def remove_storage_node(self, node_id: str) -> List[Dict[str, Any]]:
        """Remove a storage node and return migration plan."""
        partitions = self.strategy.remove_node(node_id)
        self.metrics["total_nodes"] -= 1
        self.metrics["migrations_performed"] += len(partitions)
        
        return [
            {
                "partition_id": p.partition_id,
                "from_node": node_id,
                "size": p.size,
                "access_frequency": p.access_frequency
            }
            for p in partitions
        ]
    
    def store_data(self, key: Any, data: Any, metadata: Optional[Dict] = None) -> str:
        """Store data and return the partition_id where it's stored."""
        partition = self.strategy.get_partition_for_key(key)
        
        if not partition:
            # Create a new partition
            partition_id = f"partition_{hash(str(key)) % 10000}"
            partition = Partition(
                partition_id=partition_id,
                metadata=metadata or {}
            )
            node_id = self.strategy.add_partition(partition)
            self.metrics["total_partitions"] += 1
        else:
            node_id = partition.node_id
        
        # Update partition size
        partition.size += 1
        
        # Check for hotspots
        if partition.is_hotspot:
            self.metrics["hotspots_detected"] += 1
        
        return partition.partition_id
    
    def retrieve_data_location(self, key: Any) -> Optional[Dict[str, Any]]:
        """Get the location information for a given key."""
        partition = self.strategy.get_partition_for_key(key)
        
        if not partition:
            return None
        
        return {
            "partition_id": partition.partition_id,
            "node_id": partition.node_id,
            "size": partition.size,
            "access_frequency": partition.access_frequency,
            "is_hotspot": partition.is_hotspot
        }
    
    def rebalance_cluster(self, migration_budget: int = 10) -> Dict[str, Any]:
        """Trigger cluster rebalancing and return migration plan."""
        migrations = self.strategy.rebalance(migration_budget)
        
        total_migrations = sum(len(parts) for parts in migrations.values())
        self.metrics["migrations_performed"] += total_migrations
        
        return {
            "migrations": {
                node_id: [p.partition_id for p in partitions]
                for node_id, partitions in migrations.items()
            },
            "total_migrations": total_migrations,
            "metrics": self.metrics.copy()
        }
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status and metrics."""
        if hasattr(self.strategy, 'nodes'):
            nodes = self.strategy.nodes
        else:
            nodes = {}
        
        node_stats = {}
        for node_id, node in nodes.items():
            node_stats[node_id] = {
                "load": node.load,
                "capacity": node.capacity,
                "utilization": node.utilization,
                "region": getattr(node, 'region', None)
            }
        
        return {
            "strategy": self.strategy.__class__.__name__,
            "total_nodes": len(nodes),
            "total_partitions": len(getattr(self.strategy, 'partitions', {})),
            "node_stats": node_stats,
            "metrics": self.metrics
        }


# Example usage and demonstration
def demonstrate_social_graph_partitioning():
    """Demonstrate consistent hashing for social graph partitioning."""
    print("=== Social Graph Partitioning (Consistent Hashing) ===")
    
    strategy = ConsistentHashingStrategy(virtual_nodes_per_physical=100)
    manager = PartitionManager(strategy)
    
    # Add nodes
    nodes = [
        ("social-node-1", 1000, "us-east"),
        ("social-node-2", 1000, "us-west"),
        ("social-node-3", 1000, "eu-west"),
    ]
    
    for node_id, capacity, region in nodes:
        manager.add_storage_node(node_id, capacity, region)
    
    # Store some user data
    users = ["alice", "bob", "charlie", "david", "eve"]
    for user in users:
        partition_id = manager.store_data(
            key=user,
            data={"user_id": user, "friends": [], "posts": []},
            metadata={"type": "user_profile", "user": user}
        )
        print(f"User {user} stored in partition {partition_id}")
    
    # Check cluster status
    status = manager.get_cluster_status()
    print(f"\nCluster status: {status['total_nodes']} nodes, {status['total_partitions']} partitions")
    
    # Simulate node failure
    print("\nSimulating node failure...")
    migrations = manager.remove_storage_node("social-node-2")
    print(f"Migrated {len(migrations)} partitions")
    
    # Rebalance
    print("\nRebalancing cluster...")
    rebalance_result = manager.rebalance_cluster()
    print(f"Performed {rebalance_result['total_migrations']} migrations")


def demonstrate_time_series_partitioning():
    """Demonstrate range partitioning for time-series data."""
    print("\n=== Time-Series Data Partitioning (Range Partitioning) ===")
    
    # Create hourly ranges for the last 24 hours
    import time
    current_hour = int(time.time()) // 3600 * 3600
    ranges = [(current_hour - i*3600, current_hour - (i-1)*3600) for i in range(24, 0, -1)]
    
    strategy = RangePartitioningStrategy(initial_ranges=ranges)
    manager = PartitionManager(strategy)
    
    # Add nodes
    for i in range(3):
        manager.add_storage_node(f"timeseries-node-{i+1}", 500)
    
    # Store time-series data
    for i in range(100):
        timestamp = current_hour - random.randint(0, 86400)  # Last 24 hours
        partition_id = manager.store_data(
            key=timestamp,
            data={"timestamp": timestamp, "value": random.random()},
            metadata={"type": "metric", "metric_name": "cpu_usage"}
        )
    
    # Check for hotspots
    status = manager.get_cluster_status()
    print(f"Time-series cluster: {status['total_partitions']} partitions")
    
    # Query data in a time range
    print("\nQuerying data from last hour...")
    for i in range(10):
        timestamp = current_hour - random.randint(0, 3600)
        location = manager.retrieve_data_location(timestamp)
        if location:
            print(f"Timestamp {timestamp} in partition {location['partition_id']}")


def demonstrate_geographic_partitioning():
    """Demonstrate geographic partitioning for user sessions."""
    print("\n=== User Session Partitioning (Geographic) ===")
    
    regions = ["us-east", "us-west", "eu-west", "ap-south"]
    strategy = GeographicPartitioningStrategy(regions=regions)
    manager = PartitionManager(strategy)
    
    # Add nodes in different regions
    region_nodes = {
        "us-east": ["us-east-1", "us-east-2"],
        "us-west": ["us-west-1"],
        "eu-west": ["eu-west-1", "eu-west-2"],
        "ap-south": ["ap-south-1"]
    }
    
    for region, node_ids in region_nodes.items():
        for node_id in node_ids:
            manager.add_storage_node(node_id, 800, region)
    
    # Store user sessions
    users = [
        ("user-us-1", "us-east"),
        ("user-us-2", "us-west"),
        ("user-eu-1", "eu-west"),
        ("user-ap-1", "ap-south"),
    ]
    
    for user_id, user_region in users:
        partition_id = manager.store_data(
            key=f"{user_id}:{user_region}",
            data={"user_id": user_id, "session_data": {}, "last_active": "2024-01-01"},
            metadata={"type": "user_session", "region": user_region}
        )
        print(f"User {user_id} session in region {user_region}, partition {partition_id}")
    
    # Check cluster status
    status = manager.get_cluster_status()
    print(f"\nGeographic cluster: {status['total_nodes']} nodes across {len(regions)} regions")
    
    # Show node distribution by region
    print("\nNode distribution by region:")
    for region in regions:
        region_node_count = sum(
            1 for node_id, stats in status['node_stats'].items() 
            if stats.get('region') == region
        )
        print(f"  {region}: {region_node_count} nodes")


if __name__ == "__main__":
    # Run demonstrations
    demonstrate_social_graph_partitioning()
    demonstrate_time_series_partitioning()
    demonstrate_geographic_partitioning()
    
    print("\n=== Partitioning Strategies Summary ===")
    print("1. Consistent Hashing: Best for key-value data, caches, social graphs")
    print("2. Range Partitioning: Best for time-series, sequential data, range queries")
    print("3. Geographic Partitioning: Best for user sessions, compliance, low-latency access")
    print("\nAll strategies include:")
    print("- Virtual nodes for better distribution")
    print("- Automatic rebalancing")
    print("- Hotspot detection and mitigation")
    print("- Partition migration support")