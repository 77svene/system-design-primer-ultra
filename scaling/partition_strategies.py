# scaling/partition_strategies.py

"""
Scalable Data Partitioning Strategies

Implements consistent hashing, range partitioning, and geographic partitioning
with rebalancing algorithms. Demonstrates virtual nodes, partition migration,
and hotspot mitigation for real-world use cases.
"""

import hashlib
import bisect
import random
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass
from collections import defaultdict
import math
import json
from datetime import datetime, timedelta


@dataclass
class PartitionNode:
    """Represents a physical node in the partitioning system."""
    node_id: str
    capacity: int  # Relative capacity (e.g., 1 for standard, 2 for double)
    region: str = "default"
    load: float = 0.0  # Current load (0.0 to 1.0)
    virtual_nodes: int = 1  # Number of virtual nodes for consistent hashing


@dataclass
class Partition:
    """Represents a data partition."""
    partition_id: str
    start_key: Optional[str] = None
    end_key: Optional[str] = None
    node_id: Optional[str] = None
    region: Optional[str] = None
    size: int = 0  # Size in bytes
    access_count: int = 0  # Number of accesses per time window
    last_accessed: Optional[datetime] = None


class PartitionStrategy:
    """Base class for partitioning strategies."""
    
    def __init__(self, nodes: List[PartitionNode]):
        self.nodes = {node.node_id: node for node in nodes}
        self.partitions: Dict[str, Partition] = {}
        self.rebalancing_threshold = 0.2  # 20% load imbalance triggers rebalance
    
    def get_partition(self, key: str) -> str:
        """Get partition ID for a given key."""
        raise NotImplementedError
    
    def get_node_for_key(self, key: str) -> str:
        """Get node ID for a given key."""
        raise NotImplementedError
    
    def add_node(self, node: PartitionNode) -> List[Tuple[str, str]]:
        """Add a node and return partitions to migrate."""
        raise NotImplementedError
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and return redistribution plan."""
        raise NotImplementedError
    
    def rebalance(self) -> List[Tuple[str, str, str]]:
        """Rebalance partitions across nodes. Returns migration plan."""
        raise NotImplementedError
    
    def update_access_stats(self, key: str, size_delta: int = 0):
        """Update access statistics for hotspot detection."""
        partition_id = self.get_partition(key)
        if partition_id in self.partitions:
            partition = self.partitions[partition_id]
            partition.access_count += 1
            partition.last_accessed = datetime.now()
            partition.size += size_delta


class ConsistentHashingStrategy(PartitionStrategy):
    """
    Consistent hashing with virtual nodes for better distribution.
    
    Features:
    - Virtual nodes for load balancing
    - Minimal data movement when nodes are added/removed
    - Hotspot mitigation through virtual node replication
    """
    
    def __init__(self, nodes: List[PartitionNode], virtual_nodes_per_node: int = 100):
        super().__init__(nodes)
        self.virtual_nodes_per_node = virtual_nodes_per_node
        self.ring: List[Tuple[int, str]] = []  # (hash_value, node_id)
        self.virtual_node_map: Dict[str, List[int]] = defaultdict(list)  # node_id -> [hash_values]
        self._build_ring()
    
    def _hash(self, key: str) -> int:
        """Generate consistent hash for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _build_ring(self):
        """Build the consistent hash ring with virtual nodes."""
        self.ring = []
        self.virtual_node_map.clear()
        
        for node_id, node in self.nodes.items():
            num_virtual = node.virtual_nodes * self.virtual_nodes_per_node
            for i in range(num_virtual):
                virtual_key = f"{node_id}:vn{i}"
                hash_val = self._hash(virtual_key)
                self.ring.append((hash_val, node_id))
                self.virtual_node_map[node_id].append(hash_val)
        
        self.ring.sort()
    
    def get_partition(self, key: str) -> str:
        """Get partition ID (virtual node) for a given key."""
        if not self.ring:
            raise ValueError("No nodes in the ring")
        
        hash_val = self._hash(key)
        
        # Find the first node with hash >= key's hash
        idx = bisect.bisect_left(self.ring, (hash_val, ""))
        if idx == len(self.ring):
            idx = 0  # Wrap around
        
        return self.ring[idx][1]  # Return node_id
    
    def get_node_for_key(self, key: str) -> str:
        """Get physical node for a given key."""
        return self.get_partition(key)  # In consistent hashing, partition = node
    
    def add_node(self, node: PartitionNode) -> List[Tuple[str, str]]:
        """Add a node and return keys to migrate from existing nodes."""
        self.nodes[node.node_id] = node
        
        # Calculate how many virtual nodes this new node should take
        total_nodes = len(self.nodes)
        target_load = 1.0 / total_nodes
        
        migrations = []
        current_loads = self._calculate_loads()
        
        # Find nodes with above-average load
        overloaded_nodes = [
            nid for nid, load in current_loads.items() 
            if load > target_load * (1 + self.rebalancing_threshold)
        ]
        
        # Simulate migration (in reality, we'd compute actual key ranges)
        for overloaded_id in overloaded_nodes[:3]:  # Limit migrations
            migrations.append((overloaded_id, node.node_id))
        
        # Rebuild ring with new node
        self._build_ring()
        return migrations
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and redistribute its virtual nodes."""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")
        
        # Get virtual nodes for this node
        virtual_hashes = self.virtual_node_map.get(node_id, [])
        
        # Find new nodes for each virtual node
        redistribution = defaultdict(list)
        
        for vhash in virtual_hashes:
            # Find next node in ring (excluding the one being removed)
            idx = bisect.bisect_left(self.ring, (vhash, ""))
            if idx == len(self.ring):
                idx = 0
            
            # Skip the node being removed
            while self.ring[idx][1] == node_id:
                idx = (idx + 1) % len(self.ring)
            
            new_node_id = self.ring[idx][1]
            redistribution[new_node_id].append(f"vn_{vhash}")
        
        # Remove node
        del self.nodes[node_id]
        self._build_ring()
        
        return dict(redistribution)
    
    def rebalance(self) -> List[Tuple[str, str, str]]:
        """Rebalance by adjusting virtual node distribution."""
        migrations = []
        loads = self._calculate_loads()
        avg_load = sum(loads.values()) / len(loads) if loads else 0
        
        # Identify hotspots
        hotspots = [
            nid for nid, load in loads.items() 
            if load > avg_load * (1 + self.rebalancing_threshold)
        ]
        
        # For each hotspot, move some virtual nodes to underloaded nodes
        underloaded = [
            nid for nid, load in loads.items() 
            if load < avg_load * (1 - self.rebalancing_threshold)
        ]
        
        for hotspot_id in hotspots:
            if not underloaded:
                break
            
            target_id = underloaded.pop(0)
            # Move 10% of virtual nodes from hotspot to target
            vnodes_to_move = max(1, len(self.virtual_node_map[hotspot_id]) // 10)
            
            for _ in range(vnodes_to_move):
                if self.virtual_node_map[hotspot_id]:
                    migrations.append((hotspot_id, target_id, f"vn_move_{len(migrations)}"))
        
        return migrations
    
    def _calculate_loads(self) -> Dict[str, float]:
        """Calculate current load distribution."""
        loads = {}
        total_capacity = sum(node.capacity for node in self.nodes.values())
        
        for node_id, node in self.nodes.items():
            # Load = (virtual_nodes * capacity) / total_weighted_capacity
            vnodes = len(self.virtual_node_map.get(node_id, []))
            loads[node_id] = (vnodes * node.capacity) / (total_capacity * self.virtual_nodes_per_node)
        
        return loads
    
    def get_ring_distribution(self) -> List[Tuple[str, float]]:
        """Get the distribution of keys across the ring for visualization."""
        distribution = []
        total_hashes = len(self.ring)
        
        for i in range(len(self.ring)):
            start_hash = self.ring[i][0]
            end_hash = self.ring[(i + 1) % len(self.ring)][0]
            
            if end_hash < start_hash:  # Wrap around
                segment_size = (2**128 - start_hash) + end_hash
            else:
                segment_size = end_hash - start_hash
            
            distribution.append((self.ring[i][1], segment_size / 2**128))
        
        return distribution


class RangePartitioningStrategy(PartitionStrategy):
    """
    Range-based partitioning with dynamic splitting and merging.
    
    Features:
    - Automatic range splitting for hotspots
    - Range merging for underutilized partitions
    - Support for ordered scans
    """
    
    def __init__(self, nodes: List[PartitionNode], initial_ranges: int = 10):
        super().__init__(nodes)
        self.initial_ranges = initial_ranges
        self.split_threshold = 1000  # Access count before considering split
        self.merge_threshold = 100   # Access count below which to consider merge
        self._initialize_partitions()
    
    def _initialize_partitions(self):
        """Initialize partitions across the key space."""
        # Create initial evenly distributed partitions
        for i in range(self.initial_ranges):
            partition_id = f"range_{i}"
            start_key = str(i * (2**32 // self.initial_ranges))
            end_key = str((i + 1) * (2**32 // self.initial_ranges) - 1)
            
            # Assign to node with least load
            node_id = self._get_least_loaded_node()
            
            self.partitions[partition_id] = Partition(
                partition_id=partition_id,
                start_key=start_key,
                end_key=end_key,
                node_id=node_id,
                size=0,
                access_count=0
            )
    
    def _get_least_loaded_node(self) -> str:
        """Get the node with the least current load."""
        if not self.nodes:
            raise ValueError("No nodes available")
        
        loads = self._calculate_node_loads()
        return min(loads.items(), key=lambda x: x[1])[0]
    
    def _calculate_node_loads(self) -> Dict[str, float]:
        """Calculate load per node based on partition sizes and access patterns."""
        loads = {node_id: 0.0 for node_id in self.nodes}
        
        for partition in self.partitions.values():
            if partition.node_id:
                # Load = size_weight + access_weight
                size_weight = partition.size / (1024 * 1024 * 1024)  # Normalize to GB
                access_weight = partition.access_count / 1000  # Normalize
                loads[partition.node_id] += size_weight + access_weight
        
        return loads
    
    def get_partition(self, key: str) -> str:
        """Get partition ID for a given key using range lookup."""
        key_int = int(key) if key.isdigit() else self._hash_key(key)
        
        for partition_id, partition in self.partitions.items():
            start = int(partition.start_key) if partition.start_key else 0
            end = int(partition.end_key) if partition.end_key else 2**32
            
            if start <= key_int <= end:
                return partition_id
        
        # Default to first partition if no match
        return list(self.partitions.keys())[0]
    
    def get_node_for_key(self, key: str) -> str:
        """Get node ID for a given key."""
        partition_id = self.get_partition(key)
        return self.partitions[partition_id].node_id
    
    def _hash_key(self, key: str) -> int:
        """Hash a non-numeric key to an integer in the key space."""
        hash_digest = hashlib.md5(key.encode()).hexdigest()
        return int(hash_digest[:8], 16)  # Use first 8 hex chars (32 bits)
    
    def add_node(self, node: PartitionNode) -> List[Tuple[str, str]]:
        """Add a node and split the most loaded partition."""
        self.nodes[node.node_id] = node
        
        # Find most loaded partition
        most_loaded = max(
            self.partitions.values(),
            key=lambda p: p.size + p.access_count * 1000
        )
        
        # Split the partition
        split_point = (int(most_loaded.start_key) + int(most_loaded.end_key)) // 2
        
        # Create new partition for the second half
        new_partition_id = f"{most_loaded.partition_id}_split_{len(self.partitions)}"
        new_partition = Partition(
            partition_id=new_partition_id,
            start_key=str(split_point + 1),
            end_key=most_loaded.end_key,
            node_id=node.node_id,
            size=most_loaded.size // 2,
            access_count=most_loaded.access_count // 2
        )
        
        # Update original partition
        most_loaded.end_key = str(split_point)
        most_loaded.size //= 2
        most_loaded.access_count //= 2
        
        self.partitions[new_partition_id] = new_partition
        
        return [(most_loaded.node_id, node.node_id)]
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and merge its partitions to other nodes."""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")
        
        # Find partitions on this node
        node_partitions = [
            pid for pid, p in self.partitions.items() 
            if p.node_id == node_id
        ]
        
        redistribution = defaultdict(list)
        
        for partition_id in node_partitions:
            # Find adjacent partition to merge with
            partition = self.partitions[partition_id]
            
            # Find partition with closest range
            closest_partition = None
            min_distance = float('inf')
            
            for other_id, other in self.partitions.items():
                if other_id == partition_id or other.node_id == node_id:
                    continue
                
                # Calculate distance between ranges
                if partition.end_key and other.start_key:
                    distance = abs(int(partition.end_key) - int(other.start_key))
                    if distance < min_distance:
                        min_distance = distance
                        closest_partition = other
            
            if closest_partition:
                # Merge partitions
                closest_partition.start_key = min(
                    closest_partition.start_key or "0",
                    partition.start_key or "0"
                )
                closest_partition.end_key = max(
                    closest_partition.end_key or "0",
                    partition.end_key or "0"
                )
                closest_partition.size += partition.size
                closest_partition.access_count += partition.access_count
                
                redistribution[closest_partition.node_id].append(partition_id)
                del self.partitions[partition_id]
        
        del self.nodes[node_id]
        return dict(redistribution)
    
    def rebalance(self) -> List[Tuple[str, str, str]]:
        """Rebalance by splitting hot partitions and merging cold ones."""
        migrations = []
        
        # Split hot partitions
        for partition_id, partition in list(self.partitions.items()):
            if partition.access_count > self.split_threshold:
                # Split into two
                mid_point = (int(partition.start_key) + int(partition.end_key)) // 2
                
                # Create new partition
                new_id = f"{partition_id}_hot_{len(self.partitions)}"
                new_partition = Partition(
                    partition_id=new_id,
                    start_key=str(mid_point + 1),
                    end_key=partition.end_key,
                    node_id=self._get_least_loaded_node(),
                    size=partition.size // 2,
                    access_count=partition.access_count // 2
                )
                
                # Update original
                partition.end_key = str(mid_point)
                partition.size //= 2
                partition.access_count //= 2
                
                self.partitions[new_id] = new_partition
                migrations.append((partition.node_id, new_partition.node_id, new_id))
        
        # Merge cold partitions
        cold_partitions = [
            (pid, p) for pid, p in self.partitions.items()
            if p.access_count < self.merge_threshold
        ]
        
        # Sort by start key to find adjacent cold partitions
        cold_partitions.sort(key=lambda x: int(x[1].start_key) if x[1].start_key else 0)
        
        i = 0
        while i < len(cold_partitions) - 1:
            pid1, p1 = cold_partitions[i]
            pid2, p2 = cold_partitions[i + 1]
            
            # Check if partitions are adjacent
            if p1.end_key and p2.start_key and int(p1.end_key) + 1 == int(p2.start_key):
                # Merge p2 into p1
                p1.end_key = p2.end_key
                p1.size += p2.size
                p1.access_count += p2.access_count
                
                migrations.append((p2.node_id, p1.node_id, pid2))
                del self.partitions[pid2]
                
                # Remove from cold_partitions list
                cold_partitions.pop(i + 1)
            else:
                i += 1
        
        return migrations


class GeographicPartitioningStrategy(PartitionStrategy):
    """
    Geographic partitioning with region-aware placement.
    
    Features:
    - Data locality for compliance (GDPR, data sovereignty)
    - Cross-region replication for fault tolerance
    - Latency-based routing
    """
    
    def __init__(self, nodes: List[PartitionNode], replication_factor: int = 2):
        super().__init__(nodes)
        self.replication_factor = min(replication_factor, len(set(n.region for n in nodes)))
        self.region_nodes: Dict[str, List[str]] = defaultdict(list)
        self.user_regions: Dict[str, str] = {}  # user_id -> preferred_region
        
        # Organize nodes by region
        for node_id, node in self.nodes.items():
            self.region_nodes[node.region].append(node_id)
        
        self._initialize_geo_partitions()
    
    def _initialize_geo_partitions(self):
        """Initialize geographic partitions based on regions."""
        for region, node_ids in self.region_nodes.items():
            partition_id = f"geo_{region}"
            self.partitions[partition_id] = Partition(
                partition_id=partition_id,
                region=region,
                node_id=node_ids[0] if node_ids else None,
                size=0,
                access_count=0
            )
    
    def get_partition(self, key: str) -> str:
        """Get partition ID based on geographic affinity."""
        # Extract region from key or use default
        # Format: "region:user_id" or just "user_id"
        if ":" in key:
            region, _ = key.split(":", 1)
        else:
            # Determine region based on user_id hash
            region = self._determine_region_for_user(key)
        
        partition_id = f"geo_{region}"
        if partition_id not in self.partitions:
            # Fallback to default region
            partition_id = f"geo_{list(self.region_nodes.keys())[0]}"
        
        return partition_id
    
    def get_node_for_key(self, key: str) -> str:
        """Get primary node for a key, considering geographic affinity."""
        partition_id = self.get_partition(key)
        partition = self.partitions[partition_id]
        
        if partition.node_id:
            return partition.node_id
        
        # Fallback to any node in the region
        region = partition.region
        if region in self.region_nodes and self.region_nodes[region]:
            return self.region_nodes[region][0]
        
        # Ultimate fallback
        return list(self.nodes.keys())[0]
    
    def get_replica_nodes(self, key: str) -> List[str]:
        """Get replica nodes for fault tolerance."""
        primary_node = self.get_node_for_key(key)
        primary_region = self.nodes[primary_node].region
        
        replica_nodes = []
        
        # Add replicas from same region (for low-latency reads)
        same_region_nodes = [
            nid for nid in self.region_nodes.get(primary_region, [])
            if nid != primary_node
        ]
        replica_nodes.extend(same_region_nodes[:self.replication_factor - 1])
        
        # If we need more replicas, add from other regions
        if len(replica_nodes) < self.replication_factor - 1:
            for region, node_ids in self.region_nodes.items():
                if region != primary_region:
                    replica_nodes.extend(node_ids[:self.replication_factor - 1 - len(replica_nodes)])
                    if len(replica_nodes) >= self.replication_factor - 1:
                        break
        
        return replica_nodes
    
    def _determine_region_for_user(self, user_id: str) -> str:
        """Determine the best region for a user based on various factors."""
        # Check cache first
        if user_id in self.user_regions:
            return self.user_regions[user_id]
        
        # Simple hash-based assignment (in reality, use geolocation, latency measurements, etc.)
        hash_val = int(hashlib.md5(user_id.encode()).hexdigest(), 16)
        regions = list(self.region_nodes.keys())
        region_idx = hash_val % len(regions)
        
        selected_region = regions[region_idx]
        self.user_regions[user_id] = selected_region
        
        return selected_region
    
    def add_node(self, node: PartitionNode) -> List[Tuple[str, str]]:
        """Add a node to a region and update replication."""
        self.nodes[node.node_id] = node
        self.region_nodes[node.region].append(node.node_id)
        
        migrations = []
        
        # If this is the first node in a region, create a partition
        if len(self.region_nodes[node.region]) == 1:
            partition_id = f"geo_{node.region}"
            self.partitions[partition_id] = Partition(
                partition_id=partition_id,
                region=node.region,
                node_id=node.node_id,
                size=0,
                access_count=0
            )
        else:
            # Redistribute some load from existing nodes in the region
            existing_nodes = self.region_nodes[node.region][:-1]  # Exclude new node
            if existing_nodes:
                source_node = random.choice(existing_nodes)
                migrations.append((source_node, node.node_id))
        
        return migrations
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and update replication."""
        if node_id not in self.nodes:
            raise ValueError(f"Node {node_id} not found")
        
        node = self.nodes[node_id]
        region = node.region
        
        # Remove from region nodes
        if region in self.region_nodes and node_id in self.region_nodes[region]:
            self.region_nodes[region].remove(node_id)
        
        redistribution = defaultdict(list)
        
        # Update partitions that were using this node
        for partition_id, partition in self.partitions.items():
            if partition.node_id == node_id:
                # Find new node in same region
                if region in self.region_nodes and self.region_nodes[region]:
                    new_node_id = self.region_nodes[region][0]
                    partition.node_id = new_node_id
                    redistribution[new_node_id].append(partition_id)
                else:
                    # Region has no nodes left - migrate to another region
                    for other_region, node_ids in self.region_nodes.items():
                        if node_ids:
                            partition.node_id = node_ids[0]
                            partition.region = other_region
                            redistribution[node_ids[0]].append(partition_id)
                            break
        
        del self.nodes[node_id]
        return dict(redistribution)
    
    def rebalance(self) -> List[Tuple[str, str, str]]:
        """Rebalance by adjusting cross-region replication."""
        migrations = []
        
        # Ensure each region has optimal number of replicas
        for region, node_ids in self.region_nodes.items():
            if len(node_ids) > self.replication_factor:
                # Too many nodes in region - consider moving some to under-represented regions
                under_represented = [
                    r for r, nodes in self.region_nodes.items()
                    if len(nodes) < self.replication_factor and r != region
                ]
                
                if under_represented:
                    target_region = under_represented[0]
                    node_to_move = node_ids[-1]  # Move the last node
                    
                    migrations.append((
                        node_to_move,
                        f"move_to_{target_region}",
                        f"region_rebalance_{len(migrations)}"
                    ))
        
        return migrations
    
    def get_region_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for each region."""
        stats = {}
        
        for region, node_ids in self.region_nodes.items():
            region_partitions = [
                p for p in self.partitions.values()
                if p.region == region
            ]
            
            total_size = sum(p.size for p in region_partitions)
            total_access = sum(p.access_count for p in region_partitions)
            
            stats[region] = {
                "node_count": len(node_ids),
                "partition_count": len(region_partitions),
                "total_size_bytes": total_size,
                "total_access_count": total_access,
                "avg_load": total_access / max(1, len(node_ids))
            }
        
        return stats


# Real-world example implementations

class SocialGraphPartitioner:
    """
    Partitioner for social graph data.
    
    Strategy: Consistent hashing with social circle awareness.
    - Friends are co-located for efficient traversal
    - Hot users (celebrities) get dedicated partitions
    """
    
    def __init__(self, nodes: List[PartitionNode]):
        self.strategy = ConsistentHashingStrategy(nodes, virtual_nodes_per_node=50)
        self.user_circles: Dict[str, Set[str]] = defaultdict(set)  # user -> friends
        self.hot_users: Set[str] = set()  # Users with many connections
    
    def add_friendship(self, user1: str, user2: str):
        """Add a friendship relationship."""
        self.user_circles[user1].add(user2)
        self.user_circles[user2].add(user1)
        
        # Check if either user is becoming "hot"
        if len(self.user_circles[user1]) > 1000:
            self.hot_users.add(user1)
        if len(self.user_circles[user2]) > 1000:
            self.hot_users.add(user2)
    
    def get_partition_for_user(self, user_id: str) -> str:
        """Get partition for a user, considering social circle."""
        if user_id in self.hot_users:
            # Hot users get their own partition
            return f"hot_user_{user_id}"
        
        # Otherwise, use consistent hashing
        return self.strategy.get_partition(user_id)
    
    def get_friends_partitions(self, user_id: str) -> List[str]:
        """Get partitions containing a user's friends."""
        friends = self.user_circles.get(user_id, set())
        partitions = set()
        
        for friend in friends:
            partitions.add(self.get_partition_for_user(friend))
        
        return list(partitions)


class TimeSeriesPartitioner:
    """
    Partitioner for time-series data.
    
    Strategy: Range partitioning by time with automatic bucketing.
    - Recent data in hot partitions
    - Historical data in cold storage
    - Automatic time-based sharding
    """
    
    def __init__(self, nodes: List[PartitionNode], time_bucket_hours: int = 24):
        self.nodes = nodes
        self.time_bucket_hours = time_bucket_hours
        self.partitions: Dict[str, Partition] = {}
        self.current_bucket = self._get_current_bucket()
        
        # Initialize with recent buckets
        for i in range(7):  # Last 7 days
            bucket_time = datetime.now() - timedelta(hours=i * time_bucket_hours)
            bucket_id = self._get_bucket_id(bucket_time)
            self._create_bucket_partition(bucket_id, bucket_time)
    
    def _get_current_bucket(self) -> str:
        """Get the current time bucket ID."""
        now = datetime.now()
        bucket_start = now.replace(
            hour=(now.hour // self.time_bucket_hours) * self.time_bucket_hours,
            minute=0, second=0, microsecond=0
        )
        return bucket_start.isoformat()
    
    def _get_bucket_id(self, timestamp: datetime) -> str:
        """Get bucket ID for a timestamp."""
        bucket_start = timestamp.replace(
            hour=(timestamp.hour // self.time_bucket_hours) * self.time_bucket_hours,
            minute=0, second=0, microsecond=0
        )
        return bucket_start.isoformat()
    
    def _create_bucket_partition(self, bucket_id: str, timestamp: datetime):
        """Create a partition for a time bucket."""
        # Assign to node based on time (round-robin or hash)
        node_idx = hash(bucket_id) % len(self.nodes)
        node_id = self.nodes[node_idx].node_id
        
        self.partitions[bucket_id] = Partition(
            partition_id=bucket_id,
            start_key=timestamp.isoformat(),
            end_key=(timestamp + timedelta(hours=self.time_bucket_hours)).isoformat(),
            node_id=node_id,
            size=0,
            access_count=0
        )
    
    def get_partition_for_timestamp(self, timestamp: datetime) -> str:
        """Get partition for a specific timestamp."""
        bucket_id = self._get_bucket_id(timestamp)
        
        if bucket_id not in self.partitions:
            self._create_bucket_partition(bucket_id, timestamp)
        
        return bucket_id
    
    def get_node_for_timestamp(self, timestamp: datetime) -> str:
        """Get node for a specific timestamp."""
        partition_id = self.get_partition_for_timestamp(timestamp)
        return self.partitions[partition_id].node_id
    
    def archive_old_data(self, days_to_keep: int = 30):
        """Archive partitions older than specified days."""
        cutoff = datetime.now() - timedelta(days=days_to_keep)
        
        to_archive = []
        for bucket_id, partition in self.partitions.items():
            bucket_time = datetime.fromisoformat(partition.start_key)
            if bucket_time < cutoff:
                to_archive.append(bucket_id)
        
        # In reality, this would move data to cold storage
        for bucket_id in to_archive:
            print(f"Archiving bucket {bucket_id}")
            del self.partitions[bucket_id]
        
        return to_archive


class UserSessionPartitioner:
    """
    Partitioner for user session data.
    
    Strategy: Geographic partitioning with session affinity.
    - Sessions stay on the same node for consistency
    - Geographic routing for low latency
    - Session migration on node failure
    """
    
    def __init__(self, nodes: List[PartitionNode]):
        self.strategy = GeographicPartitioningStrategy(nodes, replication_factor=2)
        self.session_locations: Dict[str, str] = {}  # session_id -> node_id
        self.user_sessions: Dict[str, Set[str]] = defaultdict(set)  # user -> sessions
    
    def create_session(self, user_id: str, session_id: str, 
                      user_region: Optional[str] = None) -> str:
        """Create a new session and assign to optimal node."""
        if user_region:
            # Use specified region
            key = f"{user_region}:{user_id}"
        else:
            # Let strategy determine region
            key = user_id
        
        node_id = self.strategy.get_node_for_key(key)
        self.session_locations[session_id] = node_id
        self.user_sessions[user_id].add(session_id)
        
        return node_id
    
    def get_session_node(self, session_id: str) -> Optional[str]:
        """Get the node hosting a session."""
        return self.session_locations.get(session_id)
    
    def migrate_session(self, session_id: str, new_node_id: str):
        """Migrate a session to a new node."""
        if session_id in self.session_locations:
            old_node = self.session_locations[session_id]
            self.session_locations[session_id] = new_node_id
            print(f"Migrated session {session_id} from {old_node} to {new_node_id}")
    
    def handle_node_failure(self, failed_node_id: str):
        """Handle node failure by migrating sessions."""
        failed_sessions = [
            sid for sid, nid in self.session_locations.items()
            if nid == failed_node_id
        ]
        
        migrations = []
        for session_id in failed_sessions:
            # Find replica node
            # In reality, we'd look up the session's user and find their region
            # For simplicity, assign to any available node
            available_nodes = [
                nid for nid in self.strategy.nodes.keys()
                if nid != failed_node_id
            ]
            
            if available_nodes:
                new_node = random.choice(available_nodes)
                self.migrate_session(session_id, new_node)
                migrations.append((session_id, failed_node_id, new_node))
        
        return migrations


# Example usage and demonstration
if __name__ == "__main__":
    print("=== Partitioning Strategies Demo ===\n")
    
    # Create sample nodes
    nodes = [
        PartitionNode("node1", capacity=1, region="us-east"),
        PartitionNode("node2", capacity=2, region="us-west"),
        PartitionNode("node3", capacity=1, region="eu-west"),
        PartitionNode("node4", capacity=1, region="ap-southeast"),
    ]
    
    # 1. Consistent Hashing Demo
    print("1. Consistent Hashing Strategy:")
    ch_strategy = ConsistentHashingStrategy(nodes, virtual_nodes_per_node=10)
    
    test_keys = ["user123", "product456", "session789", "order012"]
    for key in test_keys:
        node = ch_strategy.get_node_for_key(key)
        print(f"  Key '{key}' -> Node: {node}")
    
    # Add a new node
    new_node = PartitionNode("node5", capacity=1, region="us-central")
    migrations = ch_strategy.add_node(new_node)
    print(f"\n  Added node5, migrations needed: {len(migrations)}")
    
    # 2. Range Partitioning Demo
    print("\n2. Range Partitioning Strategy:")
    rp_strategy = RangePartitioningStrategy(nodes[:3], initial_ranges=5)
    
    test_keys = ["100", "500", "1000", "5000"]
    for key in test_keys:
        partition = rp_strategy.get_partition(key)
        node = rp_strategy.get_node_for_key(key)
        print(f"  Key '{key}' -> Partition: {partition}, Node: {node}")
    
    # Simulate access patterns
    for _ in range(100):
        rp_strategy.update_access_stats("100", size_delta=100)
    for _ in range(10):
        rp_strategy.update_access_stats("5000", size_delta=50)
    
    print(f"\n  Partition stats after access:")
    for pid, p in rp_strategy.partitions.items():
        print(f"    {pid}: accesses={p.access_count}, size={p.size}")
    
    # 3. Geographic Partitioning Demo
    print("\n3. Geographic Partitioning Strategy:")
    geo_strategy = GeographicPartitioningStrategy(nodes, replication_factor=2)
    
    test_users = ["user_us", "user_eu", "user_asia"]
    for user in test_users:
        node = geo_strategy.get_node_for_key(user)
        replicas = geo_strategy.get_replica_nodes(user)
        print(f"  User '{user}' -> Primary: {node}, Replicas: {replicas}")
    
    # Region stats
    print("\n  Region Statistics:")
    stats = geo_strategy.get_region_stats()
    for region, region_stats in stats.items():
        print(f"    {region}: {region_stats['node_count']} nodes, "
              f"{region_stats['partition_count']} partitions")
    
    # 4. Social Graph Example
    print("\n4. Social Graph Partitioning:")
    social_partitioner = SocialGraphPartitioner(nodes)
    
    # Add some friendships
    social_partitioner.add_friendship("alice", "bob")
    social_partitioner.add_friendship("alice", "charlie")
    social_partitioner.add_friendship("bob", "dave")
    
    for user in ["alice", "bob", "charlie"]:
        partition = social_partitioner.get_partition_for_user(user)
        friends_partitions = social_partitioner.get_friends_partitions(user)
        print(f"  User '{user}' -> Partition: {partition}")
        print(f"    Friends in partitions: {friends_partitions}")
    
    # 5. Time Series Example
    print("\n5. Time Series Partitioning:")
    ts_partitioner = TimeSeriesPartitioner(nodes, time_bucket_hours=6)
    
    now = datetime.now()
    for hours_ago in [0, 6, 12, 24]:
        timestamp = now - timedelta(hours=hours_ago)
        node = ts_partitioner.get_node_for_timestamp(timestamp)
        print(f"  Data from {hours_ago}h ago -> Node: {node}")
    
    # 6. User Session Example
    print("\n6. User Session Partitioning:")
    session_partitioner = UserSessionPartitioner(nodes)
    
    sessions = [
        ("user_us", "session1", "us-east"),
        ("user_eu", "session2", "eu-west"),
        ("user_asia", "session3", "ap-southeast"),
    ]
    
    for user_id, session_id, region in sessions:
        node = session_partitioner.create_session(user_id, session_id, region)
        print(f"  Session '{session_id}' for '{user_id}' -> Node: {node}")
    
    # Simulate node failure
    print("\n  Simulating node failure (node1):")
    migrations = session_partitioner.handle_node_failure("node1")
    print(f"  Migrated {len(migrations)} sessions")
    
    print("\n=== Demo Complete ===")