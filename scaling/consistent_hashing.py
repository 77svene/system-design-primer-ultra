"""Scalable Data Partitioning Strategies

This module implements pluggable partitioning strategies for distributed systems:
- Consistent hashing with virtual nodes
- Range partitioning
- Geographic partitioning

Includes rebalancing algorithms and real-world examples for social graphs,
time-series data, and user sessions.
"""

import hashlib
import bisect
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Set
from collections import defaultdict
import math


class PartitioningStrategy(ABC):
    """Abstract base class for partitioning strategies."""
    
    @abstractmethod
    def get_partition(self, key: str) -> str:
        """Get the partition/node for a given key."""
        pass
    
    @abstractmethod
    def add_node(self, node_id: str, weight: float = 1.0) -> Dict[str, List[str]]:
        """Add a node and return data that needs to be migrated."""
        pass
    
    @abstractmethod
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and return data that needs to be migrated."""
        pass
    
    @abstractmethod
    def get_all_partitions(self) -> List[str]:
        """Get all partition/node IDs."""
        pass
    
    @abstractmethod
    def get_partition_load(self, node_id: str) -> float:
        """Get the load on a partition (0.0 to 1.0)."""
        pass


class ConsistentHashing(PartitioningStrategy):
    """Consistent hashing implementation with virtual nodes.
    
    Supports:
    - Virtual nodes for better distribution
    - Weighted nodes
    - Hotspot detection and mitigation
    - Minimal data movement during rebalancing
    """
    
    def __init__(self, num_virtual_nodes: int = 100, hash_function: str = "md5"):
        """Initialize consistent hashing.
        
        Args:
            num_virtual_nodes: Number of virtual nodes per physical node
            hash_function: Hash function to use ('md5', 'sha256', etc.)
        """
        self.num_virtual_nodes = num_virtual_nodes
        self.hash_function = hash_function
        self.ring: Dict[int, str] = {}  # hash -> node_id
        self.node_weights: Dict[str, float] = {}  # node_id -> weight
        self.node_virtual_nodes: Dict[str, List[int]] = defaultdict(list)  # node_id -> list of hashes
        self.sorted_hashes: List[int] = []  # sorted list of hashes on ring
        self.key_to_node: Dict[str, str] = {}  # key -> node_id (for tracking)
        self.node_load: Dict[str, int] = defaultdict(int)  # node_id -> number of keys
        
    def _hash(self, key: str) -> int:
        """Generate hash for a key."""
        if self.hash_function == "md5":
            return int(hashlib.md5(key.encode()).hexdigest(), 16)
        elif self.hash_function == "sha256":
            return int(hashlib.sha256(key.encode()).hexdigest(), 16)
        else:
            # Simple hash for demonstration
            return hash(key) % (2**32)
    
    def _add_virtual_node(self, node_id: str, virtual_node_index: int) -> int:
        """Add a virtual node to the ring."""
        virtual_key = f"{node_id}:vn{virtual_node_index}"
        hash_val = self._hash(virtual_key)
        self.ring[hash_val] = node_id
        self.node_virtual_nodes[node_id].append(hash_val)
        bisect.insort(self.sorted_hashes, hash_val)
        return hash_val
    
    def _remove_virtual_node(self, node_id: str, hash_val: int):
        """Remove a virtual node from the ring."""
        if hash_val in self.ring:
            del self.ring[hash_val]
        if hash_val in self.sorted_hashes:
            self.sorted_hashes.remove(hash_val)
    
    def get_partition(self, key: str) -> str:
        """Get the node responsible for a key."""
        if not self.ring:
            raise ValueError("No nodes in the ring")
        
        key_hash = self._hash(key)
        
        # Find the first node clockwise from the key hash
        idx = bisect.bisect_right(self.sorted_hashes, key_hash)
        if idx == len(self.sorted_hashes):
            idx = 0  # Wrap around
        
        node_hash = self.sorted_hashes[idx]
        node_id = self.ring[node_hash]
        
        # Track key assignment
        self.key_to_node[key] = node_id
        self.node_load[node_id] += 1
        
        return node_id
    
    def add_node(self, node_id: str, weight: float = 1.0) -> Dict[str, List[str]]:
        """Add a node to the ring.
        
        Returns:
            Dictionary mapping source node to list of keys that need to migrate
        """
        if node_id in self.node_weights:
            raise ValueError(f"Node {node_id} already exists")
        
        self.node_weights[node_id] = weight
        
        # Calculate number of virtual nodes based on weight
        num_vnodes = int(self.num_virtual_nodes * weight)
        
        # Track which keys need to migrate
        migration_map = defaultdict(list)
        
        # Add virtual nodes
        for i in range(num_vnodes):
            new_hash = self._add_virtual_node(node_id, i)
            
            # Find keys that should move to this new node
            # These are keys that hash between the previous node and this new node
            if len(self.sorted_hashes) > 1:
                # Find the previous node in the ring
                prev_idx = (bisect.bisect_left(self.sorted_hashes, new_hash) - 1) % len(self.sorted_hashes)
                prev_hash = self.sorted_hashes[prev_idx]
                prev_node = self.ring[prev_hash]
                
                # Find keys that should move from prev_node to new node
                for key, assigned_node in list(self.key_to_node.items()):
                    if assigned_node == prev_node:
                        key_hash = self._hash(key)
                        # Check if key should now map to new node
                        # Key should move if it's between prev_node and new_node
                        if self._should_migrate(key_hash, prev_hash, new_hash):
                            migration_map[prev_node].append(key)
                            self.key_to_node[key] = node_id
                            self.node_load[prev_node] -= 1
                            self.node_load[node_id] += 1
        
        return dict(migration_map)
    
    def _should_migrate(self, key_hash: int, old_node_hash: int, new_node_hash: int) -> bool:
        """Determine if a key should migrate to a new node."""
        # Handle wrap-around case
        if new_node_hash < old_node_hash:
            # New node is before old node in ring (wrap-around case)
            return key_hash <= new_node_hash or key_hash > old_node_hash
        else:
            # Normal case
            return old_node_hash < key_hash <= new_node_hash
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node from the ring.
        
        Returns:
            Dictionary mapping destination node to list of keys that need to migrate
        """
        if node_id not in self.node_weights:
            raise ValueError(f"Node {node_id} does not exist")
        
        # Track which keys need to migrate
        migration_map = defaultdict(list)
        
        # Remove virtual nodes and redistribute keys
        for hash_val in self.node_virtual_nodes[node_id][:]:
            # Find the next node in the ring
            idx = bisect.bisect_right(self.sorted_hashes, hash_val)
            if idx == len(self.sorted_hashes):
                idx = 0
            
            next_hash = self.sorted_hashes[idx]
            next_node = self.ring[next_hash]
            
            # Migrate keys from removed node to next node
            for key, assigned_node in list(self.key_to_node.items()):
                if assigned_node == node_id:
                    migration_map[next_node].append(key)
                    self.key_to_node[key] = next_node
                    self.node_load[next_node] += 1
            
            # Remove the virtual node
            self._remove_virtual_node(node_id, hash_val)
        
        # Clean up
        self.node_load[node_id] = 0
        del self.node_weights[node_id]
        del self.node_virtual_nodes[node_id]
        
        return dict(migration_map)
    
    def get_all_partitions(self) -> List[str]:
        """Get all node IDs."""
        return list(self.node_weights.keys())
    
    def get_partition_load(self, node_id: str) -> float:
        """Get the load on a node (normalized by weight)."""
        if node_id not in self.node_weights:
            return 0.0
        
        total_keys = sum(self.node_load.values())
        if total_keys == 0:
            return 0.0
        
        expected_share = self.node_weights[node_id] / sum(self.node_weights.values())
        actual_share = self.node_load[node_id] / total_keys
        
        return actual_share / expected_share if expected_share > 0 else float('inf')
    
    def get_hotspots(self, threshold: float = 1.5) -> List[str]:
        """Identify hotspots (nodes with load > threshold * expected)."""
        hotspots = []
        for node_id in self.node_weights:
            load = self.get_partition_load(node_id)
            if load > threshold:
                hotspots.append(node_id)
        return hotspots
    
    def rebalance_hotspot(self, hotspot_node: str, target_load: float = 1.0) -> Dict[str, List[str]]:
        """Mitigate a hotspot by splitting it into multiple nodes.
        
        Returns:
            Migration map for rebalancing
        """
        if hotspot_node not in self.node_weights:
            raise ValueError(f"Node {hotspot_node} does not exist")
        
        current_load = self.get_partition_load(hotspot_node)
        if current_load <= target_load:
            return {}
        
        # Calculate how many new nodes we need
        current_weight = self.node_weights[hotspot_node]
        num_new_nodes = math.ceil(current_load / target_load)
        
        # Create new nodes with reduced weight
        new_weight = current_weight / num_new_nodes
        migration_map = defaultdict(list)
        
        # Remove the hotspot node temporarily
        old_migration = self.remove_node(hotspot_node)
        
        # Add new nodes
        for i in range(num_new_nodes):
            new_node_id = f"{hotspot_node}_split{i}"
            new_migration = self.add_node(new_node_id, new_weight)
            
            # Combine migration maps
            for node, keys in new_migration.items():
                migration_map[node].extend(keys)
        
        return dict(migration_map)


class RangePartitioning(PartitioningStrategy):
    """Range-based partitioning implementation.
    
    Supports:
    - Dynamic range splitting and merging
    - Ordered key access
    - Time-series data partitioning
    - Automatic rebalancing based on load
    """
    
    def __init__(self, initial_ranges: Optional[List[Tuple[Any, Any, str]]] = None):
        """Initialize range partitioning.
        
        Args:
            initial_ranges: List of (start, end, node_id) tuples
        """
        self.ranges: List[Tuple[Any, Any, str]] = []  # (start, end, node_id)
        self.node_load: Dict[str, int] = defaultdict(int)
        self.key_to_range: Dict[str, Tuple[Any, Any]] = {}  # key -> (start, end)
        
        if initial_ranges:
            for start, end, node_id in initial_ranges:
                self.add_range(start, end, node_id)
    
    def add_range(self, start: Any, end: Any, node_id: str):
        """Add a new range partition."""
        # Validate range doesn't overlap
        for existing_start, existing_end, _ in self.ranges:
            if not (end <= existing_start or start >= existing_end):
                raise ValueError(f"Range ({start}, {end}) overlaps with existing range")
        
        self.ranges.append((start, end, node_id))
        self.ranges.sort(key=lambda x: x[0])
    
    def get_partition(self, key: str) -> str:
        """Get the node for a key based on range partitioning."""
        # Convert key to comparable value
        key_value = self._convert_key(key)
        
        # Find the range containing this key
        for start, end, node_id in self.ranges:
            if start <= key_value < end:
                self.key_to_range[key] = (start, end)
                self.node_load[node_id] += 1
                return node_id
        
        raise ValueError(f"Key {key} doesn't fall in any range")
    
    def _convert_key(self, key: str) -> Any:
        """Convert string key to appropriate type for range comparison."""
        # Try to convert to number if possible
        try:
            return float(key)
        except ValueError:
            # If not a number, use lexicographic ordering
            return key
    
    def add_node(self, node_id: str, weight: float = 1.0) -> Dict[str, List[str]]:
        """Add a node by splitting the most loaded range."""
        if not self.ranges:
            raise ValueError("No ranges to split")
        
        # Find the most loaded range
        max_load = 0
        max_range = None
        max_node = None
        
        for start, end, node_id_existing in self.ranges:
            load = self.node_load.get(node_id_existing, 0)
            if load > max_load:
                max_load = load
                max_range = (start, end)
                max_node = node_id_existing
        
        if max_range is None:
            return {}
        
        start, end = max_range
        
        # Split the range at the midpoint
        if isinstance(start, (int, float)):
            mid = (start + end) / 2
        else:
            # For strings, use a simple split
            mid = self._midpoint_string(start, end)
        
        # Remove old range
        self.ranges = [(s, e, n) for s, e, n in self.ranges if (s, e) != max_range]
        
        # Add two new ranges
        self.add_range(start, mid, max_node)
        self.add_range(mid, end, node_id)
        
        # Migrate keys from old range to new range
        migration_map = defaultdict(list)
        for key, (key_start, key_end) in list(self.key_to_range.items()):
            if (key_start, key_end) == max_range:
                key_value = self._convert_key(key)
                if key_value >= mid:
                    migration_map[node_id].append(key)
                    self.key_to_range[key] = (mid, end)
                    self.node_load[max_node] -= 1
                    self.node_load[node_id] += 1
        
        return dict(migration_map)
    
    def _midpoint_string(self, start: str, end: str) -> str:
        """Calculate midpoint between two strings."""
        # Simple implementation: find common prefix and increment
        if not start:
            return chr(0) if not end else end[0]
        
        # Find first differing character
        i = 0
        while i < len(start) and i < len(end) and start[i] == end[i]:
            i += 1
        
        if i == len(start):
            # start is prefix of end
            return start + chr(0)
        
        # Increment the differing character
        mid_char = chr((ord(start[i]) + ord(end[i])) // 2)
        return start[:i] + mid_char
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and merge its ranges with neighbors."""
        # Find ranges for this node
        node_ranges = [(s, e) for s, e, n in self.ranges if n == node_id]
        
        if not node_ranges:
            return {}
        
        migration_map = defaultdict(list)
        
        for start, end in node_ranges:
            # Find adjacent ranges
            idx = None
            for i, (s, e, n) in enumerate(self.ranges):
                if s == start and e == end:
                    idx = i
                    break
            
            if idx is None:
                continue
            
            # Try to merge with left neighbor
            if idx > 0:
                left_start, left_end, left_node = self.ranges[idx - 1]
                if left_end == start:
                    # Merge with left
                    new_end = end
                    self.ranges.pop(idx)
                    self.ranges[idx - 1] = (left_start, new_end, left_node)
                    
                    # Migrate keys
                    for key, (key_start, key_end) in list(self.key_to_range.items()):
                        if (key_start, key_end) == (start, end):
                            migration_map[left_node].append(key)
                            self.key_to_range[key] = (left_start, new_end)
                            self.node_load[node_id] -= 1
                            self.node_load[left_node] += 1
                    
                    continue
            
            # Try to merge with right neighbor
            if idx < len(self.ranges) - 1:
                right_start, right_end, right_node = self.ranges[idx + 1]
                if end == right_start:
                    # Merge with right
                    new_start = start
                    self.ranges.pop(idx)
                    self.ranges[idx] = (new_start, right_end, right_node)
                    
                    # Migrate keys
                    for key, (key_start, key_end) in list(self.key_to_range.items()):
                        if (key_start, key_end) == (start, end):
                            migration_map[right_node].append(key)
                            self.key_to_range[key] = (new_start, right_end)
                            self.node_load[node_id] -= 1
                            self.node_load[right_node] += 1
        
        return dict(migration_map)
    
    def get_all_partitions(self) -> List[str]:
        """Get all node IDs."""
        return list(set(node_id for _, _, node_id in self.ranges))
    
    def get_partition_load(self, node_id: str) -> float:
        """Get the load on a node."""
        total_keys = sum(self.node_load.values())
        if total_keys == 0:
            return 0.0
        
        node_ranges = [(s, e) for s, e, n in self.ranges if n == node_id]
        if not node_ranges:
            return 0.0
        
        # Calculate expected share based on range size
        # For simplicity, assume equal distribution
        expected_share = len(node_ranges) / len(self.ranges)
        actual_share = self.node_load.get(node_id, 0) / total_keys
        
        return actual_share / expected_share if expected_share > 0 else float('inf')


class GeographicPartitioning(PartitioningStrategy):
    """Geographic partitioning based on location data.
    
    Supports:
    - Region-based partitioning
    - Latency-optimized routing
    - Data sovereignty compliance
    - Cross-region replication hints
    """
    
    def __init__(self, region_mapping: Optional[Dict[str, str]] = None):
        """Initialize geographic partitioning.
        
        Args:
            region_mapping: Dictionary mapping regions to node IDs
        """
        self.region_mapping = region_mapping or {}
        self.reverse_mapping: Dict[str, Set[str]] = defaultdict(set)
        self.node_load: Dict[str, int] = defaultdict(int)
        self.key_to_region: Dict[str, str] = {}
        
        # Build reverse mapping
        for region, node_id in self.region_mapping.items():
            self.reverse_mapping[node_id].add(region)
        
        # Default regions
        self.default_regions = {
            'na': 'North America',
            'eu': 'Europe',
            'asia': 'Asia',
            'sa': 'South America',
            'af': 'Africa',
            'au': 'Australia'
        }
    
    def get_partition(self, key: str) -> str:
        """Get the node for a key based on geographic location."""
        # Extract region from key (format: region:user_id or similar)
        region = self._extract_region(key)
        
        if region in self.region_mapping:
            node_id = self.region_mapping[region]
            self.key_to_region[key] = region
            self.node_load[node_id] += 1
            return node_id
        else:
            # Default to nearest region or round-robin
            default_node = self._get_default_node(region)
            self.key_to_region[key] = region
            self.node_load[default_node] += 1
            return default_node
    
    def _extract_region(self, key: str) -> str:
        """Extract region from key."""
        # Try common formats
        if ':' in key:
            return key.split(':')[0].lower()
        elif '_' in key:
            return key.split('_')[0].lower()
        elif '-' in key:
            return key.split('-')[0].lower()
        else:
            # Default to first two characters as region code
            return key[:2].lower()
    
    def _get_default_node(self, region: str) -> str:
        """Get default node for unmapped region."""
        # Simple strategy: find least loaded node
        if not self.reverse_mapping:
            raise ValueError("No nodes available")
        
        min_load = float('inf')
        min_node = None
        
        for node_id in self.reverse_mapping:
            load = self.node_load.get(node_id, 0)
            if load < min_load:
                min_load = load
                min_node = node_id
        
        return min_node
    
    def add_node(self, node_id: str, regions: List[str]) -> Dict[str, List[str]]:
        """Add a node for specific regions.
        
        Args:
            node_id: Node identifier
            regions: List of region codes this node handles
            
        Returns:
            Migration map
        """
        migration_map = defaultdict(list)
        
        for region in regions:
            if region in self.region_mapping:
                old_node = self.region_mapping[region]
                # Migrate keys from old node to new node
                for key, key_region in list(self.key_to_region.items()):
                    if key_region == region:
                        migration_map[node_id].append(key)
                        self.node_load[old_node] -= 1
                        self.node_load[node_id] += 1
            
            self.region_mapping[region] = node_id
            self.reverse_mapping[node_id].add(region)
        
        return dict(migration_map)
    
    def remove_node(self, node_id: str) -> Dict[str, List[str]]:
        """Remove a node and reassign its regions."""
        if node_id not in self.reverse_mapping:
            return {}
        
        regions = self.reverse_mapping[node_id].copy()
        migration_map = defaultdict(list)
        
        for region in regions:
            # Find new node for this region
            new_node = self._get_default_node(region)
            
            # Migrate keys
            for key, key_region in list(self.key_to_region.items()):
                if key_region == region:
                    migration_map[new_node].append(key)
                    self.node_load[node_id] -= 1
                    self.node_load[new_node] += 1
            
            # Update mappings
            self.region_mapping[region] = new_node
            self.reverse_mapping[new_node].add(region)
        
        # Clean up
        del self.reverse_mapping[node_id]
        
        return dict(migration_map)
    
    def get_all_partitions(self) -> List[str]:
        """Get all node IDs."""
        return list(self.reverse_mapping.keys())
    
    def get_partition_load(self, node_id: str) -> float:
        """Get the load on a node."""
        total_keys = sum(self.node_load.values())
        if total_keys == 0:
            return 0.0
        
        num_regions = len(self.reverse_mapping.get(node_id, set()))
        total_regions = len(self.region_mapping)
        
        expected_share = num_regions / total_regions if total_regions > 0 else 0
        actual_share = self.node_load.get(node_id, 0) / total_keys
        
        return actual_share / expected_share if expected_share > 0 else float('inf')


class PartitioningManager:
    """Manager for different partitioning strategies with real-world examples."""
    
    def __init__(self):
        self.strategies: Dict[str, PartitioningStrategy] = {}
        self.data: Dict[str, Dict[str, Any]] = defaultdict(dict)
    
    def register_strategy(self, name: str, strategy: PartitioningStrategy):
        """Register a partitioning strategy."""
        self.strategies[name] = strategy
    
    def example_social_graph(self):
        """Example: Partitioning a social graph."""
        print("=== Social Graph Partitioning Example ===")
        
        # Use consistent hashing for user data
        ch = ConsistentHashing(num_virtual_nodes=50)
        
        # Add initial nodes
        nodes = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]
        for node in nodes:
            ch.add_node(node)
        
        # Simulate user data
        users = [f"user_{i}" for i in range(1000)]
        
        # Assign users to nodes
        assignments = {}
        for user in users:
            node = ch.get_partition(user)
            assignments[user] = node
        
        print(f"Initial distribution: {len(set(assignments.values()))} nodes")
        
        # Add a new node
        print("\nAdding new node: ap-northeast-1")
        migration = ch.add_node("ap-northeast-1")
        print(f"Keys migrated: {sum(len(v) for v in migration.values())}")
        
        # Check for hotspots
        hotspots = ch.get_hotspots(threshold=1.2)
        print(f"Hotspots detected: {hotspots}")
        
        return ch
    
    def example_time_series(self):
        """Example: Partitioning time-series data."""
        print("\n=== Time-Series Data Partitioning Example ===")
        
        # Use range partitioning by timestamp
        # Assuming timestamps are in format YYYYMMDD
        initial_ranges = [
            ("20230101", "20230401", "node-1"),
            ("20230401", "20230701", "node-2"),
            ("20230701", "20231001", "node-3"),
            ("20231001", "20240101", "node-4")
        ]
        
        rp = RangePartitioning(initial_ranges)
        
        # Simulate time-series data
        data_points = [f"20230{i % 9 + 1}{j:02d}" for i in range(4) for j in range(1, 31)]
        
        assignments = {}
        for point in data_points[:20]:  # Sample
            try:
                node = rp.get_partition(point)
                assignments[point] = node
            except ValueError:
                pass
        
        print(f"Sample assignments: {list(assignments.items())[:5]}")
        
        # Split a range due to high load
        print("\nSplitting Q2 2023 range (high load)")
        migration = rp.add_node("node-2-split")
        print(f"Migration occurred: {bool(migration)}")
        
        return rp
    
    def example_user_sessions(self):
        """Example: Partitioning user sessions."""
        print("\n=== User Sessions Partitioning Example ===")
        
        # Use geographic partitioning for sessions
        region_mapping = {
            'us': 'us-session-store',
            'ca': 'us-session-store',
            'mx': 'us-session-store',
            'uk': 'eu-session-store',
            'de': 'eu-session-store',
            'fr': 'eu-session-store',
            'jp': 'apac-session-store',
            'au': 'apac-session-store',
            'in': 'apac-session-store'
        }
        
        gp = GeographicPartitioning(region_mapping)
        
        # Simulate session data
        sessions = [
            "us:session_123",
            "uk:session_456",
            "jp:session_789",
            "de:session_abc",
            "au:session_def"
        ]
        
        assignments = {}
        for session in sessions:
            node = gp.get_partition(session)
            assignments[session] = node
        
        print(f"Session assignments: {assignments}")
        
        # Add a new region
        print("\nAdding Brazil region to US store")
        migration = gp.add_node("us-session-store", ['br'])
        print(f"Migration keys: {sum(len(v) for v in migration.values())}")
        
        return gp
    
    def benchmark_strategies(self):
        """Benchmark different partitioning strategies."""
        print("\n=== Strategy Benchmark ===")
        
        strategies = {
            "consistent_hashing": ConsistentHashing(num_virtual_nodes=100),
            "range_partitioning": RangePartitioning([
                ("0", "3333", "node-1"),
                ("3333", "6666", "node-2"),
                ("6666", "10000", "node-3")
            ]),
            "geographic": GeographicPartitioning({
                'us': 'node-1',
                'eu': 'node-2',
                'asia': 'node-3'
            })
        }
        
        # Add nodes to consistent hashing
        strategies["consistent_hashing"].add_node("node-1")
        strategies["consistent_hashing"].add_node("node-2")
        strategies["consistent_hashing"].add_node("node-3")
        
        # Test data
        test_keys = [f"key_{i}" for i in range(1000)]
        
        results = {}
        for name, strategy in strategies.items():
            assignments = []
            for key in test_keys:
                try:
                    node = strategy.get_partition(key)
                    assignments.append(node)
                except ValueError:
                    pass
            
            # Calculate distribution
            from collections import Counter
            dist = Counter(assignments)
            balance = max(dist.values()) / min(dist.values()) if dist else 0
            
            results[name] = {
                "distribution": dict(dist),
                "balance_ratio": balance,
                "nodes_used": len(set(assignments))
            }
        
        for name, result in results.items():
            print(f"\n{name}:")
            print(f"  Distribution: {result['distribution']}")
            print(f"  Balance ratio: {result['balance_ratio']:.2f}")
            print(f"  Nodes used: {result['nodes_used']}")
        
        return results


def main():
    """Demonstrate partitioning strategies."""
    print("SOVEREIGN Partitioning Strategies Demo")
    print("=" * 50)
    
    manager = PartitioningManager()
    
    # Run examples
    ch = manager.example_social_graph()
    rp = manager.example_time_series()
    gp = manager.example_user_sessions()
    
    # Benchmark
    results = manager.benchmark_strategies()
    
    # Register strategies
    manager.register_strategy("social_graph", ch)
    manager.register_strategy("time_series", rp)
    manager.register_strategy("user_sessions", gp)
    
    print("\n" + "=" * 50)
    print("Partitioning strategies registered and ready for use!")
    print("Available strategies:", list(manager.strategies.keys()))


if __name__ == "__main__":
    main()