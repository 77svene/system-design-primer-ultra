class Item(object):

    def __init__(self, key, value):
        self.key = key
        self.value = value


class HashTable(object):

    def __init__(self, size):
        self.size = size
        self.table = [[] for _ in range(self.size)]

    def _hash_function(self, key):
        return key % self.size

    def set(self, key, value):
        hash_index = self._hash_function(key)
        for item in self.table[hash_index]:
            if item.key == key:
                item.value = value
                return
        self.table[hash_index].append(Item(key, value))

    def get(self, key):
        hash_index = self._hash_function(key)
        for item in self.table[hash_index]:
            if item.key == key:
                return item.value
        raise KeyError('Key not found')

    def remove(self, key):
        hash_index = self._hash_function(key)
        for index, item in enumerate(self.table[hash_index]):
            if item.key == key:
                del self.table[hash_index][index]
                return
        raise KeyError('Key not found')

    def items(self):
        """Return all key-value pairs in the hash table."""
        for bucket in self.table:
            for item in bucket:
                yield item.key, item.value


class PartitioningStrategy:
    """Base class for data partitioning strategies."""
    
    def get_partition(self, key, num_partitions):
        """Return partition index for given key."""
        raise NotImplementedError
    
    def rebalance(self, old_partitions, new_partitions, data_map):
        """Rebalance data when partitions change. Returns migration plan."""
        raise NotImplementedError


class ConsistentHashing(PartitioningStrategy):
    """Consistent hashing with virtual nodes for minimal redistribution."""
    
    def __init__(self, virtual_nodes=100):
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_keys = []
    
    def _hash(self, key):
        """Generate hash for key."""
        return hash(key) % (2**32)
    
    def _add_virtual_node(self, partition_idx):
        """Add virtual nodes for a partition to the ring."""
        for i in range(self.virtual_nodes):
            virtual_key = f"partition_{partition_idx}_vnode_{i}"
            hash_val = self._hash(virtual_key)
            self.ring[hash_val] = partition_idx
            self.sorted_keys.append(hash_val)
        self.sorted_keys.sort()
    
    def _remove_virtual_node(self, partition_idx):
        """Remove virtual nodes for a partition from the ring."""
        to_remove = []
        for hash_val, p_idx in self.ring.items():
            if p_idx == partition_idx:
                to_remove.append(hash_val)
        
        for hash_val in to_remove:
            del self.ring[hash_val]
            self.sorted_keys.remove(hash_val)
    
    def get_partition(self, key, num_partitions):
        """Find partition for key using consistent hashing."""
        if not self.ring:
            # Initialize ring with all partitions
            for i in range(num_partitions):
                self._add_virtual_node(i)
        
        key_hash = self._hash(key)
        
        # Binary search for first node with hash >= key_hash
        idx = self._binary_search(key_hash)
        
        # If we've gone past the end, wrap around to first node
        if idx == len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def _binary_search(self, key_hash):
        """Find first index in sorted_keys with value >= key_hash."""
        left, right = 0, len(self.sorted_keys)
        
        while left < right:
            mid = (left + right) // 2
            if self.sorted_keys[mid] < key_hash:
                left = mid + 1
            else:
                right = mid
        
        return left
    
    def rebalance(self, old_partitions, new_partitions, data_map):
        """Rebalance data when partitions change."""
        migration_plan = []
        
        # Determine added and removed partitions
        added = set(new_partitions) - set(old_partitions)
        removed = set(old_partitions) - set(new_partitions)
        
        # Add new partitions to ring
        for partition in added:
            self._add_virtual_node(partition)
        
        # Remove old partitions from ring
        for partition in removed:
            self._remove_virtual_node(partition)
        
        # Calculate which keys need to move
        for key in data_map:
            old_partition = data_map[key]
            new_partition = self.get_partition(key, len(new_partitions))
            
            if old_partition != new_partition:
                migration_plan.append((key, old_partition, new_partition))
        
        return migration_plan


class RangePartitioning(PartitioningStrategy):
    """Range-based partitioning for ordered data."""
    
    def __init__(self, partition_ranges=None):
        self.partition_ranges = partition_ranges or []
        self.partition_boundaries = []
        self._update_boundaries()
    
    def _update_boundaries(self):
        """Update partition boundaries for binary search."""
        self.partition_boundaries = []
        for i, (start, end) in enumerate(self.partition_ranges):
            self.partition_boundaries.append((start, i))
        self.partition_boundaries.sort(key=lambda x: x[0])
    
    def get_partition(self, key, num_partitions):
        """Find partition for key using range lookup."""
        if not self.partition_boundaries:
            # Default to hash-based if no ranges defined
            return hash(key) % num_partitions
        
        # Binary search for correct range
        idx = self._binary_search_range(key)
        
        if idx < len(self.partition_ranges):
            start, end = self.partition_ranges[idx]
            if start <= key <= end:
                return idx
        
        # Fallback to hash-based
        return hash(key) % num_partitions
    
    def _binary_search_range(self, key):
        """Find partition index for key using binary search."""
        left, right = 0, len(self.partition_boundaries)
        
        while left < right:
            mid = (left + right) // 2
            if self.partition_boundaries[mid][0] <= key:
                left = mid + 1
            else:
                right = mid
        
        return left - 1 if left > 0 else 0
    
    def rebalance(self, old_partitions, new_partitions, data_map):
        """Rebalance data when partitions change."""
        migration_plan = []
        
        # Recalculate ranges for new partition count
        new_ranges = self._calculate_ranges(new_partitions)
        
        # Determine which keys need to move
        for key in data_map:
            old_partition = data_map[key]
            new_partition = self.get_partition(key, len(new_partitions))
            
            if old_partition != new_partition:
                migration_plan.append((key, old_partition, new_partition))
        
        # Update partition ranges
        self.partition_ranges = new_ranges
        self._update_boundaries()
        
        return migration_plan
    
    def _calculate_ranges(self, partitions):
        """Calculate new partition ranges based on partition count."""
        # This is a simplified version - in practice, you'd analyze data distribution
        total_range = 1000000  # Example total range
        range_size = total_range // len(partitions)
        
        new_ranges = []
        for i in range(len(partitions)):
            start = i * range_size
            end = (i + 1) * range_size - 1 if i < len(partitions) - 1 else total_range - 1
            new_ranges.append((start, end))
        
        return new_ranges


class GeographicPartitioning(PartitioningStrategy):
    """Geographic partitioning based on location data."""
    
    def __init__(self, region_mapping=None):
        self.region_mapping = region_mapping or {}
        self.default_region = "us-east"
    
    def get_partition(self, key, num_partitions):
        """Find partition based on geographic location in key."""
        # Extract region from key (assuming key contains location info)
        region = self._extract_region(key)
        
        if region in self.region_mapping:
            partition = self.region_mapping[region]
            # Ensure partition index is within bounds
            return partition % num_partitions
        
        # Fallback: hash the region
        return hash(region) % num_partitions
    
    def _extract_region(self, key):
        """Extract region from key. Key format: 'user:123:us-west'"""
        if isinstance(key, str) and ':' in key:
            parts = key.split(':')
            if len(parts) >= 3:
                return parts[-1]  # Last part is region
        
        return self.default_region
    
    def rebalance(self, old_partitions, new_partitions, data_map):
        """Rebalance data when partitions change."""
        migration_plan = []
        
        # Recalculate region mapping
        new_mapping = self._recalculate_mapping(new_partitions)
        
        # Determine which keys need to move
        for key in data_map:
            old_partition = data_map[key]
            new_partition = self.get_partition(key, len(new_partitions))
            
            if old_partition != new_partition:
                migration_plan.append((key, old_partition, new_partition))
        
        # Update region mapping
        self.region_mapping = new_mapping
        
        return migration_plan
    
    def _recalculate_mapping(self, partitions):
        """Recalculate region to partition mapping."""
        # Simple round-robin assignment
        regions = ["us-east", "us-west", "eu-west", "eu-central", "ap-south", "ap-northeast"]
        mapping = {}
        
        for i, region in enumerate(regions):
            mapping[region] = i % len(partitions)
        
        return mapping


class PartitionedHashTable:
    """Hash table with pluggable partitioning strategies."""
    
    def __init__(self, num_partitions=3, strategy=None):
        self.num_partitions = num_partitions
        self.strategy = strategy or ConsistentHashing()
        self.partitions = [HashTable(100) for _ in range(num_partitions)]
        self.data_map = {}  # Track which partition each key is in
    
    def set(self, key, value):
        """Set key-value pair in appropriate partition."""
        partition_idx = self.strategy.get_partition(key, self.num_partitions)
        self.partitions[partition_idx].set(key, value)
        self.data_map[key] = partition_idx
    
    def get(self, key):
        """Get value for key from appropriate partition."""
        partition_idx = self.strategy.get_partition(key, self.num_partitions)
        return self.partitions[partition_idx].get(key)
    
    def remove(self, key):
        """Remove key from appropriate partition."""
        partition_idx = self.strategy.get_partition(key, self.num_partitions)
        self.partitions[partition_idx].remove(key)
        if key in self.data_map:
            del self.data_map[key]
    
    def add_partition(self):
        """Add a new partition and rebalance data."""
        self.num_partitions += 1
        self.partitions.append(HashTable(100))
        
        # Rebalance data
        old_partitions = list(range(self.num_partitions - 1))
        new_partitions = list(range(self.num_partitions))
        
        migration_plan = self.strategy.rebalance(
            old_partitions, new_partitions, self.data_map
        )
        
        # Execute migration
        for key, from_partition, to_partition in migration_plan:
            if key in self.data_map:
                value = self.partitions[from_partition].get(key)
                self.partitions[from_partition].remove(key)
                self.partitions[to_partition].set(key, value)
                self.data_map[key] = to_partition
    
    def remove_partition(self, partition_idx):
        """Remove a partition and rebalance data."""
        if partition_idx >= self.num_partitions:
            raise ValueError("Invalid partition index")
        
        # Move all data from partition to be removed
        for key, value in self.partitions[partition_idx].items():
            # Find new partition for this key
            new_partition = self.strategy.get_partition(key, self.num_partitions - 1)
            if new_partition == partition_idx:
                # If key would stay in same partition, find alternative
                new_partition = (partition_idx + 1) % (self.num_partitions - 1)
            
            self.partitions[new_partition].set(key, value)
            self.data_map[key] = new_partition
        
        # Remove the partition
        del self.partitions[partition_idx]
        self.num_partitions -= 1
        
        # Update data_map for all keys
        for key in list(self.data_map.keys()):
            if self.data_map[key] > partition_idx:
                self.data_map[key] -= 1
    
    def get_partition_stats(self):
        """Get statistics for each partition."""
        stats = []
        for i, partition in enumerate(self.partitions):
            count = sum(1 for _ in partition.items())
            stats.append({
                'partition': i,
                'key_count': count,
                'load_factor': count / partition.size
            })
        return stats


# Example usage patterns for different data types
class SocialGraphPartitioner:
    """Partitioning strategy optimized for social graphs."""
    
    def __init__(self):
        self.user_to_partition = {}
        self.partition_loads = {}
    
    def partition_user(self, user_id, num_partitions):
        """Partition user and their connections together."""
        # Use consistent hashing for user distribution
        base_partition = hash(user_id) % num_partitions
        
        # Co-locate user with their most active connections
        # This is simplified - in practice, you'd analyze the social graph
        return base_partition
    
    def rebalance_social_graph(self, old_partitions, new_partitions, user_connections):
        """Rebalance social graph data."""
        migration_plan = []
        
        # Analyze connection patterns to minimize cross-partition edges
        for user_id in user_connections:
            old_partition = self.user_to_partition.get(user_id)
            new_partition = self.partition_user(user_id, len(new_partitions))
            
            if old_partition != new_partition:
                migration_plan.append((user_id, old_partition, new_partition))
        
        return migration_plan


class TimeSeriesPartitioner:
    """Partitioning strategy for time-series data."""
    
    def __init__(self, time_range_hours=24):
        self.time_range_hours = time_range_hours
        self.time_windows = []
    
    def partition_by_time(self, timestamp, num_partitions):
        """Partition data by time window."""
        # Round timestamp to nearest time window
        window = int(timestamp) // (self.time_range_hours * 3600)
        return window % num_partitions
    
    def rebalance_time_series(self, old_partitions, new_partitions, time_series_data):
        """Rebalance time-series data."""
        migration_plan = []
        
        for timestamp, data in time_series_data.items():
            old_partition = self.partition_by_time(timestamp, len(old_partitions))
            new_partition = self.partition_by_time(timestamp, len(new_partitions))
            
            if old_partition != new_partition:
                migration_plan.append((timestamp, old_partition, new_partition))
        
        return migration_plan


class SessionPartitioner:
    """Partitioning strategy for user sessions."""
    
    def __init__(self):
        self.session_affinity = {}
    
    def partition_session(self, session_id, user_id, num_partitions):
        """Partition session with user affinity."""
        # Prefer to keep user's sessions on same partition
        if user_id in self.session_affinity:
            return self.session_affinity[user_id]
        
        # Otherwise, use consistent hashing
        partition = hash(session_id) % num_partitions
        self.session_affinity[user_id] = partition
        return partition
    
    def rebalance_sessions(self, old_partitions, new_partitions, active_sessions):
        """Rebalance session data."""
        migration_plan = []
        
        for session_id, (user_id, data) in active_sessions.items():
            old_partition = self.partition_session(session_id, user_id, len(old_partitions))
            new_partition = self.partition_session(session_id, user_id, len(new_partitions))
            
            if old_partition != new_partition:
                migration_plan.append((session_id, old_partition, new_partition))
        
        return migration_plan


# Hotspot mitigation strategies
class HotspotMitigator:
    """Strategies to mitigate partition hotspots."""
    
    @staticmethod
    def detect_hotspots(partition_stats, threshold=0.8):
        """Detect overloaded partitions."""
        hotspots = []
        for stats in partition_stats:
            if stats['load_factor'] > threshold:
                hotspots.append(stats['partition'])
        return hotspots
    
    @staticmethod
    def split_hotspot_partition(partitioned_hash_table, hotspot_idx):
        """Split an overloaded partition into two."""
        # Create new partition
        partitioned_hash_table.add_partition()
        
        # Move half the keys from hotspot to new partition
        hotspot = partitioned_hash_table.partitions[hotspot_idx]
        new_partition_idx = partitioned_hash_table.num_partitions - 1
        
        keys_to_move = []
        for key, value in hotspot.items():
            if hash(key) % 2 == 0:  # Simple split strategy
                keys_to_move.append((key, value))
        
        for key, value in keys_to_move:
            hotspot.remove(key)
            partitioned_hash_table.partitions[new_partition_idx].set(key, value)
            partitioned_hash_table.data_map[key] = new_partition_idx
    
    @staticmethod
    def add_replica_for_hotspot(partitioned_hash_table, hotspot_idx):
        """Add read replica for hotspot partition (simplified)."""
        # In practice, this would create a read-only copy
        # For demonstration, we'll just log the action
        print(f"Adding replica for hotspot partition {hotspot_idx}")
        return True