# solutions/system_design/query_cache/consistency_manager.py

import hashlib
import bisect
import time
import threading
import random
from typing import Dict, List, Set, Optional, Tuple, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CacheMode(Enum):
    """Cache consistency modes"""
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    READ_REPAIR = "read_repair"
    EVENTUAL = "eventual"


class NodeStatus(Enum):
    """Cache node status"""
    ACTIVE = "active"
    SUSPECTED = "suspected"
    DOWN = "down"
    RECOVERING = "recovering"


@dataclass
class CacheEntry:
    """Cache entry with version tracking"""
    key: str
    value: Any
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    node_id: str = ""
    ttl: int = 300  # 5 minutes default
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    
    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl
    
    def touch(self):
        self.access_count += 1
        self.last_accessed = time.time()


@dataclass
class VersionVector:
    """Vector clock for tracking causality"""
    node_versions: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, node_id: str):
        self.node_versions[node_id] = self.node_versions.get(node_id, 0) + 1
    
    def merge(self, other: 'VersionVector'):
        for node_id, version in other.node_versions.items():
            current = self.node_versions.get(node_id, 0)
            self.node_versions[node_id] = max(current, version)
    
    def compare(self, other: 'VersionVector') -> str:
        """Compare two version vectors: 'before', 'after', 'concurrent', or 'equal'"""
        all_nodes = set(self.node_versions.keys()) | set(other.node_versions.keys())
        self_greater = False
        other_greater = False
        
        for node_id in all_nodes:
            self_v = self.node_versions.get(node_id, 0)
            other_v = other.node_versions.get(node_id, 0)
            
            if self_v > other_v:
                self_greater = True
            elif other_v > self_v:
                other_greater = True
        
        if self_greater and not other_greater:
            return "after"
        elif other_greater and not self_greater:
            return "before"
        elif not self_greater and not other_greater:
            return "equal"
        else:
            return "concurrent"
    
    def to_dict(self) -> Dict:
        return self.node_versions.copy()
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'VersionVector':
        return cls(node_versions=data)


class ConsistentHashRing:
    """Consistent hashing with virtual nodes for distribution"""
    
    def __init__(self, virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}  # hash -> node_id
        self.sorted_keys: List[int] = []
        self.nodes: Set[str] = set()
    
    def _hash(self, key: str) -> int:
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node_id: str):
        """Add a node with virtual nodes to the ring"""
        self.nodes.add(node_id)
        
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:{i}"
            hash_val = self._hash(virtual_key)
            self.ring[hash_val] = node_id
            bisect.insort(self.sorted_keys, hash_val)
    
    def remove_node(self, node_id: str):
        """Remove a node and its virtual nodes from the ring"""
        if node_id not in self.nodes:
            return
        
        self.nodes.remove(node_id)
        
        # Remove all virtual nodes for this node
        keys_to_remove = []
        for hash_val, node in self.ring.items():
            if node == node_id:
                keys_to_remove.append(hash_val)
        
        for hash_val in keys_to_remove:
            del self.ring[hash_val]
            self.sorted_keys.remove(hash_val)
    
    def get_node(self, key: str) -> Optional[str]:
        """Get the node responsible for a key"""
        if not self.ring:
            return None
        
        hash_val = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hash_val)
        
        if idx == len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def get_nodes_for_replication(self, key: str, replication_factor: int = 3) -> List[str]:
        """Get multiple nodes for replication"""
        if not self.ring:
            return []
        
        hash_val = self._hash(key)
        idx = bisect.bisect(self.sorted_keys, hash_val)
        
        nodes = []
        seen_nodes = set()
        
        for i in range(len(self.sorted_keys)):
            current_idx = (idx + i) % len(self.sorted_keys)
            node = self.ring[self.sorted_keys[current_idx]]
            
            if node not in seen_nodes:
                nodes.append(node)
                seen_nodes.add(node)
                
                if len(nodes) >= replication_factor:
                    break
        
        return nodes


class CacheNode:
    """Individual cache node in the distributed system"""
    
    def __init__(self, node_id: str, capacity: int = 1000):
        self.node_id = node_id
        self.capacity = capacity
        self.cache: Dict[str, CacheEntry] = {}
        self.version_vector = VersionVector()
        self.status = NodeStatus.ACTIVE
        self.last_heartbeat = time.time()
        self.lock = threading.RLock()
        
        # Statistics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get a value from the node's cache"""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]
                if entry.is_expired():
                    del self.cache[key]
                    self.misses += 1
                    return None
                
                entry.touch()
                self.hits += 1
                return entry
            
            self.misses += 1
            return None
    
    def put(self, key: str, value: Any, version: int = 0, ttl: int = 300) -> bool:
        """Put a value in the node's cache"""
        with self.lock:
            # Check capacity and evict if needed
            if len(self.cache) >= self.capacity and key not in self.cache:
                self._evict_lru()
            
            entry = CacheEntry(
                key=key,
                value=value,
                version=version,
                timestamp=time.time(),
                node_id=self.node_id,
                ttl=ttl
            )
            
            self.cache[key] = entry
            self.version_vector.increment(self.node_id)
            return True
    
    def delete(self, key: str) -> bool:
        """Delete a value from the node's cache"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                self.version_vector.increment(self.node_id)
                return True
            return False
    
    def _evict_lru(self):
        """Evict the least recently used entry"""
        if not self.cache:
            return
        
        # Find LRU entry
        lru_key = min(
            self.cache.keys(),
            key=lambda k: self.cache[k].last_accessed
        )
        
        del self.cache[lru_key]
        self.evictions += 1
    
    def get_stats(self) -> Dict:
        """Get node statistics"""
        with self.lock:
            return {
                "node_id": self.node_id,
                "status": self.status.value,
                "cache_size": len(self.cache),
                "capacity": self.capacity,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "hit_ratio": self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0,
                "last_heartbeat": self.last_heartbeat
            }
    
    def clear(self):
        """Clear the cache"""
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0
            self.evictions = 0


class ConsistencyManager:
    """
    Distributed Query Cache with Consistency Guarantees
    
    Features:
    - Consistent hashing for data distribution
    - Multiple consistency modes (write-through, write-behind, etc.)
    - Version vectors for conflict detection
    - Cache warming strategies
    - Network partition handling
    - Replication for fault tolerance
    """
    
    def __init__(
        self,
        mode: CacheMode = CacheMode.WRITE_THROUGH,
        replication_factor: int = 3,
        virtual_nodes: int = 100,
        node_capacity: int = 1000,
        write_behind_delay: float = 0.5,
        partition_detection_interval: float = 5.0,
        warming_strategy: str = "lazy"
    ):
        self.mode = mode
        self.replication_factor = replication_factor
        self.write_behind_delay = write_behind_delay
        self.warming_strategy = warming_strategy
        
        # Core components
        self.hash_ring = ConsistentHashRing(virtual_nodes)
        self.nodes: Dict[str, CacheNode] = {}
        
        # Consistency tracking
        self.pending_writes: Dict[str, List[Tuple[str, Any, int]]] = defaultdict(list)
        self.write_behind_queue: List[Tuple[str, Any, int, float]] = []
        
        # Partition handling
        self.partition_detection_interval = partition_detection_interval
        self.partition_detected = False
        self.partition_nodes: Set[str] = set()
        
        # Warming
        self.warming_keys: Set[str] = set()
        self.warming_thread: Optional[threading.Thread] = None
        self.warming_enabled = True
        
        # Background threads
        self.write_behind_thread: Optional[threading.Thread] = None
        self.partition_detection_thread: Optional[threading.Thread] = None
        self.running = False
        
        # Statistics
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.consistency_violations = 0
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Start background threads
        self.start()
        
        logger.info(f"ConsistencyManager initialized with mode={mode.value}, replication_factor={replication_factor}")
    
    def start(self):
        """Start background threads"""
        self.running = True
        
        if self.mode == CacheMode.WRITE_BEHIND:
            self.write_behind_thread = threading.Thread(
                target=self._write_behind_worker,
                daemon=True
            )
            self.write_behind_thread.start()
        
        self.partition_detection_thread = threading.Thread(
            target=self._partition_detection_worker,
            daemon=True
        )
        self.partition_detection_thread.start()
        
        if self.warming_strategy != "none":
            self.warming_thread = threading.Thread(
                target=self._warming_worker,
                daemon=True
            )
            self.warming_thread.start()
    
    def stop(self):
        """Stop background threads"""
        self.running = False
        
        if self.write_behind_thread:
            self.write_behind_thread.join(timeout=2)
        
        if self.partition_detection_thread:
            self.partition_detection_thread.join(timeout=2)
        
        if self.warming_thread:
            self.warming_thread.join(timeout=2)
    
    def add_node(self, node_id: str, capacity: int = 1000) -> bool:
        """Add a new cache node to the cluster"""
        with self.lock:
            if node_id in self.nodes:
                logger.warning(f"Node {node_id} already exists")
                return False
            
            node = CacheNode(node_id, capacity)
            self.nodes[node_id] = node
            self.hash_ring.add_node(node_id)
            
            # Trigger cache warming for new node
            if self.warming_strategy == "eager":
                self._warm_node(node_id)
            
            logger.info(f"Added node {node_id} to cluster")
            return True
    
    def remove_node(self, node_id: str) -> bool:
        """Remove a cache node from the cluster"""
        with self.lock:
            if node_id not in self.nodes:
                logger.warning(f"Node {node_id} not found")
                return False
            
            # Redistribute data from removed node
            self._redistribute_node_data(node_id)
            
            del self.nodes[node_id]
            self.hash_ring.remove_node(node_id)
            
            logger.info(f"Removed node {node_id} from cluster")
            return True
    
    def get(self, key: str, consistency_level: int = 1) -> Optional[Any]:
        """
        Get a value from the cache with consistency guarantees
        
        Args:
            key: Cache key
            consistency_level: Number of nodes that must agree (1 = eventual, 2+ = stronger)
        
        Returns:
            Cached value or None if not found
        """
        self.total_requests += 1
        
        # Get nodes responsible for this key
        nodes = self.hash_ring.get_nodes_for_replication(key, self.replication_factor)
        
        if not nodes:
            self.cache_misses += 1
            return None
        
        # Collect responses from nodes
        responses = []
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                entry = self.nodes[node_id].get(key)
                if entry:
                    responses.append((node_id, entry))
        
        if not responses:
            self.cache_misses += 1
            
            # Trigger cache warming if enabled
            if self.warming_strategy == "on_miss":
                self._schedule_warming(key)
            
            return None
        
        # Check consistency
        if len(responses) >= consistency_level:
            # Find most recent version
            latest_entry = max(responses, key=lambda x: x[1].version)[1]
            
            # Read repair if we detected inconsistency
            if len(responses) > 1 and not self._check_consistency(responses):
                self.consistency_violations += 1
                self._perform_read_repair(key, latest_entry, nodes)
            
            self.cache_hits += 1
            latest_entry.touch()
            return latest_entry.value
        else:
            self.cache_misses += 1
            return None
    
    def put(
        self,
        key: str,
        value: Any,
        ttl: int = 300,
        consistency_level: int = 1
    ) -> bool:
        """
        Put a value in the cache with consistency guarantees
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            consistency_level: Number of nodes that must acknowledge write
        
        Returns:
            True if successful
        """
        # Get nodes responsible for this key
        nodes = self.hash_ring.get_nodes_for_replication(key, self.replication_factor)
        
        if not nodes:
            return False
        
        # Get current version for this key
        current_version = self._get_current_version(key, nodes)
        new_version = current_version + 1
        
        # Handle based on consistency mode
        if self.mode == CacheMode.WRITE_THROUGH:
            return self._write_through(key, value, new_version, ttl, nodes, consistency_level)
        elif self.mode == CacheMode.WRITE_BEHIND:
            return self._write_behind(key, value, new_version, ttl, nodes)
        elif self.mode == CacheMode.READ_REPAIR:
            return self._write_read_repair(key, value, new_version, ttl, nodes, consistency_level)
        else:  # EVENTUAL
            return self._write_eventual(key, value, new_version, ttl, nodes)
    
    def delete(self, key: str, consistency_level: int = 1) -> bool:
        """Delete a value from the cache"""
        nodes = self.hash_ring.get_nodes_for_replication(key, self.replication_factor)
        
        if not nodes:
            return False
        
        # Delete from all responsible nodes
        success_count = 0
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                if self.nodes[node_id].delete(key):
                    success_count += 1
        
        return success_count >= consistency_level
    
    def _write_through(
        self,
        key: str,
        value: Any,
        version: int,
        ttl: int,
        nodes: List[str],
        consistency_level: int
    ) -> bool:
        """Write-through consistency: synchronously update all nodes"""
        success_count = 0
        
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                if self.nodes[node_id].put(key, value, version, ttl):
                    success_count += 1
        
        # Wait for required consistency level
        if success_count >= consistency_level:
            return True
        else:
            # Rollback on failure
            self._rollback_write(key, nodes)
            return False
    
    def _write_behind(
        self,
        key: str,
        value: Any,
        version: int,
        ttl: int,
        nodes: List[str]
    ) -> bool:
        """Write-behind consistency: asynchronously update nodes"""
        # Immediately update one node for read-your-writes consistency
        primary_node = nodes[0]
        if primary_node in self.nodes and self.nodes[primary_node].status == NodeStatus.ACTIVE:
            self.nodes[primary_node].put(key, value, version, ttl)
        
        # Queue for async replication
        with self.lock:
            self.write_behind_queue.append((key, value, version, ttl, nodes, time.time()))
        
        return True
    
    def _write_read_repair(
        self,
        key: str,
        value: Any,
        version: int,
        ttl: int,
        nodes: List[str],
        consistency_level: int
    ) -> bool:
        """Read-repair consistency: update on read"""
        # Update only one node immediately
        primary_node = nodes[0]
        if primary_node in self.nodes and self.nodes[primary_node].status == NodeStatus.ACTIVE:
            self.nodes[primary_node].put(key, value, version, ttl)
        
        # Schedule repair for other nodes
        with self.lock:
            self.pending_writes[key].append((value, version, ttl, nodes[1:]))
        
        return True
    
    def _write_eventual(
        self,
        key: str,
        value: Any,
        version: int,
        ttl: int,
        nodes: List[str]
    ) -> bool:
        """Eventual consistency: fire and forget"""
        # Update any available nodes
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                # Non-blocking update
                threading.Thread(
                    target=self.nodes[node_id].put,
                    args=(key, value, version, ttl),
                    daemon=True
                ).start()
        
        return True
    
    def _get_current_version(self, key: str, nodes: List[str]) -> int:
        """Get the current version of a key across nodes"""
        max_version = 0
        
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                entry = self.nodes[node_id].get(key)
                if entry and entry.version > max_version:
                    max_version = entry.version
        
        return max_version
    
    def _check_consistency(self, responses: List[Tuple[str, CacheEntry]]) -> bool:
        """Check if responses are consistent"""
        if len(responses) <= 1:
            return True
        
        # Compare versions
        first_version = responses[0][1].version
        for _, entry in responses[1:]:
            if entry.version != first_version:
                return False
        
        return True
    
    def _perform_read_repair(self, key: str, latest_entry: CacheEntry, nodes: List[str]):
        """Perform read repair to fix inconsistencies"""
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                entry = self.nodes[node_id].get(key)
                if not entry or entry.version < latest_entry.version:
                    # Update with latest version
                    self.nodes[node_id].put(
                        key,
                        latest_entry.value,
                        latest_entry.version,
                        latest_entry.ttl
                    )
    
    def _rollback_write(self, key: str, nodes: List[str]):
        """Rollback a failed write"""
        for node_id in nodes:
            if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                self.nodes[node_id].delete(key)
    
    def _redistribute_node_data(self, node_id: str):
        """Redistribute data from a removed node"""
        if node_id not in self.nodes:
            return
        
        node = self.nodes[node_id]
        
        # Get all keys from the node
        with node.lock:
            keys = list(node.cache.keys())
        
        # Redistribute each key
        for key in keys:
            entry = node.get(key)
            if entry:
                # Find new nodes for this key
                new_nodes = self.hash_ring.get_nodes_for_replication(key, self.replication_factor)
                
                # Replicate to new nodes
                for new_node_id in new_nodes:
                    if new_node_id != node_id and new_node_id in self.nodes:
                        self.nodes[new_node_id].put(
                            key,
                            entry.value,
                            entry.version,
                            entry.ttl
                        )
    
    def _write_behind_worker(self):
        """Background worker for write-behind replication"""
        while self.running:
            try:
                with self.lock:
                    if self.write_behind_queue:
                        # Process oldest write
                        key, value, version, ttl, nodes, timestamp = self.write_behind_queue.pop(0)
                        
                        # Check if write is still relevant
                        if time.time() - timestamp < ttl:
                            # Replicate to remaining nodes
                            for node_id in nodes[1:]:
                                if node_id in self.nodes and self.nodes[node_id].status == NodeStatus.ACTIVE:
                                    self.nodes[node_id].put(key, value, version, ttl)
                
                time.sleep(self.write_behind_delay)
            except Exception as e:
                logger.error(f"Error in write-behind worker: {e}")
                time.sleep(1)
    
    def _partition_detection_worker(self):
        """Background worker for detecting network partitions"""
        while self.running:
            try:
                current_time = time.time()
                suspected_nodes = []
                
                with self.lock:
                    for node_id, node in self.nodes.items():
                        if current_time - node.last_heartbeat > self.partition_detection_interval * 2:
                            node.status = NodeStatus.SUSPECTED
                            suspected_nodes.append(node_id)
                        elif node.status == NodeStatus.SUSPECTED:
                            # Check if node recovered
                            if current_time - node.last_heartbeat <= self.partition_detection_interval:
                                node.status = NodeStatus.ACTIVE
                
                if suspected_nodes:
                    self.partition_detected = True
                    self.partition_nodes.update(suspected_nodes)
                    logger.warning(f"Network partition detected. Suspected nodes: {suspected_nodes}")
                else:
                    if self.partition_detected:
                        logger.info("Network partition resolved")
                    self.partition_detected = False
                    self.partition_nodes.clear()
                
                time.sleep(self.partition_detection_interval)
            except Exception as e:
                logger.error(f"Error in partition detection worker: {e}")
                time.sleep(1)
    
    def _warming_worker(self):
        """Background worker for cache warming"""
        while self.running:
            try:
                if self.warming_strategy == "lazy" and self.warming_keys:
                    with self.lock:
                        if self.warming_keys:
                            key = self.warming_keys.pop()
                            self._warm_key(key)
                
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error in warming worker: {e}")
                time.sleep(1)
    
    def _schedule_warming(self, key: str):
        """Schedule a key for warming"""
        with self.lock:
            self.warming_keys.add(key)
    
    def _warm_key(self, key: str):
        """Warm a specific key"""
        # This would typically fetch from database
        # For now, we'll just log it
        logger.debug(f"Warming key: {key}")
    
    def _warm_node(self, node_id: str):
        """Warm a new node with popular data"""
        logger.info(f"Warming node {node_id}")
        # In a real implementation, this would fetch popular keys from other nodes
    
    def get_cluster_stats(self) -> Dict:
        """Get statistics for the entire cluster"""
        with self.lock:
            node_stats = {}
            total_cache_size = 0
            total_hits = 0
            total_misses = 0
            
            for node_id, node in self.nodes.items():
                stats = node.get_stats()
                node_stats[node_id] = stats
                total_cache_size += stats["cache_size"]
                total_hits += stats["hits"]
                total_misses += stats["misses"]
            
            return {
                "total_nodes": len(self.nodes),
                "active_nodes": sum(1 for n in self.nodes.values() if n.status == NodeStatus.ACTIVE),
                "total_cache_size": total_cache_size,
                "total_hits": total_hits,
                "total_misses": total_misses,
                "cluster_hit_ratio": total_hits / (total_hits + total_misses) if (total_hits + total_misses) > 0 else 0,
                "total_requests": self.total_requests,
                "cache_hits": self.cache_hits,
                "cache_misses": self.cache_misses,
                "consistency_violations": self.consistency_violations,
                "partition_detected": self.partition_detected,
                "partition_nodes": list(self.partition_nodes),
                "pending_writes": len(self.pending_writes),
                "write_behind_queue_size": len(self.write_behind_queue),
                "warming_keys": len(self.warming_keys),
                "node_stats": node_stats
            }
    
    def simulate_partition(self, node_ids: List[str]):
        """Simulate a network partition for testing"""
        with self.lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    self.nodes[node_id].status = NodeStatus.DOWN
                    self.partition_nodes.add(node_id)
            
            self.partition_detected = True
            logger.warning(f"Simulated partition for nodes: {node_ids}")
    
    def heal_partition(self, node_ids: List[str]):
        """Heal a simulated partition"""
        with self.lock:
            for node_id in node_ids:
                if node_id in self.nodes:
                    self.nodes[node_id].status = NodeStatus.ACTIVE
                    self.nodes[node_id].last_heartbeat = time.time()
                    self.partition_nodes.discard(node_id)
            
            if not self.partition_nodes:
                self.partition_detected = False
            
            logger.info(f"Healed partition for nodes: {node_ids}")


# Factory function for easy integration
def create_consistency_manager(
    mode: str = "write_through",
    replication_factor: int = 3,
    **kwargs
) -> ConsistencyManager:
    """
    Factory function to create a ConsistencyManager
    
    Args:
        mode: Cache mode (write_through, write_behind, read_repair, eventual)
        replication_factor: Number of replicas for each key
        **kwargs: Additional arguments for ConsistencyManager
    
    Returns:
        Configured ConsistencyManager instance
    """
    mode_map = {
        "write_through": CacheMode.WRITE_THROUGH,
        "write_behind": CacheMode.WRITE_BEHIND,
        "read_repair": CacheMode.READ_REPAIR,
        "eventual": CacheMode.EVENTUAL
    }
    
    cache_mode = mode_map.get(mode.lower(), CacheMode.WRITE_THROUGH)
    
    return ConsistencyManager(
        mode=cache_mode,
        replication_factor=replication_factor,
        **kwargs
    )


# Example usage and testing
if __name__ == "__main__":
    # Create a consistency manager
    cache = create_consistency_manager(
        mode="write_through",
        replication_factor=3,
        virtual_nodes=50,
        node_capacity=500
    )
    
    # Add nodes to the cluster
    cache.add_node("node1", capacity=500)
    cache.add_node("node2", capacity=500)
    cache.add_node("node3", capacity=500)
    
    # Test basic operations
    print("Testing basic cache operations...")
    
    # Put some values
    cache.put("user:123", {"name": "John Doe", "email": "john@example.com"}, ttl=60)
    cache.put("product:456", {"name": "Laptop", "price": 999.99}, ttl=120)
    
    # Get values
    user = cache.get("user:123")
    product = cache.get("product:456")
    
    print(f"User: {user}")
    print(f"Product: {product}")
    
    # Test cache miss
    missing = cache.get("nonexistent:key")
    print(f"Missing key: {missing}")
    
    # Get cluster statistics
    stats = cache.get_cluster_stats()
    print(f"\nCluster Statistics:")
    print(f"Total nodes: {stats['total_nodes']}")
    print(f"Active nodes: {stats['active_nodes']}")
    print(f"Total cache size: {stats['total_cache_size']}")
    print(f"Cluster hit ratio: {stats['cluster_hit_ratio']:.2%}")
    print(f"Consistency violations: {stats['consistency_violations']}")
    
    # Simulate a network partition
    print("\nSimulating network partition...")
    cache.simulate_partition(["node2"])
    
    # Try to get data during partition
    user_during_partition = cache.get("user:123")
    print(f"User during partition: {user_during_partition}")
    
    # Heal the partition
    print("\nHealing partition...")
    cache.heal_partition(["node2"])
    
    # Clean up
    cache.stop()
    print("\nCache stopped.")