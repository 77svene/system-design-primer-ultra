import time
import threading
import hashlib
import json
from collections import defaultdict, deque
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

class ConsistencyLevel(Enum):
    STRONG = "strong"  # Linearizable, synchronous replication
    EVENTUAL = "eventual"  # Async replication, may read stale
    CAUSAL = "causal"  # Causal consistency via version vectors

class VersionVector:
    """Vector clock for conflict resolution"""
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.versions: Dict[str, int] = defaultdict(int)
        self.increment()
    
    def increment(self):
        self.versions[self.node_id] += 1
        return self
    
    def merge(self, other: 'VersionVector'):
        for node, version in other.versions.items():
            self.versions[node] = max(self.versions.get(node, 0), version)
    
    def __lt__(self, other: 'VersionVector') -> bool:
        """Check if self is strictly less than other (for conflict detection)"""
        all_less_or_equal = True
        at_least_one_less = False
        for node in set(self.versions.keys()) | set(other.versions.keys()):
            s = self.versions.get(node, 0)
            o = other.versions.get(node, 0)
            if s > o:
                all_less_or_equal = False
                break
            if s < o:
                at_least_one_less = True
        return all_less_or_equal and at_least_one_less
    
    def __eq__(self, other: 'VersionVector') -> bool:
        return self.versions == other.versions
    
    def to_dict(self) -> Dict:
        return {"node_id": self.node_id, "versions": dict(self.versions)}
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'VersionVector':
        vv = cls(data["node_id"])
        vv.versions = defaultdict(int, data["versions"])
        return vv

class Node(object):
    def __init__(self, key: str, value: Any, version_vector: VersionVector):
        self.key = key
        self.value = value
        self.version_vector = version_vector
        self.timestamp = time.time()
        self.next = None
        self.prev = None

class LinkedList(object):
    def __init__(self):
        self.head = None
        self.tail = None

    def move_to_front(self, node: Node):
        if node == self.head:
            return
        self._remove_node(node)
        self._add_to_front(node)

    def append_to_front(self, node: Node):
        self._add_to_front(node)

    def remove_from_tail(self) -> Optional[Node]:
        if not self.tail:
            return None
        node = self.tail
        self._remove_node(node)
        return node

    def _add_to_front(self, node: Node):
        node.next = self.head
        node.prev = None
        if self.head:
            self.head.prev = node
        self.head = node
        if not self.tail:
            self.tail = node

    def _remove_node(self, node: Node):
        if node.prev:
            node.prev.next = node.next
        if node.next:
            node.next.prev = node.prev
        if node == self.head:
            self.head = node.next
        if node == self.tail:
            self.tail = node.prev
        node.next = node.prev = None

class InvalidationMessage:
    def __init__(self, key: str, version_vector: Dict, source_node: str, operation: str = "invalidate"):
        self.key = key
        self.version_vector = version_vector
        self.source_node = source_node
        self.operation = operation
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict:
        return {
            "key": self.key,
            "version_vector": self.version_vector,
            "source_node": self.source_node,
            "operation": self.operation,
            "timestamp": self.timestamp
        }

class DistributedCache:
    """
    Distributed cache with multiple consistency guarantees.
    Supports cache-aside, write-through, and eventual consistency patterns.
    """
    
    def __init__(self, node_id: str, max_size: int = 1000, 
                 consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
                 replication_factor: int = 3, write_quorum: int = 2, read_quorum: int = 1):
        self.node_id = node_id
        self.MAX_SIZE = max_size
        self.consistency_level = consistency_level
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum
        self.read_quorum = read_quorum
        
        # Local cache storage
        self.lookup: Dict[str, Node] = {}
        self.linked_list = LinkedList()
        self.size = 0
        
        # Distributed state
        self.version_vectors: Dict[str, VersionVector] = {}
        self.peers: Set[str] = set()  # Other cache nodes
        self.pending_invalidations: Dict[str, List[InvalidationMessage]] = defaultdict(list)
        self.lock = threading.RLock()
        
        # Pub/Sub for invalidation propagation
        self.subscribers: Dict[str, callable] = {}
        self.message_queue = deque()
        
        # Statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "invalidations_received": 0,
            "invalidations_sent": 0,
            "conflicts_resolved": 0
        }
        
        # Start background threads
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Start background threads for maintenance tasks"""
        self.invalidation_thread = threading.Thread(target=self._process_invalidations, daemon=True)
        self.invalidation_thread.start()
        
        self.warming_thread = threading.Thread(target=self._warm_cache_periodically, daemon=True)
        self.warming_thread.start()
    
    def _process_invalidations(self):
        """Process invalidation messages from other nodes"""
        while True:
            time.sleep(0.01)  # Small delay to prevent busy waiting
            with self.lock:
                while self.message_queue:
                    msg = self.message_queue.popleft()
                    self._handle_invalidation(msg)
    
    def _handle_invalidation(self, message: InvalidationMessage):
        """Handle incoming invalidation message"""
        self.stats["invalidations_received"] += 1
        
        if message.operation == "invalidate":
            self._invalidate_key(message.key, message.version_vector)
        elif message.operation == "update":
            # For write-through pattern, we might receive updates
            self._update_from_peer(message.key, message.version_vector)
    
    def _invalidate_key(self, key: str, remote_version: Dict):
        """Invalidate a key based on remote version"""
        with self.lock:
            if key in self.lookup:
                local_vv = self.version_vectors.get(key)
                remote_vv = VersionVector.from_dict(remote_version)
                
                # Only invalidate if remote version is newer
                if local_vv and remote_vv > local_vv:
                    self._remove_key(key)
    
    def _update_from_peer(self, key: str, remote_version: Dict):
        """Update local cache from peer (for write-through)"""
        # In a real implementation, this would fetch the value from the peer
        pass
    
    def _remove_key(self, key: str):
        """Remove a key from local cache"""
        if key in self.lookup:
            node = self.lookup[key]
            self.linked_list._remove_node(node)
            del self.lookup[key]
            del self.version_vectors[key]
            self.size -= 1
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get value from cache with consistency guarantees.
        Implements cache-aside pattern.
        """
        with self.lock:
            # Check local cache first
            node = self.lookup.get(key)
            if node:
                self.stats["hits"] += 1
                self.linked_list.move_to_front(node)
                return node.value
            
            self.stats["misses"] += 1
            
            # For strong consistency, we might need to check other nodes
            if self.consistency_level == ConsistencyLevel.STRONG:
                value = self._get_from_replicas(key)
                if value is not None:
                    return value
            
            return default
    
    def _get_from_replicas(self, key: str) -> Optional[Any]:
        """Get value from replica nodes (for strong consistency)"""
        # In a real implementation, this would query other nodes
        # and use quorum reads
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set value in cache with consistency guarantees.
        Supports both cache-aside and write-through patterns.
        """
        with self.lock:
            # Create or update version vector
            if key in self.version_vectors:
                vv = self.version_vectors[key]
                vv.increment()
            else:
                vv = VersionVector(self.node_id)
                self.version_vectors[key] = vv
            
            # Update local cache
            if key in self.lookup:
                node = self.lookup[key]
                node.value = value
                node.version_vector = vv
                node.timestamp = time.time()
                self.linked_list.move_to_front(node)
            else:
                if self.size >= self.MAX_SIZE:
                    self._evict_one()
                
                node = Node(key, value, vv)
                self.lookup[key] = node
                self.linked_list.append_to_front(node)
                self.size += 1
            
            # Propagate invalidation/update based on consistency level
            self._propagate_update(key, vv)
            
            return True
    
    def _evict_one(self):
        """Evict one entry using LRU policy"""
        node = self.linked_list.remove_from_tail()
        if node:
            del self.lookup[node.key]
            if node.key in self.version_vectors:
                del self.version_vectors[node.key]
            self.size -= 1
    
    def _propagate_update(self, key: str, version_vector: VersionVector):
        """Propagate update to other nodes based on consistency level"""
        if self.consistency_level == ConsistencyLevel.EVENTUAL:
            # Async invalidation propagation
            self._send_invalidation(key, version_vector, "invalidate")
        elif self.consistency_level == ConsistencyLevel.STRONG:
            # Synchronous replication
            self._synchronous_replication(key, version_vector)
        elif self.consistency_level == ConsistencyLevel.CAUSAL:
            # Causal consistency - include version vector
            self._send_invalidation(key, version_vector, "update")
    
    def _send_invalidation(self, key: str, version_vector: VersionVector, operation: str):
        """Send invalidation message to peers"""
        message = InvalidationMessage(
            key=key,
            version_vector=version_vector.to_dict(),
            source_node=self.node_id,
            operation=operation
        )
        
        # In a real implementation, this would use pub/sub or RPC
        self.stats["invalidations_sent"] += 1
        
        # Simulate sending to peers
        for peer in self.peers:
            # Would send message to peer
            pass
    
    def _synchronous_replication(self, key: str, version_vector: VersionVector):
        """Synchronously replicate to write quorum nodes"""
        # In a real implementation, this would block until write quorum is achieved
        pass
    
    def add_peer(self, peer_id: str):
        """Add a peer node to the cluster"""
        with self.lock:
            self.peers.add(peer_id)
    
    def remove_peer(self, peer_id: str):
        """Remove a peer node from the cluster"""
        with self.lock:
            self.peers.discard(peer_id)
    
    def receive_invalidation(self, message: Dict):
        """Receive invalidation message from another node"""
        msg = InvalidationMessage(
            key=message["key"],
            version_vector=message["version_vector"],
            source_node=message["source_node"],
            operation=message["operation"]
        )
        self.message_queue.append(msg)
    
    def _warm_cache_periodically(self):
        """Periodically warm cache with frequently accessed keys"""
        while True:
            time.sleep(300)  # Warm every 5 minutes
            self.warm_cache()
    
    def warm_cache(self, keys: Optional[List[str]] = None):
        """
        Warm cache with specified keys or most frequently accessed keys.
        Implements cache warming strategy.
        """
        if keys is None:
            # In a real implementation, this would analyze access patterns
            # and warm with most frequently accessed keys
            pass
        
        # Simulate warming by pre-fetching from database
        for key in keys or []:
            # In a real implementation, this would fetch from database
            # and populate cache
            pass
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self.lock:
            hit_rate = self.stats["hits"] / max(1, self.stats["hits"] + self.stats["misses"])
            return {
                **self.stats,
                "size": self.size,
                "max_size": self.MAX_SIZE,
                "hit_rate": hit_rate,
                "peer_count": len(self.peers),
                "consistency_level": self.consistency_level.value
            }
    
    def clear(self):
        """Clear the cache"""
        with self.lock:
            self.lookup.clear()
            self.linked_list = LinkedList()
            self.version_vectors.clear()
            self.size = 0
    
    def benchmark(self, operations: int = 10000, read_ratio: float = 0.8) -> Dict:
        """
        Benchmark cache performance with different consistency levels.
        Returns performance metrics.
        """
        import random
        import string
        
        def random_key():
            return ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        
        def random_value():
            return ''.join(random.choices(string.ascii_letters + string.digits, k=100))
        
        start_time = time.time()
        read_times = []
        write_times = []
        
        for i in range(operations):
            key = random_key()
            
            if random.random() < read_ratio:
                # Read operation
                op_start = time.time()
                self.get(key)
                read_times.append(time.time() - op_start)
            else:
                # Write operation
                value = random_value()
                op_start = time.time()
                self.set(key, value)
                write_times.append(time.time() - op_start)
        
        total_time = time.time() - start_time
        
        return {
            "total_operations": operations,
            "total_time": total_time,
            "operations_per_second": operations / total_time,
            "avg_read_time": sum(read_times) / max(1, len(read_times)),
            "avg_write_time": sum(write_times) / max(1, len(write_times)),
            "read_ratio": read_ratio,
            "consistency_level": self.consistency_level.value,
            "stats": self.get_stats()
        }

# Factory function to create cache with specific consistency level
def create_distributed_cache(node_id: str, consistency_level: str = "eventual", **kwargs) -> DistributedCache:
    """Factory function to create distributed cache with specified consistency level"""
    level = ConsistencyLevel(consistency_level)
    return DistributedCache(node_id, consistency_level=level, **kwargs)

# Example usage and benchmarks
if __name__ == "__main__":
    # Create caches with different consistency levels
    strong_cache = create_distributed_cache("node1", "strong", max_size=1000)
    eventual_cache = create_distributed_cache("node2", "eventual", max_size=1000)
    causal_cache = create_distributed_cache("node3", "causal", max_size=1000)
    
    # Run benchmarks
    print("Running benchmarks...")
    print("\nStrong Consistency:")
    strong_results = strong_cache.benchmark(operations=5000, read_ratio=0.7)
    print(f"  OPS: {strong_results['operations_per_second']:.2f}")
    print(f"  Avg Read: {strong_results['avg_read_time']*1000:.2f}ms")
    print(f"  Avg Write: {strong_results['avg_write_time']*1000:.2f}ms")
    
    print("\nEventual Consistency:")
    eventual_results = eventual_cache.benchmark(operations=5000, read_ratio=0.7)
    print(f"  OPS: {eventual_results['operations_per_second']:.2f}")
    print(f"  Avg Read: {eventual_results['avg_read_time']*1000:.2f}ms")
    print(f"  Avg Write: {eventual_results['avg_write_time']*1000:.2f}ms")
    
    print("\nCausal Consistency:")
    causal_results = causal_cache.benchmark(operations=5000, read_ratio=0.7)
    print(f"  OPS: {causal_results['operations_per_second']:.2f}")
    print(f"  Avg Read: {causal_results['avg_read_time']*1000:.2f}ms")
    print(f"  Avg Write: {causal_results['avg_write_time']*1000:.2f}ms")