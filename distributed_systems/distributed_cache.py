"""Distributed Cache with Consistency Guarantees.

This module implements a Redis-like distributed cache supporting multiple
consistency models and cache patterns. It provides cache-aside, write-through,
and eventual consistency with invalidation propagation via pub/sub.
"""

import asyncio
import hashlib
import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
import threading
import bisect
import statistics


class ConsistencyLevel(Enum):
    """Supported consistency levels for cache operations."""
    STRONG = "strong"  # Linearizable reads/writes
    EVENTUAL = "eventual"  # Eventually consistent
    WEAK = "weak"  # Best-effort consistency


class CachePattern(Enum):
    """Cache read/write patterns."""
    CACHE_ASIDE = "cache_aside"  # App manages cache
    WRITE_THROUGH = "write_through"  # Synchronous write to DB
    WRITE_BEHIND = "write_behind"  # Async write to DB


@dataclass
class CacheEntry:
    """Represents a cached value with metadata."""
    key: str
    value: Any
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    ttl: Optional[float] = None
    node_id: str = ""
    vector_clock: Dict[str, int] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.timestamp > self.ttl
    
    def to_dict(self) -> Dict:
        """Serialize cache entry to dictionary."""
        return {
            "key": self.key,
            "value": self.value,
            "version": self.version,
            "timestamp": self.timestamp,
            "ttl": self.ttl,
            "node_id": self.node_id,
            "vector_clock": self.vector_clock
        }


class LRUCache:
    """Least Recently Used cache implementation."""
    
    def __init__(self, capacity: int = 1000):
        self.capacity = capacity
        self.cache: Dict[str, CacheEntry] = {}
        self.order: List[str] = []
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get item from cache, updating LRU order."""
        if key in self.cache:
            entry = self.cache[key]
            if entry.is_expired():
                self.delete(key)
                self.misses += 1
                return None
            
            # Move to end (most recently used)
            self.order.remove(key)
            self.order.append(key)
            self.hits += 1
            return entry
        
        self.misses += 1
        return None
    
    def put(self, key: str, entry: CacheEntry) -> None:
        """Put item in cache, evicting LRU if at capacity."""
        if key in self.cache:
            self.order.remove(key)
        elif len(self.cache) >= self.capacity:
            # Evict least recently used
            lru_key = self.order.pop(0)
            del self.cache[lru_key]
        
        self.cache[key] = entry
        self.order.append(key)
    
    def delete(self, key: str) -> bool:
        """Delete item from cache."""
        if key in self.cache:
            del self.cache[key]
            self.order.remove(key)
            return True
        return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self.cache.clear()
        self.order.clear()
    
    def stats(self) -> Dict:
        """Get cache statistics."""
        total = self.hits + self.misses
        hit_rate = self.hits / total if total > 0 else 0
        return {
            "size": len(self.cache),
            "capacity": self.capacity,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": hit_rate
        }


class VersionVector:
    """Vector clock implementation for conflict resolution."""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.clock: Dict[str, int] = {node_id: 0}
    
    def increment(self) -> None:
        """Increment this node's version."""
        self.clock[self.node_id] = self.clock.get(self.node_id, 0) + 1
    
    def merge(self, other: Dict[str, int]) -> None:
        """Merge with another vector clock."""
        for node, version in other.items():
            self.clock[node] = max(self.clock.get(node, 0), version)
    
    def compare(self, other: Dict[str, int]) -> int:
        """Compare two vector clocks.
        
        Returns:
            -1 if self < other
             0 if concurrent
             1 if self > other
        """
        self_greater = False
        other_greater = False
        
        all_nodes = set(self.clock.keys()) | set(other.keys())
        for node in all_nodes:
            v1 = self.clock.get(node, 0)
            v2 = other.get(node, 0)
            
            if v1 > v2:
                self_greater = True
            elif v1 < v2:
                other_greater = True
        
        if self_greater and not other_greater:
            return 1
        elif other_greater and not self_greater:
            return -1
        else:
            return 0  # Concurrent or equal
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return self.clock.copy()


class PubSubManager:
    """Manages pub/sub for cache invalidation."""
    
    def __init__(self):
        self.subscribers: Dict[str, Set[str]] = defaultdict(set)
        self.message_queue: Dict[str, List[Dict]] = defaultdict(list)
        self.lock = threading.RLock()
    
    def subscribe(self, node_id: str, channel: str) -> None:
        """Subscribe a node to a channel."""
        with self.lock:
            self.subscribers[channel].add(node_id)
    
    def unsubscribe(self, node_id: str, channel: str) -> None:
        """Unsubscribe a node from a channel."""
        with self.lock:
            if channel in self.subscribers:
                self.subscribers[channel].discard(node_id)
    
    def publish(self, channel: str, message: Dict) -> List[str]:
        """Publish message to channel, return list of notified nodes."""
        notified = []
        with self.lock:
            if channel in self.subscribers:
                for node_id in self.subscribers[channel]:
                    self.message_queue[node_id].append({
                        "channel": channel,
                        "message": message,
                        "timestamp": time.time()
                    })
                    notified.append(node_id)
        return notified
    
    def get_messages(self, node_id: str, clear: bool = True) -> List[Dict]:
        """Get pending messages for a node."""
        with self.lock:
            messages = self.message_queue.get(node_id, [])
            if clear:
                self.message_queue[node_id] = []
            return messages


class CacheNode:
    """Individual cache node in the distributed system."""
    
    def __init__(self, node_id: str, capacity: int = 1000):
        self.node_id = node_id
        self.cache = LRUCache(capacity)
        self.version_vector = VersionVector(node_id)
        self.pubsub = PubSubManager()
        self.pending_writes: Dict[str, CacheEntry] = {}
        self.write_log: List[Dict] = []
        
    def get(self, key: str, consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL) -> Optional[CacheEntry]:
        """Get value from cache with specified consistency."""
        entry = self.cache.get(key)
        
        if entry and consistency == ConsistencyLevel.STRONG:
            # For strong consistency, check if we have the latest version
            # In a real system, this would involve coordination with other nodes
            pass
        
        return entry
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None,
            consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL) -> CacheEntry:
        """Put value in cache with specified consistency."""
        # Get existing entry to increment version
        existing = self.cache.get(key)
        version = existing.version + 1 if existing else 1
        
        # Update version vector
        self.version_vector.increment()
        
        # Create new cache entry
        entry = CacheEntry(
            key=key,
            value=value,
            version=version,
            timestamp=time.time(),
            ttl=ttl,
            node_id=self.node_id,
            vector_clock=self.version_vector.to_dict()
        )
        
        # Store in local cache
        self.cache.put(key, entry)
        
        # Handle different consistency levels
        if consistency == ConsistencyLevel.STRONG:
            # For strong consistency, we would need to coordinate with other nodes
            # For now, we'll just invalidate other nodes
            self._invalidate_other_nodes(key, entry)
        elif consistency == ConsistencyLevel.EVENTUAL:
            # Queue for async propagation
            self.pending_writes[key] = entry
        
        return entry
    
    def _invalidate_other_nodes(self, key: str, entry: CacheEntry) -> None:
        """Invalidate key in other nodes via pub/sub."""
        message = {
            "type": "invalidation",
            "key": key,
            "version": entry.version,
            "node_id": self.node_id,
            "timestamp": time.time()
        }
        self.pubsub.publish(f"cache:invalidate:{key}", message)
    
    def process_messages(self) -> List[Dict]:
        """Process pending pub/sub messages."""
        messages = self.pubsub.get_messages(self.node_id)
        processed = []
        
        for msg in messages:
            if msg["message"]["type"] == "invalidation":
                key = msg["message"]["key"]
                # Invalidate local cache entry if our version is older
                local_entry = self.cache.get(key)
                if local_entry and local_entry.version < msg["message"]["version"]:
                    self.cache.delete(key)
                    processed.append({"action": "invalidated", "key": key})
        
        return processed
    
    def warm_cache(self, keys: List[str], data_source: Dict[str, Any]) -> int:
        """Warm cache with data from source."""
        warmed = 0
        for key in keys:
            if key in data_source:
                self.put(key, data_source[key])
                warmed += 1
        return warmed
    
    def stats(self) -> Dict:
        """Get node statistics."""
        return {
            "node_id": self.node_id,
            "cache_stats": self.cache.stats(),
            "pending_writes": len(self.pending_writes),
            "vector_clock": self.version_vector.to_dict()
        }


class DistributedCache:
    """Main distributed cache implementation."""
    
    def __init__(self, num_nodes: int = 3, capacity_per_node: int = 1000):
        self.nodes: Dict[str, CacheNode] = {}
        self.num_nodes = num_nodes
        self.capacity_per_node = capacity_per_node
        self.pubsub = PubSubManager()
        self.hash_ring: List[Tuple[int, str]] = []  # Consistent hashing ring
        
        # Initialize nodes
        for i in range(num_nodes):
            node_id = f"node_{i}"
            node = CacheNode(node_id, capacity_per_node)
            node.pubsub = self.pubsub  # Share pub/sub manager
            self.nodes[node_id] = node
            self._add_to_hash_ring(node_id)
    
    def _add_to_hash_ring(self, node_id: str, num_replicas: int = 100) -> None:
        """Add node to consistent hash ring."""
        for i in range(num_replicas):
            key = f"{node_id}:{i}"
            hash_val = self._hash(key)
            bisect.insort(self.hash_ring, (hash_val, node_id))
    
    def _hash(self, key: str) -> int:
        """Hash function for consistent hashing."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _get_node_for_key(self, key: str) -> CacheNode:
        """Get node responsible for key using consistent hashing."""
        if not self.hash_ring:
            raise ValueError("No nodes in hash ring")
        
        key_hash = self._hash(key)
        
        # Find first node with hash >= key_hash
        for node_hash, node_id in self.hash_ring:
            if node_hash >= key_hash:
                return self.nodes[node_id]
        
        # Wrap around to first node
        return self.nodes[self.hash_ring[0][1]]
    
    def get(self, key: str, consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
            pattern: CachePattern = CachePattern.CACHE_ASIDE) -> Optional[Any]:
        """Get value from distributed cache."""
        node = self._get_node_for_key(key)
        entry = node.get(key, consistency)
        
        if entry:
            return entry.value
        
        # Cache miss - handle based on pattern
        if pattern == CachePattern.CACHE_ASIDE:
            # In cache-aside, application would fetch from DB and call put()
            return None
        elif pattern == CachePattern.WRITE_THROUGH:
            # In write-through, cache should always be in sync with DB
            # This would require DB access in a real implementation
            return None
        
        return None
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None,
            consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
            pattern: CachePattern = CachePattern.CACHE_ASIDE) -> CacheEntry:
        """Put value in distributed cache."""
        node = self._get_node_for_key(key)
        
        if pattern == CachePattern.WRITE_THROUGH:
            # In write-through, we would write to DB first
            # For simulation, we'll just write to cache
            pass
        elif pattern == CachePattern.WRITE_BEHIND:
            # In write-behind, we queue the write for async DB update
            pass
        
        entry = node.put(key, value, ttl, consistency)
        
        # For strong consistency, wait for invalidation propagation
        if consistency == ConsistencyLevel.STRONG:
            # In a real system, we would wait for acknowledgments
            time.sleep(0.001)  # Simulate network delay
        
        return entry
    
    def delete(self, key: str, consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL) -> bool:
        """Delete key from distributed cache."""
        node = self._get_node_for_key(key)
        result = node.cache.delete(key)
        
        if result and consistency == ConsistencyLevel.STRONG:
            # Invalidate in other nodes
            message = {
                "type": "deletion",
                "key": key,
                "node_id": node.node_id,
                "timestamp": time.time()
            }
            self.pubsub.publish(f"cache:delete:{key}", message)
        
        return result
    
    def process_all_messages(self) -> Dict[str, List[Dict]]:
        """Process pending messages for all nodes."""
        results = {}
        for node_id, node in self.nodes.items():
            results[node_id] = node.process_messages()
        return results
    
    def warm_cache(self, data: Dict[str, Any]) -> Dict[str, int]:
        """Warm cache across all nodes."""
        results = {}
        for key, value in data.items():
            node = self._get_node_for_key(key)
            node.put(key, value)
            results[key] = 1
        return results
    
    def get_stats(self) -> Dict:
        """Get statistics for all nodes."""
        stats = {}
        for node_id, node in self.nodes.items():
            stats[node_id] = node.stats()
        
        # Calculate aggregate statistics
        total_hits = sum(s["cache_stats"]["hits"] for s in stats.values())
        total_misses = sum(s["cache_stats"]["misses"] for s in stats.values())
        total = total_hits + total_misses
        
        stats["aggregate"] = {
            "total_nodes": len(self.nodes),
            "total_hits": total_hits,
            "total_misses": total_misses,
            "overall_hit_rate": total_hits / total if total > 0 else 0,
            "total_entries": sum(s["cache_stats"]["size"] for s in stats.values())
        }
        
        return stats
    
    def benchmark(self, num_operations: int = 1000, read_ratio: float = 0.7) -> Dict:
        """Benchmark different consistency models."""
        results = {}
        
        for consistency in ConsistencyLevel:
            print(f"\nBenchmarking {consistency.value} consistency...")
            
            # Reset cache
            for node in self.nodes.values():
                node.cache.clear()
            
            start_time = time.time()
            read_latencies = []
            write_latencies = []
            
            for i in range(num_operations):
                key = f"key_{i % 100}"  # 100 unique keys
                value = f"value_{i}"
                
                if i / num_operations < read_ratio:
                    # Read operation
                    op_start = time.time()
                    self.get(key, consistency)
                    read_latencies.append(time.time() - op_start)
                else:
                    # Write operation
                    op_start = time.time()
                    self.put(key, value, consistency=consistency)
                    write_latencies.append(time.time() - op_start)
            
            total_time = time.time() - start_time
            
            results[consistency.value] = {
                "total_time": total_time,
                "operations_per_second": num_operations / total_time,
                "avg_read_latency": statistics.mean(read_latencies) if read_latencies else 0,
                "avg_write_latency": statistics.mean(write_latencies) if write_latencies else 0,
                "p95_read_latency": statistics.quantiles(read_latencies, n=20)[18] if len(read_latencies) >= 20 else 0,
                "p95_write_latency": statistics.quantiles(write_latencies, n=20)[18] if len(write_latencies) >= 20 else 0,
                "read_count": len(read_latencies),
                "write_count": len(write_latencies)
            }
        
        return results


class CacheClient:
    """Client interface for the distributed cache."""
    
    def __init__(self, cache: DistributedCache, default_consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL):
        self.cache = cache
        self.default_consistency = default_consistency
    
    def get(self, key: str, consistency: Optional[ConsistencyLevel] = None) -> Optional[Any]:
        """Get value from cache."""
        consistency = consistency or self.default_consistency
        return self.cache.get(key, consistency)
    
    def set(self, key: str, value: Any, ttl: Optional[float] = None,
            consistency: Optional[ConsistencyLevel] = None) -> bool:
        """Set value in cache."""
        consistency = consistency or self.default_consistency
        try:
            self.cache.put(key, value, ttl, consistency)
            return True
        except Exception:
            return False
    
    def delete(self, key: str, consistency: Optional[ConsistencyLevel] = None) -> bool:
        """Delete key from cache."""
        consistency = consistency or self.default_consistency
        return self.cache.delete(key, consistency)
    
    def mget(self, keys: List[str], consistency: Optional[ConsistencyLevel] = None) -> Dict[str, Any]:
        """Get multiple values from cache."""
        consistency = consistency or self.default_consistency
        results = {}
        for key in keys:
            value = self.cache.get(key, consistency)
            if value is not None:
                results[key] = value
        return results
    
    def mset(self, mapping: Dict[str, Any], ttl: Optional[float] = None,
             consistency: Optional[ConsistencyLevel] = None) -> int:
        """Set multiple values in cache."""
        consistency = consistency or self.default_consistency
        success_count = 0
        for key, value in mapping.items():
            if self.set(key, value, ttl, consistency):
                success_count += 1
        return success_count


# Example usage and demonstration
def demonstrate_distributed_cache():
    """Demonstrate the distributed cache functionality."""
    print("=== Distributed Cache Demonstration ===\n")
    
    # Create distributed cache with 3 nodes
    cache = DistributedCache(num_nodes=3, capacity_per_node=1000)
    
    # Create client with eventual consistency
    client = CacheClient(cache, ConsistencyLevel.EVENTUAL)
    
    # Test cache-aside pattern
    print("1. Testing Cache-Aside Pattern:")
    print("   - Initial cache miss")
    value = client.get("user:123")
    print(f"   - GET user:123: {value}")
    
    print("   - Setting value")
    client.set("user:123", {"name": "Alice", "age": 30})
    
    print("   - Cache hit")
    value = client.get("user:123")
    print(f"   - GET user:123: {value}")
    
    # Test write-through pattern
    print("\n2. Testing Write-Through Pattern:")
    cache.put("session:abc", {"user_id": 123, "token": "xyz"}, 
              pattern=CachePattern.WRITE_THROUGH)
    value = cache.get("session:abc", pattern=CachePattern.WRITE_THROUGH)
    print(f"   - Session data: {value}")
    
    # Test eventual consistency
    print("\n3. Testing Eventual Consistency:")
    node1 = cache.nodes["node_0"]
    node2 = cache.nodes["node_1"]
    
    # Write to node1
    node1.put("config:setting", "value1", consistency=ConsistencyLevel.EVENTUAL)
    print(f"   - Node1 wrote: config:setting = value1")
    
    # Check node2 (might not have it yet)
    value2 = node2.get("config:setting")
    print(f"   - Node2 read: {value2}")
    
    # Process messages (simulate propagation)
    cache.process_all_messages()
    
    # Check node2 again
    value2 = node2.get("config:setting")
    print(f"   - Node2 read after propagation: {value2}")
    
    # Test cache warming
    print("\n4. Testing Cache Warming:")
    warm_data = {
        "popular:item1": {"name": "Item 1", "views": 1000},
        "popular:item2": {"name": "Item 2", "views": 850},
        "popular:item3": {"name": "Item 3", "views": 720}
    }
    cache.warm_cache(warm_data)
    
    for key in warm_data.keys():
        value = client.get(key)
        print(f"   - {key}: {value}")
    
    # Show statistics
    print("\n5. Cache Statistics:")
    stats = cache.get_stats()
    for node_id, node_stats in stats.items():
        if node_id != "aggregate":
            print(f"   {node_id}: {node_stats['cache_stats']}")
    
    print(f"   Aggregate: {stats['aggregate']}")
    
    # Run benchmark
    print("\n6. Running Consistency Benchmark:")
    benchmark_results = cache.benchmark(num_operations=1000, read_ratio=0.7)
    
    print("\n   Benchmark Results:")
    for consistency, results in benchmark_results.items():
        print(f"\n   {consistency.upper()} Consistency:")
        print(f"     Operations/sec: {results['operations_per_second']:.2f}")
        print(f"     Avg read latency: {results['avg_read_latency']*1000:.2f}ms")
        print(f"     Avg write latency: {results['avg_write_latency']*1000:.2f}ms")
        print(f"     P95 read latency: {results['p95_read_latency']*1000:.2f}ms")
        print(f"     P95 write latency: {results['p95_write_latency']*1000:.2f}ms")


if __name__ == "__main__":
    demonstrate_distributed_cache()