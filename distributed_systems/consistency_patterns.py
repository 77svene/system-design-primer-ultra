"""Distributed Cache with Consistency Guarantees

This module implements a Redis-like distributed cache with support for multiple
consistency patterns including cache-aside, write-through, and eventual consistency
with invalidation propagation. It features version vectors for conflict resolution,
cache warming strategies, and benchmarks for consistency models.

Integrates with existing system-design-primer-ultra codebase.
"""

import asyncio
import hashlib
import json
import logging
import random
import time
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from concurrent.futures import ThreadPoolExecutor
import heapq

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConsistencyLevel(Enum):
    """Consistency levels for distributed cache operations."""
    STRONG = auto()          # Linearizable consistency
    EVENTUAL = auto()        # Eventually consistent
    CAUSAL = auto()          # Causal consistency
    READ_YOUR_WRITES = auto()  # Read-your-writes consistency


class InvalidationStrategy(Enum):
    """Cache invalidation strategies."""
    BROADCAST = auto()       # Broadcast invalidation to all nodes
    DIRECTORY = auto()       # Use directory-based invalidation
    HYBRID = auto()          # Combination of broadcast and directory


@dataclass
class CacheEntry:
    """Represents a cached value with metadata."""
    key: str
    value: Any
    version: int = 0
    timestamp: float = field(default_factory=time.time)
    ttl: Optional[float] = None
    vector_clock: Dict[str, int] = field(default_factory=dict)
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.timestamp > self.ttl
    
    def touch(self):
        """Update access metadata."""
        self.access_count += 1
        self.last_accessed = time.time()


class VersionVector:
    """Version vector for conflict resolution in distributed systems."""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.versions: Dict[str, int] = defaultdict(int)
        self.versions[node_id] = 0
    
    def increment(self) -> int:
        """Increment version for this node."""
        self.versions[self.node_id] += 1
        return self.versions[self.node_id]
    
    def update(self, other: Dict[str, int]):
        """Update version vector with another vector."""
        for node, version in other.items():
            self.versions[node] = max(self.versions[node], version)
    
    def compare(self, other: Dict[str, int]) -> int:
        """
        Compare version vectors.
        Returns: 1 if self > other, -1 if self < other, 0 if concurrent
        """
        self_greater = False
        other_greater = False
        
        all_nodes = set(self.versions.keys()) | set(other.keys())
        for node in all_nodes:
            v1 = self.versions.get(node, 0)
            v2 = other.get(node, 0)
            if v1 > v2:
                self_greater = True
            elif v1 < v2:
                other_greater = True
        
        if self_greater and not other_greater:
            return 1
        elif other_greater and not self_greater:
            return -1
        return 0  # Concurrent modifications


class CacheNode:
    """Represents a single cache node in the distributed cache."""
    
    def __init__(self, node_id: str, capacity: int = 1000):
        self.node_id = node_id
        self.capacity = capacity
        self.cache: Dict[str, CacheEntry] = {}
        self.version_vector = VersionVector(node_id)
        self.lock = threading.RLock()
        self.hit_count = 0
        self.miss_count = 0
        self.eviction_count = 0
        
        # LRU tracking
        self.access_order: deque = deque()
        
    def get(self, key: str) -> Optional[CacheEntry]:
        """Get a value from cache."""
        with self.lock:
            if key not in self.cache:
                self.miss_count += 1
                return None
            
            entry = self.cache[key]
            
            # Check expiration
            if entry.is_expired():
                self._evict(key)
                self.miss_count += 1
                return None
            
            # Update LRU
            self._update_access_order(key)
            entry.touch()
            self.hit_count += 1
            
            return entry
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None, 
            vector_clock: Optional[Dict[str, int]] = None) -> CacheEntry:
        """Put a value in cache."""
        with self.lock:
            # Check if we need to evict
            if key not in self.cache and len(self.cache) >= self.capacity:
                self._evict_lru()
            
            # Create or update entry
            if key in self.cache:
                entry = self.cache[key]
                entry.value = value
                entry.timestamp = time.time()
                entry.ttl = ttl
                if vector_clock:
                    self.version_vector.update(vector_clock)
                    entry.vector_clock = self.version_vector.versions.copy()
                else:
                    entry.version = self.version_vector.increment()
                    entry.vector_clock = self.version_vector.versions.copy()
            else:
                version = self.version_vector.increment()
                entry = CacheEntry(
                    key=key,
                    value=value,
                    version=version,
                    timestamp=time.time(),
                    ttl=ttl,
                    vector_clock=self.version_vector.versions.copy()
                )
                self.cache[key] = entry
            
            # Update LRU
            self._update_access_order(key)
            
            return entry
    
    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry."""
        with self.lock:
            if key in self.cache:
                self._evict(key)
                return True
            return False
    
    def _evict(self, key: str):
        """Evict a cache entry."""
        if key in self.cache:
            del self.cache[key]
            self.eviction_count += 1
            # Remove from access order
            try:
                self.access_order.remove(key)
            except ValueError:
                pass
    
    def _evict_lru(self):
        """Evict least recently used entry."""
        if self.access_order:
            lru_key = self.access_order.popleft()
            self._evict(lru_key)
    
    def _update_access_order(self, key: str):
        """Update LRU access order."""
        try:
            self.access_order.remove(key)
        except ValueError:
            pass
        self.access_order.append(key)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.hit_count + self.miss_count
            hit_rate = self.hit_count / total_requests if total_requests > 0 else 0
            
            return {
                'node_id': self.node_id,
                'size': len(self.cache),
                'capacity': self.capacity,
                'hit_count': self.hit_count,
                'miss_count': self.miss_count,
                'hit_rate': hit_rate,
                'eviction_count': self.eviction_count,
                'avg_access_count': sum(e.access_count for e in self.cache.values()) / len(self.cache) if self.cache else 0
            }


class PubSubSystem:
    """Simple pub/sub system for cache invalidation."""
    
    def __init__(self):
        self.subscribers: Dict[str, Set[Callable]] = defaultdict(set)
        self.lock = threading.Lock()
    
    def subscribe(self, topic: str, callback: Callable):
        """Subscribe to a topic."""
        with self.lock:
            self.subscribers[topic].add(callback)
    
    def unsubscribe(self, topic: str, callback: Callable):
        """Unsubscribe from a topic."""
        with self.lock:
            if topic in self.subscribers:
                self.subscribers[topic].discard(callback)
    
    def publish(self, topic: str, message: Any):
        """Publish a message to a topic."""
        with self.lock:
            if topic in self.subscribers:
                for callback in self.subscribers[topic]:
                    try:
                        callback(message)
                    except Exception as e:
                        logger.error(f"Error in pub/sub callback: {e}")


class DistributedCache:
    """
    Distributed cache with multiple consistency patterns.
    
    Supports cache-aside, write-through, and eventual consistency patterns
    with invalidation propagation and conflict resolution.
    """
    
    def __init__(self, 
                 num_nodes: int = 3,
                 consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
                 invalidation_strategy: InvalidationStrategy = InvalidationStrategy.BROADCAST,
                 replication_factor: int = 2,
                 node_capacity: int = 1000):
        """
        Initialize distributed cache.
        
        Args:
            num_nodes: Number of cache nodes
            consistency_level: Consistency level for operations
            invalidation_strategy: Strategy for cache invalidation
            replication_factor: Number of replicas per key
            node_capacity: Capacity per cache node
        """
        self.nodes: Dict[str, CacheNode] = {}
        self.consistency_level = consistency_level
        self.invalidation_strategy = invalidation_strategy
        self.replication_factor = min(replication_factor, num_nodes)
        self.pub_sub = PubSubSystem()
        self.executor = ThreadPoolExecutor(max_workers=10)
        
        # Create nodes
        for i in range(num_nodes):
            node_id = f"node_{i}"
            self.nodes[node_id] = CacheNode(node_id, node_capacity)
        
        # Virtual nodes for consistent hashing
        self.virtual_nodes: List[Tuple[int, str]] = []
        self._init_virtual_nodes()
        
        # Background tasks
        self._running = True
        self._cleanup_thread = threading.Thread(target=self._cleanup_expired, daemon=True)
        self._cleanup_thread.start()
        
        logger.info(f"Initialized distributed cache with {num_nodes} nodes, "
                   f"consistency: {consistency_level.name}, "
                   f"invalidation: {invalidation_strategy.name}")
    
    def _init_virtual_nodes(self, virtual_factor: int = 100):
        """Initialize virtual nodes for consistent hashing."""
        for node_id in self.nodes:
            for i in range(virtual_factor):
                key = f"{node_id}:{i}"
                hash_val = self._hash_key(key)
                self.virtual_nodes.append((hash_val, node_id))
        
        self.virtual_nodes.sort()
    
    def _hash_key(self, key: str) -> int:
        """Hash a key using consistent hashing."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _get_nodes_for_key(self, key: str) -> List[str]:
        """Get nodes responsible for a key using consistent hashing."""
        if not self.virtual_nodes:
            return list(self.nodes.keys())[:self.replication_factor]
        
        hash_val = self._hash_key(key)
        
        # Find the first node with hash >= key hash
        idx = 0
        for i, (node_hash, _) in enumerate(self.virtual_nodes):
            if node_hash >= hash_val:
                idx = i
                break
        else:
            idx = 0
        
        # Get unique nodes for replication
        nodes = []
        seen = set()
        for i in range(len(self.virtual_nodes)):
            actual_idx = (idx + i) % len(self.virtual_nodes)
            _, node_id = self.virtual_nodes[actual_idx]
            if node_id not in seen:
                nodes.append(node_id)
                seen.add(node_id)
                if len(nodes) >= self.replication_factor:
                    break
        
        return nodes
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value from cache.
        
        Args:
            key: Cache key
            default: Default value if not found
            
        Returns:
            Cached value or default
        """
        nodes = self._get_nodes_for_key(key)
        
        if self.consistency_level == ConsistencyLevel.STRONG:
            return self._get_strong(key, nodes, default)
        elif self.consistency_level == ConsistencyLevel.EVENTUAL:
            return self._get_eventual(key, nodes, default)
        elif self.consistency_level == ConsistencyLevel.CAUSAL:
            return self._get_causal(key, nodes, default)
        elif self.consistency_level == ConsistencyLevel.READ_YOUR_WRITES:
            return self._get_read_your_writes(key, nodes, default)
        else:
            return self._get_eventual(key, nodes, default)
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """
        Put a value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
            
        Returns:
            True if successful
        """
        nodes = self._get_nodes_for_key(key)
        
        if self.consistency_level == ConsistencyLevel.STRONG:
            return self._put_strong(key, value, nodes, ttl)
        elif self.consistency_level == ConsistencyLevel.EVENTUAL:
            return self._put_eventual(key, value, nodes, ttl)
        elif self.consistency_level == ConsistencyLevel.CAUSAL:
            return self._put_causal(key, value, nodes, ttl)
        elif self.consistency_level == ConsistencyLevel.READ_YOUR_WRITES:
            return self._put_read_your_writes(key, value, nodes, ttl)
        else:
            return self._put_eventual(key, value, nodes, ttl)
    
    def delete(self, key: str) -> bool:
        """
        Delete a key from cache.
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if key was deleted
        """
        nodes = self._get_nodes_for_key(key)
        deleted = False
        
        for node_id in nodes:
            if self.nodes[node_id].invalidate(key):
                deleted = True
        
        # Propagate invalidation
        if deleted and self.invalidation_strategy == InvalidationStrategy.BROADCAST:
            self._broadcast_invalidation(key)
        
        return deleted
    
    def _get_strong(self, key: str, nodes: List[str], default: Any) -> Any:
        """Strong consistency get - read from majority."""
        results = []
        for node_id in nodes:
            entry = self.nodes[node_id].get(key)
            if entry:
                results.append(entry)
        
        if not results:
            return default
        
        # For strong consistency, return the entry with latest timestamp
        latest = max(results, key=lambda e: e.timestamp)
        return latest.value
    
    def _put_strong(self, key: str, value: Any, nodes: List[str], ttl: Optional[float]) -> bool:
        """Strong consistency put - write to all nodes synchronously."""
        # Get current version vector from majority
        version_vectors = []
        for node_id in nodes:
            entry = self.nodes[node_id].get(key)
            if entry:
                version_vectors.append(entry.vector_clock)
        
        # Merge version vectors
        merged_vector = {}
        for vector in version_vectors:
            for node, version in vector.items():
                merged_vector[node] = max(merged_vector.get(node, 0), version)
        
        # Write to all nodes
        success_count = 0
        for node_id in nodes:
            try:
                self.nodes[node_id].put(key, value, ttl, merged_vector)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to write to node {node_id}: {e}")
        
        # Invalidate other nodes if using directory-based invalidation
        if self.invalidation_strategy == InvalidationStrategy.DIRECTORY:
            self._invalidate_directory(key, nodes)
        
        return success_count == len(nodes)
    
    def _get_eventual(self, key: str, nodes: List[str], default: Any) -> Any:
        """Eventual consistency get - read from any node."""
        # Try nodes in random order
        shuffled_nodes = nodes.copy()
        random.shuffle(shuffled_nodes)
        
        for node_id in shuffled_nodes:
            entry = self.nodes[node_id].get(key)
            if entry:
                # Asynchronously update other replicas
                self._async_replicate(key, entry.value, nodes, exclude_node=node_id)
                return entry.value
        
        return default
    
    def _put_eventual(self, key: str, value: Any, nodes: List[str], ttl: Optional[float]) -> bool:
        """Eventual consistency put - write to one node, async propagate."""
        # Write to first node
        primary_node = nodes[0]
        entry = self.nodes[primary_node].put(key, value, ttl)
        
        # Asynchronously replicate to other nodes
        self._async_replicate(key, value, nodes, exclude_node=primary_node)
        
        # Broadcast invalidation if needed
        if self.invalidation_strategy == InvalidationStrategy.BROADCAST:
            self._broadcast_invalidation(key)
        
        return True
    
    def _get_causal(self, key: str, nodes: List[str], default: Any) -> Any:
        """Causal consistency get - respect causal dependencies."""
        # For simplicity, we'll use version vectors to ensure causal ordering
        latest_entry = None
        latest_vector = {}
        
        for node_id in nodes:
            entry = self.nodes[node_id].get(key)
            if entry:
                if latest_entry is None:
                    latest_entry = entry
                    latest_vector = entry.vector_clock.copy()
                else:
                    # Compare version vectors
                    comparison = self.nodes[node_id].version_vector.compare(latest_vector)
                    if comparison > 0:  # Current node has newer version
                        latest_entry = entry
                        latest_vector = entry.vector_clock.copy()
        
        return latest_entry.value if latest_entry else default
    
    def _put_causal(self, key: str, value: Any, nodes: List[str], ttl: Optional[float]) -> bool:
        """Causal consistency put - maintain causal dependencies."""
        # Get current causal context
        causal_context = {}
        for node_id in nodes:
            entry = self.nodes[node_id].get(key)
            if entry:
                for node, version in entry.vector_clock.items():
                    causal_context[node] = max(causal_context.get(node, 0), version)
        
        # Write with updated causal context
        success_count = 0
        for node_id in nodes:
            try:
                self.nodes[node_id].put(key, value, ttl, causal_context)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to write to node {node_id}: {e}")
        
        return success_count > 0
    
    def _get_read_your_writes(self, key: str, nodes: List[str], default: Any) -> Any:
        """Read-your-writes consistency - ensure client sees its own writes."""
        # For simplicity, we'll read from the node that was written to last
        # In a real implementation, this would track client sessions
        return self._get_strong(key, nodes, default)
    
    def _put_read_your_writes(self, key: str, value: Any, nodes: List[str], ttl: Optional[float]) -> bool:
        """Write with read-your-writes guarantee."""
        return self._put_strong(key, value, nodes, ttl)
    
    def _async_replicate(self, key: str, value: Any, nodes: List[str], exclude_node: Optional[str] = None):
        """Asynchronously replicate data to other nodes."""
        def replicate():
            for node_id in nodes:
                if node_id != exclude_node:
                    try:
                        self.nodes[node_id].put(key, value)
                    except Exception as e:
                        logger.error(f"Failed to replicate to node {node_id}: {e}")
        
        self.executor.submit(replicate)
    
    def _broadcast_invalidation(self, key: str):
        """Broadcast invalidation to all nodes."""
        message = {'type': 'invalidation', 'key': key, 'timestamp': time.time()}
        self.pub_sub.publish('cache_invalidation', message)
    
    def _invalidate_directory(self, key: str, exclude_nodes: List[str]):
        """Invalidate key on nodes not in the primary set."""
        for node_id in self.nodes:
            if node_id not in exclude_nodes:
                self.nodes[node_id].invalidate(key)
    
    def _cleanup_expired(self):
        """Background thread to clean up expired entries."""
        while self._running:
            time.sleep(60)  # Run every minute
            for node in self.nodes.values():
                with node.lock:
                    expired_keys = [key for key, entry in node.cache.items() if entry.is_expired()]
                    for key in expired_keys:
                        node._evict(key)
    
    def warm_cache(self, data_source: Callable[[str], Any], keys: List[str]):
        """
        Warm cache with data from source.
        
        Args:
            data_source: Function that returns data for a key
            keys: List of keys to warm
        """
        def warm_key(key: str):
            try:
                value = data_source(key)
                if value is not None:
                    self.put(key, value)
            except Exception as e:
                logger.error(f"Failed to warm cache for key {key}: {e}")
        
        # Warm in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(warm_key, keys)
        
        logger.info(f"Warmed cache with {len(keys)} keys")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        stats = {
            'consistency_level': self.consistency_level.name,
            'invalidation_strategy': self.invalidation_strategy.name,
            'replication_factor': self.replication_factor,
            'nodes': {}
        }
        
        total_hits = 0
        total_misses = 0
        total_size = 0
        
        for node_id, node in self.nodes.items():
            node_stats = node.get_stats()
            stats['nodes'][node_id] = node_stats
            total_hits += node_stats['hit_count']
            total_misses += node_stats['miss_count']
            total_size += node_stats['size']
        
        total_requests = total_hits + total_misses
        stats['total_hits'] = total_hits
        stats['total_misses'] = total_misses
        stats['total_requests'] = total_requests
        stats['overall_hit_rate'] = total_hits / total_requests if total_requests > 0 else 0
        stats['total_size'] = total_size
        
        return stats
    
    def shutdown(self):
        """Shutdown the cache system."""
        self._running = False
        self._cleanup_thread.join(timeout=5)
        self.executor.shutdown(wait=True)


class CacheAsidePattern:
    """
    Cache-aside pattern implementation.
    
    Application is responsible for loading data into cache.
    """
    
    def __init__(self, cache: DistributedCache, data_source: Callable[[str], Any]):
        self.cache = cache
        self.data_source = data_source
    
    def get(self, key: str) -> Any:
        """Get data using cache-aside pattern."""
        # Try cache first
        value = self.cache.get(key)
        
        if value is None:
            # Cache miss - load from source
            value = self.data_source(key)
            if value is not None:
                # Store in cache
                self.cache.put(key, value)
        
        return value
    
    def put(self, key: str, value: Any):
        """Update data using cache-aside pattern."""
        # Update source first
        # (In real implementation, this would update the database)
        # Then invalidate cache
        self.cache.delete(key)


class WriteThroughPattern:
    """
    Write-through pattern implementation.
    
    Data is written to cache and storage simultaneously.
    """
    
    def __init__(self, cache: DistributedCache, storage: Callable[[str, Any], None]):
        self.cache = cache
        self.storage = storage
    
    def get(self, key: str) -> Any:
        """Get data using write-through pattern."""
        return self.cache.get(key)
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None):
        """Write data using write-through pattern."""
        # Write to cache
        self.cache.put(key, value, ttl)
        
        # Write to storage (synchronously)
        try:
            self.storage(key, value)
        except Exception as e:
            logger.error(f"Failed to write to storage: {e}")
            # In production, you might want to rollback cache write


class EventualConsistencyPattern:
    """
    Eventual consistency pattern implementation.
    
    Uses asynchronous replication and invalidation.
    """
    
    def __init__(self, cache: DistributedCache):
        self.cache = cache
    
    def get(self, key: str) -> Any:
        """Get data with eventual consistency."""
        return self.cache.get(key)
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None):
        """Write data with eventual consistency."""
        self.cache.put(key, value, ttl)


class ConsistencyBenchmark:
    """Benchmark different consistency models."""
    
    def __init__(self, num_operations: int = 10000, num_keys: int = 1000):
        self.num_operations = num_operations
        self.num_keys = num_keys
        self.results = {}
    
    def run_benchmark(self, consistency_level: ConsistencyLevel, 
                     num_nodes: int = 3, replication_factor: int = 2) -> Dict[str, Any]:
        """
        Run benchmark for a specific consistency level.
        
        Args:
            consistency_level: Consistency level to test
            num_nodes: Number of cache nodes
            replication_factor: Replication factor
            
        Returns:
            Benchmark results
        """
        logger.info(f"Running benchmark for {consistency_level.name} consistency")
        
        # Create cache
        cache = DistributedCache(
            num_nodes=num_nodes,
            consistency_level=consistency_level,
            replication_factor=replication_factor
        )
        
        # Generate test data
        keys = [f"key_{i}" for i in range(self.num_keys)]
        values = {key: f"value_{hashlib.md5(key.encode()).hexdigest()}" for key in keys}
        
        # Warm cache
        def data_source(key):
            return values.get(key)
        
        cache.warm_cache(data_source, keys[:self.num_keys // 2])
        
        # Run operations
        start_time = time.time()
        read_latencies = []
        write_latencies = []
        
        for i in range(self.num_operations):
            op_type = 'read' if random.random() < 0.7 else 'write'  # 70% reads
            key = random.choice(keys)
            
            op_start = time.time()
            
            if op_type == 'read':
                cache.get(key)
                read_latencies.append(time.time() - op_start)
            else:
                value = f"updated_value_{i}"
                cache.put(key, value)
                write_latencies.append(time.time() - op_start)
        
        total_time = time.time() - start_time
        
        # Get stats
        stats = cache.get_stats()
        
        # Calculate metrics
        avg_read_latency = sum(read_latencies) / len(read_latencies) if read_latencies else 0
        avg_write_latency = sum(write_latencies) / len(write_latencies) if write_latencies else 0
        throughput = self.num_operations / total_time
        
        results = {
            'consistency_level': consistency_level.name,
            'total_operations': self.num_operations,
            'total_time': total_time,
            'throughput_ops_per_sec': throughput,
            'avg_read_latency_ms': avg_read_latency * 1000,
            'avg_write_latency_ms': avg_write_latency * 1000,
            'hit_rate': stats['overall_hit_rate'],
            'total_size': stats['total_size'],
            'read_operations': len(read_latencies),
            'write_operations': len(write_latencies)
        }
        
        cache.shutdown()
        
        return results
    
    def run_all_benchmarks(self) -> Dict[str, Dict[str, Any]]:
        """Run benchmarks for all consistency levels."""
        consistency_levels = [
            ConsistencyLevel.STRONG,
            ConsistencyLevel.EVENTUAL,
            ConsistencyLevel.CAUSAL,
            ConsistencyLevel.READ_YOUR_WRITES
        ]
        
        all_results = {}
        
        for level in consistency_levels:
            results = self.run_benchmark(level)
            all_results[level.name] = results
            logger.info(f"{level.name}: {results['throughput_ops_per_sec']:.2f} ops/sec, "
                       f"Read latency: {results['avg_read_latency_ms']:.2f}ms, "
                       f"Write latency: {results['avg_write_latency_ms']:.2f}ms")
        
        self.results = all_results
        return all_results
    
    def print_comparison(self):
        """Print comparison of benchmark results."""
        if not self.results:
            print("No benchmark results available. Run benchmarks first.")
            return
        
        print("\n" + "="*80)
        print("CONSISTENCY MODEL BENCHMARK COMPARISON")
        print("="*80)
        
        # Table header
        print(f"{'Model':<20} {'Throughput (ops/s)':<20} {'Read Latency (ms)':<20} "
              f"{'Write Latency (ms)':<20} {'Hit Rate':<10}")
        print("-"*80)
        
        # Table rows
        for model, results in self.results.items():
            print(f"{model:<20} {results['throughput_ops_per_sec']:<20.2f} "
                  f"{results['avg_read_latency_ms']:<20.2f} "
                  f"{results['avg_write_latency_ms']:<20.2f} "
                  f"{results['hit_rate']:<10.2%}")
        
        print("="*80)
        
        # Find best models
        best_throughput = max(self.results.items(), key=lambda x: x[1]['throughput_ops_per_sec'])
        best_read_latency = min(self.results.items(), key=lambda x: x[1]['avg_read_latency_ms'])
        best_write_latency = min(self.results.items(), key=lambda x: x[1]['avg_write_latency_ms'])
        
        print(f"\nBest throughput: {best_throughput[0]} ({best_throughput[1]['throughput_ops_per_sec']:.2f} ops/s)")
        print(f"Best read latency: {best_read_latency[0]} ({best_read_latency[1]['avg_read_latency_ms']:.2f}ms)")
        print(f"Best write latency: {best_write_latency[0]} ({best_write_latency[1]['avg_write_latency_ms']:.2f}ms)")


# Integration with existing LRU cache
class LRUCacheAdapter:
    """
    Adapter to integrate with existing LRU cache implementation.
    
    Provides backward compatibility while using distributed cache.
    """
    
    def __init__(self, distributed_cache: DistributedCache, capacity: int = 1000):
        self.cache = distributed_cache
        self.capacity = capacity
        self.local_cache: Dict[str, Any] = {}
        self.access_order: deque = deque()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache (LRU behavior)."""
        # Try local cache first
        if key in self.local_cache:
            # Update LRU order
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.local_cache[key]
        
        # Try distributed cache
        value = self.cache.get(key)
        if value is not None:
            # Store in local cache
            self._put_local(key, value)
            return value
        
        return None
    
    def put(self, key: str, value: Any):
        """Put value in cache."""
        # Update distributed cache
        self.cache.put(key, value)
        
        # Update local cache
        self._put_local(key, value)
    
    def _put_local(self, key: str, value: Any):
        """Put in local LRU cache."""
        if key in self.local_cache:
            self.access_order.remove(key)
        elif len(self.local_cache) >= self.capacity:
            # Evict LRU
            lru_key = self.access_order.popleft()
            del self.local_cache[lru_key]
        
        self.local_cache[key] = value
        self.access_order.append(key)


# Example usage and demonstration
def demonstrate_distributed_cache():
    """Demonstrate the distributed cache functionality."""
    print("Distributed Cache with Consistency Guarantees Demo")
    print("="*60)
    
    # Create distributed cache with eventual consistency
    cache = DistributedCache(
        num_nodes=5,
        consistency_level=ConsistencyLevel.EVENTUAL,
        invalidation_strategy=InvalidationStrategy.BROADCAST,
        replication_factor=3
    )
    
    # Example 1: Basic operations
    print("\n1. Basic Cache Operations:")
    cache.put("user:123", {"name": "Alice", "age": 30})
    cache.put("user:456", {"name": "Bob", "age": 25})
    
    user = cache.get("user:123")
    print(f"Retrieved user: {user}")
    
    # Example 2: Cache-aside pattern
    print("\n2. Cache-Aside Pattern:")
    def load_user(user_id):
        # Simulate database load
        print(f"  Loading user {user_id} from database...")
        return {"name": f"User {user_id}", "age": 20 + int(user_id) % 50}
    
    cache_aside = CacheAsidePattern(cache, load_user)
    user = cache_aside.get("user:789")
    print(f"  Retrieved user: {user}")
    user = cache_aside.get("user:789")  # Should hit cache
    print(f"  Retrieved user again: {user}")
    
    # Example 3: Write-through pattern
    print("\n3. Write-Through Pattern:")
    def save_to_database(key, value):
        print(f"  Saving {key} to database...")
        # Simulate database write
        time.sleep(0.01)
    
    write_through = WriteThroughPattern(cache, save_to_database)
    write_through.put("product:100", {"name": "Laptop", "price": 999.99})
    
    # Example 4: Cache warming
    print("\n4. Cache Warming:")
    def product_source(product_id):
        return {"name": f"Product {product_id}", "price": random.uniform(10, 1000)}
    
    product_keys = [f"product:{i}" for i in range(100, 110)]
    cache.warm_cache(product_source, product_keys)
    
    # Example 5: Statistics
    print("\n5. Cache Statistics:")
    stats = cache.get_stats()
    print(f"  Total requests: {stats['total_requests']}")
    print(f"  Hit rate: {stats['overall_hit_rate']:.2%}")
    print(f"  Total size: {stats['total_size']}")
    
    # Example 6: Benchmark comparison
    print("\n6. Running Consistency Model Benchmarks...")
    benchmark = ConsistencyBenchmark(num_operations=5000, num_keys=500)
    benchmark.run_all_benchmarks()
    benchmark.print_comparison()
    
    # Cleanup
    cache.shutdown()
    print("\nDemo completed!")


if __name__ == "__main__":
    demonstrate_distributed_cache()