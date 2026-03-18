# system-design-primer-ultra/distributed_systems/cache_invalidation.py

import time
import json
import threading
import hashlib
import logging
from typing import Any, Dict, Optional, List, Set, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, OrderedDict
import uuid
import asyncio
import aioredis
from concurrent.futures import ThreadPoolExecutor
import statistics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ConsistencyLevel(Enum):
    """Consistency levels for distributed cache operations"""
    STRONG = "strong"          # Linearizable consistency
    EVENTUAL = "eventual"      # Eventual consistency
    WEAK = "weak"              # Weak consistency (no guarantees)
    CAUSAL = "causal"          # Causal consistency

class CacheStrategy(Enum):
    """Cache population/invalidation strategies"""
    CACHE_ASIDE = "cache_aside"
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"
    READ_THROUGH = "read_through"

@dataclass
class VersionVector:
    """Version vector for conflict resolution in distributed systems"""
    vector: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, node_id: str) -> 'VersionVector':
        """Increment version for a specific node"""
        new_vector = VersionVector(self.vector.copy())
        new_vector.vector[node_id] = new_vector.vector.get(node_id, 0) + 1
        return new_vector
    
    def merge(self, other: 'VersionVector') -> 'VersionVector':
        """Merge two version vectors (take maximum of each component)"""
        merged = VersionVector(self.vector.copy())
        for node_id, version in other.vector.items():
            merged.vector[node_id] = max(merged.vector.get(node_id, 0), version)
        return merged
    
    def dominates(self, other: 'VersionVector') -> bool:
        """Check if this version vector dominates another"""
        for node_id, version in self.vector.items():
            if version < other.vector.get(node_id, 0):
                return False
        return True
    
    def concurrent(self, other: 'VersionVector') -> bool:
        """Check if two version vectors are concurrent (conflicting)"""
        return not self.dominates(other) and not other.dominates(self)
    
    def to_json(self) -> str:
        return json.dumps(self.vector)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'VersionVector':
        return cls(json.loads(json_str))

@dataclass
class CacheEntry:
    """Cache entry with metadata for distributed consistency"""
    key: str
    value: Any
    version: VersionVector
    timestamp: float
    ttl: Optional[float] = None
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    dirty: bool = False
    node_id: str = ""
    
    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return time.time() > self.timestamp + self.ttl
    
    def touch(self):
        """Update access metadata"""
        self.last_accessed = time.time()
        self.access_count += 1

class InvalidationMessage:
    """Message for cache invalidation propagation"""
    def __init__(self, key: str, operation: str, version: VersionVector, 
                 source_node: str, timestamp: float = None):
        self.key = key
        self.operation = operation  # 'invalidate', 'update', 'delete'
        self.version = version
        self.source_node = source_node
        self.timestamp = timestamp or time.time()
        self.message_id = str(uuid.uuid4())
    
    def to_dict(self) -> Dict:
        return {
            'key': self.key,
            'operation': self.operation,
            'version': self.version.to_json(),
            'source_node': self.source_node,
            'timestamp': self.timestamp,
            'message_id': self.message_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'InvalidationMessage':
        return cls(
            key=data['key'],
            operation=data['operation'],
            version=VersionVector.from_json(data['version']),
            source_node=data['source_node'],
            timestamp=data['timestamp']
        )

class DistributedLRUCache:
    """LRU cache with distributed consistency support"""
    
    def __init__(self, capacity: int = 1000, node_id: str = None):
        self.capacity = capacity
        self.node_id = node_id or str(uuid.uuid4())
        self.cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self.lock = threading.RLock()
        self.version_vectors: Dict[str, VersionVector] = {}
        
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache with LRU update"""
        with self.lock:
            if key not in self.cache:
                return None
            
            entry = self.cache[key]
            if entry.is_expired():
                del self.cache[key]
                return None
            
            # Move to end (most recently used)
            self.cache.move_to_end(key)
            entry.touch()
            return entry.value
    
    def put(self, key: str, value: Any, ttl: Optional[float] = None, 
            version: VersionVector = None) -> bool:
        """Put value in cache with version tracking"""
        with self.lock:
            if version is None:
                version = self._get_next_version(key)
            
            if key in self.cache:
                # Update existing entry
                entry = self.cache[key]
                if version.concurrent(entry.version):
                    # Conflict detected - need resolution
                    resolved = self._resolve_conflict(entry, value, version)
                    if not resolved:
                        return False
                
                entry.value = value
                entry.version = version.merge(entry.version)
                entry.timestamp = time.time()
                entry.ttl = ttl
                entry.dirty = True
                self.cache.move_to_end(key)
            else:
                # Add new entry
                if len(self.cache) >= self.capacity:
                    # Evict least recently used
                    self.cache.popitem(last=False)
                
                entry = CacheEntry(
                    key=key,
                    value=value,
                    version=version,
                    timestamp=time.time(),
                    ttl=ttl,
                    node_id=self.node_id,
                    dirty=True
                )
                self.cache[key] = entry
            
            self.version_vectors[key] = version
            return True
    
    def delete(self, key: str) -> bool:
        """Delete key from cache"""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                if key in self.version_vectors:
                    del self.version_vectors[key]
                return True
            return False
    
    def _get_next_version(self, key: str) -> VersionVector:
        """Get next version vector for a key"""
        current = self.version_vectors.get(key, VersionVector())
        return current.increment(self.node_id)
    
    def _resolve_conflict(self, existing: CacheEntry, new_value: Any, 
                         new_version: VersionVector) -> bool:
        """Resolve conflict between concurrent versions"""
        # Simple last-writer-wins based on timestamp
        # In production, you might want more sophisticated conflict resolution
        if new_version.concurrent(existing.version):
            # Use timestamp as tie-breaker
            if new_version.vector.get(self.node_id, 0) > existing.version.vector.get(existing.node_id, 0):
                return True
        return False
    
    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self.lock:
            return {
                'size': len(self.cache),
                'capacity': self.capacity,
                'hit_rate': self._calculate_hit_rate(),
                'node_id': self.node_id
            }
    
    def _calculate_hit_rate(self) -> float:
        """Calculate cache hit rate (simplified)"""
        # In production, you'd track hits/misses
        return 0.0

class RedisDistributedCache:
    """Redis-based distributed cache with consistency guarantees"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379", 
                 node_id: str = None, 
                 consistency: ConsistencyLevel = ConsistencyLevel.EVENTUAL):
        self.redis_url = redis_url
        self.node_id = node_id or str(uuid.uuid4())
        self.consistency = consistency
        self.redis = None
        self.pubsub = None
        self.local_cache = DistributedLRUCache(capacity=1000, node_id=self.node_id)
        self.invalidation_channel = f"cache:invalidation:{self.node_id}"
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.running = False
        self.message_queue = asyncio.Queue()
        
        # Statistics
        self.stats = {
            'hits': 0,
            'misses': 0,
            'invalidations_received': 0,
            'invalidations_sent': 0,
            'conflicts_resolved': 0
        }
    
    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis = await aioredis.from_url(self.redis_url)
            self.pubsub = self.redis.pubsub()
            await self.pubsub.subscribe(self.invalidation_channel)
            self.running = True
            
            # Start background tasks
            asyncio.create_task(self._process_invalidation_messages())
            asyncio.create_task(self._periodic_sync())
            
            logger.info(f"Connected to Redis as node {self.node_id}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Redis"""
        self.running = False
        if self.pubsub:
            await self.pubsub.unsubscribe(self.invalidation_channel)
        if self.redis:
            await self.redis.close()
    
    async def get(self, key: str, strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE) -> Optional[Any]:
        """Get value with specified cache strategy"""
        # Try local cache first
        value = self.local_cache.get(key)
        if value is not None:
            self.stats['hits'] += 1
            return value
        
        self.stats['misses'] += 1
        
        # Fetch from Redis
        try:
            redis_value = await self.redis.get(f"cache:{key}")
            if redis_value is None:
                return None
            
            data = json.loads(redis_value)
            value = data['value']
            version = VersionVector.from_json(data['version'])
            
            # Store in local cache
            self.local_cache.put(key, value, version=version)
            
            return value
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            return None
    
    async def put(self, key: str, value: Any, ttl: Optional[int] = None,
                  strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE,
                  consistency: ConsistencyLevel = None) -> bool:
        """Put value with specified cache strategy and consistency"""
        consistency = consistency or self.consistency
        
        try:
            if strategy == CacheStrategy.CACHE_ASIDE:
                return await self._put_cache_aside(key, value, ttl, consistency)
            elif strategy == CacheStrategy.WRITE_THROUGH:
                return await self._put_write_through(key, value, ttl, consistency)
            elif strategy == CacheStrategy.WRITE_BEHIND:
                return await self._put_write_behind(key, value, ttl, consistency)
            else:
                return await self._put_cache_aside(key, value, ttl, consistency)
        except Exception as e:
            logger.error(f"Error putting key {key}: {e}")
            return False
    
    async def _put_cache_aside(self, key: str, value: Any, ttl: Optional[int],
                              consistency: ConsistencyLevel) -> bool:
        """Cache-aside pattern implementation"""
        # Update local cache first
        local_version = self.local_cache._get_next_version(key)
        self.local_cache.put(key, value, ttl, local_version)
        
        # Update Redis
        data = {
            'value': value,
            'version': local_version.to_json(),
            'timestamp': time.time(),
            'node_id': self.node_id
        }
        
        if ttl:
            await self.redis.setex(f"cache:{key}", ttl, json.dumps(data))
        else:
            await self.redis.set(f"cache:{key}", json.dumps(data))
        
        # Propagate invalidation for strong consistency
        if consistency == ConsistencyLevel.STRONG:
            await self._propagate_invalidation(key, 'update', local_version)
        
        return True
    
    async def _put_write_through(self, key: str, value: Any, ttl: Optional[int],
                                consistency: ConsistencyLevel) -> bool:
        """Write-through pattern implementation"""
        # Update Redis first
        local_version = self.local_cache._get_next_version(key)
        data = {
            'value': value,
            'version': local_version.to_json(),
            'timestamp': time.time(),
            'node_id': self.node_id
        }
        
        if ttl:
            await self.redis.setex(f"cache:{key}", ttl, json.dumps(data))
        else:
            await self.redis.set(f"cache:{key}", json.dumps(data))
        
        # Then update local cache
        self.local_cache.put(key, value, ttl, local_version)
        
        # Propagate invalidation
        await self._propagate_invalidation(key, 'update', local_version)
        
        return True
    
    async def _put_write_behind(self, key: str, value: Any, ttl: Optional[int],
                               consistency: ConsistencyLevel) -> bool:
        """Write-behind (write-back) pattern implementation"""
        # Update local cache immediately
        local_version = self.local_cache._get_next_version(key)
        self.local_cache.put(key, value, ttl, local_version)
        
        # Queue async update to Redis
        asyncio.create_task(self._async_update_redis(key, value, ttl, local_version))
        
        return True
    
    async def _async_update_redis(self, key: str, value: Any, ttl: Optional[int],
                                 version: VersionVector):
        """Asynchronously update Redis"""
        try:
            data = {
                'value': value,
                'version': version.to_json(),
                'timestamp': time.time(),
                'node_id': self.node_id
            }
            
            if ttl:
                await self.redis.setex(f"cache:{key}", ttl, json.dumps(data))
            else:
                await self.redis.set(f"cache:{key}", json.dumps(data))
            
            # Propagate invalidation
            await self._propagate_invalidation(key, 'update', version)
        except Exception as e:
            logger.error(f"Async Redis update failed for key {key}: {e}")
    
    async def delete(self, key: str, strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE) -> bool:
        """Delete key from cache"""
        try:
            # Delete from local cache
            self.local_cache.delete(key)
            
            # Delete from Redis
            await self.redis.delete(f"cache:{key}")
            
            # Propagate invalidation
            version = self.local_cache._get_next_version(key)
            await self._propagate_invalidation(key, 'delete', version)
            
            return True
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            return False
    
    async def _propagate_invalidation(self, key: str, operation: str, 
                                     version: VersionVector):
        """Propagate cache invalidation to other nodes"""
        message = InvalidationMessage(
            key=key,
            operation=operation,
            version=version,
            source_node=self.node_id
        )
        
        try:
            # Publish to invalidation channel
            await self.redis.publish(
                "cache:invalidations",
                json.dumps(message.to_dict())
            )
            self.stats['invalidations_sent'] += 1
        except Exception as e:
            logger.error(f"Failed to propagate invalidation: {e}")
    
    async def _process_invalidation_messages(self):
        """Process incoming invalidation messages"""
        while self.running:
            try:
                message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    data = json.loads(message['data'])
                    invalidation = InvalidationMessage.from_dict(data)
                    
                    # Don't process our own messages
                    if invalidation.source_node != self.node_id:
                        await self._handle_invalidation(invalidation)
                        self.stats['invalidations_received'] += 1
            except Exception as e:
                logger.error(f"Error processing invalidation: {e}")
                await asyncio.sleep(0.1)
    
    async def _handle_invalidation(self, message: InvalidationMessage):
        """Handle incoming invalidation message"""
        local_entry = self.local_cache.cache.get(message.key)
        
        if local_entry:
            if message.operation == 'invalidate' or message.operation == 'delete':
                # Check version to avoid unnecessary invalidations
                if message.version.dominates(local_entry.version):
                    self.local_cache.delete(message.key)
                    logger.debug(f"Invalidated key {message.key} from node {message.source_node}")
            
            elif message.operation == 'update':
                # Fetch updated value from Redis
                try:
                    redis_value = await self.redis.get(f"cache:{message.key}")
                    if redis_value:
                        data = json.loads(redis_value)
                        new_version = VersionVector.from_json(data['version'])
                        
                        # Update local cache if version is newer
                        if new_version.dominates(local_entry.version):
                            self.local_cache.put(
                                message.key,
                                data['value'],
                                version=new_version
                            )
                            logger.debug(f"Updated key {message.key} from node {message.source_node}")
                except Exception as e:
                    logger.error(f"Failed to update from invalidation: {e}")
    
    async def _periodic_sync(self):
        """Periodically sync with Redis for eventual consistency"""
        while self.running:
            try:
                await asyncio.sleep(30)  # Sync every 30 seconds
                
                # Sync version vectors
                for key in list(self.local_cache.cache.keys()):
                    try:
                        redis_value = await self.redis.get(f"cache:{key}")
                        if redis_value:
                            data = json.loads(redis_value)
                            remote_version = VersionVector.from_json(data['version'])
                            local_entry = self.local_cache.cache.get(key)
                            
                            if local_entry and remote_version.concurrent(local_entry.version):
                                # Conflict detected - resolve
                                self.stats['conflicts_resolved'] += 1
                                # Simple resolution: take the one with higher timestamp
                                if data['timestamp'] > local_entry.timestamp:
                                    self.local_cache.put(
                                        key,
                                        data['value'],
                                        version=remote_version
                                    )
                    except Exception as e:
                        logger.debug(f"Sync failed for key {key}: {e}")
                        continue
                        
            except Exception as e:
                logger.error(f"Periodic sync error: {e}")
    
    async def warm_cache(self, keys: List[str], fetch_func: Callable[[str], Any]):
        """Warm cache by preloading frequently accessed keys"""
        logger.info(f"Warming cache for {len(keys)} keys")
        
        for key in keys:
            try:
                value = await fetch_func(key)
                if value is not None:
                    await self.put(key, value, strategy=CacheStrategy.CACHE_ASIDE)
            except Exception as e:
                logger.error(f"Failed to warm cache for key {key}: {e}")
    
    async def get_stats(self) -> Dict:
        """Get comprehensive cache statistics"""
        local_stats = self.local_cache.get_stats()
        
        return {
            **local_stats,
            **self.stats,
            'hit_rate': self.stats['hits'] / max(1, self.stats['hits'] + self.stats['misses']),
            'consistency_level': self.consistency.value,
            'node_id': self.node_id
        }

class CacheBenchmark:
    """Benchmark different cache consistency models"""
    
    def __init__(self, num_nodes: int = 3, num_operations: int = 10000):
        self.num_nodes = num_nodes
        self.num_operations = num_operations
        self.caches: List[RedisDistributedCache] = []
        self.results: Dict[str, List[float]] = defaultdict(list)
    
    async def setup(self):
        """Setup benchmark environment"""
        # Create multiple cache nodes
        for i in range(self.num_nodes):
            cache = RedisDistributedCache(
                redis_url="redis://localhost:6379",
                node_id=f"node_{i}",
                consistency=ConsistencyLevel.EVENTUAL
            )
            await cache.connect()
            self.caches.append(cache)
    
    async def teardown(self):
        """Cleanup benchmark environment"""
        for cache in self.caches:
            await cache.disconnect()
    
    async def run_benchmark(self, consistency_level: ConsistencyLevel):
        """Run benchmark for specific consistency level"""
        logger.info(f"Running benchmark with {consistency_level.value} consistency")
        
        # Update consistency for all caches
        for cache in self.caches:
            cache.consistency = consistency_level
        
        latencies = []
        conflicts = 0
        
        # Run concurrent operations
        tasks = []
        for i in range(self.num_operations):
            node_idx = i % self.num_nodes
            key = f"key_{i % 100}"  # 100 unique keys
            value = f"value_{i}"
            
            task = self._perform_operation(self.caches[node_idx], key, value)
            tasks.append(task)
        
        # Execute all operations
        start_time = time.time()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        end_time = time.time()
        
        # Collect results
        for result in results:
            if isinstance(result, tuple):
                latency, conflict = result
                latencies.append(latency)
                if conflict:
                    conflicts += 1
        
        # Calculate statistics
        total_time = end_time - start_time
        ops_per_second = self.num_operations / total_time
        avg_latency = statistics.mean(latencies) if latencies else 0
        p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else 0
        p99_latency = statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else 0
        
        result = {
            'consistency_level': consistency_level.value,
            'total_operations': self.num_operations,
            'total_time': total_time,
            'ops_per_second': ops_per_second,
            'avg_latency_ms': avg_latency * 1000,
            'p95_latency_ms': p95_latency * 1000,
            'p99_latency_ms': p99_latency * 1000,
            'conflicts': conflicts,
            'conflict_rate': conflicts / self.num_operations
        }
        
        logger.info(f"Benchmark results: {result}")
        return result
    
    async def _perform_operation(self, cache: RedisDistributedCache, 
                                key: str, value: str) -> Tuple[float, bool]:
        """Perform a single cache operation and measure latency"""
        start = time.time()
        conflict = False
        
        try:
            # Mix of read and write operations
            if hash(key) % 3 == 0:  # 33% writes
                await cache.put(key, value)
            else:  # 67% reads
                await cache.get(key)
        except Exception as e:
            logger.error(f"Operation failed: {e}")
            conflict = True
        
        latency = time.time() - start
        return latency, conflict
    
    async def run_all_benchmarks(self) -> List[Dict]:
        """Run benchmarks for all consistency levels"""
        results = []
        
        for consistency in [ConsistencyLevel.STRONG, ConsistencyLevel.EVENTUAL, 
                           ConsistencyLevel.WEAK, ConsistencyLevel.CAUSAL]:
            result = await self.run_benchmark(consistency)
            results.append(result)
            await asyncio.sleep(1)  # Cool down between benchmarks
        
        return results
    
    def generate_report(self, results: List[Dict]) -> str:
        """Generate benchmark report"""
        report = ["=" * 80]
        report.append("DISTRIBUTED CACHE BENCHMARK REPORT")
        report.append("=" * 80)
        report.append("")
        
        for result in results:
            report.append(f"Consistency Level: {result['consistency_level'].upper()}")
            report.append("-" * 40)
            report.append(f"Operations/second: {result['ops_per_second']:.2f}")
            report.append(f"Average latency: {result['avg_latency_ms']:.2f} ms")
            report.append(f"P95 latency: {result['p95_latency_ms']:.2f} ms")
            report.append(f"P99 latency: {result['p99_latency_ms']:.2f} ms")
            report.append(f"Conflict rate: {result['conflict_rate']:.2%}")
            report.append("")
        
        # Add comparison
        report.append("COMPARISON")
        report.append("-" * 40)
        report.append(f"{'Consistency':<15} {'Ops/sec':<12} {'Avg Latency':<15} {'Conflict Rate':<15}")
        report.append("-" * 60)
        
        for result in results:
            report.append(
                f"{result['consistency_level']:<15} "
                f"{result['ops_per_second']:<12.2f} "
                f"{result['avg_latency_ms']:<15.2f} "
                f"{result['conflict_rate']:<15.2%}"
            )
        
        return "\n".join(report)

class CacheManager:
    """High-level cache manager with strategy selection"""
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.caches: Dict[str, RedisDistributedCache] = {}
        self.default_consistency = ConsistencyLevel.EVENTUAL
        self.default_strategy = CacheStrategy.CACHE_ASIDE
    
    async def get_cache(self, name: str, consistency: ConsistencyLevel = None) -> RedisDistributedCache:
        """Get or create a named cache instance"""
        if name not in self.caches:
            cache = RedisDistributedCache(
                redis_url=self.redis_url,
                node_id=name,
                consistency=consistency or self.default_consistency
            )
            await cache.connect()
            self.caches[name] = cache
        
        return self.caches[name]
    
    async def get(self, cache_name: str, key: str, 
                  strategy: CacheStrategy = None) -> Optional[Any]:
        """Get value from named cache"""
        cache = await self.get_cache(cache_name)
        return await cache.get(key, strategy or self.default_strategy)
    
    async def put(self, cache_name: str, key: str, value: Any, 
                  ttl: Optional[int] = None,
                  strategy: CacheStrategy = None,
                  consistency: ConsistencyLevel = None) -> bool:
        """Put value to named cache"""
        cache = await self.get_cache(cache_name)
        return await cache.put(key, value, ttl, strategy or self.default_strategy, consistency)
    
    async def delete(self, cache_name: str, key: str,
                    strategy: CacheStrategy = None) -> bool:
        """Delete key from named cache"""
        cache = await self.get_cache(cache_name)
        return await cache.delete(key, strategy or self.default_strategy)
    
    async def close_all(self):
        """Close all cache connections"""
        for cache in self.caches.values():
            await cache.disconnect()
        self.caches.clear()

# Example usage and testing
async def example_usage():
    """Example demonstrating the distributed cache usage"""
    
    # Create cache manager
    manager = CacheManager(redis_url="redis://localhost:6379")
    
    try:
        # Get cache instances with different consistency levels
        strong_cache = await manager.get_cache("strong_cache", ConsistencyLevel.STRONG)
        eventual_cache = await manager.get_cache("eventual_cache", ConsistencyLevel.EVENTUAL)
        
        # Cache-aside pattern
        print("Testing cache-aside pattern...")
        await strong_cache.put("user:123", {"name": "John", "age": 30}, 
                              strategy=CacheStrategy.CACHE_ASIDE)
        user = await strong_cache.get("user:123")
        print(f"Retrieved user: {user}")
        
        # Write-through pattern
        print("\nTesting write-through pattern...")
        await eventual_cache.put("product:456", {"name": "Laptop", "price": 999.99},
                                strategy=CacheStrategy.WRITE_THROUGH)
        
        # Eventual consistency
        print("\nTesting eventual consistency...")
        await eventual_cache.put("config:global", {"setting": "value"}, 
                                consistency=ConsistencyLevel.EVENTUAL)
        
        # Cache warming
        print("\nWarming cache...")
        keys_to_warm = [f"popular_item:{i}" for i in range(10)]
        await eventual_cache.warm_cache(keys_to_warm, 
                                       lambda key: f"preloaded_value_{key}")
        
        # Get statistics
        stats = await strong_cache.get_stats()
        print(f"\nCache statistics: {stats}")
        
        # Run benchmark
        print("\nRunning benchmarks...")
        benchmark = CacheBenchmark(num_nodes=2, num_operations=1000)
        await benchmark.setup()
        results = await benchmark.run_all_benchmarks()
        report = benchmark.generate_report(results)
        print(report)
        await benchmark.teardown()
        
    finally:
        await manager.close_all()

if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())