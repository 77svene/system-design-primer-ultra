"""
Distributed Query Cache with Consistency Guarantees

This module implements a distributed query cache system with:
- Consistent hashing for even distribution across nodes
- Multiple cache invalidation protocols (write-through, write-behind)
- Version vectors for consistency management
- Cache warming strategies
- Network partition handling and recovery

Author: System Design Primer Contributors
"""

import hashlib
import json
import logging
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from concurrent.futures import ThreadPoolExecutor
import random

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CacheProtocol(Enum):
    """Cache invalidation protocols"""
    WRITE_THROUGH = auto()
    WRITE_BEHIND = auto()
    READ_THROUGH = auto()


class ConsistencyLevel(Enum):
    """Consistency levels for distributed systems"""
    STRONG = auto()
    EVENTUAL = auto()
    CAUSAL = auto()


class PartitionState(Enum):
    """Network partition states"""
    HEALTHY = auto()
    PARTITIONED = auto()
    RECOVERING = auto()


@dataclass
class VersionVector:
    """
    Version vector for multi-version consistency tracking.
    Each node has a version counter.
    """
    node_id: str
    version: int = 0
    
    def increment(self) -> 'VersionVector':
        self.version += 1
        return self
    
    def merge(self, other: 'VersionVector') -> 'VersionVector':
        """Merge two version vectors, taking the maximum version for each node"""
        if other.node_id == self.node_id:
            self.version = max(self.version, other.version)
        return self
    
    def is_causally_ordered(self, other: 'VersionVector') -> bool:
        """Check if this version is causally after the other"""
        return self.version >= other.version


@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    key: str
    value: Any
    version: VersionVector
    timestamp: float = field(default_factory=time.time)
    ttl_seconds: int = 300  # 5 minutes default
    access_count: int = 0
    hit_count: int = 0
    
    def is_expired(self) -> bool:
        return time.time() > self.timestamp + self.ttl_seconds
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'key': self.key,
            'value': self.value,
            'version': self.version.node_id,
            'timestamp': self.timestamp,
            'ttl_seconds': self.ttl_seconds
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CacheEntry':
        return cls(
            key=data['key'],
            value=data['value'],
            version=VersionVector(node_id=data['version']),
            timestamp=data.get('timestamp', time.time()),
            ttl_seconds=data.get('ttl_seconds', 300)
        )


class ConsistentHashRing:
    """
    Consistent hashing ring for distributed cache partitioning.
    """
    
    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self.nodes = nodes
        self.virtual_nodes = virtual_nodes
        self._ring = self._build_ring()
        self._lock = threading.RLock()
    
    def _build_ring(self) -> Dict[str, str]:
        """Build the hash ring with virtual nodes"""
        ring = {}
        for node in self.nodes:
            for i in range(self.virtual_nodes):
                key = f"{node}-{i}"
                hash_val = self._hash(key) % (2 ** 32)
                ring[hash_val] = node
        return ring
    
    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2 ** 32)
    
    def get_node(self, key: str) -> Optional[str]:
        """Get the node responsible for a given key"""
        if not self._ring:
            return None
        
        with self._lock:
            hash_val = self._hash(key)
            
            # Find the first node with hash >= key hash
            candidates = [
                node for node_hash, node in self._ring.items()
                if node_hash >= hash_val
            ]
            
            if candidates:
                return candidates[0]
            else:
                # Wrap around to first node
                return next(iter(self._ring.values()))
    
    def get_nodes(self, key: str) -> List[str]:
        """Get multiple nodes for a key (for replication)"""
        if not self._ring:
            return []
        
        with self._lock:
            hash_val = self._hash(key)
            candidates = [
                node for node_hash, node in self._ring.items()
                if node_hash >= hash_val
            ]
            
            if candidates:
                return candidates[:3]  # Return up to 3 nodes
            return list(self._ring.values())[:3]
    
    def add_node(self, node: str) -> bool:
        """Add a node to the ring"""
        with self._lock:
            for i in range(self.virtual_nodes):
                key = f"{node}-{i}"
                hash_val = self._hash(key)
                self._ring[hash_val] = node
            return True
    
    def remove_node(self, node: str) -> bool:
        """Remove a node from the ring"""
        with self._lock:
            keys_to_remove = [
                key for key in self._ring.keys()
                if key.startswith(f"{node}-")
            ]
            
            for key in keys_to_remove:
                del self._ring[key]
            
            return True
    
    def get_replica_nodes(self, key: str, num_replicas: int = 2) -> List[str]:
        """Get replica nodes for a key"""
        nodes = self.get_nodes(key)
        return nodes[:num_replicas]


class CachePartition:
    """
    Handles network partition detection and recovery.
    """
    
    def __init__(self, node_id: str, partition_state: PartitionState = PartitionState.HEALTHY):
        self.node_id = node_id
        self.partition_state = partition_state
        self.partition_time: Optional[float] = None
        self._lock = threading.RLock()
        self._partition_timeout = 30  # seconds
    
    def detect_partition(self) -> bool:
        """Detect if partition has occurred"""
        with self._lock:
            if self.partition_state == PartitionState.HEALTHY:
                self.partition_state = PartitionState.PARTITIONED
                self.partition_time = time.time()
                return True
            return False
    
    def is_partitioned(self) -> bool:
        return self.partition_state == PartitionState.PARTITIONED
    
    def recover(self) -> bool:
        """Attempt to recover from partition"""
        with self._lock:
            if self.partition_time:
                elapsed = time.time() - self.partition_time
                if elapsed > self._partition_timeout:
                    self.partition_state = PartitionState.HEALTHY
                    self.partition_time = None
                    return True
                elif elapsed > self._partition_timeout / 2:
                    self.partition_state = PartitionState.RECOVERING
                    return True
            return False
    
    def get_state(self) -> PartitionState:
        return self.partition_state


class DistributedQueryCache:
    """
    Main distributed query cache implementation with consistency guarantees.
    """
    
    def __init__(
        self,
        nodes: List[str],
        cache_protocol: CacheProtocol = CacheProtocol.WRITE_THROUGH,
        consistency_level: ConsistencyLevel = ConsistencyLevel.EVENTUAL,
        replication_factor: int = 2,
        default_ttl: int = 300,
        partition_timeout: int = 30
    ):
        self.nodes = nodes
        self.hash_ring = ConsistentHashRing(nodes)
        self.cache_protocol = cache_protocol
        self.consistency_level = consistency_level
        self.replication_factor = replication_factor
        self.default_ttl = default_ttl
        self.partition_timeout = partition_timeout
        
        # Local cache storage
        self._cache: Dict[str, Dict[str, CacheEntry]] = defaultdict(dict)
        self._version_vectors: Dict[str, VersionVector] = {}
        
        # Partition handling
        self._partitions: Dict[str, CachePartition] = {
            node: CachePartition(node) for node in nodes
        }
        
        # Write-behind queue
        self._write_queue: List[Tuple[str, Any, str]] = []
        self._write_lock = threading.Lock()
        
        # Cache warming
        self._warming_tasks: Dict[str, threading.Thread] = {}
        self._warming_lock = threading.Lock()
        
        # Metrics
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0,
            'partitions_detected': 0,
            'partitions_recovered': 0
        }
        
        # Start background threads
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """Start background tasks for cache maintenance"""
        self._executor.submit(self._process_write_queue)
        self._executor.submit(self._check_partitions)
        self._executor.submit(self._cleanup_expired_entries)
    
    def get(self, key: str, node_id: Optional[str] = None) -> Optional[Any]:
        """
        Get a value from cache with consistency checks.
        """
        if node_id is None:
            node_id = self.hash_ring.get_node(key)
        
        if self._partitions.get(node_id, CachePartition(node_id)).is_partitioned():
            logger.warning(f"Node {node_id} is partitioned, returning cached value if available")
        
        # Check local cache first
        if key in self._cache[node_id]:
            entry = self._cache[node_id][key]
            if not entry.is_expired():
                entry.hit_count += 1
                self._stats['hits'] += 1
                return entry.value
        
        # Check replicas for consistency
        replica_nodes = self.hash_ring.get_replica_nodes(key, self.replication_factor)
        for replica_node in replica_nodes:
            if replica_node == node_id:
                continue
            
            if replica_node in self._cache and key in self._cache[replica_node]:
                replica_entry = self._cache[replica_node][key]
                if not replica_entry.is_expired():
                    # Version vector check for consistency
                    if self._check_version_consistency(replica_entry, node_id):
                        self._cache[node_id][key] = replica_entry
                        self._stats['hits'] += 1
                        return replica_entry.value
        
        self._stats['misses'] += 1
        return None
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        node_id: Optional[str] = None
    ) -> bool:
        """
        Set a value in cache with appropriate invalidation protocol.
        """
        if node_id is None:
            node_id = self.hash_ring.get_node(key)
        
        if self._partitions.get(node_id, CachePartition(node_id)).is_partitioned():
            logger.warning(f"Cannot write to partitioned node {node_id}")
            return False
        
        ttl = ttl_seconds or self.default_ttl
        entry = CacheEntry(
            key=key,
            value=value,
            version=VersionVector(node_id=node_id),
            timestamp=time.time(),
            ttl_seconds=ttl
        )
        
        # Handle write protocol
        if self.cache_protocol == CacheProtocol.WRITE_THROUGH:
            return self._write_through(key, entry)
        elif self.cache_protocol == CacheProtocol.WRITE_BEHIND:
            return self._write_behind(key, entry)
        elif self.cache_protocol == CacheProtocol.READ_THROUGH:
            return self._read_through(key, entry)
        
        return False
    
    def _write_through(self, key: str, entry: CacheEntry) -> bool:
        """Write-through: write to all replicas immediately"""
        try:
            self._cache[key][key] = entry
            self._increment_version(key)
            
            # Write to replicas
            for replica_node in self.hash_ring.get_replica_nodes(key):
                replica_entry = CacheEntry(
                    key=key,
                    value=entry.value,
                    version=VersionVector(node_id=replica_node),
                    timestamp=time.time(),
                    ttl_seconds=entry.ttl_seconds
                )
                if replica_node in self._cache:
                    self._cache[replica_node][key] = replica_entry
            
            self._stats['evictions'] += 1
            return True
        except Exception as e:
            logger.error(f"Write-through failed for key {key}: {e}")
            return False
    
    def _write_behind(self, key: str, entry: CacheEntry) -> bool:
        """Write-behind: add to queue, write asynchronously"""
        with self._write_lock:
            self._write_queue.append((key, entry.value, time.time()))
        
        # Write locally immediately
        self._cache[key][key] = entry
        self._increment_version(key)
        
        # Submit async write to replicas
        self._executor.submit(self._async_replicate, key)
        
        return True
    
    def _async_replicate(self, key: str):
        """Async replication for write-behind"""
        # Find the entry in queue
        for queue_item in self._write_queue:
            if queue_item[0] == key:
                self._replicate_to_replicas(key, queue_item[1])
                break
    
    def _replicate_to_replicas(self, key: str, value: Any):
        """Replicate value to all replica nodes"""
        replica_nodes = self.hash_ring.get_replica_nodes(key, self.replication_factor)
        
        for replica_node in replica_nodes:
            try:
                replica_entry = CacheEntry(
                    key=key,
                    value=value,
                    version=VersionVector(node_id=replica_node),
                    timestamp=time.time(),
                    ttl_seconds=self.default_ttl
                )
                if replica_node in self._cache:
                    self._cache[replica_node][key] = replica_entry
            except Exception as e:
                logger.error(f"Replication failed for key {key} to node {replica_node}: {e}")
    
    def _read_through(self, key: str, entry: CacheEntry) -> bool:
        """Read-through: fetch from underlying source on miss"""
        # This is a simplified implementation
        # In production, you would fetch from database or external source
        source_value = self._fetch_from_source(key)
        
        if source_value is not None:
            entry.value = source_value
            return self.set(key, source_value, node_id=self.hash_ring.get_node(key))
        
        return False
    
    def _fetch_from_source(self, key: str) -> Optional[Any]:
        """Fetch from underlying data source (stub)"""
        # In production, this would be a database or external API
        logger.info(f"Fetching from source for key: {key}")
        return None
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache and replicas"""
        node_id = self.hash_ring.get_node(key)
        
        if key in self._cache.get(node_id, {}):
            del self._cache[node_id][key]
        
        # Delete from replicas
        for replica_node in self.hash_ring.get_replica_nodes(key):
            if replica_node in self._cache and key in self._cache[replica_node]:
                del self._cache[replica_node][key]
        
        return True
    
    def _increment_version(self, key: str):
        """Increment version for a key"""
        if key not in self._version_vectors:
            self._version_vectors[key] = VersionVector(node_id=self.hash_ring.get_node(key))
        
        self._version_vectors[key].increment()
    
    def _check_version_consistency(self, entry: CacheEntry, node_id: str) -> bool:
        """Check if entry version is consistent with node's version"""
        if node_id not in self._version_vectors:
            self._version_vectors[node_id] = VersionVector(node_id=node_id)
        
        # Simple version comparison
        return entry.version.version >= self._version_vectors.get(node_id, VersionVector(node_id=node_id)).version
    
    def _process_write_queue(self):
        """Process write-behind queue asynchronously"""
        while True:
            try:
                time