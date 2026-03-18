import threading
import time
from typing import Any, Callable, Optional, Dict


class Node:
    """Doubly-linked list node with key, value, and expiration tracking."""
    __slots__ = ('key', 'value', 'prev', 'next', 'expire_at')
    
    def __init__(self, key: Any, value: Any, ttl: float = 0):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None
        self.expire_at = time.time() + ttl if ttl > 0 else 0


class DoublyLinkedList:
    """Doubly-linked list with sentinel nodes for O(1) operations."""
    __slots__ = ('head', 'tail', 'size')
    
    def __init__(self):
        # Sentinel nodes to eliminate edge cases
        self.head = Node(None, None)
        self.tail = Node(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self.size = 0
    
    def add_to_front(self, node: Node) -> None:
        """Add node right after head sentinel."""
        node.prev = self.head
        node.next = self.head.next
        self.head.next.prev = node
        self.head.next = node
        self.size += 1
    
    def remove(self, node: Node) -> None:
        """Remove node from list."""
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = node.next = None
        self.size -= 1
    
    def move_to_front(self, node: Node) -> None:
        """Move existing node to front (most recently used)."""
        self.remove(node)
        self.add_to_front(node)
    
    def remove_from_tail(self) -> Optional[Node]:
        """Remove and return the node before tail sentinel (least recently used)."""
        if self.size == 0:
            return None
        node = self.tail.prev
        self.remove(node)
        return node
    
    def peek_tail(self) -> Optional[Node]:
        """Return tail node without removing it."""
        return self.tail.prev if self.size > 0 else None


class LRUCache:
    """
    Thread-safe LRU Cache with O(1) operations, TTL support, and eviction callbacks.
    
    Features:
    - O(1) get/put operations using hash map + doubly-linked list
    - Thread safety with reentrant locks
    - Time-to-live (TTL) support for automatic expiration
    - Eviction callbacks for custom cleanup logic
    - Memory-efficient with __slots__ and sentinel nodes
    """
    
    __slots__ = ('_capacity', '_ttl', '_lock', '_map', '_list', 
                 '_eviction_callback', '_hits', '_misses')
    
    def __init__(self, 
                 capacity: int, 
                 ttl: float = 0,
                 eviction_callback: Optional[Callable[[Any, Any], None]] = None):
        """
        Initialize LRU Cache.
        
        Args:
            capacity: Maximum number of items to store
            ttl: Time-to-live in seconds (0 = no expiration)
            eviction_callback: Function to call when items are evicted
        """
        if capacity <= 0:
            raise ValueError("Capacity must be positive")
        
        self._capacity = capacity
        self._ttl = ttl
        self._lock = threading.RLock()
        self._map: Dict[Any, Node] = {}
        self._list = DoublyLinkedList()
        self._eviction_callback = eviction_callback
        self._hits = 0
        self._misses = 0
    
    def get(self, key: Any) -> Optional[Any]:
        """
        Get value for key, returning None if not found or expired.
        
        Time complexity: O(1)
        """
        with self._lock:
            node = self._map.get(key)
            
            if node is None:
                self._misses += 1
                return None
            
            # Check if node has expired
            if self._is_expired(node):
                self._remove_node(node)
                self._misses += 1
                return None
            
            # Move to front (mark as recently used)
            self._list.move_to_front(node)
            self._hits += 1
            return node.value
    
    def put(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:
        """
        Insert or update key-value pair.
        
        Time complexity: O(1)
        
        Args:
            key: Key to store
            value: Value to store
            ttl: Optional TTL override for this specific item
        """
        with self._lock:
            node = self._map.get(key)
            
            if node is not None:
                # Update existing node
                node.value = value
                node.expire_at = time.time() + (ttl if ttl is not None else self._ttl)
                self._list.move_to_front(node)
            else:
                # Evict if at capacity
                if len(self._map) >= self._capacity:
                    self._evict_one()
                
                # Create and insert new node
                effective_ttl = ttl if ttl is not None else self._ttl
                new_node = Node(key, value, effective_ttl)
                self._map[key] = new_node
                self._list.add_to_front(new_node)
    
    def delete(self, key: Any) -> bool:
        """
        Delete key from cache.
        
        Time complexity: O(1)
        
        Returns:
            True if key was found and deleted, False otherwise
        """
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return False
            
            self._remove_node(node)
            return True
    
    def clear(self) -> None:
        """Clear all items from cache."""
        with self._lock:
            self._map.clear()
            # Reset linked list
            self._list = DoublyLinkedList()
    
    def contains(self, key: Any) -> bool:
        """Check if key exists and is not expired."""
        with self._lock:
            node = self._map.get(key)
            if node is None:
                return False
            if self._is_expired(node):
                self._remove_node(node)
                return False
            return True
    
    def _evict_one(self) -> None:
        """Evict one item (least recently used or expired)."""
        # First try to evict expired items
        evicted = self._evict_expired()
        if not evicted:
            # Evict LRU item
            lru_node = self._list.remove_from_tail()
            if lru_node:
                del self._map[lru_node.key]
                if self._eviction_callback:
                    self._eviction_callback(lru_node.key, lru_node.value)
    
    def _evict_expired(self) -> bool:
        """Evict all expired items. Returns True if any were evicted."""
        evicted = False
        current_time = time.time()
        
        # Check from tail (oldest items)
        node = self._list.peek_tail()
        while node and node != self._list.head:
            next_node = node.prev  # Move towards head
            if node.expire_at > 0 and node.expire_at <= current_time:
                self._remove_node(node)
                evicted = True
            node = next_node
        
        return evicted
    
    def _remove_node(self, node: Node) -> None:
        """Remove node from both map and list."""
        del self._map[node.key]
        self._list.remove(node)
        if self._eviction_callback:
            self._eviction_callback(node.key, node.value)
    
    def _is_expired(self, node: Node) -> bool:
        """Check if node has expired."""
        return node.expire_at > 0 and node.expire_at <= time.time()
    
    @property
    def size(self) -> int:
        """Current number of items in cache."""
        with self._lock:
            return len(self._map)
    
    @property
    def capacity(self) -> int:
        """Maximum capacity of cache."""
        return self._capacity
    
    @property
    def stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            return {
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
                'size': self.size,
                'capacity': self._capacity
            }
    
    def __len__(self) -> int:
        return self.size
    
    def __contains__(self, key: Any) -> bool:
        return self.contains(key)
    
    def __getitem__(self, key: Any) -> Any:
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value
    
    def __setitem__(self, key: Any, value: Any) -> None:
        self.put(key, value)
    
    def __delitem__(self, key: Any) -> None:
        if not self.delete(key):
            raise KeyError(key)
    
    def __repr__(self) -> str:
        return f"LRUCache(size={self.size}, capacity={self._capacity}, hits={self._hits}, misses={self._misses})"


# Backward compatibility aliases
class Cache(LRUCache):
    """Backward compatible alias for LRUCache."""
    
    def __init__(self, MAX_SIZE: int):
        super().__init__(MAX_SIZE)
    
    def get(self, query: Any) -> Optional[Any]:
        return super().get(query)
    
    def set(self, results: Any, query: Any) -> None:
        super().put(query, results)


# Comprehensive tests
if __name__ == "__main__":
    import unittest
    import concurrent.futures
    import random
    
    class TestLRUCache(unittest.TestCase):
        
        def test_basic_operations(self):
            cache = LRUCache(3)
            
            # Test put and get
            cache.put("a", 1)
            cache.put("b", 2)
            cache.put("c", 3)
            
            self.assertEqual(cache.get("a"), 1)
            self.assertEqual(cache.get("b"), 2)
            self.assertEqual(cache.get("c"), 3)
            self.assertIsNone(cache.get("d"))
        
        def test_lru_eviction(self):
            cache = LRUCache(2)
            
            cache.put("a", 1)
            cache.put("b", 2)
            cache.get("a")  # Access a, making b LRU
            cache.put("c", 3)  # Should evict b
            
            self.assertEqual(cache.get("a"), 1)
            self.assertIsNone(cache.get("b"))
            self.assertEqual(cache.get("c"), 3)
        
        def test_ttl_expiration(self):
            cache = LRUCache(10, ttl=0.1)
            
            cache.put("a", 1)
            self.assertEqual(cache.get("a"), 1)
            
            time.sleep(0.2)
            self.assertIsNone(cache.get("a"))
        
        def test_eviction_callback(self):
            evicted_items = []
            
            def eviction_callback(key, value):
                evicted_items.append((key, value))
            
            cache = LRUCache(2, eviction_callback=eviction_callback)
            
            cache.put("a", 1)
            cache.put("b", 2)
            cache.put("c", 3)  # Should evict "a"
            
            self.assertEqual(evicted_items, [("a", 1)])
        
        def test_thread_safety(self):
            cache = LRUCache(1000)
            
            def worker(thread_id):
                for i in range(1000):
                    key = f"thread_{thread_id}_key_{i}"
                    cache.put(key, i)
                    cache.get(key)
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(worker, i) for i in range(10)]
                concurrent.futures.wait(futures)
            
            # Cache should have exactly 1000 items (capacity)
            self.assertEqual(cache.size, 1000)
        
        def test_o1_performance(self):
            cache = LRUCache(10000)
            
            # Insert 10000 items
            for i in range(10000):
                cache.put(i, i * 2)
            
            # Access all items (should be O(1) each)
            start_time = time.time()
            for i in range(10000):
                cache.get(i)
            elapsed = time.time() - start_time
            
            # Should complete in reasonable time (O(n) total)
            self.assertLess(elapsed, 1.0)  # 1 second for 10k operations
        
        def test_update_existing_key(self):
            cache = LRUCache(2)
            
            cache.put("a", 1)
            cache.put("b", 2)
            cache.put("a", 10)  # Update existing key
            
            self.assertEqual(cache.get("a"), 10)
            self.assertEqual(cache.size, 2)
        
        def test_contains_method(self):
            cache = LRUCache(10, ttl=0.1)
            
            cache.put("a", 1)
            self.assertTrue("a" in cache)
            
            time.sleep(0.2)
            self.assertFalse("a" in cache)
        
        def test_stats(self):
            cache = LRUCache(100)
            
            for i in range(50):
                cache.put(i, i)
            
            for i in range(25):
                cache.get(i)
            
            stats = cache.stats
            self.assertEqual(stats['size'], 50)
            self.assertEqual(stats['capacity'], 100)
            self.assertEqual(stats['hits'], 25)
            self.assertEqual(stats['misses'], 25)
            self.assertEqual(stats['hit_rate'], 50.0)
        
        def test_backward_compatibility(self):
            # Test that old Cache class still works
            cache = Cache(3)
            
            cache.set(1, "a")
            cache.set(2, "b")
            cache.set(3, "c")
            
            self.assertEqual(cache.get("a"), 1)
            self.assertEqual(cache.get("b"), 2)
            self.assertEqual(cache.get("c"), 3)
    
    # Run tests
    unittest.main(verbosity=2)