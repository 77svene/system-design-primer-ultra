import uuid
import time
import threading
import queue
import hashlib
import json
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import base64

# Encryption patterns
class EncryptionService:
    """Simple encryption service for message security."""
    
    @staticmethod
    def generate_key_pair() -> Tuple[str, str]:
        """Generate public/private key pair (simplified)."""
        private_key = hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()
        public_key = hashlib.sha256(private_key.encode()).hexdigest()
        return private_key, public_key
    
    @staticmethod
    def encrypt_message(message: str, public_key: str) -> str:
        """Encrypt message with public key (simplified XOR cipher)."""
        key_hash = hashlib.sha256(public_key.encode()).digest()
        encrypted = []
        for i, char in enumerate(message):
            key_char = key_hash[i % len(key_hash)]
            encrypted.append(chr(ord(char) ^ key_char))
        return base64.b64encode(''.join(encrypted).encode()).decode()
    
    @staticmethod
    def decrypt_message(encrypted: str, private_key: str) -> str:
        """Decrypt message with private key."""
        try:
            decoded = base64.b64decode(encrypted.encode()).decode()
            key_hash = hashlib.sha256(private_key.encode()).digest()
            decrypted = []
            for i, char in enumerate(decoded):
                key_char = key_hash[i % len(key_hash)]
                decrypted.append(chr(ord(char) ^ key_char))
            return ''.join(decrypted)
        except:
            return "[Decryption Failed]"

# Delivery guarantee strategies
class DeliveryGuarantee(Enum):
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"

# Vector clock for message ordering
@dataclass
class VectorClock:
    """Vector clock for causal ordering of messages."""
    clocks: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, node_id: str) -> None:
        """Increment clock for a node."""
        self.clocks[node_id] = self.clocks.get(node_id, 0) + 1
    
    def merge(self, other: 'VectorClock') -> None:
        """Merge with another vector clock (take maximum)."""
        for node_id, timestamp in other.clocks.items():
            self.clocks[node_id] = max(self.clocks.get(node_id, 0), timestamp)
    
    def compare(self, other: 'VectorClock') -> int:
        """Compare two vector clocks: -1 (before), 0 (concurrent), 1 (after)."""
        less = greater = False
        all_nodes = set(self.clocks.keys()) | set(other.clocks.keys())
        
        for node in all_nodes:
            t1 = self.clocks.get(node, 0)
            t2 = other.clocks.get(node, 0)
            if t1 < t2:
                less = True
            elif t1 > t2:
                greater = True
        
        if less and not greater:
            return -1
        elif greater and not less:
            return 1
        elif not less and not greater:
            return 0
        else:
            return 0  # Concurrent
    
    def copy(self) -> 'VectorClock':
        """Create a copy of the vector clock."""
        return VectorClock(clocks=self.clocks.copy())

# Message data structure
@dataclass
class Message:
    """Chat message with delivery metadata."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender_id: str = ""
    receiver_id: str = ""
    content: str = ""
    timestamp: float = field(default_factory=time.time)
    vector_clock: VectorClock = field(default_factory=VectorClock)
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    encrypted: bool = False
    delivered: bool = False
    acknowledged: bool = False
    retry_count: int = 0
    max_retries: int = 3
    sequence_number: int = 0
    
    def to_dict(self) -> dict:
        """Serialize message to dictionary."""
        return {
            "id": self.id,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "content": self.content,
            "timestamp": self.timestamp,
            "vector_clock": self.vector_clock.clocks,
            "delivery_guarantee": self.delivery_guarantee.value,
            "encrypted": self.encrypted,
            "delivered": self.delivered,
            "acknowledged": self.acknowledged,
            "retry_count": self.retry_count,
            "sequence_number": self.sequence_number
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Message':
        """Deserialize message from dictionary."""
        msg = cls(
            id=data["id"],
            sender_id=data["sender_id"],
            receiver_id=data["receiver_id"],
            content=data["content"],
            timestamp=data["timestamp"],
            encrypted=data.get("encrypted", False),
            delivered=data.get("delivered", False),
            acknowledged=data.get("acknowledged", False),
            retry_count=data.get("retry_count", 0),
            sequence_number=data.get("sequence_number", 0)
        )
        msg.vector_clock = VectorClock(clocks=data.get("vector_clock", {}))
        msg.delivery_guarantee = DeliveryGuarantee(data.get("delivery_guarantee", "at_least_once"))
        return msg

# Presence detection
class PresenceStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    AWAY = "away"
    BUSY = "busy"

@dataclass
class UserPresence:
    """User presence information."""
    user_id: str
    status: PresenceStatus = PresenceStatus.OFFLINE
    last_seen: float = field(default_factory=time.time)
    device_info: str = ""
    typing: bool = False
    typing_to: Optional[str] = None

# Offline message storage
class OfflineMessageStore:
    """Storage for messages when users are offline."""
    
    def __init__(self):
        self._storage: Dict[str, deque] = defaultdict(deque)
        self._lock = threading.RLock()
        self._max_messages_per_user = 1000
    
    def store_message(self, user_id: str, message: Message) -> bool:
        """Store a message for an offline user."""
        with self._lock:
            if len(self._storage[user_id]) >= self._max_messages_per_user:
                # Remove oldest message
                self._storage[user_id].popleft()
            self._storage[user_id].append(message)
            return True
    
    def retrieve_messages(self, user_id: str, limit: int = 100) -> List[Message]:
        """Retrieve stored messages for a user."""
        with self._lock:
            messages = []
            while self._storage[user_id] and len(messages) < limit:
                messages.append(self._storage[user_id].popleft())
            return messages
    
    def has_messages(self, user_id: str) -> bool:
        """Check if user has stored messages."""
        with self._lock:
            return len(self._storage[user_id]) > 0
    
    def clear_messages(self, user_id: str) -> None:
        """Clear all stored messages for a user."""
        with self._lock:
            if user_id in self._storage:
                del self._storage[user_id]

# Message queue with delivery guarantees
class MessageQueue(ABC):
    """Abstract base class for message queues with delivery guarantees."""
    
    @abstractmethod
    def enqueue(self, message: Message) -> bool:
        """Add message to queue."""
        pass
    
    @abstractmethod
    def dequeue(self) -> Optional[Message]:
        """Remove and return next message."""
        pass
    
    @abstractmethod
    def acknowledge(self, message_id: str) -> bool:
        """Acknowledge message delivery."""
        pass
    
    @abstractmethod
    def retry_failed(self) -> List[Message]:
        """Get messages that need retry."""
        pass

class AtMostOnceQueue(MessageQueue):
    """Queue with at-most-once delivery guarantee."""
    
    def __init__(self):
        self._queue = queue.Queue()
        self._sent_messages: Set[str] = set()
    
    def enqueue(self, message: Message) -> bool:
        if message.id not in self._sent_messages:
            self._queue.put(message)
            self._sent_messages.add(message.id)
            return True
        return False
    
    def dequeue(self) -> Optional[Message]:
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None
    
    def acknowledge(self, message_id: str) -> bool:
        # No acknowledgment needed for at-most-once
        return True
    
    def retry_failed(self) -> List[Message]:
        return []  # No retries for at-most-once

class AtLeastOnceQueue(MessageQueue):
    """Queue with at-least-once delivery guarantee."""
    
    def __init__(self):
        self._queue = queue.Queue()
        self._pending_ack: Dict[str, Message] = {}
        self._lock = threading.RLock()
    
    def enqueue(self, message: Message) -> bool:
        with self._lock:
            self._queue.put(message)
            self._pending_ack[message.id] = message
            return True
    
    def dequeue(self) -> Optional[Message]:
        try:
            message = self._queue.get_nowait()
            return message
        except queue.Empty:
            return None
    
    def acknowledge(self, message_id: str) -> bool:
        with self._lock:
            if message_id in self._pending_ack:
                del self._pending_ack[message_id]
                return True
            return False
    
    def retry_failed(self) -> List[Message]:
        with self._lock:
            retry_messages = []
            current_time = time.time()
            for message_id, message in list(self._pending_ack.items()):
                if current_time - message.timestamp > 5.0:  # 5 second timeout
                    message.retry_count += 1
                    if message.retry_count <= message.max_retries:
                        retry_messages.append(message)
                        self._queue.put(message)
                    else:
                        del self._pending_ack[message_id]
            return retry_messages

class ExactlyOnceQueue(MessageQueue):
    """Queue with exactly-once delivery guarantee."""
    
    def __init__(self):
        self._queue = queue.Queue()
        self._processed: Set[str] = set()
        self._pending_ack: Dict[str, Message] = {}
        self._lock = threading.RLock()
    
    def enqueue(self, message: Message) -> bool:
        with self._lock:
            if message.id not in self._processed:
                self._queue.put(message)
                self._pending_ack[message.id] = message
                return True
            return False
    
    def dequeue(self) -> Optional[Message]:
        try:
            message = self._queue.get_nowait()
            return message
        except queue.Empty:
            return None
    
    def acknowledge(self, message_id: str) -> bool:
        with self._lock:
            if message_id in self._pending_ack:
                self._processed.add(message_id)
                del self._pending_ack[message_id]
                return True
            return False
    
    def retry_failed(self) -> List[Message]:
        with self._lock:
            retry_messages = []
            current_time = time.time()
            for message_id, message in list(self._pending_ack.items()):
                if current_time - message.timestamp > 5.0:
                    message.retry_count += 1
                    if message.retry_count <= message.max_retries:
                        retry_messages.append(message)
                        self._queue.put(message)
                    else:
                        del self._pending_ack[message_id]
            return retry_messages

# User class
class User:
    """Represents a chat user."""
    
    def __init__(self, user_id: str, name: str, public_key: str = ""):
        self.user_id = user_id
        self.name = name
        self.public_key = public_key
        self.private_key = ""
        self.presence = UserPresence(user_id=user_id)
        self.message_queue = AtLeastOnceQueue()  # Default guarantee
        self.vector_clock = VectorClock()
        self.sequence_counter = 0
        self.contacts: Set[str] = set()
        self.groups: Set[str] = set()
    
    def set_delivery_guarantee(self, guarantee: DeliveryGuarantee) -> None:
        """Set the delivery guarantee for this user's queue."""
        if guarantee == DeliveryGuarantee.AT_MOST_ONCE:
            self.message_queue = AtMostOnceQueue()
        elif guarantee == DeliveryGuarantee.AT_LEAST_ONCE:
            self.message_queue = AtLeastOnceQueue()
        elif guarantee == DeliveryGuarantee.EXACTLY_ONCE:
            self.message_queue = ExactlyOnceQueue()
    
    def send_message(self, receiver_id: str, content: str, 
                    guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
                    encrypt: bool = False) -> Message:
        """Create and send a message."""
        self.sequence_counter += 1
        self.vector_clock.increment(self.user_id)
        
        message = Message(
            sender_id=self.user_id,
            receiver_id=receiver_id,
            content=content,
            vector_clock=self.vector_clock.copy(),
            delivery_guarantee=guarantee,
            encrypted=encrypt,
            sequence_number=self.sequence_counter
        )
        
        if encrypt and self.private_key:
            message.content = EncryptionService.encrypt_message(content, self.public_key)
        
        return message
    
    def receive_message(self, message: Message) -> None:
        """Process a received message."""
        # Update vector clock
        self.vector_clock.merge(message.vector_clock)
        self.vector_clock.increment(self.user_id)
        
        # Decrypt if needed
        if message.encrypted and self.private_key:
            message.content = EncryptionService.decrypt_message(message.content, self.private_key)

# Chat server
class ChatServer:
    """Main chat server coordinating all components."""
    
    def __init__(self):
        self.users: Dict[str, User] = {}
        self.offline_store = OfflineMessageStore()
        self.message_history: Dict[str, List[Message]] = defaultdict(list)
        self.group_chats: Dict[str, Set[str]] = defaultdict(set)
        self._lock = threading.RLock()
        self._running = False
        self._delivery_thread: Optional[threading.Thread] = None
        self._presence_thread: Optional[threading.Thread] = None
    
    def start(self) -> None:
        """Start the chat server."""
        self._running = True
        self._delivery_thread = threading.Thread(target=self._delivery_loop, daemon=True)
        self._presence_thread = threading.Thread(target=self._presence_loop, daemon=True)
        self._delivery_thread.start()
        self._presence_thread.start()
    
    def stop(self) -> None:
        """Stop the chat server."""
        self._running = False
        if self._delivery_thread:
            self._delivery_thread.join(timeout=5.0)
        if self._presence_thread:
            self._presence_thread.join(timeout=5.0)
    
    def register_user(self, user_id: str, name: str) -> User:
        """Register a new user."""
        with self._lock:
            if user_id in self.users:
                raise ValueError(f"User {user_id} already registered")
            
            private_key, public_key = EncryptionService.generate_key_pair()
            user = User(user_id=user_id, name=name, public_key=public_key)
            user.private_key = private_key
            self.users[user_id] = user
            return user
    
    def user_login(self, user_id: str, device_info: str = "") -> bool:
        """Handle user login."""
        with self._lock:
            if user_id not in self.users:
                return False
            
            user = self.users[user_id]
            user.presence.status = PresenceStatus.ONLINE
            user.presence.last_seen = time.time()
            user.presence.device_info = device_info
            
            # Deliver offline messages
            self._deliver_offline_messages(user_id)
            return True
    
    def user_logout(self, user_id: str) -> bool:
        """Handle user logout."""
        with self._lock:
            if user_id not in self.users:
                return False
            
            user = self.users[user_id]
            user.presence.status = PresenceStatus.OFFLINE
            user.presence.last_seen = time.time()
            return True
    
    def send_message(self, sender_id: str, receiver_id: str, 
                    content: str, guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
                    encrypt: bool = False) -> Optional[Message]:
        """Send a message from one user to another."""
        with self._lock:
            if sender_id not in self.users:
                return None
            
            sender = self.users[sender_id]
            message = sender.send_message(receiver_id, content, guarantee, encrypt)
            
            # Store in history
            self.message_history[f"{sender_id}:{receiver_id}"].append(message)
            
            # Check if receiver is online
            if receiver_id in self.users and self.users[receiver_id].presence.status == PresenceStatus.ONLINE:
                # Try to deliver immediately
                self._deliver_message(message)
            else:
                # Store for offline delivery
                self.offline_store.store_message(receiver_id, message)
            
            return message
    
    def send_group_message(self, sender_id: str, group_id: str, 
                          content: str, guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE) -> Optional[Message]:
        """Send a message to a group."""
        with self._lock:
            if sender_id not in self.users or group_id not in self.group_chats:
                return None
            
            sender = self.users[sender_id]
            message = sender.send_message(group_id, content, guarantee)
            
            # Store in history
            self.message_history[f"group:{group_id}"].append(message)
            
            # Deliver to all online group members
            for member_id in self.group_chats[group_id]:
                if member_id != sender_id:
                    if member_id in self.users and self.users[member_id].presence.status == PresenceStatus.ONLINE:
                        self._deliver_message(message)
                    else:
                        self.offline_store.store_message(member_id, message)
            
            return message
    
    def acknowledge_message(self, user_id: str, message_id: str) -> bool:
        """Acknowledge message receipt."""
        with self._lock:
            if user_id not in self.users:
                return False
            
            user = self.users[user_id]
            return user.message_queue.acknowledge(message_id)
    
    def create_group(self, group_id: str, creator_id: str, member_ids: List[str]) -> bool:
        """Create a new group chat."""
        with self._lock:
            if group_id in self.group_chats:
                return False
            
            self.group_chats[group_id] = set(member_ids)
            self.group_chats[group_id].add(creator_id)
            
            # Add group to users
            for user_id in [creator_id] + member_ids:
                if user_id in self.users:
                    self.users[user_id].groups.add(group_id)
            
            return True
    
    def update_presence(self, user_id: str, status: PresenceStatus, 
                       typing: bool = False, typing_to: Optional[str] = None) -> bool:
        """Update user presence information."""
        with self._lock:
            if user_id not in self.users:
                return False
            
            user = self.users[user_id]
            user.presence.status = status
            user.presence.last_seen = time.time()
            user.presence.typing = typing
            user.presence.typing_to = typing_to
            
            # Notify contacts about presence change
            self._notify_presence_change(user_id)
            return True
    
    def get_message_history(self, user1_id: str, user2_id: str, 
                           limit: int = 50) -> List[Message]:
        """Get message history between two users."""
        with self._lock:
            key1 = f"{user1_id}:{user2_id}"
            key2 = f"{user2_id}:{user1_id}"
            
            messages = []
            if key1 in self.message_history:
                messages.extend(self.message_history[key1])
            if key2 in self.message_history:
                messages.extend(self.message_history[key2])
            
            # Sort by timestamp and sequence number
            messages.sort(key=lambda m: (m.timestamp, m.sequence_number))
            return messages[-limit:]
    
    def _deliver_message(self, message: Message) -> bool:
        """Attempt to deliver a message to its recipient."""
        receiver_id = message.receiver_id
        
        if receiver_id in self.users:
            receiver = self.users[receiver_id]
            
            if receiver.presence.status == PresenceStatus.ONLINE:
                # Add to receiver's queue
                if receiver.message_queue.enqueue(message):
                    # Simulate delivery
                    message.delivered = True
                    return True
        
        return False
    
    def _deliver_offline_messages(self, user_id: str) -> None:
        """Deliver stored offline messages to a user."""
        if self.offline_store.has_messages(user_id):
            messages = self.offline_store.retrieve_messages(user_id)
            for message in messages:
                self._deliver_message(message)
    
    def _delivery_loop(self) -> None:
        """Background thread for message delivery and retries."""
        while self._running:
            time.sleep(1.0)  # Check every second
            
            with self._lock:
                # Retry failed messages
                for user in self.users.values():
                    retry_messages = user.message_queue.retry_failed()
                    for message in retry_messages:
                        self._deliver_message(message)
    
    def _presence_loop(self) -> None:
        """Background thread for presence updates."""
        while self._running:
            time.sleep(30.0)  # Update every 30 seconds
            
            with self._lock:
                current_time = time.time()
                for user in self.users.values():
                    # Mark users as away if inactive for 5 minutes
                    if (user.presence.status == PresenceStatus.ONLINE and 
                        current_time - user.presence.last_seen > 300):
                        user.presence.status = PresenceStatus.AWAY
    
    def _notify_presence_change(self, changed_user_id: str) -> None:
        """Notify contacts about presence changes."""
        changed_user = self.users.get(changed_user_id)
        if not changed_user:
            return
        
        # Create presence update message
        presence_msg = Message(
            sender_id="system",
            receiver_id="",  # Will be set per contact
            content=f"User {changed_user.name} is now {changed_user.presence.status.value}",
            delivery_guarantee=DeliveryGuarantee.AT_MOST_ONCE
        )
        
        # Notify all contacts
        for contact_id in changed_user.contacts:
            if contact_id in self.users:
                contact = self.users[contact_id]
                if contact.presence.status == PresenceStatus.ONLINE:
                    presence_msg.receiver_id = contact_id
                    contact.message_queue.enqueue(presence_msg)

# Example usage and integration
def example_usage():
    """Example demonstrating the chat system."""
    # Create and start server
    server = ChatServer()
    server.start()
    
    # Register users
    alice = server.register_user("alice", "Alice Smith")
    bob = server.register_user("bob", "Bob Johnson")
    
    # Set delivery guarantees
    alice.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
    bob.set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    
    # Login users
    server.user_login("alice", "iPhone 12")
    server.user_login("bob", "Web Browser")
    
    # Send messages
    msg1 = server.send_message("alice", "bob", "Hello Bob!", 
                              DeliveryGuarantee.AT_LEAST_ONCE, encrypt=True)
    msg2 = server.send_message("bob", "alice", "Hi Alice! How are you?")
    
    # Create group chat
    server.create_group("project_team", "alice", ["bob"])
    server.send_group_message("alice", "project_team", "Team meeting at 3 PM")
    
    # Update presence
    server.update_presence("bob", PresenceStatus.AWAY)
    
    # Get message history
    history = server.get_message_history("alice", "bob")
    print(f"Message history: {len(history)} messages")
    
    # Simulate offline scenario
    server.user_logout("bob")
    offline_msg = server.send_message("alice", "bob", "This will be stored offline")
    
    # Bob comes back online
    server.user_login("bob")
    
    # Cleanup
    server.stop()

# Integration with existing online_chat module
class OnlineChatSystem:
    """
    Integration wrapper for existing online_chat module.
    This class provides the interface expected by the existing codebase.
    """
    
    def __init__(self):
        self.server = ChatServer()
        self.server.start()
    
    def register_user(self, user_id: str, name: str) -> bool:
        """Register a new user."""
        try:
            self.server.register_user(user_id, name)
            return True
        except ValueError:
            return False
    
    def login(self, user_id: str) -> bool:
        """User login."""
        return self.server.user_login(user_id)
    
    def logout(self, user_id: str) -> bool:
        """User logout."""
        return self.server.user_logout(user_id)
    
    def send_message(self, from_user: str, to_user: str, message: str) -> bool:
        """Send a message."""
        result = self.server.send_message(from_user, to_user, message)
        return result is not None
    
    def get_messages(self, user_id: str) -> List[dict]:
        """Get messages for a user (simplified interface)."""
        # This would need to be implemented based on the existing interface
        return []
    
    def shutdown(self) -> None:
        """Shutdown the chat system."""
        self.server.stop()

# For backward compatibility with existing online_chat.py
# This allows the existing module to import and use our enhanced system
if __name__ == "__main__":
    example_usage()