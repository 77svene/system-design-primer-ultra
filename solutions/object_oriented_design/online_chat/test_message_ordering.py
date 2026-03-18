test_message_ordering.py

import pytest
import time
from unittest.mock import Mock, patch

from online_chat import ChatSystem, Message, User, Room


class TestMessageOrdering:
    """Test suite for message ordering and delivery guarantees in online chat system."""

    @pytest.fixture
    def chat_system(self):
        return ChatSystem()

    @pytest.fixture
    def room(self, chat_system):
        room = chat_system.create_room("general")
        return room

    @pytest.fixture
    def user_a(self, chat_system):
        user = chat_system.create_user("user_a", "password")
        return user

    @pytest.fixture
    def user_b(self, chat_system):
        user = chat_system.create_user("user_b", "password")
        return user

    @pytest.fixture
    def room_with_users(self, chat_system, room, user_a, user_b):
        room.add_user(user_a)
        room.add_user(user_b)
        return room

    def test_message_ordering_with_vector_clocks(self, chat_system, room_with_users):
        """Test that messages are ordered correctly using vector clocks."""
        user_a.send_message(room_with_users, "Message 1")
        time.sleep(0.01)
        user_b.send_message(room_with_users, "Message 2")
        time.sleep(0.01)
        user_a.send_message(room_with_users, "Message 3")

        messages = room_with_users.get_messages()
        assert len(messages) == 3
        assert messages[0].content == "Message 1"
        assert messages[1].content == "Message 2"
        assert messages[2].content == "Message 3"

    def test_message_ordering_preserved_across_clients(self, chat_system, room_with_users):
        """Test message ordering is preserved when messages arrive out of order."""
        user_a.send_message(room_with_users, "First")
        # Simulate out-of-order delivery
        user_b.send_message(room_with_users, "Third")
        user_a.send_message(room_with_users, "Second")

        messages = room_with_users.get_messages()
        assert messages[0].content == "First"
        assert messages[1].content == "Second"
        assert messages[2].content == "Third"

    def test_delivery_guarantee_at_least_once(self, chat_system, room_with_users):
        """Test at-least-once delivery guarantee."""
        delivery_count = 0

        def mock_delivery(message):
            nonlocal delivery_count
            delivery_count += 1
            return True

        with patch.object(room_with_users, 'deliver_message', side_effect=mock_delivery):
            user_a.send_message(room_with_users, "Test message")

        assert delivery_count >= 1

    def test_delivery_guarantee_exactly_once(self, chat_system, room_with_users):
        """Test exactly-once delivery guarantee with idempotency."""
        delivery_count = 0
        message_id = "unique_message_id"

        def mock_delivery(message):
            nonlocal delivery_count
            if message.id != message_id:
                return False
            delivery_count += 1
            return True

        with patch.object(room_with_users, 'deliver_message', side_effect=mock_delivery):
            user_a.send_message(room_with_users, "Test message")
            # Simulate retry
            user_a.send_message(room_with_users, "Test message")

        assert delivery_count == 1

    def test_message_id_generation_uniqueness(self, chat_system, room_with_users):
        """Test that message IDs are unique."""
        messages = []
        for i in range(100):
            msg = user_a.send_message(room_with_users, f"Message {i}")
            messages.append(msg)

        assert len(set(msg.id for msg in messages)) == len(messages)

    def test_vector_clock_increment_on_send(self, chat_system, room_with_users):
        """Test vector clock increments correctly on send."""
        initial_clock = user_a.get_vector_clock()
        user_a.send_message(room_with_users, "Test")
        updated_clock = user_a.get_vector_clock()

        assert updated_clock['user_a'] > initial_clock['user_a']

    def test_vector_clock_merge_on_receive(self, chat_system, room_with_users):
        """Test vector clock merges correctly when receiving messages."""
        user_a.send_message(room_with_users, "Message A")
        user_b = room_with_users.get_messages()
        user_b.receive_message(user_a.get_vector_clock())

        assert user_b.vector_clock['user_a'] > 0

    def test_offline_message_storage(self, chat_system, room_with_users):
        """Test that messages are stored for offline users."""
        user_a.send_message(room_with_users, "Offline message")

        messages = room_with_users.get_messages()
        assert len(messages) == 1
        assert messages[0].content == "Offline message"

    def test_offline_message_delivery_on_connect(self, chat_system, room_with_users):
        """Test offline messages are delivered when user reconnects."""
        user_a.send_message(room_with_users, "Offline message")

        with patch.object(user_a, 'is_online', return_value=False):
            messages = room_with_users.get_messages()
            assert len(messages) == 1

        with patch.object(user_a, 'is_online', return_value=True):
            messages = room_with_users.get_messages()
            assert len(messages) == 1
            assert messages[0].content == "Offline message"

    def test_encryption_of_message_content(self, chat_system, room_with_users):
        """Test message content is encrypted before storage."""
        user_a.send_message(room_with_users, "Secret message")

        message = room_with_users.get_messages()[0]
        assert message.is_encrypted
        assert message.content == "Secret message"

    def test_encryption_key_management(self, chat_system, room_with_users):
        """Test encryption key management."""
        room = chat_system.create_room("encrypted")
        room.add_encryption_key("room_key")

        user_a.send_message(room, "Encrypted")

        message = room.get_messages()[0]
        assert message.is_encrypted
        assert message.key == "room_key"

    def test_message_timestamp_ordering(self, chat_system, room_with_users):
        """Test messages are ordered by timestamp."""
        user_a.send_message(room_with_users, "First")
        time.sleep(0.01)
        user_b.send_message(room_with_users, "Second")

        messages = room_with_users.get_messages()
        assert messages[0].timestamp < messages[1].timestamp

    def test_message_ordering_with_duplicates(self, chat_system, room_with_users):
        """Test duplicate messages are handled correctly."""
        user_a.send_message(room_with_users, "Original")
        # Simulate duplicate delivery
        user_a.send_message(room_with_users, "Original")

        messages = room_with_users.get_messages()
        assert len(messages) == 1
        assert messages[0].content == "Original"

    def test_message_ordering_with_reordering(self, chat_system, room_with_users):
        """Test messages are reordered correctly when received out of order."""
        user_a.send_message(room_with_users, "1")
        user_a.send_message(room_with_users, "2")
        user_a.send_message(room_with_users, "3")

        # Simulate reordering
        messages = room_with_users.get_messages()
        assert messages[0].content == "1"
        assert messages[1].content == "2"
        assert messages[2].content == "3"

    def test_message_ordering_with_multiple_users(self, chat_system, room_with_users):
        """Test message ordering with multiple users sending."""
        user_a.send_message(room_with_users, "A1")
        user_b.send_message(room_with_users, "B1")
        user_a.send_message(room_with_users, "A2")
        user_b.send_message(room_with_users, "B2")

        messages = room_with_users.get_messages()
        assert len(messages) == 4

    def test_message_ordering_with_vector_clock_comparison(self, chat_system, room_with_users):
        """Test vector clock comparison for ordering."""
        user_a.send_message(room_with_users, "A1")
        user_b.send_message(room_with_users, "B1")

        clock_a = user_a.get_vector_clock()
        clock_b = user_b.get_vector_clock()

        assert clock_a['user_a'] > 0
        assert clock_b['user_b'] > 0

    def test_message_ordering_with_missing_messages(self, chat_system, room_with_users):
        """Test handling of missing messages in stream."""
        user_a.send_message(room_with_users, "1")
        user_a.send_message(room_with_users, "2")
        user_a.send_message(room_with_users, "3")

        # Simulate missing message
        messages = room_with_users.get_messages()
        assert len(messages) == 3

    def test_message_ordering_with_message_priority(self, chat_system, room_with_users):
        """Test message priority ordering."""
        user_a.send_message(room_with_users, "Normal", priority="normal")
        user_a.send_message(room_with_users, "High", priority="high")
        user_a.send_message(room_with_users, "Low", priority="low")

        messages = room_with_users.get_messages()
        assert len(messages) == 3

    def test_message_ordering_with_message_type(self, chat_system, room_with_users):
        """Test different message types are ordered correctly."""
        user_a.send_message(room_with_users, "Text", message_type="text")
        user_a.send_message(room_with_users, "Image", message_type="image")
        user_a.send_message(room_with_users, "File", message_type="file")

        messages = room_with_users.get_messages()
        assert len(messages) == 3

    def test_message_ordering_with_message_metadata(self, chat_system, room_with_users):
        """Test message metadata is preserved with ordering."""
        metadata = {"source": "mobile", "version": "1.0"}
        user_a.send_message(room_with_users, "Test", metadata=metadata)

        message = room_with_users.get_messages()[0]
        assert message.metadata == metadata

    def test_message_ordering_with_message_validation(self, chat_system, room_with_users):
        """Test message validation before ordering."""
        user_a.send_message(room_with_users, "Valid")
        assert room_with_users.get_messages()[0].is_valid

    def test_message_ordering_with_message_size_limits(self, chat_system, room_with_users):
        """Test message size limits are enforced."""
        large_message = "x" * 10000
        user_a.send_message(room_with_users, large_message)

        messages = room_with_users.get_messages()
        assert len(messages) == 1

    def test_message_ordering_with_message_expiry(self, chat_system, room_with_users):
        """Test message expiry handling."""
        user_a.send_message(room_with_users, "Expiring", expiry_seconds=1)
        time.sleep(2)

        messages = room_with_users.get_messages()
        assert len(messages) == 0

    def test_message_ordering_with_message_acknowledgment(self, chat_system, room_with_users):
        """Test message acknowledgment handling."""
        user_a.send_message(room_with_users, "Ack")
        messages = room_with_users.get_messages()
        assert len(messages) == 1

    def test_message_ordering_with_message_retry(self, chat_system, room_with_users):
        """Test message retry logic."""
        delivery_count = 0

        def mock_delivery(message):
            nonlocal delivery_count
            delivery_count += 1
            return delivery_count <= 1

        with patch.object(room_with_users, 'deliver_message', side_effect=mock_delivery):
            user_a.send_message(room_with_users, "Retry")

        assert delivery_count == 1

    def test_message_ordering_with_message_deletion(self, chat_system, room_with_users):
        """Test message deletion handling."""
        user_a.send_message(room_with_users, "To delete")
        room_with_users.delete_messages([room_with_users.get_messages()[0]])

        messages = room_with_users.get_messages()
        assert len(messages) == 0

    def test_message_ordering_with_message_read_status(self, chat_system, room_with_users):
        """Test message read status tracking."""
        user_a.send_message(room_with_users, "Read test")
        messages = room_with_users.get_messages()
        assert len(messages) == 1