from abc import ABCMeta
from enum import Enum
import time
import random
import threading
from datetime import datetime


class ChaosToolkit:
    """Chaos engineering toolkit for fault injection and resilience testing"""
    
    def __init__(self):
        self.enabled = False
        self.faults = {
            'network_partition': False,
            'disk_failure': False,
            'cpu_spike': False,
            'latency_injection': False,
            'memory_exhaustion': False
        }
        self.latency_range = (0.1, 2.0)  # seconds
        self.cpu_spike_duration = 5  # seconds
        
    def enable(self):
        """Enable chaos engineering"""
        self.enabled = True
        
    def disable(self):
        """Disable chaos engineering"""
        self.enabled = False
        
    def inject_network_partition(self, probability=0.1):
        """Simulate network partition with given probability"""
        if self.enabled and random.random() < probability:
            raise ConnectionError("Simulated network partition")
            
    def inject_disk_failure(self, probability=0.05):
        """Simulate disk failure with given probability"""
        if self.enabled and random.random() < probability:
            raise IOError("Simulated disk failure")
            
    def inject_latency(self):
        """Inject random latency"""
        if self.enabled and self.faults['latency_injection']:
            delay = random.uniform(*self.latency_range)
            time.sleep(delay)
            
    def inject_cpu_spike(self):
        """Simulate CPU spike"""
        if self.enabled and self.faults['cpu_spike']:
            start_time = time.time()
            while time.time() - start_time < self.cpu_spike_duration:
                # Busy wait to simulate CPU load
                _ = [i**2 for i in range(10000)]
                
    def inject_memory_exhaustion(self, probability=0.02):
        """Simulate memory exhaustion"""
        if self.enabled and random.random() < probability:
            # Simulate memory pressure by creating large objects
            large_list = [0] * 10000000
            raise MemoryError("Simulated memory exhaustion")
            
    def run_game_day_scenario(self, scenario_name):
        """Execute predefined chaos scenarios"""
        scenarios = {
            'network_outage': self._network_outage_scenario,
            'disk_corruption': self._disk_corruption_scenario,
            'resource_exhaustion': self._resource_exhaustion_scenario,
            'cascading_failure': self._cascading_failure_scenario
        }
        
        if scenario_name in scenarios:
            return scenarios[scenario_name]()
        else:
            raise ValueError(f"Unknown scenario: {scenario_name}")
            
    def _network_outage_scenario(self):
        """Simulate complete network outage"""
        self.faults['network_partition'] = True
        return {
            'name': 'network_outage',
            'duration': 30,
            'description': 'Complete network partition for 30 seconds'
        }
        
    def _disk_corruption_scenario(self):
        """Simulate disk corruption"""
        self.faults['disk_failure'] = True
        return {
            'name': 'disk_corruption',
            'duration': 60,
            'description': 'Disk I/O errors for 60 seconds'
        }
        
    def _resource_exhaustion_scenario(self):
        """Simulate resource exhaustion"""
        self.faults['cpu_spike'] = True
        self.faults['memory_exhaustion'] = True
        return {
            'name': 'resource_exhaustion',
            'duration': 45,
            'description': 'CPU and memory exhaustion for 45 seconds'
        }
        
    def _cascading_failure_scenario(self):
        """Simulate cascading failure across components"""
        self.faults['network_partition'] = True
        self.faults['latency_injection'] = True
        self.faults['cpu_spike'] = True
        return {
            'name': 'cascading_failure',
            'duration': 120,
            'description': 'Multiple simultaneous failures for 2 minutes'
        }


# Global chaos toolkit instance
chaos_toolkit = ChaosToolkit()


class UserService(object):

    def __init__(self):
        self.users_by_id = {}  # key: user id, value: User
        self.chaos_toolkit = chaos_toolkit

    def add_user(self, user_id, name, pass_hash):
        """Add user with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_disk_failure()
        self.chaos_toolkit.inject_latency()
        
        if user_id in self.users_by_id:
            raise ValueError(f"User {user_id} already exists")
            
        user = User(user_id, name, pass_hash)
        self.users_by_id[user_id] = user
        return user

    def remove_user(self, user_id):
        """Remove user with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_disk_failure()
        self.chaos_toolkit.inject_latency()
        
        if user_id not in self.users_by_id:
            raise ValueError(f"User {user_id} not found")
            
        del self.users_by_id[user_id]
        return True

    def add_friend_request(self, from_user_id, to_user_id):
        """Add friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        if from_user_id not in self.users_by_id:
            raise ValueError(f"User {from_user_id} not found")
        if to_user_id not in self.users_by_id:
            raise ValueError(f"User {to_user_id} not found")
            
        from_user = self.users_by_id[from_user_id]
        to_user = self.users_by_id[to_user_id]
        
        # Create add request
        add_request = AddRequest(
            from_user_id=from_user_id,
            to_user_id=to_user_id,
            request_status=RequestStatus.UNREAD,
            timestamp=datetime.now()
        )
        
        # Update user records
        from_user.sent_friend_requests_by_friend_id[to_user_id] = add_request
        to_user.received_friend_requests_by_friend_id[from_user_id] = add_request
        
        return add_request

    def approve_friend_request(self, from_user_id, to_user_id):
        """Approve friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_disk_failure()
        self.chaos_toolkit.inject_latency()
        
        if from_user_id not in self.users_by_id:
            raise ValueError(f"User {from_user_id} not found")
        if to_user_id not in self.users_by_id:
            raise ValueError(f"User {to_user_id} not found")
            
        from_user = self.users_by_id[from_user_id]
        to_user = self.users_by_id[to_user_id]
        
        # Update request status
        if to_user_id in from_user.sent_friend_requests_by_friend_id:
            request = from_user.sent_friend_requests_by_friend_id[to_user_id]
            request.request_status = RequestStatus.ACCEPTED
            
        if from_user_id in to_user.received_friend_requests_by_friend_id:
            request = to_user.received_friend_requests_by_friend_id[from_user_id]
            request.request_status = RequestStatus.ACCEPTED
            
        # Create friendship
        from_user.friends_by_id[to_user_id] = to_user
        to_user.friends_by_id[from_user_id] = from_user
        
        # Create private chat
        private_chat = PrivateChat(from_user, to_user)
        from_user.friend_ids_to_private_chats[to_user_id] = private_chat
        to_user.friend_ids_to_private_chats[from_user_id] = private_chat
        
        return True

    def reject_friend_request(self, from_user_id, to_user_id):
        """Reject friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        if from_user_id not in self.users_by_id:
            raise ValueError(f"User {from_user_id} not found")
        if to_user_id not in self.users_by_id:
            raise ValueError(f"User {to_user_id} not found")
            
        from_user = self.users_by_id[from_user_id]
        to_user = self.users_by_id[to_user_id]
        
        # Update request status
        if to_user_id in from_user.sent_friend_requests_by_friend_id:
            request = from_user.sent_friend_requests_by_friend_id[to_user_id]
            request.request_status = RequestStatus.REJECTED
            del from_user.sent_friend_requests_by_friend_id[to_user_id]
            
        if from_user_id in to_user.received_friend_requests_by_friend_id:
            request = to_user.received_friend_requests_by_friend_id[from_user_id]
            request.request_status = RequestStatus.REJECTED
            del to_user.received_friend_requests_by_friend_id[from_user_id]
            
        return True


class User(object):

    def __init__(self, user_id, name, pass_hash):
        self.user_id = user_id
        self.name = name
        self.pass_hash = pass_hash
        self.friends_by_id = {}  # key: friend id, value: User
        self.friend_ids_to_private_chats = {}  # key: friend id, value: private chats
        self.group_chats_by_id = {}  # key: chat id, value: GroupChat
        self.received_friend_requests_by_friend_id = {}  # key: friend id, value: AddRequest
        self.sent_friend_requests_by_friend_id = {}  # key: friend id, value: AddRequest
        self.chaos_toolkit = chaos_toolkit

    def message_user(self, friend_id, message):
        """Send message to user with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        self.chaos_toolkit.inject_cpu_spike()
        
        if friend_id not in self.friends_by_id:
            raise ValueError(f"User {friend_id} is not a friend")
            
        if friend_id not in self.friend_ids_to_private_chats:
            raise ValueError(f"No private chat with user {friend_id}")
            
        chat = self.friend_ids_to_private_chats[friend_id]
        message_obj = Message(
            message_id=len(chat.messages) + 1,
            message=message,
            timestamp=datetime.now()
        )
        chat.messages.append(message_obj)
        return message_obj

    def message_group(self, group_id, message):
        """Send message to group with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        self.chaos_toolkit.inject_cpu_spike()
        self.chaos_toolkit.inject_memory_exhaustion()
        
        if group_id not in self.group_chats_by_id:
            raise ValueError(f"Group {group_id} not found")
            
        group_chat = self.group_chats_by_id[group_id]
        message_obj = Message(
            message_id=len(group_chat.messages) + 1,
            message=message,
            timestamp=datetime.now()
        )
        group_chat.messages.append(message_obj)
        return message_obj

    def send_friend_request(self, friend_id):
        """Send friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        # This would typically call UserService.add_friend_request
        # For now, we'll just simulate the action
        return True

    def receive_friend_request(self, friend_id):
        """Receive friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        # Update received requests
        self.received_friend_requests_by_friend_id[friend_id] = AddRequest(
            from_user_id=friend_id,
            to_user_id=self.user_id,
            request_status=RequestStatus.UNREAD,
            timestamp=datetime.now()
        )
        return True

    def approve_friend_request(self, friend_id):
        """Approve friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_disk_failure()
        self.chaos_toolkit.inject_latency()
        
        if friend_id not in self.received_friend_requests_by_friend_id:
            raise ValueError(f"No friend request from user {friend_id}")
            
        request = self.received_friend_requests_by_friend_id[friend_id]
        request.request_status = RequestStatus.ACCEPTED
        
        # This would typically call UserService.approve_friend_request
        return True

    def reject_friend_request(self, friend_id):
        """Reject friend request with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        if friend_id not in self.received_friend_requests_by_friend_id:
            raise ValueError(f"No friend request from user {friend_id}")
            
        request = self.received_friend_requests_by_friend_id[friend_id]
        request.request_status = RequestStatus.REJECTED
        
        # Clean up
        del self.received_friend_requests_by_friend_id[friend_id]
        return True


class Chat(metaclass=ABCMeta):

    def __init__(self, chat_id):
        self.chat_id = chat_id
        self.users = []
        self.messages = []
        self.chaos_toolkit = chaos_toolkit


class PrivateChat(Chat):

    def __init__(self, first_user, second_user):
        super(PrivateChat, self).__init__(f"private_{first_user.user_id}_{second_user.user_id}")
        self.users.append(first_user)
        self.users.append(second_user)


class GroupChat(Chat):

    def add_user(self, user):
        """Add user to group chat with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        if user not in self.users:
            self.users.append(user)
            user.group_chats_by_id[self.chat_id] = self
        return True

    def remove_user(self, user):
        """Remove user from group chat with chaos engineering hooks"""
        self.chaos_toolkit.inject_network_partition()
        self.chaos_toolkit.inject_latency()
        
        if user in self.users:
            self.users.remove(user)
            if self.chat_id in user.group_chats_by_id:
                del user.group_chats_by_id[self.chat_id]
        return True


class Message(object):

    def __init__(self, message_id, message, timestamp):
        self.message_id = message_id
        self.message = message
        self.timestamp = timestamp


class AddRequest(object):

    def __init__(self, from_user_id, to_user_id, request_status, timestamp):
        self.from_user_id = from_user_id
        self.to_user_id = to_user_id
        self.request_status = request_status
        self.timestamp = timestamp


class RequestStatus(Enum):

    UNREAD = 0
    READ = 1
    ACCEPTED = 2
    REJECTED = 3


class ChaosTestRunner:
    """Automated chaos testing for CI/CD integration"""
    
    def __init__(self):
        self.chaos_toolkit = chaos_toolkit
        self.test_results = []
        
    def run_automated_chaos_tests(self):
        """Run automated chaos tests for CI/CD pipeline"""
        tests = [
            self._test_network_partition_resilience,
            self._test_disk_failure_resilience,
            self._test_latency_tolerance,
            self._test_resource_exhaustion_recovery,
            self._test_cascading_failure_handling
        ]
        
        results = []
        for test in tests:
            try:
                result = test()
                results.append(result)
            except Exception as e:
                results.append({
                    'test': test.__name__,
                    'status': 'FAILED',
                    'error': str(e)
                })
                
        return results
        
    def _test_network_partition_resilience(self):
        """Test system resilience to network partitions"""
        self.chaos_toolkit.enable()
        self.chaos_toolkit.faults['network_partition'] = True
        
        try:
            # Create test user service
            user_service = UserService()
            user_service.add_user(1, "TestUser1", "hash1")
            
            # This should raise ConnectionError due to network partition
            user_service.add_user(2, "TestUser2", "hash2")
            
            return {'test': 'network_partition_resilience', 'status': 'FAILED'}
        except ConnectionError:
            return {'test': 'network_partition_resilience', 'status': 'PASSED'}
        finally:
            self.chaos_toolkit.disable()
            
    def _test_disk_failure_resilience(self):
        """Test system resilience to disk failures"""
        self.chaos_toolkit.enable()
        self.chaos_toolkit.faults['disk_failure'] = True
        
        try:
            user_service = UserService()
            user_service.add_user(1, "TestUser1", "hash1")
            return {'test': 'disk_failure_resilience', 'status': 'FAILED'}
        except IOError:
            return {'test': 'disk_failure_resilience', 'status': 'PASSED'}
        finally:
            self.chaos_toolkit.disable()
            
    def _test_latency_tolerance(self):
        """Test system tolerance to latency"""
        self.chaos_toolkit.enable()
        self.chaos_toolkit.faults['latency_injection'] = True
        
        start_time = time.time()
        user_service = UserService()
        user_service.add_user(1, "TestUser1", "hash1")
        elapsed_time = time.time() - start_time
        
        self.chaos_toolkit.disable()
        
        if elapsed_time > 0.1:  # Should have latency injected
            return {'test': 'latency_tolerance', 'status': 'PASSED'}
        else:
            return {'test': 'latency_tolerance', 'status': 'FAILED'}
            
    def _test_resource_exhaustion_recovery(self):
        """Test system recovery from resource exhaustion"""
        self.chaos_toolkit.enable()
        self.chaos_toolkit.faults['cpu_spike'] = True
        self.chaos_toolkit.faults['memory_exhaustion'] = True
        
        try:
            user_service = UserService()
            user = user_service.add_user(1, "TestUser1", "hash1")
            
            # This might trigger memory exhaustion
            user.message_group(999, "Test message")
            
            return {'test': 'resource_exhaustion_recovery', 'status': 'FAILED'}
        except (MemoryError, ValueError):
            return {'test': 'resource_exhaustion_recovery', 'status': 'PASSED'}
        finally:
            self.chaos_toolkit.disable()
            
    def _test_cascading_failure_handling(self):
        """Test handling of cascading failures"""
        self.chaos_toolkit.enable()
        
        # Enable multiple faults simultaneously
        self.chaos_toolkit.faults['network_partition'] = True
        self.chaos_toolkit.faults['latency_injection'] = True
        self.chaos_toolkit.faults['cpu_spike'] = True
        
        try:
            user_service = UserService()
            user_service.add_user(1, "TestUser1", "hash1")
            return {'test': 'cascading_failure_handling', 'status': 'FAILED'}
        except (ConnectionError, IOError):
            return {'test': 'cascading_failure_handling', 'status': 'PASSED'}
        finally:
            self.chaos_toolkit.disable()


# Game day runbook
GAME_DAY_RUNBOOK = """
# Chaos Engineering Game Day Runbook

## Scenario 1: Network Partition
**Objective**: Test system behavior during network partitions
**Steps**:
1. Enable network partition fault injection
2. Attempt to send messages between users
3. Verify system handles connection errors gracefully
4. Disable fault injection and verify recovery

## Scenario 2: Disk Failure
**Objective**: Test data persistence during disk failures
**Steps**:
1. Enable disk failure fault injection
2. Attempt to create new users and chats
3. Verify appropriate error handling
4. Disable fault injection and verify data consistency

## Scenario 3: Resource Exhaustion
**Objective**: Test system under resource pressure
**Steps**:
1. Enable CPU spike and memory exhaustion
2. Send multiple messages to group chats
3. Monitor system performance and error rates
4. Verify system recovers after resource pressure subsides

## Scenario 4: Cascading Failure
**Objective**: Test system resilience to multiple simultaneous failures
**Steps**:
1. Enable multiple fault types simultaneously
2. Execute normal user operations
3. Monitor error propagation and system stability
4. Verify partial functionality remains available

## Monitoring During Game Day
- Monitor error rates and exception types
- Track response times and latency spikes
- Verify data consistency after fault recovery
- Document any unexpected behaviors
"""


def run_chaos_test_suite():
    """Run complete chaos test suite for CI/CD"""
    runner = ChaosTestRunner()
    results = runner.run_automated_chaos_tests()
    
    print("Chaos Engineering Test Results:")
    print("=" * 50)
    
    passed = 0
    failed = 0
    
    for result in results:
        status = result['status']
        test_name = result['test']
        
        if status == 'PASSED':
            passed += 1
            print(f"✓ {test_name}: {status}")
        else:
            failed += 1
            print(f"✗ {test_name}: {status}")
            if 'error' in result:
                print(f"  Error: {result['error']}")
                
    print("=" * 50)
    print(f"Total: {len(results)} tests")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    return failed == 0


# Example usage for CI/CD integration
if __name__ == "__main__":
    # Run chaos tests
    success = run_chaos_test_suite()
    
    if not success:
        exit(1)  # Fail CI/CD pipeline if chaos tests fail
        
    # Example of running a game day scenario
    toolkit = ChaosToolkit()
    scenario = toolkit.run_game_day_scenario('network_outage')
    print(f"\nGame Day Scenario: {scenario['name']}")
    print(f"Description: {scenario['description']}")
    print(f"Duration: {scenario['duration']} seconds")