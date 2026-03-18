import unittest
import tempfile
import os
import shutil
import time
import threading
import random
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from typing import List, Dict, Tuple, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import pickle
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    KILLED = "killed"

class TaskType(Enum):
    MAP = "map"
    REDUCE = "reduce"
    COMBINE = "combine"

@dataclass
class Task:
    task_id: str
    task_type: TaskType
    input_files: List[str]
    output_dir: str
    status: TaskStatus = TaskStatus.PENDING
    worker_id: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    attempts: int = 0
    checkpoint: Optional[str] = None
    locality_score: float = 0.0

@dataclass
class WorkerNode:
    worker_id: str
    host: str
    data_local_files: List[str] = field(default_factory=list)
    is_alive: bool = True
    current_tasks: List[str] = field(default_factory=list)
    last_heartbeat: float = field(default_factory=time.time)

class MapReduceException(Exception):
    pass

class Mapper:
    """Base class for all mappers"""
    def map(self, key: Any, value: Any) -> List[Tuple[Any, Any]]:
        raise NotImplementedError("Subclasses must implement map method")
    
    def setup(self):
        """Called before mapping starts"""
        pass
    
    def cleanup(self):
        """Called after mapping completes"""
        pass

class Reducer:
    """Base class for all reducers"""
    def reduce(self, key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
        raise NotImplementedError("Subclasses must implement reduce method")
    
    def setup(self):
        """Called before reducing starts"""
        pass
    
    def cleanup(self):
        """Called after reducing completes"""
        pass

class Combiner:
    """Base class for combiners (optional optimization)"""
    def combine(self, key: Any, values: List[Any]) -> List[Tuple[Any, Any]]:
        raise NotImplementedError("Subclasses must implement combine method")

class CheckpointManager:
    """Manages checkpoints for fault tolerance"""
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints = {}
    
    def save_checkpoint(self, task_id: str, data: Any) -> str:
        """Save checkpoint data and return checkpoint ID"""
        checkpoint_id = hashlib.md5(f"{task_id}_{time.time()}".encode()).hexdigest()
        checkpoint_path = self.checkpoint_dir / f"{checkpoint_id}.ckpt"
        
        with open(checkpoint_path, 'wb') as f:
            pickle.dump({
                'task_id': task_id,
                'timestamp': time.time(),
                'data': data
            }, f)
        
        self.checkpoints[task_id] = checkpoint_id
        logger.info(f"Saved checkpoint {checkpoint_id} for task {task_id}")
        return checkpoint_id
    
    def load_checkpoint(self, task_id: str) -> Optional[Any]:
        """Load checkpoint data for a task"""
        if task_id not in self.checkpoints:
            return None
        
        checkpoint_id = self.checkpoints[task_id]
        checkpoint_path = self.checkpoint_dir / f"{checkpoint_id}.ckpt"
        
        if not checkpoint_path.exists():
            return None
        
        try:
            with open(checkpoint_path, 'rb') as f:
                checkpoint_data = pickle.load(f)
            logger.info(f"Loaded checkpoint {checkpoint_id} for task {task_id}")
            return checkpoint_data['data']
        except Exception as e:
            logger.error(f"Failed to load checkpoint {checkpoint_id}: {e}")
            return None
    
    def cleanup_checkpoints(self, task_id: str):
        """Remove checkpoints for a completed task"""
        if task_id in self.checkpoints:
            checkpoint_id = self.checkpoints[task_id]
            checkpoint_path = self.checkpoint_dir / f"{checkpoint_id}.ckpt"
            if checkpoint_path.exists():
                checkpoint_path.unlink()
            del self.checkpoints[task_id]

class DataPartitioner:
    """Handles data partitioning for shuffle phase"""
    @staticmethod
    def hash_partition(key: Any, num_partitions: int) -> int:
        """Hash-based partitioning"""
        return hash(key) % num_partitions
    
    @staticmethod
    def range_partition(key: Any, ranges: List[Any]) -> int:
        """Range-based partitioning"""
        for i, range_end in enumerate(ranges):
            if key <= range_end:
                return i
        return len(ranges)

class SpeculativeExecutionManager:
    """Manages speculative execution for straggler tasks"""
    def __init__(self, threshold_multiplier: float = 1.5):
        self.threshold_multiplier = threshold_multiplier
        self.task_times = defaultdict(list)
        self.speculative_tasks = {}
    
    def record_task_time(self, task_type: TaskType, duration: float):
        """Record task completion time for baseline calculation"""
        self.task_times[task_type].append(duration)
        # Keep only recent times (last 10)
        if len(self.task_times[task_type]) > 10:
            self.task_times[task_type].pop(0)
    
    def should_speculate(self, task: Task, current_duration: float) -> bool:
        """Determine if a task should be speculated"""
        if task.task_type not in self.task_times:
            return False
        
        times = self.task_times[task.task_type]
        if len(times) < 3:  # Need enough samples
            return False
        
        avg_time = sum(times) / len(times)
        threshold = avg_time * self.threshold_multiplier
        
        return current_duration > threshold
    
    def mark_speculative(self, task_id: str, speculative_task_id: str):
        """Mark a task as having a speculative copy"""
        self.speculative_tasks[task_id] = speculative_task_id
    
    def is_speculative_winner(self, task_id: str, completed_task_id: str) -> bool:
        """Determine which task completed first (original or speculative)"""
        if task_id not in self.speculative_tasks:
            return True  # Original task
        
        return completed_task_id != self.speculative_tasks[task_id]

class DistributedFileSystem:
    """Simulates a distributed file system with data locality"""
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.file_locations = defaultdict(list)  # file -> list of worker_ids
        self.replication_factor = 3
    
    def write_file(self, filename: str, data: str, worker_id: Optional[str] = None) -> str:
        """Write file to DFS with optional worker affinity"""
        file_path = self.base_dir / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w') as f:
            f.write(data)
        
        # Record location
        if worker_id:
            self.file_locations[filename].append(worker_id)
        
        # Simulate replication
        self._replicate_file(filename)
        
        return str(file_path)
    
    def _replicate_file(self, filename: str):
        """Simulate file replication across nodes"""
        if filename not in self.file_locations:
            return
        
        current_locations = self.file_locations[filename]
        if len(current_locations) < self.replication_factor:
            # In real DFS, this would copy to other nodes
            pass
    
    def get_file_locations(self, filename: str) -> List[str]:
        """Get list of workers that have this file"""
        return self.file_locations.get(filename, [])
    
    def read_file(self, filename: str) -> str:
        """Read file from DFS"""
        file_path = self.base_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"File {filename} not found in DFS")
        
        with open(file_path, 'r') as f:
            return f.read()

class MapReduceFramework:
    """Complete MapReduce framework with fault tolerance and optimizations"""
    
    def __init__(self, 
                 num_workers: int = 4,
                 checkpoint_dir: str = None,
                 dfs_dir: str = None,
                 enable_speculative_execution: bool = True,
                 enable_combiner: bool = True,
                 enable_checkpointing: bool = True):
        
        self.num_workers = num_workers
        self.enable_speculative_execution = enable_speculative_execution
        self.enable_combiner = enable_combiner
        self.enable_checkpointing = enable_checkpointing
        
        # Initialize components
        self.checkpoint_dir = checkpoint_dir or tempfile.mkdtemp(prefix="mapreduce_checkpoints_")
        self.dfs_dir = dfs_dir or tempfile.mkdtemp(prefix="mapreduce_dfs_")
        
        self.checkpoint_manager = CheckpointManager(self.checkpoint_dir)
        self.dfs = DistributedFileSystem(self.dfs_dir)
        self.speculative_manager = SpeculativeExecutionManager()
        
        # Worker management
        self.workers = {}
        self.worker_lock = threading.Lock()
        self._initialize_workers()
        
        # Task management
        self.tasks = {}
        self.task_queue = []
        self.completed_tasks = []
        self.failed_tasks = []
        
        # Job state
        self.job_id = None
        self.mapper_class = None
        self.reducer_class = None
        self.combiner_class = None
        self.num_map_tasks = 0
        self.num_reduce_tasks = 0
        
        # Thread pool for task execution
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        
        logger.info(f"Initialized MapReduce framework with {num_workers} workers")
    
    def _initialize_workers(self):
        """Initialize worker nodes"""
        for i in range(self.num_workers):
            worker_id = f"worker_{i}"
            self.workers[worker_id] = WorkerNode(
                worker_id=worker_id,
                host=f"node_{i}",
                data_local_files=[]
            )
    
    def _calculate_data_locality(self, task: Task) -> float:
        """Calculate data locality score for task assignment"""
        if not task.input_files:
            return 0.0
        
        locality_scores = []
        for worker_id, worker in self.workers.items():
            if not worker.is_alive:
                continue
            
            local_files = sum(1 for f in task.input_files if f in worker.data_local_files)
            score = local_files / len(task.input_files)
            locality_scores.append((worker_id, score))
        
        if not locality_scores:
            return 0.0
        
        # Return best locality score
        return max(score for _, score in locality_scores)
    
    def _assign_task_to_worker(self, task: Task) -> Optional[str]:
        """Assign task to worker based on data locality and load balancing"""
        best_worker = None
        best_score = -1
        
        with self.worker_lock:
            for worker_id, worker in self.workers.items():
                if not worker.is_alive:
                    continue
                
                # Calculate locality score
                locality_score = 0.0
                for file in task.input_files:
                    if file in worker.data_local_files:
                        locality_score += 1.0
                locality_score = locality_score / len(task.input_files) if task.input_files else 0.0
                
                # Consider current load
                load_factor = 1.0 / (len(worker.current_tasks) + 1)
                combined_score = locality_score * 0.7 + load_factor * 0.3
                
                if combined_score > best_score:
                    best_score = combined_score
                    best_worker = worker_id
            
            if best_worker:
                self.workers[best_worker].current_tasks.append(task.task_id)
                task.worker_id = best_worker
                task.locality_score = best_score
        
        return best_worker
    
    def _execute_map_task(self, task: Task) -> List[Tuple[Any, Any]]:
        """Execute a map task with fault tolerance"""
        mapper = self.mapper_class()
        intermediate_results = []
        
        # Try to load from checkpoint
        if self.enable_checkpointing:
            checkpoint_data = self.checkpoint_manager.load_checkpoint(task.task_id)
            if checkpoint_data:
                logger.info(f"Resuming map task {task.task_id} from checkpoint")
                return checkpoint_data
        
        try:
            mapper.setup()
            
            for input_file in task.input_files:
                # Read input from DFS
                input_data = self.dfs.read_file(input_file)
                
                # Process each line as key-value pair (line number -> content)
                for line_num, line in enumerate(input_data.split('\n')):
                    if line.strip():
                        results = mapper.map(line_num, line)
                        intermediate_results.extend(results)
                        
                        # Periodic checkpointing
                        if (self.enable_checkpointing and 
                            len(intermediate_results) % 1000 == 0):
                            self.checkpoint_manager.save_checkpoint(
                                task.task_id, intermediate_results
                            )
            
            mapper.cleanup()
            
            # Apply combiner if enabled
            if self.enable_combiner and self.combiner_class:
                intermediate_results = self._apply_combiner(intermediate_results)
            
            # Save final checkpoint
            if self.enable_checkpointing:
                self.checkpoint_manager.save_checkpoint(task.task_id, intermediate_results)
            
            return intermediate_results
            
        except Exception as e:
            logger.error(f"Map task {task.task_id} failed: {e}")
            raise MapReduceException(f"Map task failed: {e}")
    
    def _apply_combiner(self, intermediate_results: List[Tuple[Any, Any]]) -> List[Tuple[Any, Any]]:
        """Apply combiner to intermediate results"""
        combiner = self.combiner_class()
        
        # Group by key
        grouped = defaultdict(list)
        for key, value in intermediate_results:
            grouped[key].append(value)
        
        # Apply combiner
        combined_results = []
        for key, values in grouped.items():
            combined = combiner.combine(key, values)
            combined_results.extend(combined)
        
        return combined_results
    
    def _shuffle_and_sort(self, map_results: List[Tuple[Any, Any]]) -> Dict[Any, List[Any]]:
        """Shuffle and sort phase: group values by key"""
        shuffled = defaultdict(list)
        
        for key, value in map_results:
            shuffled[key].append(value)
        
        # Sort keys for reducer
        sorted_keys = sorted(shuffled.keys())
        return {key: shuffled[key] for key in sorted_keys}
    
    def _execute_reduce_task(self, task: Task, 
                            grouped_data: Dict[Any, List[Any]]) -> List[Tuple[Any, Any]]:
        """Execute a reduce task with fault tolerance"""
        reducer = self.reducer_class()
        output_results = []
        
        # Try to load from checkpoint
        if self.enable_checkpointing:
            checkpoint_data = self.checkpoint_manager.load_checkpoint(task.task_id)
            if checkpoint_data:
                logger.info(f"Resuming reduce task {task.task_id} from checkpoint")
                return checkpoint_data
        
        try:
            reducer.setup()
            
            # Process each key group
            for key, values in grouped_data.items():
                results = reducer.reduce(key, values)
                output_results.extend(results)
                
                # Periodic checkpointing
                if (self.enable_checkpointing and 
                    len(output_results) % 1000 == 0):
                    self.checkpoint_manager.save_checkpoint(
                        task.task_id, output_results
                    )
            
            reducer.cleanup()
            
            # Save final checkpoint
            if self.enable_checkpointing:
                self.checkpoint_manager.save_checkpoint(task.task_id, output_results)
            
            return output_results
            
        except Exception as e:
            logger.error(f"Reduce task {task.task_id} failed: {e}")
            raise MapReduceException(f"Reduce task failed: {e}")
    
    def _handle_task_failure(self, task: Task, error: Exception):
        """Handle task failure with retry logic"""
        task.attempts += 1
        task.status = TaskStatus.FAILED
        
        logger.warning(f"Task {task.task_id} failed (attempt {task.attempts}): {error}")
        
        # Clean up checkpoint on final failure
        if task.attempts >= 3:  # Max retries
            self.failed_tasks.append(task)
            if self.enable_checkpointing:
                self.checkpoint_manager.cleanup_checkpoints(task.task_id)
            logger.error(f"Task {task.task_id} permanently failed after {task.attempts} attempts")
        else:
            # Requeue for retry
            task.status = TaskStatus.PENDING
            self.task_queue.append(task)
            logger.info(f"Requeued task {task.task_id} for retry")
    
    def _monitor_speculative_tasks(self):
        """Monitor tasks for speculative execution"""
        if not self.enable_speculative_execution:
            return
        
        current_time = time.time()
        
        for task_id, task in list(self.tasks.items()):
            if task.status != TaskStatus.RUNNING:
                continue
            
            if task.start_time is None:
                continue
            
            duration = current_time - task.start_time
            
            # Check if we should speculate
            if self.speculative_manager.should_speculate(task, duration):
                logger.info(f"Starting speculative execution for task {task_id}")
                
                # Create speculative task
                speculative_task = Task(
                    task_id=f"{task_id}_speculative",
                    task_type=task.task_type,
                    input_files=task.input_files,
                    output_dir=task.output_dir
                )
                
                # Assign to different worker
                worker_id = self._assign_task_to_worker(speculative_task)
                if worker_id:
                    self.tasks[speculative_task.task_id] = speculative_task
                    self.speculative_manager.mark_speculative(task_id, speculative_task.task_id)
                    
                    # Submit speculative task
                    self._submit_task(speculative_task)
    
    def _submit_task(self, task: Task):
        """Submit a task for execution"""
        task.status = TaskStatus.RUNNING
        task.start_time = time.time()
        
        # Submit to thread pool
        future = self.executor.submit(self._execute_task, task)
        future.add_done_callback(lambda f: self._task_completed(task, f))
    
    def _execute_task(self, task: Task) -> Any:
        """Execute a single task"""
        try:
            if task.task_type == TaskType.MAP:
                return self._execute_map_task(task)
            elif task.task_type == TaskType.REDUCE:
                # For reduce tasks, we need the shuffled data
                # This would normally come from the map phase
                return []
            else:
                raise ValueError(f"Unknown task type: {task.task_type}")
        except Exception as e:
            raise e
    
    def _task_completed(self, task: Task, future):
        """Handle task completion"""
        try:
            result = future.result()
            task.end_time = time.time()
            duration = task.end_time - task.start_time
            
            # Record time for speculative execution baseline
            self.speculative_manager.record_task_time(task.task_type, duration)
            
            # Check if this was a speculative task
            original_task_id = task.task_id.replace("_speculative", "")
            if original_task_id in self.tasks:
                original_task = self.tasks[original_task_id]
                
                if not self.speculative_manager.is_speculative_winner(
                    original_task_id, task.task_id):
                    # Original task won, kill speculative
                    task.status = TaskStatus.KILLED
                    logger.info(f"Killed speculative task {task.task_id}, original completed first")
                    return
            
            task.status = TaskStatus.COMPLETED
            self.completed_tasks.append(task)
            
            # Clean up checkpoint
            if self.enable_checkpointing:
                self.checkpoint_manager.cleanup_checkpoints(task.task_id)
            
            # Update worker
            with self.worker_lock:
                if task.worker_id in self.workers:
                    self.workers[task.worker_id].current_tasks.remove(task.task_id)
            
            logger.info(f"Task {task.task_id} completed in {duration:.2f}s")
            
        except Exception as e:
            self._handle_task_failure(task, e)
    
    def run_job(self, 
                input_files: List[str],
                mapper_class: type,
                reducer_class: type,
                combiner_class: Optional[type] = None,
                num_reduce_tasks: int = 2,
                output_dir: str = None) -> Dict[Any, Any]:
        """Run a complete MapReduce job"""
        
        # Initialize job
        self.job_id = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        self.combiner_class = combiner_class
        self.num_reduce_tasks = num_reduce_tasks
        
        output_dir = output_dir or tempfile.mkdtemp(prefix=f"mapreduce_output_{self.job_id}_")
        
        logger.info(f"Starting MapReduce job {self.job_id}")
        logger.info(f"Input files: {len(input_files)}")
        logger.info(f"Map tasks: {len(input_files)}, Reduce tasks: {num_reduce_tasks}")
        
        # Create map tasks
        map_tasks = []
        for i, input_file in enumerate(input_files):
            task = Task(
                task_id=f"map_{i}",
                task_type=TaskType.MAP,
                input_files=[input_file],
                output_dir=os.path.join(output_dir, "map_output")
            )
            map_tasks.append(task)
            self.tasks[task.task_id] = task
        
        # Execute map phase
        logger.info("Starting map phase")
        map_results = []
        
        # Submit all map tasks
        for task in map_tasks:
            self.task_queue.append(task)
        
        # Process tasks
        while self.task_queue or any(t.status == TaskStatus.RUNNING for t in self.tasks.values()):
            # Monitor for speculative execution
            self._monitor_speculative_tasks()
            
            # Assign pending tasks
            while self.task_queue:
                task = self.task_queue.pop(0)
                worker_id = self._assign_task_to_worker(task)
                if worker_id:
                    self._submit_task(task)
                else:
                    self.task_queue.append(task)  # Requeue if no worker available
            
            time.sleep(0.1)  # Small delay to prevent busy waiting
        
        # Collect map results
        for task in map_tasks:
            if task.status == TaskStatus.COMPLETED:
                checkpoint_data = self.checkpoint_manager.load_checkpoint(task.task_id)
                if checkpoint_data:
                    map_results.extend(checkpoint_data)
        
        # Shuffle and sort
        logger.info("Starting shuffle and sort phase")
        shuffled_data = self._shuffle_and_sort(map_results)
        
        # Create reduce tasks
        reduce_tasks = []
        for i in range(num_reduce_tasks):
            task = Task(
                task_id=f"reduce_{i}",
                task_type=TaskType.REDUCE,
                input_files=[],  # Reduce tasks get data from shuffle
                output_dir=os.path.join(output_dir, "reduce_output")
            )
            reduce_tasks.append(task)
            self.tasks[task.task_id] = task
        
        # Partition data for reducers
        partitioned_data = defaultdict(dict)
        for key, values in shuffled_data.items():
            partition = DataPartitioner.hash_partition(key, num_reduce_tasks)
            partitioned_data[partition][key] = values
        
        # Execute reduce phase
        logger.info("Starting reduce phase")
        final_results = {}
        
        # Submit reduce tasks with their data
        futures = []
        for i, task in enumerate(reduce_tasks):
            if i in partitioned_data:
                future = self.executor.submit(
                    self._execute_reduce_task, task, partitioned_data[i]
                )
                futures.append((task, future))
        
        # Collect reduce results
        for task, future in futures:
            try:
                results = future.result(timeout=300)  # 5 minute timeout
                for key, value in results:
                    final_results[key] = value
                task.status = TaskStatus.COMPLETED
            except Exception as e:
                self._handle_task_failure(task, e)
        
        # Write final output
        output_file = os.path.join(output_dir, "final_output.txt")
        with open(output_file, 'w') as f:
            for key, value in sorted(final_results.items()):
                f.write(f"{key}\t{value}\n")
        
        logger.info(f"Job {self.job_id} completed. Output written to {output_file}")
        
        # Cleanup
        self.executor.shutdown(wait=False)
        
        return final_results
    
    def get_job_stats(self) -> Dict[str, Any]:
        """Get statistics about the job execution"""
        stats = {
            'total_tasks': len(self.tasks),
            'completed_tasks': len(self.completed_tasks),
            'failed_tasks': len(self.failed_tasks),
            'map_tasks': sum(1 for t in self.tasks.values() if t.task_type == TaskType.MAP),
            'reduce_tasks': sum(1 for t in self.tasks.values() if t.task_type == TaskType.REDUCE),
            'speculative_tasks': sum(1 for t in self.tasks.values() if 'speculative' in t.task_id),
            'avg_locality_score': 0.0,
            'total_duration': 0.0
        }
        
        if self.completed_tasks:
            stats['avg_locality_score'] = sum(t.locality_score for t in self.completed_tasks) / len(self.completed_tasks)
            durations = [t.end_time - t.start_time for t in self.completed_tasks if t.end_time and t.start_time]
            if durations:
                stats['total_duration'] = max(durations)  # Wall clock time
        
        return stats


# Example Mapper and Reducer implementations for testing
class WordCountMapper(Mapper):
    def map(self, key: Any, value: Any) -> List[Tuple[str, int]]:
        words = value.lower().split()
        return [(word, 1) for word in words if word]

class WordCountReducer(Reducer):
    def reduce(self, key: str, values: List[int]) -> List[Tuple[str, int]]:
        return [(key, sum(values))]

class WordCountCombiner(Combiner):
    def combine(self, key: str, values: List[int]) -> List[Tuple[str, int]]:
        return [(key, sum(values))]


class TestMapReduceFramework(unittest.TestCase):
    """Test cases for the complete MapReduce framework"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.test_dir, "input")
        self.output_dir = os.path.join(self.test_dir, "output")
        self.checkpoint_dir = os.path.join(self.test_dir, "checkpoints")
        self.dfs_dir = os.path.join(self.test_dir, "dfs")
        
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create test input files
        self.test_files = []
        for i in range(3):
            filename = os.path.join(self.input_dir, f"input_{i}.txt")
            with open(filename, 'w') as f:
                f.write(f"hello world\nhello mapreduce\nworld hello test\n")
            self.test_files.append(filename)
        
        # Initialize framework
        self.framework = MapReduceFramework(
            num_workers=2,
            checkpoint_dir=self.checkpoint_dir,
            dfs_dir=self.dfs_dir,
            enable_speculative_execution=True,
            enable_combiner=True,
            enable_checkpointing=True
        )
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_basic_mapreduce(self):
        """Test basic MapReduce functionality"""
        results = self.framework.run_job(
            input_files=self.test_files,
            mapper_class=WordCountMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        
        # Verify results
        self.assertIsInstance(results, dict)
        self.assertIn('hello', results)
        self.assertIn('world', results)
        self.assertEqual(results['hello'], 6)  # 3 files * 2 occurrences each
        self.assertEqual(results['world'], 3)  # 3 files * 1 occurrence each
    
    def test_combiner_optimization(self):
        """Test that combiner reduces data transfer"""
        # Run with combiner
        results_with_combiner = self.framework.run_job(
            input_files=self.test_files,
            mapper_class=WordCountMapper,
            reducer_class=WordCountReducer,
            combiner_class=WordCountCombiner,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        
        # Run without combiner
        framework_no_combiner = MapReduceFramework(
            num_workers=2,
            checkpoint_dir=self.checkpoint_dir,
            dfs_dir=self.dfs_dir,
            enable_speculative_execution=False,
            enable_combiner=False,
            enable_checkpointing=False
        )
        
        results_without_combiner = framework_no_combiner.run_job(
            input_files=self.test_files,
            mapper_class=WordCountMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        
        # Results should be the same
        self.assertEqual(results_with_combiner, results_without_combiner)
    
    def test_fault_tolerance(self):
        """Test checkpointing and recovery"""
        # Create a mapper that fails on first attempt
        class FailingMapper(Mapper):
            attempt_count = 0
            
            def map(self, key: Any, value: Any) -> List[Tuple[str, int]]:
                FailingMapper.attempt_count += 1
                if FailingMapper.attempt_count <= 2:  # Fail first two attempts
                    raise Exception("Simulated failure")
                return super().map(key, value)
        
        # Run job with failing mapper
        results = self.framework.run_job(
            input_files=self.test_files,
            mapper_class=FailingMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        
        # Job should still complete despite failures
        self.assertIsInstance(results, dict)
        self.assertIn('hello', results)
    
    def test_speculative_execution(self):
        """Test speculative execution for stragglers"""
        # Create a slow mapper
        class SlowMapper(Mapper):
            def map(self, key: Any, value: Any) -> List[Tuple[str, int]]:
                time.sleep(0.5)  # Simulate slow processing
                return super().map(key, value)
        
        # Run with speculative execution enabled
        start_time = time.time()
        results = self.framework.run_job(
            input_files=self.test_files,
            mapper_class=SlowMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        duration_with_speculative = time.time() - start_time
        
        # Run without speculative execution
        framework_no_speculative = MapReduceFramework(
            num_workers=2,
            checkpoint_dir=self.checkpoint_dir,
            dfs_dir=self.dfs_dir,
            enable_speculative_execution=False,
            enable_combiner=True,
            enable_checkpointing=True
        )
        
        start_time = time.time()
        results_no_speculative = framework_no_speculative.run_job(
            input_files=self.test_files,
            mapper_class=SlowMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        duration_without_speculative = time.time() - start_time
        
        # Speculative execution should help (or at least not hurt)
        self.assertLessEqual(duration_with_speculative, duration_without_speculative * 1.5)
    
    def test_data_locality(self):
        """Test data locality optimization"""
        # Simulate data locality by marking files on specific workers
        for i, file_path in enumerate(self.test_files):
            filename = os.path.basename(file_path)
            worker_id = f"worker_{i % 2}"  # Alternate between workers
            self.framework.workers[worker_id].data_local_files.append(filename)
        
        # Run job
        results = self.framework.run_job(
            input_files=self.test_files,
            mapper_class=WordCountMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        
        # Check that locality scores are reasonable
        stats = self.framework.get_job_stats()
        self.assertGreater(stats['avg_locality_score'], 0.0)
    
    def test_large_scale_job(self):
        """Test with larger input"""
        # Create more input files
        large_input_files = []
        for i in range(10):
            filename = os.path.join(self.input_dir, f"large_input_{i}.txt")
            with open(filename, 'w') as f:
                # Generate larger content
                for j in range(100):
                    f.write(f"word{j} test{ j % 10} data{ j % 5}\n")
            large_input_files.append(filename)
        
        # Run job
        results = self.framework.run_job(
            input_files=large_input_files,
            mapper_class=WordCountMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=4,
            output_dir=self.output_dir
        )
        
        # Verify some expected results
        self.assertIsInstance(results, dict)
        self.assertGreater(len(results), 0)
        
        # Check job statistics
        stats = self.framework.get_job_stats()
        self.assertEqual(stats['total_tasks'], 10 + 4)  # 10 map + 4 reduce
        self.assertEqual(stats['completed_tasks'], 14)
    
    def test_job_statistics(self):
        """Test job statistics collection"""
        results = self.framework.run_job(
            input_files=self.test_files,
            mapper_class=WordCountMapper,
            reducer_class=WordCountReducer,
            num_reduce_tasks=2,
            output_dir=self.output_dir
        )
        
        stats = self.framework.get_job_stats()
        
        self.assertIn('total_tasks', stats)
        self.assertIn('completed_tasks', stats)
        self.assertIn('failed_tasks', stats)
        self.assertIn('map_tasks', stats)
        self.assertIn('reduce_tasks', stats)
        self.assertIn('avg_locality_score', stats)
        self.assertIn('total_duration', stats)
        
        self.assertEqual(stats['map_tasks'], 3)
        self.assertEqual(stats['reduce_tasks'], 2)
        self.assertEqual(stats['completed_tasks'], 5)


class TestCheckpointManager(unittest.TestCase):
    """Test checkpoint manager functionality"""
    
    def setUp(self):
        self.checkpoint_dir = tempfile.mkdtemp()
        self.manager = CheckpointManager(self.checkpoint_dir)
    
    def tearDown(self):
        shutil.rmtree(self.checkpoint_dir, ignore_errors=True)
    
    def test_save_and_load_checkpoint(self):
        """Test saving and loading checkpoints"""
        task_id = "test_task_1"
        test_data = {"key": "value", "numbers": [1, 2, 3]}
        
        # Save checkpoint
        checkpoint_id = self.manager.save_checkpoint(task_id, test_data)
        self.assertIsNotNone(checkpoint_id)
        
        # Load checkpoint
        loaded_data = self.manager.load_checkpoint(task_id)
        self.assertEqual(loaded_data, test_data)
    
    def test_cleanup_checkpoints(self):
        """Test checkpoint cleanup"""
        task_id = "test_task_2"
        test_data = {"data": "to_cleanup"}
        
        # Save and then cleanup
        self.manager.save_checkpoint(task_id, test_data)
        self.manager.cleanup_checkpoints(task_id)
        
        # Should not be able to load after cleanup
        loaded_data = self.manager.load_checkpoint(task_id)
        self.assertIsNone(loaded_data)


class TestDataPartitioner(unittest.TestCase):
    """Test data partitioning strategies"""
    
    def test_hash_partition(self):
        """Test hash-based partitioning"""
        # Test with various keys
        partitions = set()
        for i in range(100):
            partition = DataPartitioner.hash_partition(f"key_{i}", 4)
            partitions.add(partition)
            self.assertGreaterEqual(partition, 0)
            self.assertLess(partition, 4)
        
        # Should distribute across partitions
        self.assertGreater(len(partitions), 1)
    
    def test_range_partition(self):
        """Test range-based partitioning"""
        ranges = [10, 20, 30, 40]
        
        self.assertEqual(DataPartitioner.range_partition(5, ranges), 0)
        self.assertEqual(DataPartitioner.range_partition(15, ranges), 1)
        self.assertEqual(DataPartitioner.range_partition(25, ranges), 2)
        self.assertEqual(DataPartitioner.range_partition(35, ranges), 3)
        self.assertEqual(DataPartitioner.range_partition(45, ranges), 4)


class TestSpeculativeExecutionManager(unittest.TestCase):
    """Test speculative execution manager"""
    
    def setUp(self):
        self.manager = SpeculativeExecutionManager(threshold_multiplier=1.5)
    
    def test_record_task_time(self):
        """Test recording task times"""
        self.manager.record_task_time(TaskType.MAP, 10.0)
        self.manager.record_task_time(TaskType.MAP, 12.0)
        self.manager.record_task_time(TaskType.MAP, 8.0)
        
        self.assertEqual(len(self.manager.task_times[TaskType.MAP]), 3)
    
    def test_should_speculate(self):
        """Test speculation decision"""
        # Record some baseline times
        for _ in range(5):
            self.manager.record_task_time(TaskType.MAP, 10.0)
        
        # Create a task
        task = Task(
            task_id="test_task",
            task_type=TaskType.MAP,
            input_files=[],
            output_dir=""
        )
        
        # Should not speculate for normal duration
        self.assertFalse(self.manager.should_speculate(task, 12.0))
        
        # Should speculate for slow duration
        self.assertTrue(self.manager.should_speculate(task, 20.0))


if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)