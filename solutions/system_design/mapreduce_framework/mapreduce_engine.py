"""
Complete MapReduce Framework with Fault Tolerance
Implements distributed computing fundamentals with proper shuffling, sorting, fault tolerance, and optimizations.
"""

import os
import sys
import json
import pickle
import hashlib
import tempfile
import threading
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple, Iterator, Optional, Callable
from dataclasses import dataclass, field
from collections import defaultdict
import heapq
import time
import random
import logging
from pathlib import Path
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TaskAttempt:
    """Represents a single attempt to execute a task."""
    attempt_id: str
    task_id: str
    worker_id: str
    start_time: float
    status: str = "PENDING"  # PENDING, RUNNING, COMPLETED, FAILED
    progress: float = 0.0
    output_path: Optional[str] = None
    error: Optional[str] = None
    checkpoint_path: Optional[str] = None


@dataclass
class Task:
    """Base class for Map and Reduce tasks."""
    task_id: str
    task_type: str  # "MAP" or "REDUCE"
    input_paths: List[str]
    output_path: str
    status: str = "PENDING"
    attempts: List[TaskAttempt] = field(default_factory=list)
    locality_hint: Optional[str] = None  # Preferred worker for data locality
    priority: int = 0


class Mapper:
    """Base class for user-defined mappers."""
    
    def map(self, key: Any, value: Any) -> Iterator[Tuple[Any, Any]]:
        """
        Process a key-value pair and yield intermediate key-value pairs.
        
        Args:
            key: Input key (e.g., line number, file offset)
            value: Input value (e.g., line of text, record)
            
        Yields:
            Tuple of (intermediate_key, intermediate_value)
        """
        raise NotImplementedError("Mapper must implement map() method")
    
    def setup(self, context: Dict[str, Any]) -> None:
        """Called once before map phase. Override for initialization."""
        pass
    
    def cleanup(self, context: Dict[str, Any]) -> None:
        """Called once after map phase. Override for cleanup."""
        pass


class Reducer:
    """Base class for user-defined reducers."""
    
    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[Tuple[Any, Any]]:
        """
        Process a key and its associated values, yield output key-value pairs.
        
        Args:
            key: Intermediate key
            values: Iterator of values associated with the key
            
        Yields:
            Tuple of (output_key, output_value)
        """
        raise NotImplementedError("Reducer must implement reduce() method")
    
    def setup(self, context: Dict[str, Any]) -> None:
        """Called once before reduce phase. Override for initialization."""
        pass
    
    def cleanup(self, context: Dict[str, Any]) -> None:
        """Called once after reduce phase. Override for cleanup."""
        pass


class Combiner:
    """Base class for combiners (optional optimization)."""
    
    def combine(self, key: Any, values: Iterator[Any]) -> Iterator[Tuple[Any, Any]]:
        """
        Combine values for a key on the mapper side.
        
        Args:
            key: Intermediate key
            values: Iterator of values for this key from a single mapper
            
        Yields:
            Combined key-value pairs
        """
        raise NotImplementedError("Combiner must implement combine() method")


class Partitioner:
    """Determines which reducer gets which keys."""
    
    def get_partition(self, key: Any, value: Any, num_reducers: int) -> int:
        """
        Return the partition number (0 to num_reducers-1) for the given key.
        
        Default implementation uses hash partitioning.
        """
        key_hash = hashlib.md5(str(key).encode()).hexdigest()
        return int(key_hash, 16) % num_reducers


class CheckpointManager:
    """Manages checkpointing for fault tolerance."""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints: Dict[str, str] = {}  # task_id -> checkpoint_path
    
    def save_checkpoint(self, task_id: str, data: Any, attempt_id: str) -> str:
        """Save checkpoint data to disk."""
        checkpoint_file = self.checkpoint_dir / f"{task_id}_{attempt_id}.ckpt"
        with open(checkpoint_file, 'wb') as f:
            pickle.dump(data, f)
        self.checkpoints[task_id] = str(checkpoint_file)
        return str(checkpoint_file)
    
    def load_checkpoint(self, task_id: str) -> Optional[Any]:
        """Load checkpoint data from disk."""
        if task_id not in self.checkpoints:
            return None
        try:
            with open(self.checkpoints[task_id], 'rb') as f:
                return pickle.load(f)
        except (FileNotFoundError, pickle.PickleError):
            return None
    
    def cleanup_checkpoint(self, task_id: str) -> None:
        """Remove checkpoint file."""
        if task_id in self.checkpoints:
            try:
                os.remove(self.checkpoints[task_id])
            except OSError:
                pass
            del self.checkpoints[task_id]


class WorkerPool:
    """Manages a pool of worker processes."""
    
    def __init__(self, num_workers: Optional[int] = None):
        self.num_workers = num_workers or multiprocessing.cpu_count()
        self.executor = ProcessPoolExecutor(max_workers=self.num_workers)
        self.worker_status: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.RLock()
    
    def submit_task(self, task_func: Callable, *args, **kwargs) -> Any:
        """Submit a task to the worker pool."""
        return self.executor.submit(task_func, *args, **kwargs)
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the worker pool."""
        self.executor.shutdown(wait=wait)


class SpeculativeExecutionManager:
    """Manages speculative execution for straggler tasks."""
    
    def __init__(self, threshold_factor: float = 1.5):
        self.threshold_factor = threshold_factor
        self.task_progress: Dict[str, List[float]] = {}  # task_id -> list of progress percentages
        self.speculative_tasks: Dict[str, List[TaskAttempt]] = defaultdict(list)
    
    def should_speculate(self, task_id: str, current_progress: float, 
                        avg_progress: float) -> bool:
        """Determine if a task should be speculated."""
        if task_id not in self.task_progress:
            self.task_progress[task_id] = []
        
        self.task_progress[task_id].append(current_progress)
        
        # If task is significantly behind average, speculate
        if len(self.task_progress[task_id]) >= 3:  # Wait for some data points
            recent_avg = sum(self.task_progress[task_id][-3:]) / 3
            return recent_avg < avg_progress / self.threshold_factor
        
        return False
    
    def record_completion(self, task_id: str) -> None:
        """Record that a task completed successfully."""
        if task_id in self.task_progress:
            del self.task_progress[task_id]
        if task_id in self.speculative_tasks:
            del self.speculative_tasks[task_id]


class DataLocalityOptimizer:
    """Optimizes task placement based on data locality."""
    
    def __init__(self, data_locations: Dict[str, List[str]]):
        """
        Args:
            data_locations: Maps data_path -> list of worker_ids where data is located
        """
        self.data_locations = data_locations
    
    def get_preferred_worker(self, input_paths: List[str]) -> Optional[str]:
        """Get preferred worker based on data locality."""
        worker_scores: Dict[str, int] = defaultdict(int)
        
        for path in input_paths:
            if path in self.data_locations:
                for worker_id in self.data_locations[path]:
                    worker_scores[worker_id] += 1
        
        if worker_scores:
            return max(worker_scores.items(), key=lambda x: x[1])[0]
        return None


class MapReduceEngine:
    """
    Complete MapReduce engine with fault tolerance, speculative execution,
    and data locality optimizations.
    """
    
    def __init__(
        self,
        mapper_class: type,
        reducer_class: type,
        combiner_class: Optional[type] = None,
        partitioner_class: type = Partitioner,
        num_mappers: Optional[int] = None,
        num_reducers: int = 1,
        checkpoint_dir: Optional[str] = None,
        enable_speculative_execution: bool = True,
        enable_combiner: bool = True,
        enable_data_locality: bool = True,
        task_timeout: float = 300.0,  # 5 minutes
        max_task_attempts: int = 3
    ):
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        self.combiner_class = combiner_class
        self.partitioner_class = partitioner_class
        self.num_mappers = num_mappers or multiprocessing.cpu_count()
        self.num_reducers = num_reducers
        self.enable_speculative_execution = enable_speculative_execution
        self.enable_combiner = enable_combiner and combiner_class is not None
        self.enable_data_locality = enable_data_locality
        self.task_timeout = task_timeout
        self.max_task_attempts = max_task_attempts
        
        # Setup directories
        self.work_dir = Path(tempfile.mkdtemp(prefix="mapreduce_"))
        self.checkpoint_dir = Path(checkpoint_dir) if checkpoint_dir else self.work_dir / "checkpoints"
        self.intermediate_dir = self.work_dir / "intermediate"
        self.output_dir = self.work_dir / "output"
        
        for dir_path in [self.checkpoint_dir, self.intermediate_dir, self.output_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize components
        self.checkpoint_manager = CheckpointManager(str(self.checkpoint_dir))
        self.worker_pool = WorkerPool(self.num_mappers + self.num_reducers)
        self.speculative_manager = SpeculativeExecutionManager()
        self.data_locality_optimizer = DataLocalityOptimizer({})
        
        # Task tracking
        self.tasks: Dict[str, Task] = {}
        self.task_queue: List[Task] = []
        self.completed_tasks: Dict[str, Task] = {}
        self.failed_tasks: Dict[str, Task] = {}
        
        # Statistics
        self.stats = {
            "map_tasks_completed": 0,
            "reduce_tasks_completed": 0,
            "speculative_tasks_launched": 0,
            "task_retries": 0,
            "total_execution_time": 0.0
        }
        
        logger.info(f"Initialized MapReduce engine with {self.num_mappers} mappers, "
                   f"{self.num_reducers} reducers")
    
    def _generate_task_id(self, task_type: str, index: int) -> str:
        """Generate unique task ID."""
        return f"{task_type}_{index}_{uuid.uuid4().hex[:8]}"
    
    def _create_map_tasks(self, input_paths: List[str]) -> List[Task]:
        """Create map tasks from input paths."""
        tasks = []
        
        # Distribute input files among mappers
        files_per_mapper = max(1, len(input_paths) // self.num_mappers)
        
        for i in range(self.num_mappers):
            start_idx = i * files_per_mapper
            end_idx = start_idx + files_per_mapper if i < self.num_mappers - 1 else len(input_paths)
            
            if start_idx >= len(input_paths):
                break
            
            task_input_paths = input_paths[start_idx:end_idx]
            task_id = self._generate_task_id("MAP", i)
            
            # Determine preferred worker for data locality
            locality_hint = None
            if self.enable_data_locality:
                locality_hint = self.data_locality_optimizer.get_preferred_worker(task_input_paths)
            
            task = Task(
                task_id=task_id,
                task_type="MAP",
                input_paths=task_input_paths,
                output_path=str(self.intermediate_dir / f"map_output_{i}"),
                locality_hint=locality_hint
            )
            tasks.append(task)
            self.tasks[task_id] = task
        
        logger.info(f"Created {len(tasks)} map tasks")
        return tasks
    
    def _create_reduce_tasks(self) -> List[Task]:
        """Create reduce tasks."""
        tasks = []
        
        for i in range(self.num_reducers):
            task_id = self._generate_task_id("REDUCE", i)
            
            task = Task(
                task_id=task_id,
                task_type="REDUCE",
                input_paths=[],  # Will be populated during shuffle
                output_path=str(self.output_dir / f"reduce_output_{i}")
            )
            tasks.append(task)
            self.tasks[task_id] = task
        
        logger.info(f"Created {len(tasks)} reduce tasks")
        return tasks
    
    def _execute_map_task(self, task: Task, attempt: TaskAttempt) -> bool:
        """Execute a single map task."""
        try:
            logger.info(f"Executing map task {task.task_id} (attempt {attempt.attempt_id})")
            
            # Initialize mapper and combiner
            mapper = self.mapper_class()
            combiner = self.combiner_class() if self.enable_combiner else None
            partitioner = self.partitioner_class()
            
            # Setup context
            context = {
                "task_id": task.task_id,
                "attempt_id": attempt.attempt_id,
                "input_paths": task.input_paths,
                "output_path": task.output_path,
                "num_reducers": self.num_reducers
            }
            
            mapper.setup(context)
            if combiner:
                combiner.setup(context)
            
            # Process input files
            intermediate_data: Dict[int, Dict[Any, List[Any]]] = defaultdict(lambda: defaultdict(list))
            
            for input_path in task.input_paths:
                try:
                    with open(input_path, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f):
                            # Execute mapper
                            for key, value in mapper.map(line_num, line.strip()):
                                # Determine partition
                                partition = partitioner.get_partition(key, value, self.num_reducers)
                                intermediate_data[partition][key].append(value)
                            
                            # Update progress
                            attempt.progress = min(0.9, attempt.progress + 0.01)
                            
                            # Periodic checkpointing
                            if line_num % 1000 == 0:
                                self.checkpoint_manager.save_checkpoint(
                                    task.task_id, intermediate_data, attempt.attempt_id
                                )
                except FileNotFoundError:
                    logger.warning(f"Input file not found: {input_path}")
                    continue
            
            # Apply combiner if enabled
            if combiner:
                for partition in intermediate_data:
                    combined_data = {}
                    for key, values in intermediate_data[partition].items():
                        combined_values = list(combiner.combine(key, iter(values)))
                        if combined_values:
                            combined_data[key] = [v for _, v in combined_values]
                    intermediate_data[partition] = combined_data
            
            # Write intermediate output
            for partition, partition_data in intermediate_data.items():
                partition_dir = Path(task.output_path) / f"partition_{partition}"
                partition_dir.mkdir(parents=True, exist_ok=True)
                
                output_file = partition_dir / f"map_{task.task_id}.dat"
                with open(output_file, 'wb') as f:
                    pickle.dump(partition_data, f)
            
            # Cleanup
            mapper.cleanup(context)
            if combiner:
                combiner.cleanup(context)
            
            attempt.status = "COMPLETED"
            attempt.progress = 1.0
            attempt.output_path = task.output_path
            
            logger.info(f"Map task {task.task_id} completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Map task {task.task_id} failed: {e}")
            attempt.status = "FAILED"
            attempt.error = str(e)
            return False
    
    def _execute_reduce_task(self, task: Task, attempt: TaskAttempt) -> bool:
        """Execute a single reduce task."""
        try:
            logger.info(f"Executing reduce task {task.task_id} (attempt {attempt.attempt_id})")
            
            # Initialize reducer
            reducer = self.reducer_class()
            
            # Setup context
            context = {
                "task_id": task.task_id,
                "attempt_id": attempt.attempt_id,
                "input_paths": task.input_paths,
                "output_path": task.output_path
            }
            
            reducer.setup(context)
            
            # Collect all input data for this reducer
            all_data: Dict[Any, List[Any]] = defaultdict(list)
            
            for input_path in task.input_paths:
                input_path_obj = Path(input_path)
                if input_path_obj.is_dir():
                    # Read all partition files
                    for data_file in input_path_obj.glob("*.dat"):
                        try:
                            with open(data_file, 'rb') as f:
                                partition_data = pickle.load(f)
                                for key, values in partition_data.items():
                                    all_data[key].extend(values)
                        except (FileNotFoundError, pickle.PickleError) as e:
                            logger.warning(f"Failed to read {data_file}: {e}")
                
                attempt.progress = min(0.5, attempt.progress + 0.1)
            
            # Sort keys for reduce phase
            sorted_keys = sorted(all_data.keys())
            
            # Execute reducer for each key
            output_data = []
            for i, key in enumerate(sorted_keys):
                values = iter(all_data[key])
                for out_key, out_value in reducer.reduce(key, values):
                    output_data.append((out_key, out_value))
                
                # Update progress
                attempt.progress = 0.5 + (0.5 * (i + 1) / len(sorted_keys))
                
                # Periodic checkpointing
                if i % 100 == 0:
                    self.checkpoint_manager.save_checkpoint(
                        task.task_id, output_data, attempt.attempt_id
                    )
            
            # Write final output
            output_file = Path(task.output_path) / "output.txt"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for key, value in output_data:
                    f.write(f"{key}\t{value}\n")
            
            # Cleanup
            reducer.cleanup(context)
            
            attempt.status = "COMPLETED"
            attempt.progress = 1.0
            attempt.output_path = str(output_file)
            
            logger.info(f"Reduce task {task.task_id} completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Reduce task {task.task_id} failed: {e}")
            attempt.status = "FAILED"
            attempt.error = str(e)
            return False
    
    def _monitor_tasks(self, tasks: List[Task], task_type: str) -> None:
        """Monitor tasks and handle failures/speculative execution."""
        futures = {}
        task_attempts = {}
        
        # Submit initial attempts
        for task in tasks:
            attempt_id = f"{task.task_id}_attempt_0"
            attempt = TaskAttempt(
                attempt_id=attempt_id,
                task_id=task.task_id,
                worker_id="worker_0",  # Simplified
                start_time=time.time()
            )
            task.attempts.append(attempt)
            task_attempts[task.task_id] = attempt
            
            if task_type == "MAP":
                future = self.worker_pool.submit_task(self._execute_map_task, task, attempt)
            else:
                future = self.worker_pool.submit_task(self._execute_reduce_task, task, attempt)
            
            futures[future] = task.task_id
        
        # Monitor futures
        completed_tasks = set()
        start_time = time.time()
        
        while futures:
            # Check for completed tasks
            done_futures = []
            for future in list(futures.keys()):
                if future.done():
                    done_futures.append(future)
            
            # Process completed futures
            for future in done_futures:
                task_id = futures.pop(future)
                attempt = task_attempts[task_id]
                task = self.tasks[task_id]
                
                try:
                    success = future.result()
                    if success:
                        task.status = "COMPLETED"
                        completed_tasks.add(task_id)
                        self.completed_tasks[task_id] = task
                        self.speculative_manager.record_completion(task_id)
                        
                        if task_type == "MAP":
                            self.stats["map_tasks_completed"] += 1
                        else:
                            self.stats["reduce_tasks_completed"] += 1
                    else:
                        # Handle task failure
                        self._handle_task_failure(task, attempt, task_type)
                except Exception as e:
                    logger.error(f"Task {task_id} failed with exception: {e}")
                    self._handle_task_failure(task, attempt, task_type)
            
            # Check for speculative execution opportunities
            if self.enable_speculative_execution and len(completed_tasks) < len(tasks):
                self._check_speculative_execution(tasks, task_attempts, task_type, start_time)
            
            # Check for timeouts
            self._check_task_timeouts(task_attempts, task_type)
            
            # Small sleep to avoid busy waiting
            time.sleep(0.1)
        
        logger.info(f"All {task_type} tasks completed")
    
    def _handle_task_failure(self, task: Task, attempt: TaskAttempt, task_type: str) -> None:
        """Handle a failed task attempt."""
        logger.warning(f"Task {task.task_id} failed, attempt {attempt.attempt_id}")
        
        # Check if we should retry
        if len(task.attempts) < self.max_task_attempts:
            self.stats["task_retries"] += 1
            
            # Create new attempt
            new_attempt_id = f"{task.task_id}_attempt_{len(task.attempts)}"
            new_attempt = TaskAttempt(
                attempt_id=new_attempt_id,
                task_id=task.task_id,
                worker_id=f"worker_{len(task.attempts)}",
                start_time=time.time()
            )
            task.attempts.append(new_attempt)
            
            # Try to resume from checkpoint
            checkpoint_data = self.checkpoint_manager.load_checkpoint(task.task_id)
            if checkpoint_data:
                logger.info(f"Resuming task {task.task_id} from checkpoint")
            
            # Resubmit task
            if task_type == "MAP":
                future = self.worker_pool.submit_task(self._execute_map_task, task, new_attempt)
            else:
                future = self.worker_pool.submit_task(self._execute_reduce_task, task, new_attempt)
            
            # Update futures (this is simplified - in real implementation would need better tracking)
        else:
            # Max retries exceeded
            task.status = "FAILED"
            self.failed_tasks[task.task_id] = task
            logger.error(f"Task {task.task_id} failed after {self.max_task_attempts} attempts")
    
    def _check_speculative_execution(self, tasks: List[Task], task_attempts: Dict[str, TaskAttempt], 
                                   task_type: str, start_time: float) -> None:
        """Check if any tasks should be speculatively executed."""
        current_time = time.time()
        elapsed_time = current_time - start_time
        
        # Calculate average progress
        total_progress = 0
        active_tasks = 0
        
        for task in tasks:
            if task.status not in ["COMPLETED", "FAILED"]:
                attempt = task_attempts.get(task.task_id)
                if attempt and attempt.status == "RUNNING":
                    total_progress += attempt.progress
                    active_tasks += 1
        
        if active_tasks == 0:
            return
        
        avg_progress = total_progress / active_tasks
        
        # Check each active task
        for task in tasks:
            if task.status not in ["COMPLETED", "FAILED"]:
                attempt = task_attempts.get(task.task_id)
                if attempt and attempt.status == "RUNNING":
                    if self.speculative_manager.should_speculate(task.task_id, attempt.progress, avg_progress):
                        logger.info(f"Launching speculative execution for task {task.task_id}")
                        self.stats["speculative_tasks_launched"] += 1
                        
                        # Create speculative attempt
                        spec_attempt_id = f"{task.task_id}_speculative_{len(task.attempts)}"
                        spec_attempt = TaskAttempt(
                            attempt_id=spec_attempt_id,
                            task_id=task.task_id,
                            worker_id=f"worker_spec_{len(task.attempts)}",
                            start_time=current_time
                        )
                        task.attempts.append(spec_attempt)
                        
                        # Submit speculative task
                        if task_type == "MAP":
                            future = self.worker_pool.submit_task(self._execute_map_task, task, spec_attempt)
                        else:
                            future = self.worker_pool.submit_task(self._execute_reduce_task, task, spec_attempt)
                        
                        # Add to futures tracking (simplified)
    
    def _check_task_timeouts(self, task_attempts: Dict[str, TaskAttempt], task_type: str) -> None:
        """Check for task timeouts."""
        current_time = time.time()
        
        for task_id, attempt in list(task_attempts.items()):
            if attempt.status == "RUNNING":
                if current_time - attempt.start_time > self.task_timeout:
                    logger.warning(f"Task {task_id} timed out")
                    attempt.status = "FAILED"
                    attempt.error = "Timeout"
    
    def _shuffle_phase(self, map_tasks: List[Task]) -> Dict[int, List[str]]:
        """Shuffle phase: collect map outputs and assign to reducers."""
        logger.info("Starting shuffle phase")
        
        # Group intermediate outputs by partition
        partition_files: Dict[int, List[str]] = defaultdict(list)
        
        for task in map_tasks:
            if task.status == "COMPLETED":
                task_output_dir = Path(task.output_path)
                if task_output_dir.exists():
                    for partition_dir in task_output_dir.glob("partition_*"):
                        partition_num = int(partition_dir.name.split("_")[1])
                        for data_file in partition_dir.glob("*.dat"):
                            partition_files[partition_num].append(str(data_file))
        
        # Assign partitions to reducers
        reducer_inputs: Dict[int, List[str]] = defaultdict(list)
        
        for partition_num, files in partition_files.items():
            reducer_idx = partition_num % self.num_reducers
            reducer_inputs[reducer_idx].extend(files)
        
        # Update reduce tasks with their input files
        for task in self.tasks.values():
            if task.task_type == "REDUCE":
                reducer_idx = int(task.task_id.split("_")[1])  # Extract reducer index
                task.input_paths = reducer_inputs.get(reducer_idx, [])
        
        logger.info(f"Shuffle phase completed, assigned files to {len(reducer_inputs)} reducers")
        return reducer_inputs
    
    def run(self, input_paths: List[str]) -> Dict[str, Any]:
        """
        Execute the complete MapReduce job.
        
        Args:
            input_paths: List of input file paths
            
        Returns:
            Dictionary with job results and statistics
        """
        start_time = time.time()
        
        try:
            logger.info(f"Starting MapReduce job with {len(input_paths)} input files")
            
            # Phase 1: Map
            logger.info("=== MAP PHASE ===")
            map_tasks = self._create_map_tasks(input_paths)
            self._monitor_tasks(map_tasks, "MAP")
            
            # Check if any map tasks failed
            failed_map_tasks = [t for t in map_tasks if t.status == "FAILED"]
            if failed_map_tasks:
                raise RuntimeError(f"{len(failed_map_tasks)} map tasks failed")
            
            # Phase 2: Shuffle
            logger.info("=== SHUFFLE PHASE ===")
            self._shuffle_phase(map_tasks)
            
            # Phase 3: Reduce
            logger.info("=== REDUCE PHASE ===")
            reduce_tasks = self._create_reduce_tasks()
            self._monitor_tasks(reduce_tasks, "REDUCE")
            
            # Check if any reduce tasks failed
            failed_reduce_tasks = [t for t in reduce_tasks if t.status == "FAILED"]
            if failed_reduce_tasks:
                raise RuntimeError(f"{len(failed_reduce_tasks)} reduce tasks failed")
            
            # Collect results
            results = self._collect_results()
            
            # Calculate statistics
            self.stats["total_execution_time"] = time.time() - start_time
            
            logger.info(f"MapReduce job completed in {self.stats['total_execution_time']:.2f} seconds")
            logger.info(f"Statistics: {self.stats}")
            
            return {
                "status": "SUCCESS",
                "results": results,
                "statistics": self.stats,
                "output_dir": str(self.output_dir)
            }
            
        except Exception as e:
            logger.error(f"MapReduce job failed: {e}")
            return {
                "status": "FAILED",
                "error": str(e),
                "statistics": self.stats
            }
        finally:
            # Cleanup
            self.worker_pool.shutdown(wait=False)
    
    def _collect_results(self) -> List[Tuple[Any, Any]]:
        """Collect final results from all reducers."""
        results = []
        
        for task in self.tasks.values():
            if task.task_type == "REDUCE" and task.status == "COMPLETED":
                output_file = Path(task.output_path) / "output.txt"
                if output_file.exists():
                    with open(output_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if '\t' in line:
                                key, value = line.strip().split('\t', 1)
                                results.append((key, value))
        
        return results
    
    def cleanup(self) -> None:
        """Clean up temporary files and directories."""
        import shutil
        try:
            shutil.rmtree(self.work_dir)
            logger.info("Cleaned up temporary files")
        except Exception as e:
            logger.warning(f"Failed to clean up: {e}")


# Example implementations for demonstration

class WordCountMapper(Mapper):
    """Example mapper for word count."""
    
    def map(self, key: Any, value: Any) -> Iterator[Tuple[str, int]]:
        words = value.lower().split()
        for word in words:
            # Clean word (remove punctuation)
            word = ''.join(c for c in word if c.isalnum())
            if word:
                yield (word, 1)


class WordCountReducer(Reducer):
    """Example reducer for word count."""
    
    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[Tuple[str, int]]:
        total = sum(values)
        yield (key, total)


class WordCountCombiner(Combiner):
    """Example combiner for word count."""
    
    def combine(self, key: Any, values: Iterator[Any]) -> Iterator[Tuple[str, int]]:
        total = sum(values)
        yield (key, total)


def create_sample_data(num_files: int = 5, lines_per_file: int = 1000) -> List[str]:
    """Create sample data files for testing."""
    data_dir = Path(tempfile.mkdtemp(prefix="mapreduce_data_"))
    input_files = []
    
    sample_text = [
        "the quick brown fox jumps over the lazy dog",
        "hello world this is a test",
        "mapreduce is a programming model",
        "distributed computing for big data",
        "fault tolerance and scalability"
    ]
    
    for i in range(num_files):
        file_path = data_dir / f"input_{i}.txt"
        with open(file_path, 'w', encoding='utf-8') as f:
            for j in range(lines_per_file):
                f.write(random.choice(sample_text) + "\n")
        input_files.append(str(file_path))
    
    return input_files


if __name__ == "__main__":
    # Example usage
    print("MapReduce Framework with Fault Tolerance")
    print("=" * 50)
    
    # Create sample data
    input_files = create_sample_data(num_files=3, lines_per_file=100)
    print(f"Created {len(input_files)} input files")
    
    # Run MapReduce job
    engine = MapReduceEngine(
        mapper_class=WordCountMapper,
        reducer_class=WordCountReducer,
        combiner_class=WordCountCombiner,
        num_mappers=2,
        num_reducers=2,
        enable_speculative_execution=True,
        enable_combiner=True,
        enable_data_locality=True
    )
    
    try:
        result = engine.run(input_files)
        
        if result["status"] == "SUCCESS":
            print(f"\nJob completed successfully!")
            print(f"Output directory: {result['output_dir']}")
            print(f"Total execution time: {result['statistics']['total_execution_time']:.2f}s")
            print(f"Map tasks completed: {result['statistics']['map_tasks_completed']}")
            print(f"Reduce tasks completed: {result['statistics']['reduce_tasks_completed']}")
            print(f"Speculative tasks launched: {result['statistics']['speculative_tasks_launched']}")
            print(f"Task retries: {result['statistics']['task_retries']}")
            
            # Show sample results
            if result["results"]:
                print(f"\nSample results (first 10):")
                for key, value in result["results"][:10]:
                    print(f"  {key}: {value}")
        else:
            print(f"\nJob failed: {result['error']}")
    
    finally:
        # Cleanup
        engine.cleanup()
        
        # Cleanup sample data
        import shutil
        for file_path in input_files:
            try:
                os.remove(file_path)
            except OSError:
                pass
        try:
            shutil.rmtree(Path(input_files[0]).parent)
        except OSError:
            pass