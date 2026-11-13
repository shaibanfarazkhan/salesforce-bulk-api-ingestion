"""
Parallel Processing Worker Pool Service.
Implements bounded multi-threaded worker pool consuming from a thread-safe task queue
with configurable concurrency, graceful shutdown, and worker telemetry tracking.
"""

import time
import uuid
import queue
import threading
import logging
from typing import Callable, Any, Optional, Dict, Tuple
from dataclasses import dataclass

from app.utils.exceptions import QueueFullException
from app.utils.telemetry import metrics_collector

logger = logging.getLogger(__name__)

@dataclass
class TaskItem:
    """Runnable task container dispatched to worker threads."""
    task_id: str
    func: Callable
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    submitted_at: float

class IngestionWorkerPool:
    """
    Thread-safe parallel worker pool for Bulk API ingestion execution.
    """

    def __init__(self, max_workers: int = 4, max_queue_size: int = 1000) -> None:
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._workers: list[threading.Thread] = []
        self._is_running = False
        self._shutdown_event = threading.Event()
        self._active_workers = 0
        self._lock = threading.Lock()
        self._completed_tasks = 0
        self._failed_tasks = 0

    def start(self) -> None:
        """Start worker threads."""
        with self._lock:
            if self._is_running:
                return
            self._is_running = True
            self._shutdown_event.clear()

            for i in range(self.max_workers):
                worker_thread = threading.Thread(
                    target=self._worker_loop,
                    name=f"IngestionWorker-{i+1}",
                    daemon=True
                )
                self._workers.append(worker_thread)
                worker_thread.start()

            logger.info(f"IngestionWorkerPool started with {self.max_workers} worker threads.")

    def submit(self, func: Callable, *args: Any, **kwargs: Any) -> str:
        """
        Submit a callable task to the worker queue.
        Raises QueueFullException if maximum queue capacity is saturated.
        """
        if not self._is_running:
            self.start()

        task_id = str(uuid.uuid4())
        task_item = TaskItem(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            submitted_at=time.time()
        )

        try:
            self._queue.put_nowait(task_item)
            metrics_collector.set_queue_size(self._queue.qsize())
            logger.debug(f"Submitted task {task_id} to worker pool (Queue size: {self._queue.qsize()})")
            return task_id
        except queue.Full:
            logger.error(f"Worker queue full! Capacity: {self.max_queue_size}")
            raise QueueFullException(
                f"Worker queue is full ({self.max_queue_size} tasks). Cannot enqueue new jobs."
            )

    def _worker_loop(self) -> None:
        """Continuous execution loop executed by each worker thread."""
        thread_name = threading.current_thread().name
        logger.debug(f"Worker thread {thread_name} started.")

        while not self._shutdown_event.is_set():
            try:
                task_item: TaskItem = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            with self._lock:
                self._active_workers += 1
                metrics_collector.set_active_workers(self._active_workers)
                metrics_collector.set_queue_size(self._queue.qsize())

            logger.info(f"[{thread_name}] Executing task {task_item.task_id}...")
            try:
                task_item.func(*task_item.args, **task_item.kwargs)
                with self._lock:
                    self._completed_tasks += 1
                logger.info(f"[{thread_name}] Task {task_item.task_id} completed successfully.")
            except Exception as exc:
                with self._lock:
                    self._failed_tasks += 1
                logger.error(f"[{thread_name}] Task {task_item.task_id} failed: {str(exc)}", exc_info=True)
            finally:
                self._queue.task_done()
                with self._lock:
                    self._active_workers -= 1
                    metrics_collector.set_active_workers(self._active_workers)
                    metrics_collector.set_queue_size(self._queue.qsize())

        logger.debug(f"Worker thread {thread_name} stopped.")

    def shutdown(self, wait: bool = True, timeout: float = 5.0) -> None:
        """
        Gracefully stop all worker threads.
        """
        with self._lock:
            if not self._is_running:
                return
            logger.info("Initiating graceful shutdown of IngestionWorkerPool...")
            self._is_running = False
            self._shutdown_event.set()

        if wait:
            start_join = time.time()
            for worker in self._workers:
                remaining = max(0.1, timeout - (time.time() - start_join))
                worker.join(timeout=remaining)

        with self._lock:
            self._workers.clear()
            self._active_workers = 0
            metrics_collector.set_active_workers(0)
            logger.info("IngestionWorkerPool gracefully stopped.")

    def is_running(self) -> bool:
        """Check if worker pool is currently running."""
        return self._is_running

    def active_worker_count(self) -> int:
        """Return number of worker threads currently processing tasks."""
        with self._lock:
            return self._active_workers

    def queue_size(self) -> int:
        """Return number of tasks waiting in queue."""
        return self._queue.qsize()

    def get_stats(self) -> Dict[str, Any]:
        """Return operational telemetry snapshot of the worker pool."""
        with self._lock:
            return {
                "max_workers": self.max_workers,
                "active_workers": self._active_workers,
                "idle_workers": max(0, self.max_workers - self._active_workers) if self._is_running else 0,
                "queue_size": self._queue.qsize(),
                "queue_capacity": self.max_queue_size,
                "is_running": self._is_running,
                "completed_tasks": self._completed_tasks,
                "failed_tasks": self._failed_tasks
            }

