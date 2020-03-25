"""AsyncTaskQueue base class and related objects"""
import asyncio
from collections import deque
from logging import Logger
from typing import Any, Awaitable, Callable, Deque, List

from .render import render


class AsyncTask:
    """Class responsible for executing an async call and storing the result"""

    def __init__(
        self, callable_: Callable[..., Awaitable], *args: Any, **kwargs: Any
    ) -> None:
        self._callable = callable_
        self._args = args
        self._kwargs = kwargs
        self.result: Any = None

    async def execute(self) -> None:
        """Execute the task represented by the instance and store the call result"""
        try:
            self.result = await self._callable(*self._args, **self._kwargs)
        except Exception as e:
            self.result = e

    def __repr__(self) -> str:
        return render(self, treelike_whitelist=(AsyncTask,))


class AsyncTaskQueue:
    """Class for managing FIFO queued concurrent task execution

    Used to execute tasks concurrently with optional control (via semaphore) over the
    max number of tasks running at the same time.

    With semaphore:
        As a task is completed and the number of running tasks is decremented, it is
        immediately replaced with the execution of the next task in the queue.

    Without semaphore:
        Tasks are completed in batches, with each batch being awaited for completion
        before processing the next batch of tasks from the queue.

    Note:
        Internal queues are collections.deque instances for performance. See deque
        documentation for details.

    Example:
        To execute 100 tasks where:
            - At most 5 tasks are running concurrently
            - Number of tasks executing concurrently should be limited by a semaphore
            - Failed tasks should be retried
            - Executing the tasks queued should timeout and be cancelled after 5 minutes

            task_queue = AsyncTaskQueue(logger, use_semaphore=True, batch_size=5, execution_timeout=300)
            task_queue.enqueue(
                [
                    AsyncTask(some_callable, arg) for arg in some_args
                ]
            )
            await task_queue.execute()

        The results of the executed tasks can be retrieved by inspecting the instance's
        `failed_tasks` and `succeeded_tasks`.

    """

    def __init__(
        self,
        logger: Logger,
        use_semaphore: bool = False,
        batch_size: int = 1,
        retry_failures: bool = True,
        execution_timeout: int = None,
    ) -> None:
        """
        Args:
            logger: The logger to log messages to.
            use_semaphore: If True, task execution is controlled with a semaphore, limiting
                the number of tasks running at any one time to be at most `batch_size`.
            batch_size: The max number of tasks to dequeue and execute concurrently.
            retry_failures: Whether or not to retry failed tasks.
            execution_timeout: Timeout in seconds for task execution to complete.

        """
        if batch_size < 1:
            raise Exception("AsyncTaskQueue must be initialized with batch_size >= 1")

        self.task_queue: Deque[AsyncTask] = deque()
        self.retry_task_queue: Deque[AsyncTask] = deque()

        # These are deques to maintain order of execution
        self.failed_tasks: Deque[AsyncTask] = deque()
        self.succeeded_tasks: Deque[AsyncTask] = deque()

        self._retry_failures = retry_failures
        self._timeout = execution_timeout
        self._batch_size = batch_size
        self._use_semaphore = use_semaphore
        self._semaphore = (
            None if not use_semaphore else asyncio.BoundedSemaphore(batch_size)
        )
        self._logger = logger

    def enqueue(self, tasks: List[AsyncTask]) -> None:
        """Add tasks to the task queue

        Args:
            tasks: The tasks to add to the task queue

        """
        self._enqueue(tasks, self.task_queue)

    def _enqueue(self, tasks: List[AsyncTask], queue: Deque[AsyncTask]) -> None:
        """Add tasks to the given queue

        Args:
            tasks: The tasks to add to the given queue
            queue: The queue to which to add the tasks

        """
        queue.extendleft(tasks)

    def dequeue(self, num_tasks: int = 1) -> List[AsyncTask]:
        """Dequeue tasks from the task queue

        Args:
            num_tasks: The number of tasks to dequeue

        Returns:
            List of tasks dequeued

        """
        return self._dequeue(num_tasks, self.task_queue)

    def _dequeue(self, num_tasks: int, queue: Deque[AsyncTask]) -> List[AsyncTask]:
        """Dequeue tasks from the given queue

        Args:
            num_tasks: The number of tasks to dequeue
            queue: The queue from which to dequeue tasks

        Returns:
            List of tasks dequeued

        """
        tasks: Deque[AsyncTask] = deque()
        try:
            for num in range(num_tasks):
                self._enqueue([queue.pop()], tasks)
        except IndexError:
            pass  # queue is empty

        return list(tasks)

    async def execute(self) -> None:
        """Execute all the tasks in the task queue

        Note: If this AsyncTaskQueue was initialized with a timeout, then processing the
            queue will be cancelled once timeout is reached. Actual time for exceution
            to cancel may exceed the timeout set since asyncio.wait_for waits until the
            future being waited for has actually been cancelled.

            This method doesn't return the results of the tasks executed, but those can
            be retrieved by inspecting `self.failed_tasks` and `self.succeeded_tasks`.

        """
        total_tasks = len(self.task_queue)
        num_retry_tasks = 0

        async def _execute() -> None:
            """Inner function to allow wrapping in asyncio.wait_for"""
            await self._execute(self.task_queue)
            nonlocal num_retry_tasks  # Allows num_retry_tasks from the outer scope to be updated
            num_retry_tasks = len(self.retry_task_queue)
            if self._retry_failures and num_retry_tasks > 0:
                self._logger.info(f"Retrying {num_retry_tasks} failed tasks")
                await self._execute(self.retry_task_queue)

            else:
                # No tasks are being retried, so mark any in the retry queue as failures
                self.failed_tasks = self.retry_task_queue.copy()
                self.retry_task_queue.clear()

        try:
            await asyncio.wait_for(_execute(), timeout=self._timeout)
        except asyncio.TimeoutError:
            self._logger.error("Task execution timed out and was stopped")

        self._logger.info(
            f"Out of {total_tasks} tasks, successfully executed {len(self.succeeded_tasks)}, retried {num_retry_tasks}, failed {len(self.failed_tasks)}"
        )

    async def _execute(self, queue: deque) -> None:
        """Execute all the tasks in the task queue

        Args:
            queue: The queue containing all the tasks to execute

        """
        self._logger.info(f"Executing {len(queue)} tasks")
        if self._use_semaphore:
            await self._execute_by_semaphore(queue)
        else:
            await self._execute_by_batch(queue)

    async def _execute_by_semaphore(self, queue: deque) -> None:
        """Execute all the tasks in the given queue using a semaphore

        As a task is completed and the number of running tasks is decremented, it is
        immediately replaced with the execution of the next task in the queue.

        Args:
            queue: The queue containing all the tasks to execute

        """
        task_executables = []
        # Empty the task queue sequentially to maintain task execution order
        while len(queue) != 0:
            task = self._dequeue(1, queue)[0]
            task_executables.append(self._execute_task(task, queue))

        await asyncio.gather(*[task_exc for task_exc in task_executables])

    async def _execute_by_batch(self, queue: deque) -> None:
        """Execute all the tasks in the given queue in batches

        Args:
            queue: The queue containing all the tasks to execute

        """
        while len(queue) != 0:
            tasks = self._dequeue(self._batch_size, queue)
            await asyncio.gather(*[self._execute_task(task, queue) for task in tasks])

    async def _execute_task(self, task: AsyncTask, queue: deque) -> None:
        """Execute the given task

        Args:
            task: The task to execute
            queue: The queue containing the task to execute

        """
        if self._use_semaphore:
            async with self._semaphore:
                await task.execute()
        else:
            await task.execute()

        if isinstance(task.result, Exception):
            self._logger.error(f"Failed task: {repr(task.result)}")
            if queue is self.retry_task_queue:
                self._enqueue([task], queue=self.failed_tasks)
            else:
                self._enqueue([task], queue=self.retry_task_queue)
        else:
            self._enqueue([task], queue=self.succeeded_tasks)

    def __repr__(self) -> str:
        return render(self, treelike_whitelist=(AsyncTaskQueue,))
