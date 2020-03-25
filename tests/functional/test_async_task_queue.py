"""Contains functional tests for AsyncTaskQueue"""

import asyncio
import logging
from time import sleep
from typing import Any
import unittest

from async_task_queue import AsyncTask, AsyncTaskQueue
from test_utils import async_test, AsyncMethodMock


class m_Exception(Exception):
    """Mock exception for testing"""

    pass


async def m_callable(arg: Any, kwarg: Any = "foo", fail: bool = False) -> str:
    """Mock callable to execute in task"""
    if fail:
        raise m_Exception("This is a failed call")
    return "success"


class AsyncTaskTest(unittest.TestCase):
    """Unit tests for the AsyncTask class"""

    @async_test
    async def test_execute(self) -> None:
        task = AsyncTask(m_callable, "some arg")
        await task.execute()
        self.assertEqual(task.result, "success")

    @async_test
    async def test_execute_exception(self) -> None:
        task = AsyncTask(m_callable, "some arg", fail=True)
        await task.execute()
        self.assertTrue(isinstance(task.result, m_Exception))


class AsyncTaskQueueTest(unittest.TestCase):
    """Unit tests for the AsyncTaskQueue"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.logger = logging.getLogger()

    @async_test
    async def test_enqueue(self) -> None:
        """Should be able to enqueue multiple tasks and maintain order"""
        queue = AsyncTaskQueue(self.logger)
        queue.enqueue([AsyncTask(m_callable, 0), AsyncTask(m_callable, 1)])
        queue.enqueue([AsyncTask(m_callable, 2), AsyncTask(m_callable, 3)])
        self.assertEqual(
            len(queue.task_queue), 4, msg="Expected to 4 tasks to be enqueued"
        )
        self.assertTrue(
            queue.task_queue[0]._args == (3,)
            and queue.task_queue[1]._args == (2,)
            and queue.task_queue[2]._args == (1,)
            and queue.task_queue[3]._args == (0,),
            msg="Expected task enqueued to be in FIFO order",
        )

    @async_test
    async def test_dequeue(self) -> None:
        """Should be able to dequeue multiple tasks and maintain order"""
        queue = AsyncTaskQueue(self.logger)
        queue.enqueue(
            [
                AsyncTask(m_callable, 0),
                AsyncTask(m_callable, 1),
                AsyncTask(m_callable, 2),
                AsyncTask(m_callable, 3),
            ]
        )

        tasks = queue.dequeue(2)
        self.assertEqual(
            len(tasks), 2, msg="Expected 2 tasks to be returned by dequeuing"
        )
        self.assertTrue(
            tasks[0]._args == (1,) and tasks[1]._args == (0,),
            msg="Expected tasks dequeued to be in FIFO order",
        )

        self.assertEqual(
            len(queue.task_queue),
            2,
            msg="Expected 2 tasks to be in task queue after dequeuing 2",
        )
        self.assertTrue(
            queue.task_queue[0]._args == (3,) and queue.task_queue[1]._args == (2,),
            msg="Expected task queue to be in FIFO order",
        )

        # Check that trying to dequeue more tasks than exists doesn't break anything
        tasks = queue.dequeue(3)
        self.assertEqual(
            len(tasks), 2, msg="Expected remaining tasks to be returned by dequeuing"
        )
        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty after dequeuing all tasks",
        )

    @async_test
    async def test_execute__no_tasks(self) -> None:
        """Should not error out"""
        queue = AsyncTaskQueue(self.logger)
        await queue.execute()

    @async_test
    async def test_execute__timeout(self) -> None:
        """Should not error out on timing out

        1 task executes due to behavior of asyncio.wait_for. See note in AsyncTaskQueue.execute
        for more details.
        """
        queue = AsyncTaskQueue(self.logger, execution_timeout=0.5)

        async def m_callable_with_sleep(arg, fail=False):
            sleep(1)
            if fail:
                raise m_Exception()
            return "success"

        queue.enqueue(
            [
                AsyncTask(m_callable_with_sleep, 1),
                AsyncTask(m_callable_with_sleep, 2, fail=True),
            ]
        )
        await queue.execute()

        self.assertEqual(
            len(queue.task_queue), 0, msg="Expected task queue to be empty"
        )
        self.assertEqual(
            len(queue.retry_task_queue), 0, msg="Expected there to be no retry tasks"
        )
        self.assertEqual(
            len(queue.failed_tasks), 0, msg="Expected there to be no failed tasks"
        )
        self.assertEqual(
            len(queue.succeeded_tasks),
            1,
            msg="Expected only 1 task to have been executed successfully",
        )
        self.assertEqual(
            queue.succeeded_tasks[0]._args,
            (1,),
            msg="Expected first task enqueued to have been executed",
        )

    @async_test
    async def test_execute__successful_tasks(self) -> None:
        """Should successfully execute all tasks"""
        queue = AsyncTaskQueue(self.logger)
        queue.enqueue(
            [
                AsyncTask(m_callable, "some arg", kwarg="some kwarg"),
                AsyncTask(m_callable, "another arg", kwarg="another kwarg"),
            ]
        )
        await queue.execute()

        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty when all tasks have been executed",
        )
        self.assertEqual(
            len(queue.retry_task_queue), 0, msg="Expected there to be no retry tasks"
        )
        self.assertEqual(
            len(queue.failed_tasks), 0, msg="Expected there to be no failed tasks"
        )
        self.assertEqual(
            len(queue.succeeded_tasks),
            2,
            msg="Expected all tasks to have been executed successfully",
        )

    @async_test
    async def test_execute__successful_tasks__batch_size_gt_num_tasks(self) -> None:
        """Should successfully execute all tasks"""
        queue = AsyncTaskQueue(self.logger, batch_size=10)
        queue.enqueue(
            [
                AsyncTask(m_callable, "some arg", kwarg="some kwarg"),
                AsyncTask(m_callable, "another arg", kwarg="another kwarg"),
            ]
        )
        await queue.execute()

        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty when all tasks have been executed",
        )
        self.assertEqual(
            len(queue.retry_task_queue), 0, msg="Expected there to be no retry tasks"
        )
        self.assertEqual(
            len(queue.failed_tasks), 0, msg="Expected there to be no failed tasks"
        )
        self.assertEqual(
            len(queue.succeeded_tasks),
            2,
            msg="Expected all tasks to have been executed successfully",
        )

    @async_test
    async def test_execute__failed_tasks(self) -> None:
        """Should fail to complete all tasks"""
        queue = AsyncTaskQueue(self.logger)
        queue.enqueue(
            [
                AsyncTask(m_callable, "some arg", fail=True),
                AsyncTask(m_callable, "another arg", fail=True),
            ]
        )
        await queue.execute()

        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty when all tasks have been executed",
        )
        self.assertEqual(
            len(queue.retry_task_queue),
            0,
            msg="Expected there to be no more retry tasks",
        )
        self.assertEqual(
            len(queue.failed_tasks), 2, msg="Expected all tasks to have failed"
        )
        self.assertEqual(
            len(queue.succeeded_tasks),
            0,
            msg="Expected no tasks to have been executed successfully",
        )

    @async_test
    async def test_execute__retry(self) -> None:
        """Should retry and update queues"""
        queue = AsyncTaskQueue(self.logger)
        m_callable_fail_succeed = AsyncMethodMock(
            side_effect=[m_Exception("This is a failed call"), "success"]
        )
        m_callable_succeed_fail = AsyncMethodMock(
            side_effect=["success", m_Exception("This is a failed call")]
        )

        queue.enqueue(
            [
                AsyncTask(m_callable, "some arg"),  # Task should execute successfully
                AsyncTask(
                    m_callable_fail_succeed
                ),  # Task should first fail then succeed on retry
                AsyncTask(m_callable, "some arg", fail=True),  # Task should fail
                AsyncTask(m_callable_succeed_fail),  # Sanity check, task should succeed
            ]
        )

        await queue.execute()
        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty when all tasks have been executed",
        )
        self.assertEqual(
            len(queue.retry_task_queue),
            0,
            msg="Expected there to be no more retry tasks",
        )
        self.assertEqual(
            len(queue.failed_tasks),
            1,
            msg="Expected only 1 task to have failed after retrying",
        )
        self.assertEqual(
            len(queue.succeeded_tasks),
            3,
            msg="Expected 3 tasks to have succeeded after retrying",
        )

    @async_test
    async def test_execute__no_retry(self) -> None:
        """Should not retry failed tasks"""
        queue = AsyncTaskQueue(self.logger, retry_failures=False)
        m_callable_fail_succeed = AsyncMethodMock(
            side_effect=[m_Exception("This is a failed call"), "success"]
        )
        m_callable_succeed_fail = AsyncMethodMock(
            side_effect=["success", m_Exception("This is a failed call")]
        )

        queue.enqueue(
            [
                AsyncTask(m_callable, "some arg"),  # Task should execute successfully
                AsyncTask(m_callable_fail_succeed),  # Task should fail with no retry
                AsyncTask(m_callable, "some arg", fail=True),  # Task should fail
                AsyncTask(m_callable_succeed_fail),  # Sanity check, task should succeed
            ]
        )

        await queue.execute()
        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty when all tasks have been executed",
        )
        self.assertEqual(
            len(queue.retry_task_queue), 0, msg="Expected there to be no tasks to retry"
        )
        self.assertEqual(
            len(queue.failed_tasks), 2, msg="Expected 2 tasks to have failed"
        )
        self.assertEqual(
            len(queue.succeeded_tasks), 2, msg="Expected 3 tasks to have succeeded"
        )

    @async_test
    async def test_execute__use_concurrency_control(self) -> None:
        """Shouldn't break anything"""
        queue = AsyncTaskQueue(self.logger, batch_size=2, use_semaphore=True)
        m_callable_fail_succeed = AsyncMethodMock(
            side_effect=[m_Exception("This is a failed call"), "success"]
        )

        queue.enqueue(
            [
                AsyncTask(m_callable, "some arg"),  # Task should execute successfully
                AsyncTask(
                    m_callable_fail_succeed
                ),  # Task should first fail then succeed on retry
                AsyncTask(m_callable, "some arg", fail=True),  # Task should fail
            ]
        )

        await queue.execute()

        self.assertTrue(
            isinstance(queue._semaphore, asyncio.BoundedSemaphore),
            msg="Expected semaphore to have been initiated",
        )
        self.assertEqual(
            len(queue.task_queue),
            0,
            msg="Expected task queue to be empty when all tasks have been executed",
        )
        self.assertEqual(
            len(queue.retry_task_queue),
            0,
            msg="Expected there to be no more tasks to retry",
        )
        self.assertEqual(
            len(queue.failed_tasks),
            1,
            msg="Expected 1 task to have failed after retrying",
        )
        self.assertEqual(
            len(queue.succeeded_tasks),
            2,
            msg="Expected 2 tasks to have succeeded after retrying",
        )
