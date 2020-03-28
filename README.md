# async-task-qeue

[![CircleCI](https://circleci.com/gh/NarrativeScience/async-task-queue/tree/master.svg?style=shield)](https://circleci.com/gh/NarrativeScience/async-task-queue/tree/master)[![](https://img.shields.io/pypi/v/async-task-queue.svg)](https://pypi.org/pypi/async-task-queue/) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

In-memory FIFO queue for concurrent task execution. Used to execute tasks concurrently with optional control (via semaphore) over the max number of tasks running at the same time.

Features:

- Queue processing summary logging
- Introspection of failed, retried, and succeeded tasks
- Task retries (optional)
- Task execution timeout (optional)
- Queue processing with semaphore (optional)
- Batch size control (optional)

TOC:

- [Installation](#installation)
- [Guide](#guide)
- [Development](#development)

## Installation

async-task-queue requires Python 3.6 or above.

```bash
pip install async-task-queue
```

## Guide

```python
import logging
from async_task_queue import AsyncTask, AsyncTaskQueue

# Initialize a logger
logger = logging.getLogger("foo")

# Initialize an AsyncTaskQueue where:
#   - At most 5 tasks are running concurrently
#   - Number of tasks executing concurrently should be limited by a semaphore
#   - Failed tasks should be retried (default behavior)
#   - Executing the tasks queued should timeout and be cancelled after 5 minutes
task_queue = AsyncTaskQueue(
    logger,
    use_semaphore=True,
    batch_size=5,
    execution_timeout=300
)

# Add async tasks to the queue
task_queue.enqueue(
    [
        AsyncTask(some_coroutine, *args, **kwargs) for args, kwargs in some_args_kwargs
    ]
)

# Start processing the queue
await task_queue.execute()
```

## Development

To develop async-task-queue, install dependencies and enable the pre-commit hook:

```bash
pip install pre-commit tox
pre-commit install
```

To run tests:

```bash
tox
```
