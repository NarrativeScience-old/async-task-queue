"""Contains test utilities"""

import asyncio
from typing import Callable
from unittest.mock import MagicMock


def async_test(async_test_func: Callable[..., None]) -> Callable[..., None]:
    """Decorator for running an async test function synchronously

    This may be useful when testing coroutines or db calls with aiopg

    Args:
        async_test_func: a test function written as a coroutine

    Returns:
        the decorated function

    """

    def sync_test_func(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_test_func(*args, **kwargs))

    return sync_test_func


def AsyncMethodMock(*args, **kwargs):
    """Helper method to mock async methods

    Found on https://blog.miguelgrinberg.com/post/unit-testing-asyncio-code

    Args:
        args: Any arguments the function passes in
        kwargs: Any kwargs passed in, can also be used to, for example,
            provide arguments like "return_value" to the internal MagicMock

    """
    m = MagicMock(*args, **kwargs)

    async def mock_coro(*args, **kwargs):
        return m(*args, **kwargs)

    mock_coro.mock = m
    mock_coro.assert_any_call = m.assert_any_call
    mock_coro.assert_has_calls = m.assert_has_calls
    mock_coro.assert_not_called = m.assert_not_called
    mock_coro.call_args_list = m.call_args_list
    mock_coro.assert_called_once_with = m.assert_called_once_with
    mock_coro.assert_called_with = m.assert_called_with
    mock_coro.assert_called_once = m.assert_called_once
    return mock_coro
