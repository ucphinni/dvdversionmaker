'''
Created on Feb 28, 2022

@author: Publishers
'''

import asyncio
import functools
import logging
import os
from pathlib import Path
import shutil
import sys
import time
import traceback
from typing import Any, Awaitable, Optional, TypeVar, Tuple


T = TypeVar('T')


def _create_task2(
    coroutine: Awaitable[T],
    *,
    logger: logging.Logger,
    message: str,
    message_args: Tuple[Any, ...] = (),
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> 'asyncio.Task[T]':  # This type annotation has to be quoted for Python < 3.9, see https://www.python.org/dev/peps/pep-0585/
    '''
    This helper function wraps a ``loop.create_task(coroutine())`` call and ensures there is
    an exception handler added to the resulting task. If the task raises an exception it is logged
    using the provided ``logger``, with additional context provided by ``message`` and optionally
    ``message_args``.
    '''
    if loop is None:
        loop = asyncio.get_running_loop()
    task = loop.create_task(coroutine)
    task.add_done_callback(
        functools.partial(_handle_task_result, logger=logger,
                          message=message, message_args=message_args)
    )
    return task


def _handle_task_result(
    task: asyncio.Task,
    *,
    logger: logging.Logger,
    message: str,
    message_args: Tuple[Any, ...] = (),
) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass  # Task cancellation should not be logged as an error.
    # Ad the pylint ignore: we want to handle all exceptions here so that the result of the task
    # is properly logged. There is no point re-raising the exception in this
    # callback.
    except Exception:  # pylint: disable=broad-except
        logger.exception(message, *message_args)


__all__ = ['CfgMgr']


class CfgMgr:
    DLDIR = None
    TRANSCODEDIR = None
    MENUDIR = None
    SQLFILE = None
    SPUMUX = None
    FFMPEG = None
    _sgton = None

    def __init__(self):
        self._all_pending_tasks = set()
        self._queue = asyncio.Queue()
        self._bg_task_run = None

    async def _run(self):
        while True:
            item = await self._queue.get()
            if item is None:
                break
            task = item
            task.cancel()
            self._queue.task_done()

    async def _quit(self):
        await self._queue.put(None)
        await self._queue.join()
        for t in self._all_pending_tasks:
            t.cancel()
        await asyncio.wait(self._all_pending_tasks)

    @classmethod
    def cancel(cls, task):
        cls._sgton._cancel(task)

    @classmethod
    async def quit(cls):
        await cls._sgton._quit()

    def _cancel(self, task):
        ctask = asyncio.current_task()
        if ctask != task:
            task.cancel()
            return
        if self._bg_task_run is None:
            self._bg_task_ru = asyncio.create_task(self._run())
        self._queue.put_nowait(task)

    @classmethod
    async def wait_tasks_complete(cls, awsary=None, aws=None, timeout=None):
        coro = cls._sgton._wait_tasks_complete(
            awsary=awsary, aws=aws, timeout=timeout)
        return await coro

    async def _wait_tasks_complete(self, awsary=None, aws=None, timeout=None):
        assert not(awsary is not None and aws is None)
        if awsary is not None:
            raise NotImplementedError()
        paws = set(aws)
        if len(paws) == 0:
            return (aws, set())

        ret = await asyncio.wait(
            paws, timeout=timeout,
            return_when=asyncio.FIRST_EXCEPTION)
        done, _ = ret
        for i in done:
            exc = i.exception()
            if exc is not None:
                if type(exc) == AssertionError:
                    _, _, tb = sys.exc_info()
                    traceback.print_tb(tb)  # Fixed format
                    tb_info = traceback.extract_tb(tb)
                    filename, line, func, text = tb_info[-1]

                    print(f'An error occurred on '
                          f'{filename}:{line} {func} '
                          f'in statement {text}'.format(
                              line, text))
                    exit(1)
                print(repr(exc))

        return ret

    async def _wait_tasks_complete2(self, awsary=None, aws=None, timeout=None):
        assert not(awsary is not None and aws is None)
        awsary_is_none = awsary is None
        if aws is not None and awsary_is_none:
            awsary = [aws]
        pawsary = list(
            map(lambda s: s.intersection(self._all_pending_tasks), awsary))
        end_time = time.monotonic() + timeout if timeout is not None else None

        while len(self._all_pending_tasks):
            timeout = (
                end_time - time.monotonic() if timeout is not None
                else None)
            if timeout is not None and timeout <= 0:
                timeout = 0
            _, self._all_pending_tasks = await asyncio.wait(
                self._all_pending_tasks, timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED)
            for i in pawsary:
                i.intersection_update(self._all_pending_tasks)
            if timeout is not None and timeout <= 0:
                break
            if next(
                map(lambda _: True,
                    filter(lambda item: len(item) == 0,
                           pawsary)), False):
                break
        ret = []
        for i, paws in enumerate(pawsary):
            d = awsary[i].difference(paws)
            p = paws

            ret.append((d, p))
        if awsary_is_none and aws is not None:
            return ret[0]
        if awsary_is_none:
            return None

        return ret

    @classmethod
    def create_task(cls, coro):
        return cls._sgton._create_task(coro)

    def _create_task(self, coro):
        task = asyncio.create_task(coro)
        self._all_pending_tasks.add(task)
        return task

    @classmethod
    def set_paths(cls, dldir, sqlfile):
        #        sys.modules['s'] = __module__
        cls.DLDIR = Path(dldir)
        if cls.DLDIR.exists() and cls.DLDIR.is_dir():
            pass
        else:
            raise NotADirectoryError(f"DLDIR:{dldir}")
        cls.SQLFILE = Path(sqlfile)
        if cls.SQLFILE.exists() and cls.SQLFILE.is_file():
            pass
        else:
            raise FileExistsError(f"No SQLFILE:{sqlfile}")
        cls.SPUMUX = shutil.which('spumux')
        cls.FFMPEG = shutil.which('ffmpeg')
        if cls.SPUMUX is not None and cls.FFMPEG is not None:
            return
        dirs = map(lambda x: Path(os.environ[x]), [
            'ProgramFiles(x86)', 'ProgramW6432'])
        for d in dirs:
            for s in [Path('DVDStyler') / 'bin']:
                searchdir = d / s
                if cls.FFMPEG is None:
                    cls.FFMPEG = shutil.which('ffmpeg', path=searchdir)
                if cls.SPUMUX is None:
                    cls.SPUMUX = shutil.which('spumux', path=searchdir)

        if cls.SPUMUX is None:
            raise FileExistsError("No SPUMUX found")
        if cls.FFMPEG is None:
            raise FileExistsError("No FFMPEG found")

        cls.MENUDIR = cls.DLDIR / 'menu'
        if cls.MENUDIR.exists() and cls.MENUDIR.is_dir():
            pass
        elif cls.MENUDIR.exists():
            raise NotADirectoryError(f"MENUDIR:{cls.MENUDIR}")
        else:
            cls.MENUDIR.mkdir()

        cls.TRANSCODEDIR = cls.DLDIR / 'transcode'
        if cls.TRANSCODEDIR.exists() and cls.TRANSCODEDIR.is_dir():
            pass
        elif cls.TRANSCODEDIR.exists():
            raise NotADirectoryError(f"TRANSCODEDIR:{cls.TRANSCODEDIR}")
        else:
            cls.TRANSCODEDIR.mkdir()

    @classmethod
    async def wait_for(
            cls, condition: asyncio.Condition, timeout: float) -> bool:
        loop = asyncio.get_event_loop()

        # Create a future that will be triggered by either completion or
        # timeout.
        waiter = loop.create_future()

        # Callback to trigger the future. The varargs are there to consume and void any arguments passed.
        # This allows the same callback to be used in loop.call_later and wait_task.add_done_callback,
        # which automatically passes the finished future in.
        def release_waiter(*_):
            if not waiter.done():
                waiter.set_result(None)

        # Set up the timeout
        timeout_handle = loop.call_later(timeout, release_waiter)

        # Launch the wait task
        wait_task = loop.create_task(condition.wait())
        wait_task.add_done_callback(release_waiter)

        try:
            await waiter  # Returns on wait complete or timeout
            if wait_task.done():
                return True
            else:
                raise asyncio.TimeoutError()

        except (asyncio.TimeoutError, asyncio.CancelledError):
            # If timeout or cancellation occur, clean up, cancel the wait, let it handle the cancellation,
            # then re-raise.
            wait_task.remove_done_callback(release_waiter)
            wait_task.cancel()
            await asyncio.wait([wait_task])
            raise

        finally:
            timeout_handle.cancel()
