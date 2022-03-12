'''
Created on Mar 8, 2022

@author: Publishers
'''
from abc import ABC
import asyncio
import logging

from pathlib import Path

from aiofile import (AIOFile, Reader)
import aiofiles.os
import httpx

from dbmgr import HashPos


async def get_file_size(fname):
    size = None
    try:
        if fname is not None:
            size = await aiofiles.os.path.getsize(fname)
    except FileNotFoundError:
        pass
    except Exception:
        logging.exception(f"get_file_size {fname}")
        pass
    return size


class TaskLoader:
    __slots__ = ('_afh', '_is_running', '_last_proc_chunk_start_pos',
                 '_last_proc_chunk_end_pos', '_read_from_file_gap_end',
                 '_taskme', '_exec_obj', '_afhset', '_done_file_sz', '_cond',
                 '_finish_up', '_force_terminate', '_source', '_loaders',
                 '_parent', 'localfile')

    def __init__(self, source):
        self._is_running = False
        self._taskme = None
        self._cond = asyncio.Condition()
        self._finish_up = False
        self._force_terminate = False
        self._loaders = []
        self._parent = None
        self._source = source
        self.localfile = None
        self._reset()

    def add_TaskLoader(self, obj: "TaskLoader") -> None:
        self.run_task()  # noop if already running.
        obj.run_task()  # noop if already running.
        if obj not in self._loaders:
            self._loaders.append(obj)
        obj._parent = self
        obj._source = self._source
        return self

    def remove_TaskLoader(self, obj: "TaskLoader") -> None:
        self._loaders.remove(obj)
        return self

    def run_task(self):
        if self._taskme is None:
            self._taskme = asyncio.create_task(self.run())
        return self

    async def finish(self, done_file_sz, force=False):
        if not force:
            await self._afh.fsync()
        self._force_terminate = force
        n = 0
        while True:
            if n == 50:
                print("Stream task hung. Force Terminate")
                self._force_terminate = True
            if n > 100:
                print("Could not terminate.. Ignore")
                break
            n += 1
            try:
                self._queue.put_nowait((done_file_sz, None))
                break
            except asyncio.QueueFull:
                await asyncio.sleep(.5)

    async def reset(self, clear=False):
        self._reset(clear)
        return

    def _reset(self, clear=False):
        clear = clear
        self._last_proc_chunk_start_pos = 0
        self._last_proc_chunk_end_pos = 0
        self._read_from_file_gap_end = None
        self._done_file_sz = None

    async def _send_chunk(self, _):
        return 0

    async def _proc_chunk(self, web_start_pos, chunk):
        assert web_start_pos is not None

        if chunk is None:
            self._read_from_file_gap_end = web_start_pos
            self._finish_up = True
            print("finish   up")
            return
        web_end_pos = len(chunk) + \
            web_start_pos if chunk is not None else web_start_pos
        # Delare all the sore incoming variables.
        WS, WE = web_start_pos, web_end_pos
        PE = self._last_proc_chunk_end_pos
        GE = self._read_from_file_gap_end
        if WE <= PE and GE != 0:
            #                PE-->
            #       WS       WE
            # State: web_buffer behind (must have been stale in queue), ignore.
            # Gap is filled.
            self._read_from_file_gap_end = None
            return
        if WS <= PE and PE < WE or GE == 0:
            #       PE----->PE)
            #       WS       WE
            # State: Can use but some possible chunk trimming work
            async with self._cond:
                self._last_proc_chunk_start_pos = PE
                n = await self._send_chunk(chunk[PE - WS:])
                if n is not None:
                    self._last_proc_chunk_end_pos = PE + n
                else:
                    self._last_proc_chunk_end_pos = WE

                self._cond.notify_all()
            if GE is not None and GE <= WE:
                #               WE
                #     <---------GE   (If Gap end exists in range, clear it)
                self._read_from_file_gap_end = None
            return

        if PE < WS:
            # ---->PE)
            #       WS      WE
            # State: Gap... Tell caller to refill and try again
            #  set _read_from file so that the caller will fill from PE
            #    to at least WS.
            self._read_from_file_gap_end = WS
            return
        raise Exception(
            f"Unknown State: {web_start_pos} {web_end_pos} " +
            f"{self._last_proc_chunk_start_pos} " +
            f"{self._last_proc_chunk_end_pos}")  # Bug

    def stop(self):
        self._is_running = False

    def is_running(self):
        return self._is_running

    async def handle_task_run_done(self):
        pass

    async def handle_source_eos(self):
        assert False

    async def run(self):
        self._is_running = True
        pos = 0
        try:
            assert self._source is not None

            async with self._source._cond:
                while self._is_running and self._source._afh is None:
                    await self._source._cond.wait()
            if not self._is_running:
                return
            reader = Reader(self._source._afh, offset=pos)
            while True:
                if not self._is_running:
                    break
                start = pos
                chunk = await reader.read_chunk()
                if not chunk:
                    async with self._source._cond:
                        self._source.file_waiter += 1
                        await self._source._cond.wait()
                        self._source.file_waiter -= 1
                        continue
                pos += len(chunk)
                await self._proc_chunk(start, chunk)

        except asyncio.exceptions.CancelledError:
            print("Got Canceled")
            pass
        finally:
            self._is_running = False

        await self.handle_task_run_done()
        return None

    def truncate(self):
        pass


class HashLoader(TaskLoader):
    __slots__ = (
        'dbmgr', '_fn', '_fntype', 'hpos',
    )

    def __init__(self, dbmgr, fntype, source, fn, hpos):
        super().__init__(source)
        self.dbmgr = dbmgr
        self._fn = fn
        self._fntype = fntype
        self.hpos = hpos

    async def reset(self, clear):
        await super().reset(clear=clear)
        self.hpos = None

    def bad_file(self, fn):
        fn = fn

    async def wait_for_hash(self):
        await self.hpos.wait_for_hash(done=False)

    def hash_match(self):
        if self.hpos is None:
            return None
        return self.hpos.isgood

    async def handle_source_eos(self):
        print("eos received")

    async def _send_chunk(self, chunk):
        if self.hpos is None:
            self.hpos = await self.dbmgr.get_new_hashpos(
                self._fn, self._fntype)
        await self._hash_chunk(chunk)
        return None

    async def _hash_chunk(self, chunk):
        try:

            dbmgr = self.dbmgr
            if not self.hpos.isgood:
                return
            await self.hpos.update(chunk)
            if not self.hpos.isgood:
                self.bad_file(self._fn)
                return
            if self.hpos.checking():
                return
            done = False
            size = self._source.finished_size()
            if size is not None:
                pos = self.hpos.pos
                done = pos == size
                assert pos <= size
            if done:
                await self.hpos.set_done()
            await dbmgr.put_hash_fn(
                self._fn, self._fntype, self.hpos,
                wait4commit=done)
            if done:
                self._source.remove_TaskLoader(self)
                self.stop()

        except Exception:
            logging.exception(self._fn)
        return


class AsyncProcExecLoader(TaskLoader, ABC):
    __slots__ = (
        '_cmd_ary', '_task', '_proc', '_is_running_apel',
        '_cwd', '_stderr_fname', '_stdout_fname',
        '_cond', '_process_terminated')

    def __init__(self, cmd_ary, source,
                 stdout_fname, stderr_fname, cwd):
        super().__init__(source)
        self._cmd_ary = cmd_ary
        self._task = None
        self._proc = None
        self._is_running_apel = False
        self._process_terminated = False
        self._cond = asyncio.Condition()
        self._stdout_fname = stdout_fname
        self._stderr_fname = stderr_fname
        self._cwd = cwd

    async def run(self):
        if self._is_running_apel:
            return
        self._is_running_apel = True
        cmd_task = asyncio.create_task(self._start_cmd(
            stdout_fname=self._stdout_fname,
            stderr_fname=self._stderr_fname,
            cwd=self._cwd))
        print("asyncprocexecloader run start", self._taskme)
        try:
            await super().run()
        except Exception:
            logging.exception(self._cmd_ary)
            raise
        print("awaiting cmd task", self._taskme, cmd_task)

        rc = await cmd_task
        print("asyncprocexecloader run done",
              self._taskme, cmd_task, rc)

    async def _wait_for_cmd_done(self):
        async with self._cond:
            while not self._process_terminated:
                await self._cond.wait()
        return

    async def _start_cmd(
            self, stdout_fname=None, stderr_fname=None,
            cwd=None):
        stdout = asyncio.subprocess.DEVNULL
        stderr = asyncio.subprocess.DEVNULL
        CREATE_NO_WINDOW = 0x08000000
        # DETACHED_PROCESS = 0x00000008
        rc = None
        try:
            if stdout_fname is not None:
                stdout = open(stdout_fname, "w+")
            if stderr_fname is not None:
                stderr = open(stderr_fname, "w+")
            p = await asyncio.create_subprocess_exec(
                *self._cmd_ary,
                stdin=asyncio.subprocess.PIPE,
                stdout=stdout,
                stderr=stderr,
                creationflags=CREATE_NO_WINDOW,
                cwd=cwd,
            )
            async with self._cond:
                self._proc = p
                self._cond.notify_all()
                proc = self._proc
            rc = await proc.wait()
            print("process done!!!")
        except OSError as e:
            return e
        finally:
            if stdout != asyncio.subprocess.DEVNULL:
                stdout.close()
            if stderr != asyncio.subprocess.DEVNULL:
                stderr.close()
            self._process_terminated = True

            print("proc done", self._taskme)
        return rc

    async def _handle_source_eos_if_self_at_end(self, stdin):
        if self._process_terminated:
            return False
        size = self._source.finished_size()
        if size is None:
            return False
        async with self._cond:
            PE = self._last_proc_chunk_end_pos

        assert PE <= size
        print("Got finished size", PE, size)

        if PE == size:
            self._process_terminated = True
            stdin.write_eof()
            self.stop()

    async def handle_source_eos(self, exc=None):
        await self._send_chunk(None)
        if exc:
            self._force_terminate = True

    async def _send_chunk(self, chunk):
        if chunk is None:
            pass
        elif len(chunk):
            pass
        else:
            return None
        while True:
            async with self._cond:
                if self._process_terminated:
                    return None
                if self._proc is None:
                    await self._cond.wait()
                    continue

                stdin = self._proc.stdin
            if stdin is None:
                return None

            if chunk is not None:
                #    print("wrting in", self, len(chunk),
                #          self._last_proc_chunk_start_pos)
                stdin.write(chunk)
            if await self._handle_source_eos_if_self_at_end(stdin):
                return None
            break

        try:
            if chunk:
                await stdin.drain()
        except ConnectionResetError as e:
            await self.handle_source_eos(exc=e)
        except BrokenPipeError as e:
            await self.handle_source_eos(exc=e)
        return


class AsyncStreamTaskMgr:
    __slots__ = ('_afh', '_ahttpclient', '_taskloaders', '_url', '_fname',
                 '_taskme', '_pos', '_taskme', '_running',
                 '_cond', '_request_download', '_download', '_file_exists',
                 'hpos', 'need2clearfile', 'need2clearhash', 'connected',
                 'file_waiter', 'localfile')

    def __init__(self, url: str = None, fname: Path = None,
                 ahttpclient: httpx.AsyncClient = None, hpos=None):
        self._fname = Path(fname)
        self._ahttpclient = ahttpclient
        self._taskloaders = []
        self._url = url
        self._afh = None
        self._pos = None
        self.hpos = hpos
        self._running = False
        self._taskme = None
        self._request_download = False
        self._download = False
        self._cond = asyncio.Condition()
        self.file_waiter = 0
        self.need2clearfile = False
        self.need2clearhash = False
        self.connected = False
        self.localfile = None

    async def wait_for_clears(self):
        if self.need2clearfile or self.need2clearhash:
            pass
        else:
            return False
        async with self._cond:
            while self.need2clearfile or self.need2clearhash:
                await self._cond.wait()
        return True

    async def wait_for_connect(self):
        if self.connected:
            return self.connected
        async with self._cond:
            while True:
                if self.connected:
                    break
                if self.need2clearfile:
                    break
                if self.need2clearhash:
                    break
                await self._cond.wait()
        return self.connected

    async def reset(self, clear=False):
        for t in self._taskloaders:
            if t is not None:
                await t.reset(clear=clear)

    async def file_exists(self):
        return (await get_file_size(self._fname)) is not None

    async def start_download(self):
        async with self._cond:
            self._request_download = True
            while self._download != self._request_download:
                self._cond.notify_all()
                await self._cond.wait()

    async def stop_download(self):
        async with self._cond:
            self._request_download = False
            while self._download != self._request_download:
                self._cond.notify_all()
                await self._cond.wait()

    async def truncate_file(self):
        self.pos = 0
        for t in self._taskloaders:
            t.reset()
        await self._afh.truncate()

    def is_offline(self):
        return self._url is None or self._afh is None

    def run_task(self):
        if self._taskme is None:
            self._taskme = asyncio.create_task(self.run())
        return self

    async def wait_for_started(self):
        while not self._running:
            await asyncio.sleep(0.01)

    async def add_TaskLoader(self, obj: "TaskLoader") -> None:
        assert obj
        if obj in self._taskloaders:
            return
        self.run_task()  # noop if already running
        obj.run_task()  # noop if already running
        async with self._cond:
            self._taskloaders.append(obj)
            self._cond.notify_all()
        obj._source = self
        return self

    async def remove_TaskLoader(self, obj: "TaskLoader") -> None:
        if obj in self._taskloaders:
            async with self._cond:
                self._taskloaders.remove(obj)
                obj._is_running = False
                self._cond.notify_all()

            obj._source = None
        return

    async def proc_chunk(self, f, pos, chunk) -> int:
        if chunk is None:  # end of stream
            async with self._cond:
                if self.file_waiter:
                    self._cond.notify_all()
            return None
        start_pos = pos
        pos += len(chunk)
        await f.write(chunk, offset=start_pos)
        async with self._cond:
            waiter = self.file_waiter
        if waiter:
            await self._afh.fsync()
        async with self._cond:
            if self.file_waiter:
                self._cond.notify_all()
        return pos

    async def wait_for_download_start(self):
        async with self._cond:
            while not self._request_download:
                await self._cond.wait()
            if self._download != self._request_download:
                self._download = self._request_download
                self._cond.notify_all()

    async def stream_local_file(self, fn, hpos) -> None:
        hpos = hpos
        self._pos = 0
        try:
            size = await get_file_size(self._fname)
            if size is None:
                logging.warn(f"{fn}: Not found: Skipping")
                return
            async with AIOFile(self._fname, 'rb') as self._afh:
                async with self._cond:
                    self._cond.notify_all()
                eof = False
                while True:
                    await self.wait_for_download_start()
                    eof = await self.read_stream_chunks(Reader(self._afh))
                    if eof:
                        break
        except FileNotFoundError:
            logging.exception("stream_local_file")

    async def setup_connection(
            self, fn, hpos: HashPos=None):
        size = await get_file_size(self._fname)
        done = False
        if hpos is not None:
            if (size is not None and
                    hpos.loaded_pos is not None and
                    size < hpos.loaded_pos and False):
                raise ValueError(
                    f"{fn}: size({size})<load pos({hpos.loaded_pos})")
            self._pos = hpos.loaded_pos
            if hpos._done:
                done = True
        if self._pos is None:
            self._pos = 0
        if done and self._pos > 0:
            rhdr1 = {'Range': 'bytes=%d-' % (self._pos)}
            rhdr2 = {'Range': 'bytes=%d-' % (self._pos - 1)}
            t1 = asyncio.create_task(
                self._ahttpclient.stream(
                    'GET', self._url,
                    headers=rhdr1)
            )

            t2 = asyncio.create_task(
                self._ahttpclient.stream(
                    'GET', self._url,
                    headers=rhdr2)
            )
            r = await asyncio.gather(*[t1, t2])

            if not (r[0].status_code == 416 and r[1].status_code == 206):
                done = False
            await r[1].aclose()
            if done:
                await r[0].aclose()
                return None
            return await r[0]

        resume_header = {'Range': 'bytes=%d-' % (self._pos)}
        # if self._pos == 0:
        #    resume_header = None
        strm = None
        valid = False
        try:
            strm = await self._ahttpclient.get(self._url,
                                               headers=resume_header)
            await asyncio.sleep(0)
            status = strm.status_code
            if (not self.need2clearhash and
                size is not None and hpos is not None
                and hpos.loaded_pos is not None and
                    size < hpos.loaded_pos):
                raise ValueError(
                    f'{fn}:fsize({size})<hsize({hpos.loaded_pos})')
            if (not self.need2clearhash and
                size is None and hpos is not None
                    and hpos.loaded_pos is not None):
                raise FileNotFoundError(f'{fn}:hsize({hpos.loaded_pos})')
            valid = (status == 206 or status == 200)
            if status == 200 and resume_header:
                if size:
                    valid = False
                    raise FileExistsError(
                        f'{fn}:Unsupported partial resume. Clearing')
            if status == 416:
                async with self._cond:
                    self.need2clearhash = True
                    self.need2clearfile = size is not None
                    self._cond.notify_all()
                return None
            strm.raise_for_status()
        except FileNotFoundError:
            async with self._cond:
                self.need2clearhash = True
                self._cond.notify_all()
        except FileExistsError:
            async with self._cond:
                self.need2clearfile = True
                self._cond.notify_all()
        except ValueError:
            async with self._cond:
                self.need2clearhash = hpos.loaded_pos is not None
                self.need2clearfile = True
                self._cond.notify_all()
            logging.exception(f"{fn}:too short for hash.")
        except httpx.HTTPStatusError:
            raise
        except FileNotFoundError:
            async with self._cond:
                self.need2clearhash = (
                    hpos.loaded_pos is not None and hpos.loaded_pos > 0)
                self._cond.notify_all()
            logging.exception(f"{fn}:hash exists but file doesnt")
        except httpx.ConnectTimeout:
            await asyncio.sleep(1)
        except httpx.ReadError:
            await asyncio.sleep(1)
        except httpx.ConnectError:
            print("Connection Error: Retry " + str(self._url))
            await asyncio.sleep(10)
        except Exception:
            raise
        finally:
            if not valid:
                if strm is not None:
                    await strm.aclose()
        return strm if valid else None

    async def read_stream_chunks(self, resp):
        async for chunk in resp:
            self._pos = await self.proc_chunk(self._afh, self._pos, chunk)
            if not self._request_download:
                return False
            await asyncio.sleep(0)
        return True

    async def stream_remote_file(
            self, fn: str, hpos: HashPos) -> None:
        self._pos = 0
        eof = False
        size = await get_file_size(self._fname)
        if hpos is not None and size and hpos.loaded_pos <= size:
            if self._afh is None:
                self._afh = AIOFile(self._fname, 'wb+')
                await self._afh.open()
                async with self._cond:
                    self._cond.notify_all()

        while True:
            try:
                strm = None
                await self.wait_for_download_start()
                try:
                    strm = await self.setup_connection(
                        fn, hpos=hpos)
                except httpx.ReadTimeout:
                    if strm is not None:
                        await strm.aclose()
                    await asyncio.sleep(1)
                    continue
                except httpx.HTTPStatusError as exc:
                    logging.error(
                        f"{fn}: HTTP err code:{exc.response.status_code}"
                        f" while requesting {exc.request.url!r}.")
                    await asyncio.sleep(10)
                    continue
                except Exception:
                    logging.exception(f"{fn}")
                    await asyncio.sleep(10)
                    continue

                hpos = None
                if strm is None:
                    continue

                if await self.wait_for_clears():
                    async with self._cond:
                        self.connected = True
                        await self._cond.notify_all()

                if self._afh is None:
                    self._afh = AIOFile(self._fname, 'wb+')
                    await self._afh.open()
                    async with self._cond:
                        self._cond.notify_all()

                try:
                    eof = await self.read_stream_chunks(
                        strm.aiter_bytes(chunk_size=32768))
                except httpx.ReadTimeout:
                    if self._afh is not None:
                        await self._afh.fsync()
                    if strm is not None:
                        await strm.aclose()
                    continue

                if not eof:
                    continue
                break

            except Exception:
                logging.exception("stream_remote_file")

    async def run(self) -> None:
        url = self._url
        if self._running:
            return

        self._running = True

        try:
            self.localfile = False
            if url and url.startswith("file://"):
                if self._fname is None:
                    print("filename does not exist")
                    self._fname = url[7:]
                    self._url = url = None
                elif Path(self._fname).name == url[7:]:
                    self._url = url = None
                else:
                    print(self._url, self._fname)
                    assert False
                self.localfile = True
            if not url:
                self.localfile = True
            if self.localfile:
                fn = Path(self._fname).name
                logging.warn(f"START amgr {fn}")
                await self.stream_local_file(fn, self.hpos)
                return
            fn = Path(self._fname).name
            logging.warn(f"START amgr {fn}")
            await self.stream_remote_file(fn, self.hpos)
        except asyncio.exceptions.CancelledError:
            pass
        finally:
            for i in self._taskloaders:
                await i.handle_source_eos()
            async with self._cond:
                while len(self._taskloaders):
                    await self._cond.wait()
            if self._afh is not None:
                await self._afh.close()
            try:
                logging.warn(f"EXIT  amgr {fn}")
            finally:
                pass
            self._running = False
            self._taskme = None
        return

    def finished_size(self):
        if self._taskme is None or self._url is None:
            return self._pos
        return None
