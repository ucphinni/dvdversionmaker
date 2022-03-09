'''
Created on Mar 8, 2022

@author: Publishers
'''
from abc import ABC
import asyncio
import logging
from pathlib import Path
from aiofile import (AIOFile, Reader)
import httpx
from dbmgr import HashPos


class TaskLoader:
    __slots__ = ('_queue', '_afh', '_is_running', '_last_proc_chunk_start_pos',
                 '_last_proc_chunk_end_pos', '_read_from_file_gap_end',
                 '_taskme', '_exec_obj', '_afhset', '_done_file_sz', '_cond',
                 '_finish_up', '_force_terminate', '_source', '_loaders',
                 '_parent',)

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

    def __init__(self, source, max_qsize=20):
        self.max_qsize = max_qsize
        self._is_running = False
        self._taskme = None
        self._cond = asyncio.Condition()
        self._finish_up = False
        self._force_terminate = False
        self._loaders = []
        self._parent = None
        self._source = source
        self._reset()

    async def reset(self, clear=False):
        self._reset(clear)

    def _reset(self, clear=False):
        clear = clear
        self._queue = asyncio.Queue(maxsize=self.max_qsize)
        self._last_proc_chunk_start_pos = 0
        self._last_proc_chunk_end_pos = 0
        self._read_from_file_gap_end = None
        self._done_file_sz = None
        if (self._is_running and self._afh is not None):
            self.set_afh(self._afh)

    def set_afh(self, afh: AIOFile):
        self._afh = afh
        if self._source is not None:
            pos = self._source.pos
            pos = pos if pos is not None else 0
            self.add_chunk(pos, bytearray(), block=False)

    async def _send_chunk(self, _):
        assert False  # Must Overide

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
                self._last_proc_chunk_end_pos = WE
                self._cond.notify_all()
            await self._send_chunk(chunk[PE - WS:])
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
        self._queue.put_nowait((None, None))

    def is_running(self):
        return self._is_running

    async def _fill_gap_from_file(self):
        pos = self._last_proc_chunk_end_pos
        reader = Reader(self._afh, offset=pos, chunk_size=512 * 1024)
        while self._is_running:
            start = pos
            chunk = await reader.read_chunk()
            if not chunk:
                break

            pos += len(chunk)
            await self._proc_chunk(start, chunk)

            if not self._queue.empty() or self._force_terminate:
                break
        return

    async def handle_task_run_done(self):
        pass

    async def handle_source_eos(self):
        assert False

    async def run(self):
        self._is_running = True
        assert self._source is not None
        try:

            while self._is_running:
                if self._source is not None:
                    self._read_from_file_gap_end = self._source.pos
                    if (self._source._afh is not None and
                            self._read_from_file_gap_end is not None and
                            self._queue.empty()):
                        await self._fill_gap_from_file()

                if self._force_terminate:
                    return

                if not self._is_running:
                    break

                chunk_item = await self._queue.get()

                if self._force_terminate:
                    return
                if chunk_item[0] is None and chunk_item[1] is None:
                    break
                await self._proc_chunk(chunk_item[0], chunk_item[1])
                if self._force_terminate:
                    return
            fin_size = self._source.finished_size()
            assert fin_size is not None
            self._read_from_file_gap_end = fin_size
            if self._read_from_file_gap_end == 0:  # boundary case
                await self._proc_chunk(0, bytearray())
            else:
                await self._fill_gap_from_file()

        except asyncio.exceptions.CancelledError:
            print("Got Canceled")
            pass
        finally:

            self._is_running = False

        await self.handle_task_run_done()
        return None

    def truncate(self):
        pass

    def add_chunk(self, start_pos, chunk, block=True):
        try:
            if self._afh is None and block:
                self._queue.put((start_pos, chunk))
                return True
            else:
                self._queue.put_nowait((start_pos, chunk))
                return True
        except asyncio.QueueFull:
            return False


class HashLoader(TaskLoader):
    def __init__(self, dbmgr, ftype, source, fn):
        super().__init__(source)
        self.dbmgr = dbmgr
        self._fn = fn
        self._ftype = ftype
        self.hpos: HashPos = None

    def reset(self):
        super().reset()
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
        self.add_chunk(None, None)

    async def load_db_hash(self, fn, fntype):
        try:
            self.hpos = await self.dbmgr.get_new_hashpos(fn, fntype)
        except Exception:
            logging.exception("load_db_hash")

    async def _send_chunk(self, chunk):
        await self._hash_chunk(chunk)

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
                self._fn, self._ftype, self.hpos,
                wait4commit=done)
            if done:
                self._source.remove_TaskLoader(self)
                self.stop()

        except Exception:
            logging.exception(self._fn)
        return


class AsyncProcExecLoader(TaskLoader, ABC):
    def __init__(self, cmd_ary, source,
                 stdout_fname, stderr_fname, cwd):
        super().__init__(source)
        self._cmd_ary = cmd_ary
        self._task = None
        self._proc = None
        self._is_running_apel = False
        self._process_terminated = False
        self._process_terminating = False
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
            return
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
                 '_taskme', 'pos', '_taskme', '_running',
                 '_cond', '_request_download', '_download', '_file_exists')

    def __init__(self, url: str = None, fname: Path = None,
                 ahttpclient: httpx.AsyncClient = None):
        self._fname = Path(fname)
        self._ahttpclient = ahttpclient
        self._taskloaders = []
        self._url = url
        self._afh = None
        self.pos = None
        self._running = False
        self._taskme = None
        self._request_download = False
        self._download = False
        self._cond = asyncio.Condition()
        self._file_exists = None

    async def reset(self, clear=False):
        for t in self._taskloaders:
            await t.reset(clear=clear)

    def file_exists(self):
        if self._file_exists is not None:
            return self._file_exists
        try:
            Path(self._fname).stat().st_size
            return True
        finally:
            return False

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

    def add_TaskLoader(self, obj: "AsyncProcExecLoader") -> None:
        if obj in self._taskloaders:
            return
        self.run_task()  # noop if already running
        obj.run_task()  # noop if already running
        self._taskloaders.append(obj)
        if self._afh is not None:
            obj.set_afh(self._afh)
        return self

    def remove_TaskLoader(self, obj: "AsyncProcExecLoader") -> None:
        if obj in self._taskloaders:
            self._taskloaders.remove(obj)
        return

    async def proc_chunk(self, f, pos, chunk) -> int:
        if chunk is None:  # end of stream
            for i in self._taskloaders:
                i.add_chunk(None, None, block=False)
            return
        start_pos = pos
        pos += len(chunk)
        await f.write(chunk, offset=start_pos)
        for i in self._taskloaders:
            m = memoryview(chunk)
            m.toreadonly()
            i.add_chunk(start_pos, m, block=True)
        return pos

    async def run(self) -> None:
        url = self._url
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
        offline = url is None
        web_file_sz = None
        try:
            file_sz = Path(self._fname).stat().st_size
            self._file_exists = True
        except FileNotFoundError:
            file_sz = 0

        if self._fname.exists() and not self._fname.is_file():
            raise FileNotFoundError("a non file is found")

        filename = str(self._fname) if self._fname else ''

        firsttime = True
        resume_header = None
        pos = 0
        if filename != '':
            f = AIOFile(self._fname, 'ab+')
            await f.open()
        try:
            if filename != '':
                self._afh = f

                for i in self._taskloaders:
                    i.set_afh(self._afh)
            self._running = True
            while True:

                if not self._fname.exists():
                    file_sz = 0
                else:
                    self.pos = pos = file_sz
                if offline:
                    self.pos = pos = web_file_sz = file_sz

                resp = None
                if web_file_sz is not None and web_file_sz == file_sz:
                    break
                async with self._cond:
                    while not self._request_download:
                        await self._cond.wait()
                    if self._download != self._request_download:
                        self._download = self._request_download
                        self._cond.notify_all()

                try:
                    if not firsttime and web_file_sz is None:
                        resume_header = {'Range': 'bytes=%d-' % (file_sz)}
                    elif not firsttime:
                        resume_header = {
                            'Range': 'bytes=%d-%d' % (file_sz, web_file_sz - 1)
                        }
                    async with self._ahttpclient.stream('GET', url,
                                                        headers=resume_header
                                                        ) as resp:

                        resp_code = resp.status_code
                        if resp_code == 206:
                            print("Got 206")
                        if (firsttime and resp_code == 200 and
                                resp.headers['Content-Length']):
                            # web_file_sz = int(resp.headers['Content-Length'])
                            firsttime = False
                            continue

                        if (resp_code == 200 or resp_code == 206 or
                                firsttime and resp_code == 416):
                            pass
                        else:
                            raise ValueError(
                                f"Bad code {resp_code} with" +
                                f" {filename} for {url}")
                        if (resp_code == 200 and web_file_sz is not None
                                and file_sz > 0 and web_file_sz != file_sz):
                            print(f"bad size {web_file_sz} {file_sz}")
                            pos = 0
                            await self.truncate_file()
                        elif (resp_code == 200 and web_file_sz is not None
                              and file_sz == web_file_sz):
                            break
                        elif resp_code == 206:
                            pos = file_sz
                        if resp_code == 416:
                            print('416 Range:', resume_header)
                            raise ValueError(
                                f"Bad code {resp_code} with " +
                                f"{filename} for {url}")
                        if web_file_sz == file_sz:
                            pos = file_sz
                        self.pos = pos
                        request_download = None
                        async for chunk in resp.aiter_bytes():
                            pos = await self.proc_chunk(f, self.pos, chunk)
                            self.pos = pos
                            async with self._cond:
                                request_download = self._request_download
                            if not request_download:
                                break
                        if not request_download:
                            continue
                        break
                except httpx.ConnectTimeout:
                    await asyncio.sleep(1)
                    await f.fsync()
                    if resp is not None:
                        await resp.aclose()
                    continue
                except httpx.ReadTimeout:
                    await f.fsync()
                    if resp is not None:
                        await resp.aclose()
                    await asyncio.sleep(10)
                    continue
                except httpx.ConnectError:
                    print("Connection Error: Retry " + str(url))
                    await asyncio.sleep(10)
                    continue
        except asyncio.exceptions.CancelledError:
            pass
        finally:
            self._taskme = None
        for i in self._taskloaders:
            await i.handle_source_eos()

        for i in self._taskloaders:
            while not i._is_running:
                await asyncio.sleep(0)

    def finished_size(self):
        if self._taskme is None or self._url is None:
            return self.pos
        return None
