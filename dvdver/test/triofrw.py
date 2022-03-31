'''
Created on Mar 14, 2022

@author: Cote Phinnizee
'''
import logging
import os

import trio


class TrioFileRW:
    def __init__(self, fn: trio.Path, pos=None, mode='wb+'):
        self.cond = trio.Condition()
        self.readers = set()
        self.afh = None
        self.fn: trio.Path = fn
        self.resetnum = 0
        self.num_waiters = 0
        self.done = False
        self.ok2read = False
        self.mode = mode
        self.start_pos = pos
        self._file_existed = None

    async def file_existed(self):
        if self._file_existed is not None:
            return self._file_existed
        if self.afh is not None:
            await self.afh.seek(0, os.SEEK_END)
            pos = await self.afh.tell()
            self._file_existed = pos > 0
            return self._file_existed

        if not await trio.Path(self.fn).is_file():
            self._file_existed = False
            return self._file_existed

        st = await trio.Path(self.fn).stat()
        self._file_existed = st.st_size > 0
        return self._file_existed

    async def pos(self):
        if self.afh is not None:
            return await self.afh.tell()
        try:
            if not await self.fn.exists():
                return None
            if not await self.fn.is_file():
                return None
            st = await self.fn.stat()
            return st.st_size
        except Exception:
            logging.exception(f"pos:{self.fn.name}")

    async def aclose(self):
        if self.afh is not None:
            await self.afh.flush()
            async with self.cond:
                self.done = True
                self.cond.notify_all()
                while len(self.readers):
                    await self.cond.wait()
            await self.afh.aclose()
            self.afh = None

    async def write(self, buff):
        if not self.afh:
            self.afh = await trio.open_file(self.fn, self.mode, 0)
            if self.start_pos is not None:
                await self.afh.truncate(self.start_pos)
                await self.file_existed()
            async with self.cond:
                self.ok2read = True
                self.cond.notify_all()
        await self.afh.write(buff)
        async with self.cond:
            if self.num_waiters or not self.ok2read:
                self.ok2read = True
            if self.num_waiters:
                self.cond.notify_all()

    async def reset_file(self, pos=0, file_size=0):
        async with self.cond:
            self.start_pos = pos
            if len(self.readers) == 0:
                return
            self.resetnum += 1
            self.ok2read = False
            self.cond.notify_all()
            while self.num_waiters != len(self.readers):
                await self.cond.wait()
            self.ok2read = True
            await self.afh.truncate(file_size)
            await self.afh.seek(pos, os.SEEK_SET)
            await self.afh.flush()
            self.cond.notify_all()

    async def new_reader(self):
        class Reader:
            def __init__(self, source: "RW"):
                self.source = source
                self.resetnum = 0
                self.afh = None
                self.done = False
                self.tobereset = False

            async def wait_on_cv_for_read(self):
                self.source.num_waiters += 1
                if len(self.source.readers) == self.source.num_waiters:
                    self.source.cond.notify_all()
                while not self.source.ok2read:
                    await self.source.cond.wait()
                self.source.num_waiters -= 1
                if not self.source.num_waiters:
                    self.source.cond.notify_all()
                self.tobereset = self.resetnum != self.source.resetnum
                return self.source.done

            async def read(self):
                if self.tobereset:
                    async with self.source.cond:
                        self.resetnum = self.source.resetnum
                elif self.afh is None:
                    async with self.source.cond:
                        self.resetnum = self.source.resetnum
                        done = await self.wait_on_cv_for_read()
                    if done:
                        return None
                    self.afh = await trio.open_file(
                        self.source.fn, 'rb', 0)
                while True:
                    ret = None
                    if not self.tobereset:
                        try:
                            ret = await self.afh.read(4096)
                        except Exception:
                            logging.exception("read")
                    if ret is not None and len(ret):
                        return ret
                    oldtobereset = self.tobereset
                    async with self.source.cond:
                        done = await self.wait_on_cv_for_read()
                        if self.tobereset:
                            return None
                    if oldtobereset and not self.tobereset:
                        await self.afh.seek(0, os.SEEK_SET)
                    if not self.tobereset:
                        try:
                            ret = await self.afh.read(4096)
                        except Exception:
                            logging.exception("read")

                    if done and (ret is None or not ret) and not self.done:
                        await self.delete()
                        self.done = done
                        ret = None

                    if ret is None or len(ret) == 0:
                        ret = None
                    if self.done:
                        return None
                    if ret is not None:
                        return ret

            def reset_needed(self):
                return self.tobereset

            async def delete(self):
                async with self.source.cond:
                    if self in self.source.readers:
                        self.source.readers.remove(self)
                        self.source.cond.notify_all()

        reader = Reader(self)

        async with self.cond:
            self.readers.add(reader)
        return reader
