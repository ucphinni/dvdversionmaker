'''
Created on Mar 14, 2022

@author: Cote Phinnizee
'''
import logging
import os
import trio


class AioFileRW:
    def __init__(self, fn):
        self.cond = trio.Condition()
        self.readers = set()
        self.afh = None
        self.fn = fn
        self.resetnum = 0
        self.num_waiters = 0
        self.done = False
        self.ok2read = False

    async def mark_done(self):
        await self.afh.flush()
        async with self.cond:
            self.done = True
            self.cond.notify_all()
            while len(self.readers):
                await self.cond.wait()
        self.afh = None

    async def write(self, buff):
        if not self.afh:
            self.afh = await trio.open_file(self.fn, 'wb+', 0)
            async with self.cond:
                self.ok2read = True
                self.cond.notify_all()
        await self.afh.write(buff)
        async with self.cond:
            if self.num_waiters:
                await self.afh.flush()
                self.ok2read = True
                self.cond.notify_all()

    async def reset_file(self):
        async with self.cond:
            self.resetnum += 1
            self.ok2read = False
            self.cond.notify_all()
            while self.num_waiters != len(self.readers):
                await self.cond.wait()
            self.ok2read = True
            await self.afh.truncate(0)
            await self.afh.seek(0, os.SEEK_SET)
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
