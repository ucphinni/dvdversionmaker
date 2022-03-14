'''
Created on Mar 13, 2022

@author: Cote Phinnizee
'''

import logging
import os

import trio


class RW:
    def __init__(self, fn):
        self.cond = trio.Condition()
        self.readers = set()
        self.size = None
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

                if self.afh is None:
                    async with self.source.cond:
                        self.resetnum = self.source.resetnum
                        await self.wait_on_cv_for_read()
                        self.afh = await trio.open_file(
                            self.source.fn, 'rb', 0)

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
                return ret

            def reset_needed(self):
                return self.tobereset

            async def delete(self):
                print("deleting self")
                async with self.source.cond:
                    self.source.readers.remove(self)
                    self.source.cond.notify_all()

            async def reset_done(self):
                async with self.source.cond:
                    self.resetnum = self.source.resetnum

        reader = Reader(self)

        async with self.cond:
            self.readers.add(reader)
        return reader

    async def del_reader(self, reader):
        await reader.delete()


async def test(nm, w):
    r = await w.new_reader()
    while True:
        buff = await r.read()
        if buff is None and r.reset_needed():
            print(f"{nm}: reset")
            await r.reset_done()
        elif buff is not None:
            print(f"{nm}: ", buff)


async def trio_main():
    async with trio.open_nursery() as nursery:
        w = RW("test")
        nursery.start_soon(test, "s1", w)
        nursery.start_soon(test, "s2", w)
        for i in range(1000):
            s = str(i) + "\n"
            await w.write(s.encode('utf-8'))
        print("reseting")
        await w.reset_file()
        print("reset")
        for i in range(2000):
            await w.write(str(i).encode('utf-8'))
        print("marking done")
        await w.mark_done()
        print("done")
trio.run(trio_main)
