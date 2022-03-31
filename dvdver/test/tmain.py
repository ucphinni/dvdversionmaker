'''
Created on Mar 13, 2022

@author: Cote Phinnizee
'''

import logging
from typing import AsyncGenerator
import urllib

from async_generator import aclosing
from httpx import AsyncClient
import httpx
import trio

from triocfgmgr import CfgMgr
from triodbm import (HashPos, DbMgr)
from triofrw import TrioFileRW


CfgMgr.set_paths(trio.Path(__file__).parent / 'download_dir',
                 trio.Path(__file__).parent / 'dl.sql')


async def async_generator_factory() -> AsyncGenerator[int, None]:
    yield 123


async def amain() -> None:
    async with aclosing(async_generator_factory()) as async_generator:
        async for element in async_generator:
            print(element)


class AClient:
    def __init__(self, aclient):
        self.aclient = aclient

    async def get(self, url, headers=None):
        return await self.aclient.get(url, headers=headers)


async def do_hash(nm, w, hp):
    r = await w.new_reader()
    try:
        while True:
            buff = await r.read()
            if buff is not None:
                await hp.update(buff)
                continue
            print(f"{nm}: empty buff")
            if r.reset_needed():
                oldhp = hp
                hp = HashPos(hp.fn, hp.fntype, hashpos=hp)
                await oldhp.mark_deleted()
                await oldhp.sync()
            else:
                print(f"{nm}: done hash")
                await hp.set_done()
                await hp.sync()
                break
    finally:
        await r.delete()


async def trio_main1():
    dbfn = CfgMgr.DLDIR / 'dl.db'

    fn, dbmgr = "test", DbMgr(
        dbfn, CfgMgr.SQLFILE)
    hashpos = HashPos(fn=fn, dbmgr=dbmgr)
    shortfn = "test"
    async with AsyncClient(
            http2=True, follow_redirects=True) as client:
        dl = UrlDownloader(
            "https://d34ji3l0qn3w2t.cloudfront.net/"
            "7218d46a-d205-4554-9598-c818aa68a0e4/1/"
            "pkon_E_025_r480P.mp4",
            trio.Path(fn), fn,
            hashpos, client)
        async with trio.open_nursery() as nursery:
            nursery.start_soon(dl.run)
            w = await dl.get_w()
            nursery.start_soon(do_hash, shortfn, w, hashpos)
        print("Done")


class UrlDownloader:
    def __init__(
            self, url, fn: trio.Path,
            shortfn: str, hashpos, aclient: AClient):
        self.url = url
        self.fn = fn
        self.shortfn = shortfn
        self.hashpos: HashPos = hashpos
        self.tfrm: TrioFileRW = None
        self.aclient = aclient
        self.file_dirty = False

    async def ensure_connection_using_hostname(self, url):
        url = url
        return True  # for now.

    async def handle_ready_for_write(self, t, first):
        pass

    async def get_w(self):
        while self.tfrm is None:
            await trio.sleep(1)
        return self.tfrm

    async def async_download_and_proc_file(self) -> AsyncGenerator:

        assert self.url and self.shortfn and self.fn
        self.tfrm = TrioFileRW(self.fn)
        if self.hashpos and self.hashpos.loaded_date_str:
            assert self.hashpos.final_size
        zero_byte_file = False
        hostname = urllib.parse.urlparse(self.url).netloc
        final_size = None
        while True:
            hp = self.hashpos
            try:
                await self.ensure_connection_using_hostname(hostname)
            except Exception:
                logging.exception(f"{self.shortfn}: presetup")
            try:
                test_header = {'Range': 'bytes=0-0'}
                async with self.aclient.stream(
                    'GET', self.url,
                        headers=test_header) as strm:
                    if strm.status_code == 206:
                        if 'content-length' not in strm.headers:
                            raise ValueError(f"{self.shortfn}: setup 206."
                                             " No Content-Length")
                        if 'content-range' not in strm.headers:
                            raise ValueError(f"{self.shortfn}: setup 206."
                                             " No content-range")
                        total_size = strm.headers['content-range'].split(
                            '/')[-1]
                        total_size = int(total_size)
                        hp.final_size, hp.etag, hp.date_str = (
                            total_size, (
                                strm.headers['etag']
                                if 'etag' in strm.headers
                                else None), (
                                strm.headers['date']
                                if 'date' in strm.headers
                                else None))
                        async for chunk in strm.aiter_bytes():
                            chunk = chunk
                            got_first_byte = True
                    elif strm.status_code == 200:
                        raise NotImplementedError(
                            "Non-Partial fill not supported")
                    elif strm.status_code == 416:
                        zero_byte_file = True
                    else:
                        strm.raise_for_status()
                        raise ValueError(f"{self.shortfn}:Unknown dl"
                                         " presetup Error")
            except httpx.ReadTimeout:
                print("read timeout")
                continue
            if got_first_byte:
                break

        file_dirty = False
        if hp.loaded_etag is None and hp.etag is None:
            cketag = None
        elif hp.loaded_etag is None:
            cketag = True
        elif hp.etag is not None:
            cketag = hp.etag == hp.loaded_etag
        else:
            logging.warn(
                f"{self.shortfn}: etag in db and not in hdr. rming")
            hp.mark_deleted()
            await hp.sync()
            self.hashpos = HashPos(hashpos=hp)

        if cketag is None:
            if hp.loaded_date_str is None and hp.date_str is None:
                ckdate_str = None
            elif hp.loaded_date_str is None:
                ckdate_str = True
            elif hp.date_str is not None:
                ckdate_str = hp.date_str == hp.loaded_date_str
            else:
                logging.warn(
                    f"{self.shortfn}: date_str in db "
                    "and not in hdr.rming")
                hp.mark_deleted()
                await hp.sync()
                self.hashpos = HashPos(hashpos=hp)

            file_dirty = ckdate_str
        else:
            file_dirty = cketag
        if (file_dirty is not None and not file_dirty and
            hp.loaded_final_size is not None and
                hp.final_size != hp.loaded_final_size):
            logging.error(
                f"{self.shortfn}: "
                "Size change but etag/date consistent. rming from db")
            hp.mark_deleted()
            await hp.sync()
            self.hashpos = HashPos(hashpos=hp)

        self.file_dirty = file_dirty
        first = True
        # this is where readers will enter.
        if zero_byte_file:
            await self.handle_ready_for_write(self.tfrm, first)
            await self.tfrm.write(bytearray())
            await self.tfrm.aclose()
            self.hashpos.final_size = 0
            await self.hashpos.set_done()
            await self.hashpos.sync()
            return

        await self.handle_ready_for_write(self.tfrm, first)
        while True:
            try:

                if self.file_dirty and not first:
                    await self.handle_ready_for_write(self.tfrm, first)
                    await self.tfrm.reset_file()
                    self.file_dirty = False
                first = False
                await self.ensure_connection_using_hostname(hostname)
            except Exception:
                logging.exception(f"{self.shortfn}: stream connect")

            try:
                pos = await self.tfrm.pos()
                resume_header = {'Range': 'bytes=%d-' % (pos)} if pos else None
                print(f"looping {pos}")
                async with self.aclient.stream(
                    'GET', self.url,
                        headers=resume_header) as strm:
                    async for chunk in strm.aiter_bytes(
                            chunk_size=32768):
                        await self.tfrm.write(chunk)
                    pos = await self.tfrm.pos()
                    await self.tfrm.aclose()
                    break
            except httpx.ReadTimeout:
                print("read timeout")
                continue
        print(f"finishing {pos}")

    async def run(self):

        try:
            await self.async_download_and_proc_file()
            print("download done")

        finally:
            if self.tfrm is not None:
                await self.tfrm.aclose()
                self.tfrm = None


async def test(nm, w):
    r = await w.new_reader()
    try:
        while True:
            buff = await r.read()
            if buff is not None:
                print(f"{nm}: ", buff)
                continue
            if r.reset_needed():
                print(f"{nm}: reset")
            else:
                print(f"{nm}: done")
                break
    finally:
        await r.delete()


async def trio_main2():
    async with trio.open_nursery() as nursery:
        w = TrioFileRW("test")
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
        print("aclose")
        await w.aclose()
        print("done")


async def trio_main():
    async with trio.open_nursery() as nursery:
        nursery.start_soon(trio_main1)
trio.run(trio_main)
