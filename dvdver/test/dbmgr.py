'''
Created on Mar 7, 2022

@author: Cote Phinnizee
'''
import asyncio
from collections import defaultdict
import hashlib
import logging
import queue
import sqlite3
import threading
import time

import aiosql
from retry import retry


def setup_db_conn(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA read_uncommitted=ON")
    conn.row_factory = sqlite3.Row


class HashPos:
    __slots__ = (
        'loop', 'loaded_hash_str', 'loaded_pos',
        'isgood', 'pos', '_done', '_hashalgoobj',
        '_waiters_on_done', '_cond',
    )

    def __init__(
            self, hashalgoobj=hashlib.blake2b(digest_size=16)):
        '''
        This class is not thread safe for speed
        '''

        self.loop = asyncio.get_running_loop()
        self.loaded_hash_str = None
        self.loaded_pos = None
        self.isgood = True
        self.pos = 0
        self._done = False
        self._hashalgoobj = hashalgoobj
        self._waiters_on_done = 0
        self._cond = asyncio.Condition()

    async def update(self, buff):
        n = len(buff)
        if (self.pos >= self.loaded_pos
                or self.pos + n < self.loaded_pos):
            self._hashalgoobj.update(buff)
            async with self._cond:
                self.pos += n
                if self._waiters_on_done:
                    self._cond.notify_all()
            return
        postloadn = self.pos + n - self.loaded_pos
        preloadn = n - postloadn
        self._hashalgoobj.update(buff[0:preloadn])
        if self.isgood and self.loaded_hash_str is not None:
            self.isgood = (
                self._hashalgoobj.digest() == self.loaded_hash_str)
        if postloadn:
            self._hashalgoobj.update(buff[preloadn:postloadn])
        async with self._cond:
            self.pos += n
            if self._waiters_on_done:
                self._cond.notify_all()

    async def set_done(self):
        if self._waiters_on_done:
            async with self._cond:
                self._done = True
                if self._waiters_on_done:
                    self._cond.notify_all()

    async def wait_for_hash(self, done=True):

        async with self._cond:
            self._waiters_on_done += 1
            try:
                while (done and not self._done or
                       not done and not (
                           self.loaded_pos is None or
                           self.pos >= self.loaded_pos
                       )
                       ):
                    await self._cond.wait()
            finally:
                self._waiters_on_done -= 1

    def checking(self):
        return self.pos < self.loaded_pos


'''
  This is farmed out to a separate thread because of
  performance reasons. Every file flows through here.
 '''


class DbHashFile(threading.Thread):
    __slots__ = (
        '_lock', '_queue', 'dbmgr', '_fns', '_wait4commit',
        '_processing', 'CHECKPOINT_TIMEOUT', 'loop',
        'nextreqsubmittm'
    )

    def __init__(self, dbmgr):
        super().__init__()

        self._lock = asyncio.Lock()
        self._queue = queue.Queue()
        self.dbmgr = dbmgr
        self._fns = defaultdict(lambda: {})
        self._wait4commit = False
        self._processing = False
        self.CHECKPOINT_TIMEOUT = 60  # seconds
        self.loop = asyncio.get_running_loop()
        self.nextreqsubmittm = None

    ''' return whether true submit needed now '''

    def _trysubmit(self, flush):
        now = self.clock()
        if not flush:
            # check to see if you need
            # to turn on flush because of expiration.
            if self.nextreqsubmittm is None:
                self.nextreqsubmittm = (
                    now + self.CHECKPOINT_TIMEOUT)
                flush = False
            elif self.nextreqsubmittm <= now:
                flush = True
        if flush:
            self.nextreqsubmittm = (
                now + self.CHECKPOINT_TIMEOUT)
        else:
            return

        new_fns = defaultdict(lambda: {})
        self._queue.put(self._fns)
        self._fns = new_fns

    async def put_hash_fn(
            self, fn, fntype, hashposobj,
            flush=False):
        async with self._lock:
            self._fns[fn][fntype] = hashposobj
            self._trysubmit(flush)

    def clock(self):
        return time.monotonic()

    async def arun_exc(self, func, *args):
        return await self.loop.run_in_executor(None, func, *args)

    def run(self):
        with sqlite3.connect(self.dbmgr._dbfn) as db:
            setup_db_conn(db)
            while True:
                fns = self._queue.get()
                if fns is None:
                    break
                self.dbmgr._replace_hash_fns(fns, db)


class DbMgr():
    __slots__ = (
        '_queries', '_dbfn', 'dbhf', 'loop',
        '_hashalgofactory')

    def __init__(self, dbfn, sqlfile, hashalgofactory):
        self._queries = aiosql.from_path(
            sqlfile, "sqlite3")
        self._dbfn = dbfn
        self.loop = asyncio.get_running_loop()
        self.dbhf = DbHashFile(self)
        self.dbhf.daemon = True
        self.dbhf.start()
        self._hashalgofactory = hashalgofactory

    def _replace_hash_fns(self, fns, db):
        queries = self._queries
        hash_fn_ary = []
        for fn, h in fns.items():
            for fntype, hashpos in h.items():
                hashstr, pos, done = (
                    hashpos._hashalgoobj.digest(),
                    hashpos.pos,
                    hashpos._done)
                hash_fn_ary.append({
                    'fn': fn,
                    'fntype': fntype,
                    'hashstr': hashstr,
                    'pos': pos,
                    'done': done,
                })

        while True:
            try:
                queries.replace_hash_fns(db, hash_fn_ary)
                db.commit()
                break
            except sqlite3.OperationalError:
                pass  # for pypy intermentant failures.
            except Exception:
                logging.exception("replace_hash_fns")
                break
        return

    async def put_hash_fn(
            self, fn, fntype, hashposobj,
            wait4commit=False):
        return await self.dbhf.put_hash_fn(fn, fntype, hashposobj, wait4commit)

    async def replace_pass1(self, fn, done):
        return await self.arun_exc(self._replace_pass1, fn, done)

    def _replace_pass1(self, fn, done):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.replace_p1_fns_done(db, fn=fn, done=done)
            except Exception:
                logging.exception()

    async def add_menubreak_rows(self, menubreak_ary):
        return await self.arun_exc(self._add_menubreak_rows, menubreak_ary)

    def _add_menubreak_rows(self, menubreak_ary):
        queries = self._queries
        if len(menubreak_ary) == 0:
            return
        with sqlite3.connect(self._dbfn) as db:
            setup_db_conn(db)
            while True:
                try:
                    queries.add_menubreak_rows(db, menubreak_ary)
                    break
                except sqlite3.OperationalError:
                    pass  # for pypy intermentant failures.
                except Exception:
                    logging.exception("add_menubreak_rows")
                    break

    async def add_dvd_menu_row(self, h):
        return await self.arun_exc(self._add_dvd_menu_row,  h)

    def _add_dvd_menu_row(self, h):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.add_dvd_menu_row(db, **h)
            except Exception:
                logging.exception("add_dvd_menu_row")

    async def arun_exc(self, func, *args):
        return await self.loop.run_in_executor(None, func, *args)

    async def get_new_hashpos(self, fn, fntype) -> HashPos:
        hashpos = HashPos(self._hashalgofactory())
        return await self.arun_exc(self._get_new_hashpos, fn, fntype, hashpos)

    def _get_new_hashpos(self, fn, fntype, hashpos) -> HashPos:
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                r = queries.get_file_hash(db, fn, fntype)
                if r is None or len(r) == 0:
                    hashpos.loaded_hash_str = None
                    hashpos.loaded_pos = 0
                    hashpos._done = None
                    return hashpos
                r = r[0]
                hashpos.loaded_hash_str = r['hashstr']
                hashpos.loaded_pos = r['pos']
                hashpos._done = r['done']
                return hashpos

            except Exception:
                logging.exception("get_file_hash2")

    async def delete_hashpos(self, fn, fntype) -> None:
        return await self.arun_exc(self._delete_hashpos, fn, fntype)

    def _delete_hashpos(self, fn, fntype) -> HashPos:
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.delete_hash_fn(db, fn, fntype)
            except Exception:
                logging.exception("delete_hashpos")

    async def delete_all_menubreaks(self):
        return await self.arun_exc(self._delete_all_menubreaks)

    def _delete_all_menubreaks(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.delete_all_menubreaks(db)
            except Exception:
                logging.exception("delete_all_menubreaks")

    async def setupdb(self, create_schema, h, fn, q):
        q2 = queue.Queue(10)
        t = asyncio.create_task(
            self.arun_exc(
                self._setupdb, create_schema, h, fn, q2))
        while True:
            item = await q.get()
            q2.put(item)
            if item is None:
                break
        await t

    def _setupdb(self, create_schema, h, fn, q):
        queries = self._queries
        fn = fn
        hary = list(
            map(lambda x: {'key': x, 'dir': str(h[x])},
                h.keys()))

        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                if create_schema:
                    queries.create_schema(db)
                queries.delete_all_local_paths(db)
                queries.add_local_paths(db, hary)
                while True:
                    try:
                        queries.start_load(db)
                        break
                    except sqlite3.OperationalError:
                        continue  # Immediate retry this for pypy
                    except Exception:
                        raise

                while True:
                    h = q.get()
                    if h is None:
                        break
                    queries.add_dvd_menu_row(db, **h)
                queries.finish_load(db)
                db.commit()

            except Exception:
                logging.exception("create_schema")

    async def get_spumux_rows(self, dvdnum, dvdmenu):
        return await self.arun_exc(self._get_spumux_rows, dvdnum, dvdmenu)

    def _get_spumux_rows(self, dvdnum, dvdmenu):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_spumux_rows(
                    db, dvdnum=dvdnum, dvdmenu=dvdmenu)
            except Exception:
                logging.exception("get_spumux_rows")

    async def get_toolbar_rows(self, dvdnum, renum_menu):
        return await self.arun_exc(self._get_toolbar_rows, dvdnum, renum_menu)

    def _get_toolbar_rows(self, dvdnum, renum_menu):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_toolbar_rows(
                    db, dvdnum=dvdnum, renum_menu=renum_menu)
            except Exception:
                logging.exception("get_toolbar_rows")

    async def get_header_rows(self, dvdnum, renum_menu):
        return await self.arun_exc(self._get_header_rows, dvdnum, renum_menu)

    def _get_header_rows(self, dvdnum, renum_menu):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_header_title_rows(
                    db, dvdnum=dvdnum, renum_menu=renum_menu)
            except Exception:
                logging.exception("get_header_rows")

    async def get_dvdauthor_rows(self, dvdnum):
        return await self.arun_exc(self._get_dvdauthor_rows, dvdnum)

    def _get_dvdauthor_rows(self, dvdnum):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_dvdauthor_rows(
                    db, dvdnum=dvdnum)
            except Exception:
                logging.exception("get_dvdauthor_rows")

    async def get_dvd_files(self):
        return await self.arun_exc(self._get_dvd_files)

    def _get_dvd_files(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_dvd_files(db)
            except Exception:
                logging.exception("get_dvd_files")

    async def get_filenames(self):
        return await self.arun_exc(self._get_filenames)

    def _get_filenames(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_filenames(db)
            except Exception:
                logging.exception("get_dvd_files")

    async def get_dvdmenu_files(self):
        return await self.arun_exc(self._get_dvdmenu_files)

    def _get_dvdmenu_files(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_dvdmenu_files(db)
            except Exception:
                logging.exception("get_dvdmenu_files")

    async def get_menu_files(self):
        return await self.arun_exc(self._get_menu_files)

    def _get_menu_files(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_menu_files(db)
            except Exception:
                logging.exception("get_menu_files")

    async def get_all_dvdfilemenu_rows(self):
        return await self.arun_exc(self._get_all_dvdfilemenu_rows)

    def _get_all_dvdfilemenu_rows(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_all_dvdfilemenu_rows(db)
            except Exception:
                logging.exception("get_all_dvdfilemenu_rows")
