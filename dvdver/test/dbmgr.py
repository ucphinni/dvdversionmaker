'''
Created on Mar 7, 2022

@author: Cote Phinnizee
'''
import asyncio
from collections import defaultdict
import logging
import queue
import sqlite3
import threading
import time

import aiosql


def setup_db_conn(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA read_uncommitted=ON")
    conn.row_factory = sqlite3.Row


class HashPos:
    def __init__(
            self, hashalgoobj, end_pos=None, loaded_hash_str=None):
        self._hashalgoobj = hashalgoobj
        if end_pos is None:
            end_pos = 0
        self.load_pos = end_pos
        self.isgood = True
        if end_pos == 0 and loaded_hash_str is None:
            loaded_hash_str = hashalgoobj.digest()
        else:
            assert end_pos != 0 or loaded_hash_str == hashalgoobj.digest()

        self.loaded_hash_str = loaded_hash_str
        self._pos = 0
        self._done = False
        self.lock = threading.Lock()

    def update(self, buff):
        n = len(buff)
        with self.lock:
            if (self._pos >= self.load_pos or self._pos + n < self.load_pos):
                self._hashalgoobj.update(buff)
                self._pos += n
                return
            postloadn = self._pos + n - self.load_pos
            preloadn = n - postloadn
            self._hashalgoobj.update(buff[0:preloadn])
            if self.isgood and self.loaded_hash_str is not None:
                self.isgood = (
                    self._hashalgoobj.digest() == self.loaded_hash_str)
            if postloadn:
                self._hashalgoobj.update(buff[preloadn:postloadn])
            self._pos += n

    async def set_done(self):
        return await self.arun_exc(
            self._set_done)

    def _set_done(self):
        with self.lock:
            self._done = True

    def get_props(self):
        with self.lock:
            return self._hashalgoobj.digest(), self._pos, self._done

    async def get_pos(self):
        return await self.arun_exc(self._get_pos)

    def _get_pos(self):
        with self.lock:
            return self._pos

    def good(self):
        with self.lock:
            return self.isgood

    def checking(self):
        with self.lock:
            return self._pos < self.load_pos

    async def arun_exc(self, func, *args):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, func, *args)


'''
  This is farmed out to a separate thread because of
  performance reasons. Every file flows through here.
 '''


class DbHashFile(threading.Thread):
    def __init__(self, dbfn, queries):
        super().__init__()

        self._cond = threading.Condition()
        self._dbfn = dbfn
        self._fns = defaultdict(lambda: {})
        self._wait4commit = False
        self._processing = False
        self._queries = queries
        self.CHECKPOINT_TIMEOUT = 5  # seconds
        self.loop = asyncio.get_running_loop()

    def put_hash_fn(
            self, fn, fntype, hashposobj,
            wait4commit=False):
        with self._cond:
            self._fns[fn][fntype] = hashposobj
            if wait4commit:
                while self._processing:
                    self._cond.wait()
                self._wait4commit = True
                self._cond.notify_all()
                while self._wait4commit:
                    self._cond.wait()

    def do_replace_hash_fns(self, queries, db):
        hash_fn_ary = []
        new_fns = defaultdict(lambda: {})
        while True:
            with self._cond:
                self._processing = True

                for fn, h in self._fns.items():
                    for fntype, hashpos in h.items():
                        hashstr, pos, done = hashpos.get_props()
                        hash_fn_ary.append({
                            'fn': fn,
                            'fntype': fntype,
                            'hashstr': hashstr,
                            'pos': pos,
                            'done': done,
                        })
                self._fns = new_fns
            try:
                queries.replace_hash_fns(db, hash_fn_ary)
                db.commit()
            except sqlite3.OperationalError:
                continue  # pypy intermentant failures.

            with self._cond:
                self._wait4commit = False
                self._processing = False
                self._cond.notify_all()
            break
        return

    def clock(self):
        return time.monotonic()

    def run(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            while True:
                end_tm = self.clock() + self.CHECKPOINT_TIMEOUT
                with self._cond:
                    while True:
                        if not self._fns:
                            pass
                        elif end_tm > self.clock() or self._wait4commit:
                            break
                        self._cond.wait(timeout=end_tm - self.clock())
                self.do_replace_hash_fns(queries, db)


class DbMgr():
    __slots__ = ['_queries', '_dbfn', 'dbhf', 'loop']

    def __init__(self, dbfn, sqlfile):
        self._queries = aiosql.from_path(
            sqlfile, "sqlite3")
        self._dbfn = dbfn
        self.dbhf = DbHashFile(dbfn, self._queries)
        self.dbhf.start()
        self.loop = asyncio.get_running_loop()

    async def put_hash_fn(
            self, fn, fntype, hashposobj,
            wait4commit=False):
        return await self.arun_exc(
            self.dbhf.put_hash_fn, fn, fntype, hashposobj, wait4commit)

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

    async def add_local_paths(self, ary):
        return await self.arun_exc(self._add_local_paths, ary)

    def _add_local_paths(self, ary):
        queries = self._queries
        if len(ary) == 0:
            return
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.delete_all_local_paths(db)
                for h in ary:
                    queries.add_local_paths(db, **h)
            except Exception:
                logging.exception("add_local_paths")

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

    async def get_file_hash(self, fn, fntype):
        return await self.arun_exc(self._get_file_hash, fn, fntype)

    def _get_file_hash(self, fn, fntype):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                r = queries.get_file_hash(db, fn, fntype)
                if r is None or len(r) == 0:
                    return None, None, None
                r = r[0]
                return r['hashstr'], r['pos'], r['done']
            except Exception:
                logging.exception("get_file_hash")

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
