'''
Created on Mar 14, 2022

@author: Publishers
'''

from collections import defaultdict
import hashlib
import logging
import queue
import sqlite3
import threading
import time

import aiosql
import trio


def setup_db_conn(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA read_uncommitted=ON")
    conn.row_factory = sqlite3.Row


class HashPos:
    __slots__ = (
        'fn', 'fntype', 'loaded_hash_str', 'loaded_pos',
        'isgood', 'pos', '_done', '_hashalgoobj',
        '_hashalgogen', '_waiters_on_done',
        '_cond', 'deleted', 'dbmgr', 'loaded_date_str',
        'final_size', 'etag', 'loaded_final_size',
        'loaded_etag', 'date_str'
    )

    def __init__(
        self, fn, fntype='D', hashalgogen=lambda:
            hashlib.blake2b(digest_size=16),
            dbmgr=None, hashpos=None):
        '''
        This class is not thread safe for speed
        '''

        self.loaded_hash_str = None
        self.loaded_pos = None
        self.final_size = None
        self.loaded_final_size = None
        self.loaded_date_str = None
        self.etag = None
        self.loaded_etag = None
        self.isgood = True
        self.pos = 0
        self._done = False
        self._hashalgogen = hashalgogen
        self._hashalgoobj = self._hashalgogen()
        self._waiters_on_done = 0
        self._cond = trio.Condition()
        self.deleted = False
        self.dbmgr = dbmgr
        self.fn = fn
        self.fntype = fntype
        if hashpos is not None:  # Limited Ctor
            self.dbmgr = hashpos.dbmgr
            self.fn = hashpos.fn
            self.fntype = hashpos.fntype
            self._hashalgogen = hashpos._hashalgogen
            self._hashalgoobj = self._hashalgogen()

    async def sync(self):
        if self.dbmgr is None:
            raise ValueError("dbmgr is None")
        await self.dbmgr.put_hash_fn(self)

    def marked_deleted(self):
        return self.deleted

    async def update(self, buff):
        n = len(buff)
        loaded_pos = self.loaded_pos if self.loaded_pos is not None else 0
        if (self.pos >= loaded_pos
                or self.pos + n < loaded_pos):
            self._hashalgoobj.update(buff)
            async with self._cond:
                self.pos += n
                if self._waiters_on_done:
                    self._cond.notify_all()
            return
        postloadn = self.pos + n - loaded_pos
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

    async def mark_deleted(self):
        async with self._cond:
            self.deleted = True
            self.pos = 0
            self.final_size = None
            self.loaded_final_size = None
            self.etag = None
            self.loaded_date_str = None
            self._hashalgoobj = self._hashalgogen()
            self.loaded_pos = 0
            self.loaded_hash_str = self._hashalgoobj.digest()
            self._cond.notify_all()
            while self._waiters_on_done:
                self._done = True
                await self._cond.wait()
            self._done = False
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

        self._lock = trio.Lock()
        self._queue = queue.Queue()
        self.dbmgr = dbmgr
        self._fns = defaultdict(lambda: {})
        self._wait4commit = False
        self._processing = False
        self.CHECKPOINT_TIMEOUT = 60  # seconds
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
            self, hashposobj,
            flush=False):
        async with self._lock:
            self._fns[hashposobj.fn][hashposobj.fntype] = hashposobj
            self._trysubmit(flush)

    def clock(self):
        return time.monotonic()

    async def arun_exc(self, func, *args):
        return await trio.to_thread.run_sync(func, *args)

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

    def __init__(self, dbfn, sqlfile,
                 hashalgofactory=lambda:
                 hashlib.blake2b(digest_size=16)):
        self._queries = aiosql.from_path(
            sqlfile, "sqlite3")
        self._dbfn = dbfn
        self.dbhf = DbHashFile(self)
        self.dbhf.daemon = True
        self.dbhf.start()
        self._hashalgofactory = hashalgofactory

    def _replace_hash_fns(self, fns, db):
        queries = self._queries
        hash_fn_ary = []
        del_hash_fn_ary = []
        for fn, h in fns.items():
            for fntype, hashpos in h.items():
                if hashpos.deleted:
                    del_hash_fn_ary.append({
                        'fn': fn,
                        'fntype': fntype,
                    })
                    continue
                hashstr, pos, done, final_size, etag, date_str = (
                    hashpos._hashalgoobj.digest(),
                    hashpos.pos,
                    hashpos._done,
                    hashpos.final_size,
                    hashpos.etag,
                    hashpos.date_str)
                hash_fn_ary.append({
                    'fn': fn,
                    'fntype': fntype,
                    'hashstr': hashstr,
                    'pos': pos,
                    'done': done,
                    'final_size': final_size,
                    'etag': etag,
                    'date_str': date_str
                })

        while True:
            try:
                queries.replace_hash_fns(db, hash_fn_ary)
                db.commit()
                break
            except sqlite3.OperationalError:
                pass  # for pypy intermentant failures.
            except Exception:
                logging.exception("delete_hash_fns")
                break
        while True:
            try:
                queries.delete_hash_fns(db, del_hash_fn_ary)
                db.commit()
                break
            except sqlite3.OperationalError:
                pass  # for pypy intermentant failures.
            except Exception:
                logging.exception("replace_hash_fns")
                break
        return

    async def put_hash_fn(
            self, hashposobj,
            wait4commit=False):
        return await self.dbhf.put_hash_fn(hashposobj, wait4commit)

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
        await trio.to_thread.run_sync(func, *args)

    async def fetch_new_hashpos(self, fn, fntype) -> HashPos:
        hashpos = HashPos(self._hashalgofactory, dbmgr=self)
        return await self.arun_exc(
            self._fetch_new_hashpos, fn, fntype, hashpos)

    def _fetch_new_hashpos(self, fn, fntype, hashpos) -> HashPos:
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                r = queries.get_file_hash(db, fn, fntype)
                hashpos: HashPos = None
                if r is None or len(r) == 0:
                    hashpos.loaded_hash_str = None
                    hashpos.loaded_pos = 0
                    hashpos._done = None
                    hashpos.date_str = None
                    hashpos.final_size = None
                    hashpos.loaded_etag = None
                    hashpos.etag = None
                    hashpos.loaded_date_str = None
                    hashpos.loaded_final_size = None
                    return hashpos
                r = r[0]
                hashpos.loaded_hash_str = r['hashstr']
                hashpos.loaded_pos = r['pos']
                hashpos._done = r['done']
                hashpos.loaded_date_str = r['date_str']
                hashpos.loaded_etag = r['etag']
                hashpos.loaded_final_size = r['file_size']

                return hashpos

            except Exception:
                logging.exception("get_file_hash2")

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
        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                self.arun_exc(self._setupdb, create_schema, h, fn, q2)
            )
            while True:
                item = await q.get()
                q2.put(item)
                if item is None:
                    break

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
