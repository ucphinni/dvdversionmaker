'''
Created on Mar 7, 2022

@author: Cote Phinnizee
'''
from collections import defaultdict
import logging
import sqlite3
import threading
import time

import aiosql

from cfgmgr import CfgMgr


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

    def set_done(self):
        with self.lock:
            self._done = True

    def get_props(self):
        with self.lock:
            return self._hashalgoobj.digest(), self._pos, self._done

    def get_pos(self):
        with self.lock:
            return self._pos

    def good(self):
        with self.lock:
            return self.isgood

    def checking(self):
        with self.lock:
            return self._pos < self.load_pos


'''
  This is farmed out to a separate thread because of
  performance reasons. Every file flows through here.
 '''


class DbHashFile(threading.Thread):
    def __init__(self, dbfn, queries):
        self._cond = threading.Condition()
        self._dbfn = dbfn
        self._fns = defaultdict(lambda: {})
        self._wait4commit = False
        self._processing = False
        self._queries = queries
        self.CHECKPOINT_TIMEOUT = 5  # seconds

    def put_hash_fn(
            self, fn, fntype, hashposobj,
            wait4commit=False):
        with self._cond:
            self._fns[fn][fntype] = hashposobj
            self._cond.notify_all()
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

        with self._cond:
            self._processing = True

            for fn, h in self._fns.items():
                for ftype, hashpos in h.items():
                    hashstr, pos, done = hashpos.get_props()
                    hash_fn_ary.append({
                        'fn': fn,
                        'ftype': ftype,
                        'hashstr': hashstr,
                        'pos': pos,
                        'done': done,
                    })
            self._fns = new_fns
            wait4commit = self._wait4commit
            if wait4commit:
                queries.replace_hash_fns(db, hash_fn_ary)
                db.commit()
            self._cond.notify_all()
        if not wait4commit:
            queries.replace_hash_fns(db, hash_fn_ary)
            db.commit()
        with self._cond:
            self._processing = False
            self._cond.notify_all()

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


class DbMgr(threading.Thread):
    def __init__(self, dbfn):
        self._queries = aiosql.from_path(CfgMgr.SQLFILE, "sqlite3")
        self._dbfn = dbfn
        self.dbhf = DbHashFile(dbfn, self._queries)
        self.dbhf.start()
        self._fn_p1_ary = []

    def put_hash_fn(
            self, fn, fntype, hashposobj,
            wait4commit=False):
        self.dbfn.put_hash_fn(fn, fntype, hashposobj, wait4commit=wait4commit)

    def replace_pass1(self, fn, done):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.replace_p1_fns_done(db, fn=fn, done=done)
            except Exception:
                logging.exception()

    def add_local_paths(self, ary):
        queries = self._queries
        if len(ary) == 0:
            return
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.delete_all_local_paths(db)
                queries.add_local_paths(db, ary)
            except Exception:
                logging.exception()

    def add_menubreak_rows(self, menubreak_ary):
        queries = self._queries
        if len(menubreak_ary) == 0:
            return
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.add_menubreak_rows(db, menubreak_ary)
            except Exception:
                logging.exception()

    def add_dvd_menu_row(self, **h):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.add_dvd_menu_row(db, **h)
            except Exception:
                logging.exception()

    def get_fn_hash(self, fn, fntype):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                r = queries.load_db_hash(db, fn, fntype)
                if r is None:
                    return None, None, None
                r = r[0]
                return r['hashstr'], r['pos'], r['done']
            except Exception:
                logging.exception()

    def delete_all_menubreaks(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.delete_all_menubreaks(db)
            except Exception:
                logging.exception()

    def create_schema(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.create_schema(db)
            except Exception:
                logging.exception()

    def start_load(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.start_load(db)
            except Exception:
                logging.exception()

    def finish_load(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                queries.finish_load(db)
            except Exception:
                logging.exception()

    def get_spumux_rows(self, dvdnum, dvdmenu):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_spumux_rows(
                    db, dvdnum=dvdnum, dvdmenu=dvdmenu)
            except Exception:
                logging.exception()

    def get_dvdauthor_rows(self, dvdnum):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_dvdauthor_rows(
                    db, dvdnum=dvdnum)
            except Exception:
                logging.exception()

    def get_dvd_files(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_dvd_files(db)
            except Exception:
                logging.exception()

    def get_dvdmenu_files(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_dvdmenu_files(db)
            except Exception:
                logging.exception()

    def get_menu_files(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_menu_files(db)
            except Exception:
                logging.exception()

    def get_all_dvdfilemenu_rows(self):
        queries = self._queries
        with sqlite3.connect(self._dbfn) as db:
            try:
                setup_db_conn(db)
                return queries.get_all_dvdfilemenu_rows(db)
            except Exception:
                logging.exception()

    def run_load(self, create_schema=False):
        arg = {'create_schema': create_schema}
        with self._cond:
            while self._run_load_args is not None:
                self._cond.wait()
            self._run_load_args = arg
            self._cond.notify_all()
            while self._run_load_args is not None:
                self._cond.wait()

    def run(self):
        with sqlite3.connect(self._dbfn) as db:
            setup_db_conn(db)
