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
    def __init__(self, hashalgoobj, end_pos=0):
        self._hashalgoobj = hashalgoobj
        self._pos = end_pos
        self._done = False
        self.lock = threading.Lock()

    def update(self, buff, end_pos) -> int:
        n = len(buff)
        delta = end_pos - self._pos
        assert n <= end_pos and delta >= 0 and not self._done
        if delta < n:
            buff = buff[n - delta:delta]
        with self.lock:
            self._hashalgoobj.update(buff)
            self._pos = end_pos
        return delta

    def finished(self):
        with self.lock:
            self._done = True

    def get_props(self):
        with self.lock:
            return self._hashalgoobj.digest(), self._pos, self._done


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
        self._fn_p1_ary = []

    def replace_pass1(self, fn, done):
        h = {'fn': fn, 'done': done}
        with self._cond:
            for i, item in enumerate(self._fn_p1_ary):
                if item['fn'] == fn:
                    self._fn_p1_ary[i] = h
                    h = None
                    break
            if h is not None:
                self._fn_p1_ary.append(h)
            self._cond.notify_all()

    def add_menubreak_rows(self, ary):
        with self._cond:
            while self._menubreak_ary is not None:
                self._cond.wait()
            self._menubreak_ary = ary
            self._cond.notify_all()
            while self._menubreak_ary is not None:
                self._cond.wait()

    def do_replace_p1_fns_done(self, queries, db, p1_fns_ary):
        while True:
            try:
                queries.replace_p1_fns_done(db, p1_fns_ary)
                break
            except sqlite3.OperationalError:
                continue
            except Exception:
                logging.exception()
                break

    def do_delete_all_menubreaks(self, queries, db):
        while True:
            try:
                queries.delete_all_menubreaks(db)
                break
            except sqlite3.OperationalError:
                continue
            except Exception:
                logging.exception()
                break

    def run_load(self, create_schema=False):
        arg = {'create_schema': create_schema}
        with self._cond:
            while self._run_load_args is not None:
                self._cond.wait()
            self._run_load_args = arg
            self._cond.notify_all()
            while self._run_load_args is not None:
                self._cond.wait()

    def do_add_menubreak_rows(self, queries, db, menubreak_ary):
        if len(menubreak_ary) == 0:
            return

        while True:
            try:
                queries.add_menubreak_rows(db, menubreak_ary)
                break
            except sqlite3.OperationalError:
                continue
            except Exception:
                logging.exception()
                break

    def run(self):
        with sqlite3.connect(self._dbfn) as db:
            setup_db_conn(db)
