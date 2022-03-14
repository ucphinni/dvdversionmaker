'''
Created on Feb 28, 2022

@author: Publishers
'''

import os
from pathlib import Path
import shutil

__all__ = ['CfgMgr']


class BadHashError(Exception):
    def __init__(self, fn, hashstr, pos=None):
        self.fn = fn
        self.hashstr = hashstr
        self.pos = pos


class CfgMgr:
    DLDIR = None
    TRANSCODEDIR = None
    MENUDIR = None
    SQLFILE = None
    SPUMUX = None
    FFMPEG = None
    _sgton = None

    @classmethod
    def set_paths(cls, dldir, sqlfile):
        #        sys.modules['s'] = __module__
        cls.DLDIR = Path(dldir)
        if cls.DLDIR.exists() and cls.DLDIR.is_dir():
            pass
        else:
            raise NotADirectoryError(f"DLDIR:{dldir}")
        cls.SQLFILE = Path(sqlfile)
        if cls.SQLFILE.exists() and cls.SQLFILE.is_file():
            pass
        else:
            raise FileExistsError(f"No SQLFILE:{sqlfile}")
        cls.SPUMUX = shutil.which('spumux')
        cls.FFMPEG = shutil.which('ffmpeg')
        if cls.SPUMUX is not None and cls.FFMPEG is not None:
            return
        dirs = map(lambda x: Path(os.environ[x]), [
            'ProgramFiles(x86)', 'ProgramW6432'])
        for d in dirs:
            for s in [Path('DVDStyler') / 'bin']:
                searchdir = d / s
                if cls.FFMPEG is None:
                    cls.FFMPEG = shutil.which('ffmpeg', path=searchdir)
                if cls.SPUMUX is None:
                    cls.SPUMUX = shutil.which('spumux', path=searchdir)

        if cls.SPUMUX is None:
            raise FileExistsError("No SPUMUX found")
        if cls.FFMPEG is None:
            raise FileExistsError("No FFMPEG found")

        cls.MENUDIR = cls.DLDIR / 'menu'
        if cls.MENUDIR.exists() and cls.MENUDIR.is_dir():
            pass
        elif cls.MENUDIR.exists():
            raise NotADirectoryError(f"MENUDIR:{cls.MENUDIR}")
        else:
            cls.MENUDIR.mkdir()

        cls.TRANSCODEDIR = cls.DLDIR / 'transcode'
        if cls.TRANSCODEDIR.exists() and cls.TRANSCODEDIR.is_dir():
            pass
        elif cls.TRANSCODEDIR.exists():
            raise NotADirectoryError(f"TRANSCODEDIR:{cls.TRANSCODEDIR}")
        else:
            cls.TRANSCODEDIR.mkdir()
