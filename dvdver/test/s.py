import asyncio
from collections import defaultdict
from enum import IntEnum
import hashlib
import logging
import os
from pathlib import Path
import subprocess

from PIL import Image
from aiofile import (AIOFile, LineReader, Writer)
import httpx

from aioloader import (AsyncStreamTaskMgr, TaskLoader,
                       HashLoader, AsyncProcExecLoader)
from cfgmgr import CfgMgr
from dbmgr import DbMgr
from menubuild import (MenuBuilder, MenuSelector)


def ffmpeg_cmd2pass(*fns, pass1=False, pass2=False,
                    E_BR='500k', VBV_MBR='9800k', E_MIK=18,
                    E_DAR='16:9', E_SZ='720x480', E_FR='30000/1001',
                    E_SAR='32:27',
                    PASS_LOG='PassLog', OUTFILE=None):
    E_INTRA = "08,16,16,16,17,18,21,24,16,16,16,16,17,19,22,25,16,16,17,"
    E_INTRA += "18,20,22,25,29,16,16,18,21,24,27,31,36,17,17,20,24,30,35,"
    E_INTRA += "41,47,18,19,22,27,35,44,54,65,21,22,25,31,41,54,70,88,24,"
    E_INTRA += "25,29,36,47,65,88,115"
    E_INTER = "18,18,18,18,19,21,23,27,18,18,18,18,19,21,24,29,18,18,19,"
    E_INTER += "20,22,24,28,32,18,18,20,24,27,30,35,40,19,19,22,27,33,39,"
    E_INTER += "46,53,21,21,24,30,39,50,61,73,23,24,28,35,46,61,79,98,27,"
    E_INTER += "29,32,40,53,73,98,129"
    VBV_MBS = '1835k'
    E_MBF = 2
    E_ABF = 2
    E_SBF = 2
    E_DBF = 40
    E_TSD = -30000
    E_RME = 0
    E_RDO = 2
    E_DIA = -4
    E_CMB = 2
    E_CMP = 2
    E_CMS = 2
    E_AMP = 2
    E_DC = 8
    E_MLM = 0.75
    E_MQ = 2.0
    E_MBL = 50.0
    E_VQ = 0.70
    E_FQ = 2.0

    ret = [CfgMgr.FFMPEG, '-threads', '16']
    ss = ''
    sr = ''
    i = 0
    use_stdin = False
    if len(fns) == 0:
        fns = ['-']
        use_stdin = True

    for fn in fns:
        ret.append('-i')
        if use_stdin:
            ret.append(fn)
        else:
            ret.append(str(CfgMgr.DLDIR / fn))
        sr += f'[{i}:v:0]scale={E_SZ}:interl=-1,setsar={E_SAR}[V{i}];'
        ss += f'[V{i}]'
        if pass2:
            ss += f'[{i}:a:0]'
        i += 1
    ret.append('-filter_complex')
    n = len(fns)
    if pass2:
        ss += f'concat=n={n}:v=1:a=1[outv][outa]'
    else:
        ss += f'concat=n={n}:v=1[outv]'
    ss += ';[outv]framerate=fps=' + E_FR + ':[outv]'
    ret.append(sr + ss)
    ret += ['-map', '[outv]']
    if pass2:
        ret += ['-map', '[outv]', '-map', '[outa]']

    if pass2:
        ret += ['-pass', '2', '-passlogfile', PASS_LOG,
                '-vcodec', 'mpeg2video', '-b:v', E_BR, '-maxrate',
                VBV_MBR, '-bufsize', VBV_MBS, '-g', E_MIK,
                '-bf', E_MBF, '-bidir_refine', '0',
                '-sc_threshold', E_TSD, '-b_sensitivity', E_DBF,
                '-me_range', E_RME, '-mpv_flags', 'mv0+naq',
                '-mv0_threshold', '0', '-mbd', E_RDO,
                '-mbcmp', E_CMB, '-precmp', E_CMP, '-subcmp',
                E_CMP, '-cmp', E_CMP, '-skip_cmp', E_CMS,
                '-dia_size', E_DIA, '-pre_dia_size', E_DIA,
                '-last_pred', E_AMP, '-dc', E_DC, '-lmin', E_MLM,
                '-mblmin', E_MBL, '-qmin', E_MQ, '-qcomp', E_VQ,
                '-intra_vlc', 'true', '-intra_matrix', E_INTRA,
                '-inter_matrix', E_INTER, '-f', 'mpeg2video',
                '-color_primaries', '5', '-color_trc', '5',
                '-colorspace', '5', '-color_range', '1',
                '-aspect', E_DAR, '-ac', '1', '-ab', '96000',
                '-r', E_FR, '-y', OUTFILE]
    elif pass1:
        ret += ['-pass', '1', '-passlogfile', PASS_LOG,
                '-vcodec', 'mpeg2video', '-maxrate', VBV_MBR,
                '-bufsize', VBV_MBS, '-g', E_MIK, '-bf', E_MBF,
                '-bidir_refine', '0', '-b_strategy', E_ABF,
                '-brd_scale', E_SBF, '-b_sensitivity', E_DBF,
                '-dc', E_DC, '-q:v', E_FQ, '-intra_vlc', 'true',
                '-intra_matrix', E_INTRA, '-inter_matrix', E_INTER,
                '-an', '-f', 'mpeg2video', '-y', os.devnull]
    return list(map(str, ret))


class TABORD(IntEnum):
    DVD_NUM = 0
    MEAN_BITRATE = 1
    MENU = 2
    START_VER = 3
    END_VER = 4
    CACHE_BMAP = 5
    DATE = 6
    NAME = 7
    URL = 8
    FNAME = 9
    START_SECS = 10
    END_SECS = 11
    CNT = 12  # This must be the limit


def create_background_pic(
        dvdary, size=(720, 480), color=(255, 255, 0)):
    for h in dvdary:
        pfn = CfgMgr.MENUDIR / h['pfn']
        if pfn.is_file():
            pfn.unlink()
        img = Image.new("RGB", size, color)
        img.save(pfn, "PNG")


def create_menu_mpg(bg_pic_fn, menu_pic_fn, menu_out_mpg,
                    ffmpeg_exe=None, dvd_mode='ntsc-dvd'
                    ):
    if ffmpeg_exe is None:
        ffmpeg_exe = CfgMgr.FFMPEG
    try:
        cmd = [ffmpeg_exe, '-loop', '1', '-i', bg_pic_fn,
               '-i', menu_pic_fn, '-f', 'lavfi',
               '-i', 'anullsrc=r=48000:cl=mono',
               '-filter_complex', '[0][1] overlay',
               '-c:v', 'mpeg2video',
               '-map', '2:a',
               '-t', '2', '-ab', '64k',
               '-crf', '25',  '-pix_fmt', 'yuv420',
               '-target', dvd_mode,
               '-loglevel', 'quiet',
               '-y', menu_out_mpg]
        subprocess.run(cmd)
    except Exception:
        logging.exception("cmd bad")
        print(bg_pic_fn, menu_pic_fn, menu_out_mpg, ffmpeg_exe, dvd_mode)


async def create_mensel(fn, mode):
    if mode == 'shfn':
        color = (255, 0, 0)
    elif mode == 'ssfn':
        color = (0, 0, 255)
    else:
        color = (0, 255, 0)
    size = (720, 480)
    img = Image.new("RGB", size, color)
    img.save(fn, "PNG")


class OnlineConfigDbMgr:
    def __init__(self, dvd_tab_url, session, dbfn):
        self._session = session
        self._dvd_tab_url = dvd_tab_url
        self._queries = None
        self._cond = asyncio.Condition()
        self._done = False
        self._load = False
        self._dbfn = dbfn
        self._db = None
        self._fdls = {}
        self._finishq = asyncio.Queue()
        self._run_load_args = None
        self._hash_fn_queue = []
        self._wait4commit = False
        self._dvdfile_loading = True
        self._fn_p1_ary = None
        self._bg_task = None
        self._menubreak_ary = None
        self.fn2astm = {}
        self.fn2dvdnum = defaultdict(lambda: set())
        self.replace_hash_on_fail = True
        self.opt_hash = True
        self.tcfn = {}
        self.dvd_tasks = {}
        self.currentfns = set()
        self.dlcnt = 4
        self.passcnt = 2
        self.cross_process_files = True
        self._dvdfile_load_wait_cnt = 0
        self.dbmgr = DbMgr(
            dbfn, CfgMgr.SQLFILE,
            lambda: hashlib.blake2b(digest_size=16))

    async def process_dvd_files(self, dvdnum):
        # run dvdauthor. Iso combine files.
        pass

    def get_transcode_loader(self, fn, tcn, source, row=''):
        class PassLoader(AsyncProcExecLoader):
            def __init__(self, dirobj, fn,
                         tcn, source, cmd_ary):
                stderr_fname = (dirobj / fn).with_suffix('.stderr')
                self.tcn = tcn
                super().__init__(
                    cmd_ary, source,
                    stdout_fname=None,
                    stderr_fname=str(stderr_fname),
                    cwd=str(dirobj))

            def exception(self):
                if self._taskme is not None:
                    return self._taskme.exception()
                return None
        g = hashlib.blake2b(digest_size=16)
        g.update(bytes(row + str(fn), "utf-8"))
        dfrowhash = g.hexdigest()
        dirobj = CfgMgr.TRANSCODEDIR / dfrowhash
        dirobj.mkdir(parents=True, exist_ok=True)

        if tcn == 1:
            cmd_ary = ffmpeg_cmd2pass(
                *[], pass1=True)
        else:
            cmd_ary = ffmpeg_cmd2pass(
                *[], pass2=True,
                OUTFILE=str(dirobj / fn))

        pl = PassLoader(dirobj, fn, tcn, source, cmd_ary)
        return pl

    async def dvd_task(self, dvdnum):
        try:
            async with self._cond:
                while True:
                    x = set(filter(lambda x: dvdnum in self.fn2dvdnum[x] and
                                   (self.tcfn[x] == "done"
                                    or self.tcfn[x] == "err"),
                                   self.tcfn.keys()))
                    y = set(filter(lambda x: dvdnum in self.fn2dvdnum[x] and
                                   self.tcfn[x] == "done",
                                   self.tcfn.keys()))
                    z = set(filter(lambda x: dvdnum in self.fn2dvdnum[x],
                                   self.tcfn.keys()))
                    if x == z:
                        break
                    await self._cond.wait()
            if y == z:
                await self.process_dvd_files(dvdnum)
            else:
                print(f"errors with 1 or more file for dvdtask {dvdnum}")
        finally:
            del self.dvd_tasks[dvdnum]

    async def fn_wait(
            self, fn, download=False, hashfn=False, hashfn_done=False,
            pass1=False, pass2=False, download_done=False,
            pass_done=False,  exc=None):
        async with self._cond:
            '''
            If you are not the current fn, then delay your excution until
            the current is done for all dvdnums
            '''
            if hashfn:
                pass
            if hashfn_done:
                pass
            if download:
                while self.dlcnt <= 0 or (
                    fn not in self.currentfns and next(
                        filter(lambda x, self=self:
                               x not in self.tcfn or
                               self.tcfn[x][0] not in [
                                   "pass", "wait4pass", "done", "err"],
                               self.currentfns), '') != ''):
                    await self._cond.wait()
                self.dlcnt -= 1
            if download_done:
                self.dlcnt += 1
                if exc is None:
                    self.tcfn[fn] = ('done', 'download')
                else:
                    self.tcfn[fn] = ('err', 'download', exc)

                self._cond.notify_all()
            if not pass_done and (pass1 or pass2):
                if pass2:
                    self.tcfn[fn] = ('waitpass')
                    self._cond.notify_all()

                while self.passcnt <= 0 or (fn not in self.currentfns and next(
                    filter(lambda x, self=self:
                           x not in self.tcfn or
                           self.tcfn[x][0] not in [
                               "done", "err"],
                           self.currentfns), '') != ''):
                    await self._cond.wait()
                if pass1:
                    self.tcfn[fn] = ('pass', 1)
                if pass2:
                    self.tcfn[fn] = ('pass', 2)
                self.passcnt -= 1
                self._cond.notify_all()
            if pass_done:
                if pass1:
                    tcn = 1
                if pass2:
                    tcn = 2
                self.passcnt += 1
                self._cond.notify_all()
                if exc is None:
                    self.tcfn[fn] = ('done', tcn)
                else:
                    self.tcfn[fn] = ('err', tcn, exc)
                self._cond.notify_all()

    def pass_dirty(self, fn, tcn):
        fn = fn
        tcn = tcn
        return True

    async def hash_task(self, fn, ftype, new_fn=False):
        await self.fn_wait(fn, hashfn=True)
        try:
            a = self.fn2astm[fn]
            hl = HashLoader(self.dbmgr, ftype, a, fn)
            await hl.load_db_hash(fn, ftype)
            a.add_TaskLoader(hl)

            if (not self.cross_process_files
                    and not new_fn):
                assert False
            hl.run_task()
            await hl.wait_for_hash()
            m = hl.hash_match()
            if m:
                return
            if m is None:
                if not (
                        self.pass_dirty(fn, 1)
                        or self.pass_dirty(fn, 2)):
                    return
            await a.reset(clear=True)
        except Exception:
            logging.exception("hash_task")
        finally:
            await self.fn_wait(fn, hashfn_done=True)

    async def pass_task(self, fn):
        a = self.fn2astm[fn]

        await self.fn_wait(fn, pass1=True)
        tcloader = self.get_transcode_loader(fn, 1, a)
        a.add_TaskLoader(tcloader)
        try:
            tcloader.run_task()
            await tcloader._taskme
        except asyncio.exceptions.CancelledError:
            await tcloader.reset(clear=True)
            tcloader.cancel()
        except Exception:
            logging.exception()
        finally:
            a.remove_TaskLoader(tcloader)
            await self.fn_wait(
                fn, pass_done=True, pass1=True,
                exc=tcloader.exception())
        try:
            await self.fn_wait(fn, pass2=True)
        except asyncio.exceptions.CancelledError:
            tcloader.reset(clear=True)
        except Exception:
            logging.exception()

        tcloader = self.get_transcode_loader(fn, 2, a)
        a.add_TaskLoader(tcloader)
        try:
            tcloader.run_task()
            await tcloader._taskme
        except asyncio.exceptions.CancelledError:
            await tcloader.reset(clear=True)
            tcloader.cancel()
        except Exception:
            logging.exception()
        finally:
            a.remove_TaskLoader(tcloader)
            await self.fn_wait(
                fn, pass_done=True, pass2=True,
                exc=tcloader.exception())

    async def file_task(self, dvdnum, fn, url, ahttpclient, cmp):
        try:
            print("file_task", fn)
            if cmp < 0:
                async with self._cond:
                    self.tcfn[fn] = ('done',)
                    self._cond.notify_all()
                return fn, self.tcfn[fn]

            dtask = None
            if dvdnum not in self.dvd_tasks:
                dtask = asyncio.create_task(
                    self.dvd_task(dvdnum))

            async with self._cond:
                self.tcfn[fn] = ('start',)
                if dtask:
                    self.dvd_tasks[dvdnum] = dtask
                if dvdnum not in self.fn2dvdnum[fn]:
                    self.fn2dvdnum[fn].add(dvdnum)

                self._cond.notify_all()

            if fn in self.fn2astm:
                a = self.fn2astm[fn]
            else:
                fname = CfgMgr.DLDIR / fn
                a = AsyncStreamTaskMgr(url=url, fname=fname,
                                       ahttpclient=ahttpclient)

                class DlLoader(TaskLoader):
                    def __init__(self, me, source, fn):
                        super().__init__(source)
                        self.me = me
                        self.fn = fn

                    async def handle_source_eos(self, exc=None):
                        await self.me.fn_wait(
                            self.fn, download_done=True,
                            exc=exc)
                        self._source.remove_TaskLoader(self)

                dl = DlLoader(self, a, fn)
                a.add_TaskLoader(dl)
                dl.run_task()

                self.fn2astm[fn] = a
                new_fn = not a.file_exists()
                a.run_task()
                glist = [
                    asyncio.create_task(
                        self.hash_task(fn, 'D', new_fn)),
                    asyncio.create_task(
                        self.pass_task(fn))
                ]
                await self.fn_wait(fn, download=True)
                glist.append(a.start_download())
                await asyncio.gather(*glist)

        except Exception:
            logging.exception("file_task")
        finally:
            # Dont have to notify here. Save on signally.
            self.currentfns.remove(fn)


# Not only should there be a wait for dvdfile_load there should be a
# dvdfile_reading and done reading.  Realistically, that is not needed.
    async def wait_for_dvdfile_load(self):
        async with self._cond:
            self._dvdfile_load_wait_cnt += 1
            while True:

                if not self._dvdfile_loading:
                    break
                await self._cond.wait()
            self._dvdfile_load_wait_cnt -= 1

    async def run_load(self, create_schema=False):
        dbmgr = self.dbmgr

        h = {'DLDIR': str(CfgMgr.DLDIR) + os.path.sep,
             'MENUDIR': str(CfgMgr.MENUDIR) + os.path.sep,
             'TRANSCODEDIR': str(CfgMgr.TRANSCODEDIR) + os.path.sep,
             'PATHSEP': os.path.sep,
             }
        if self._dvd_tab_url.startswith("file://"):
            fn = self._dvd_tab_url
            fn = Path(fn[7:])
            if not fn.is_absolute():
                fn = CfgMgr.DLDIR / fn

        print("start")
        async with self._cond:
            self._dvdfile_loading = True
            self._cond.notify_all()
        q = asyncio.Queue(maxsize=10)
        t = asyncio.create_task(
            self._read_url_config(q))
        await dbmgr.setupdb(create_schema, h, fn, q)
        async with self._cond:
            self._dvdfile_loading = False
            self._cond.notify_all()
        print("create_setup_files")
        await self.create_setup_files()
        await t

    async def _parse_streamed_lines(self, resp_lines, queue):
        async for line in resp_lines:
            split_str = line.split("\t")
            if len(split_str) == 1 and split_str[0] == '':
                break
            split_str += [None] * (TABORD.CNT.value - len(split_str))
            try:
                int(split_str[0])
            except ValueError:
                continue  # Skip Over headers
            for i in range(0, TABORD.CNT.value):
                if i in [TABORD.START_VER, TABORD.END_VER,
                         TABORD.MENU, TABORD.CACHE_BMAP,
                         TABORD.DVD_NUM]:
                    try:
                        if split_str[i] is not None:
                            split_str[i] = int(split_str[i])
                    except ValueError:
                        if i == 0:
                            continue
                else:
                    try:
                        if split_str[i] is not None:
                            split_str[i] = split_str[i].strip()
                    except IndexError:
                        split_str[i] = None
                if split_str[i] == '' and i not in []:
                    split_str[i] = None
                if split_str[TABORD.FNAME] is None:
                    split_str[TABORD.FNAME] = \
                        split_str[TABORD.URL].split('/')[-1].strip() \
                        if split_str[TABORD.URL] is not None else None
            h = {"dvdnum": split_str[0],
                 "mean_bitrate": split_str[1],
                 "menu": split_str[2],
                 "start_ver": split_str[3],
                 "end_ver": split_str[4],
                 "cache_bm": split_str[5],
                 "date": split_str[6],
                 "title": split_str[7],
                 "dl_link": split_str[8],
                 "filename": split_str[9],
                 "start_secs": split_str[10],
                 "end_secs": split_str[11]
                 }
            if h['start_secs'] is None:
                h['start_secs'] = 0.0
            await queue.put(h)
        await queue.put(None)

    async def _read_url_config(self, queue):

        if self._dvd_tab_url.startswith("file://"):
            fn = self._dvd_tab_url
            fn = Path(fn[7:])
            if not fn.is_absolute():
                fn = CfgMgr.DLDIR / fn
            async with AIOFile(fn, 'r', encoding="utf-8") as f:
                await self._parse_streamed_lines(LineReader(f), queue)
            return
        session = self._session
        while True:
            try:
                async with session.stream('GET', self._dvd_tab_url) as resp:
                    if resp.status_code != 200:
                        return
                    await self._parse_streamed_lines(resp.aiter_lines(), queue)
            except httpx.ConnectTimeout:
                await asyncio.sleep(5)
                print("ConnectTimeout")
                continue
            except httpx.ReadTimeout:
                print("ReadTimeout")
                continue

    async def create_setup_files(self):
        files = []
        loop = asyncio.get_running_loop()
        ary = None
        fftasks = []
        dbm: DbMgr = self.dbmgr
        try:

            def ensure_file(fn):
                file = Path(fn)
                file.parent.mkdir(parents=True, exist_ok=True)
                file.touch()
                return file
            drary, mfary, dvdary = await asyncio.gather(
                dbm.get_dvd_files(),
                dbm.get_menu_files(),
                dbm.get_all_dvdfilemenu_rows(),
            )

            async def write_xmlrows2file(fn, rows):
                async with AIOFile(fn, 'w') as f:
                    writer = Writer(f)
                    for tab, sstr in map(lambda r: (r['tab'], r['str']), rows):
                        await writer((' ' * (2 * tab)) + sstr + '\n')

            # dvdary = []
            dvdfns = {}
            for row in drary:
                dvdfns[row['dvdnum']] = (CfgMgr.MENUDIR / row['pfn'],)
            print("creating background")
            create_background_pic(drary)
            menusels = {}
            menubuild_rm = defaultdict(lambda: {})
            selfn = set()
            await dbm.delete_all_menubreaks()

            for row in mfary:
                # For right now, we will only use one menu selector per disk
                dvdnum = row['dvdnum']
                renum_menu = row['renum_menu']
                sifn = fn = CfgMgr.MENUDIR / row['sifn']
                if not fn.is_file() and fn not in selfn:
                    selfn.add(fn)
                    fftasks.append(asyncio.create_task(
                        create_mensel(fn, 'sifn')))
                ssfn = fn = CfgMgr.MENUDIR / row['ssfn']
                if not fn.is_file() and fn not in selfn:
                    selfn.add(fn)
                    fftasks.append(asyncio.create_task(
                        create_mensel(fn, 'ssfn')))
                shfn = fn = CfgMgr.MENUDIR / row['shfn']
                if not fn.is_file() and fn not in selfn:
                    selfn.add(fn)
                    fftasks.append(asyncio.create_task(
                        create_mensel(fn, 'shfn')))
                if dvdnum not in menusels:
                    menusels[dvdnum] = MenuSelector(sifn, shfn, ssfn)
                mb = MenuBuilder(self.dbmgr, dvdnum,
                                 renum_menu, sel=menusels[dvdnum])
                menubuild_rm[dvdnum][renum_menu] = mb
                await mb.gen_db_menubreak_rows(dvdfilemenu_ary=dvdary)
            print("gather 1")
            daary, dsary = await asyncio.gather(
                dbm.get_dvd_files(),
                dbm.get_dvdmenu_files(),
            )
            for row in mfary:
                dvdnum = row['dvdnum']
                renum_menu = row['renum_menu']
                mb = menubuild_rm[dvdnum][renum_menu]
                await mb.compute_header_footer()

            print("done")
            # for spumux
            qq = []
            mb = None
            for row in dsary:
                dvdnum = row['dvdnum']
                dvdmenu = row['dvdmenu']
                renum_menu = row['renum_menu']
                pfn = CfgMgr.MENUDIR / row['pfn']
                mfn = CfgMgr.MENUDIR / row['mfn']
                xfn = CfgMgr.MENUDIR / row['xfn']

                if renum_menu is None:
                    renum_menu = 0
                    print("found None 0")
                mb = menubuild_rm[dvdnum][renum_menu]
                print(dvdmenu, renum_menu, pfn)
                mb.add_dvdmenu_fn(dvdmenu, pfn)
                qq.append((dvdnum, dvdmenu, xfn))
            print("done calc menus")
            fftasks = []

            for dvdnum in menubuild_rm.keys():
                for renum_menu in menubuild_rm[dvdnum]:
                    fftasks.append(
                        menubuild_rm[dvdnum][renum_menu].finish_files())
            for i, _ in enumerate(qq):
                xfn = CfgMgr.MENUDIR / qq[i][2]
                ary = await dbm.get_spumux_rows(qq[i][0], qq[i][1])
                fftasks.append(write_xmlrows2file(xfn, ary))
            # for dvdauthor
            for dvdnum, xfn in map(
                lambda row: (row['dvdnum'],
                             ensure_file(CfgMgr.MENUDIR / row['xfn'])), daary):
                ary = await dbm.get_dvdauthor_rows(qq[i][0])
                await write_xmlrows2file(
                    xfn, ary)
            print("wait for tasks")
            await asyncio.gather(*fftasks)
            fftasks = []
            print("done tasks")
            for row in dsary:
                dvdnum, dvdmenu, mfn, xfn, pfn, renum_menu = (
                    row['dvdnum'],
                    row['dvdmenu'],
                    CfgMgr.MENUDIR / row['mfn'],
                    CfgMgr.MENUDIR / row['xfn'],
                    CfgMgr.MENUDIR / row['pfn'],
                    row['renum_menu']
                )
                fftasks.append(loop.run_in_executor(
                    None, create_menu_mpg,
                    dvdfns[dvdnum][0], pfn, mfn))
            print("wait for menu mpg")
            await CfgMgr.wait_tasks_complete(aws=fftasks)
            fftasks = []
            print("done menu mpg")
            u = set()
            for row in dsary:
                mfn, xfn, mfn2 = (
                    CfgMgr.MENUDIR / row['mfn'],
                    CfgMgr.MENUDIR / row['xfn'],
                    CfgMgr.MENUDIR / row['mfn2'])
                if xfn in u:
                    continue
                print(xfn)
                exec_args = [
                    CfgMgr.SPUMUX,
                    xfn
                ]
                infile = open(mfn)
                outfile = open(mfn2, 'w+')
                errfile = open(mfn2.with_suffix('.stderr'), "w+")
                files += [infile, outfile, errfile]

                fftasks.append(asyncio.create_subprocess_exec(
                    *exec_args, stdin=infile,
                    stdout=outfile,
                    stderr=errfile))
                continue

            print("wait for spumux menu mpg")
            try:
                await asyncio.gather(*fftasks)
            except Exception:
                logging.exception(self._cmd_ary)

            print("done spumux menu mpg")
        finally:
            for f in files:
                f.close()
            self._running_db_load = False

    async def load(self):
        async with self._cond:
            self._load = True
            self._cond.notify_all()
        if self._bgmpg_task is not None:
            await self._bgmpg_task
            self._bgmpg_task = None

    async def load_obj(self):
        await self.wait_for_dvdfile_load()
        dbmgr = self.dbmgr
        ary = await dbmgr.get_filenames()

        fns = set()
        fnst = set()
        for r in ary:
            dvdnum, fn, url, cmp = (r['dvdnum'], r['filename'],
                                    r['dl_link'], r['cmp'])
            if fn is None:
                continue
            if cmp == 0:
                self.currentfns.add(fn)
            t = asyncio.create_task(
                self.file_task(
                    dvdnum, fn, url, self._session, cmp))

            fns.add(fn)
            fnst.add(t)

        # No locks required up to this point as there are no
        # Contending tasks.  Even the above create_tc_task
        # only schedules the task/coroutine to run, it doesnt
        # run util you hit the first await.

        async with self._cond:
            while set(self.tcfn.keys()) != fns:
                await self._cond.wait()
        '''
        If the list changes, we are messed up anyway. So just take
        the values and of the dvd_tasks and be done with it. No
        condition variable locking required.
        '''
        await asyncio.gather(*self.dvd_tasks.values())
        await asyncio.gather(*list(fnst))

    async def stop(self):
        async with self._cond:
            self._done = True
            self._cond.notify_all()

    async def run(self, dbexists=False):
        await asyncio.gather(*[
            asyncio.create_task(self.run_load(create_schema=not dbexists)),
            asyncio.create_task(self.load_obj())
        ])


global ocd
ocd = None

DEBUG = True
if DEBUG:
    print("debug mode on !!!!!!!")


async def main_async_func():
    loop = asyncio.get_running_loop()
    dbfn = CfgMgr.DLDIR / 'dl.db'

    if dbfn.exists() and not dbfn.is_file():
        print("db filename exists but is not a plain file.")
        loop.stop()
        return
    dbexists = dbfn.exists()

    async with httpx.AsyncClient(
            http2=True, follow_redirects=True) as session:
        global ocd
        ocd = OnlineConfigDbMgr('file://t.tsv', session, dbfn)
        await ocd.run(dbexists=dbexists)
        print("done")
