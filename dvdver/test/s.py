from abc import ABC
import asyncio
from collections import defaultdict
from enum import IntEnum
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import traceback

from PIL import ImageFont, ImageDraw, Image
from aiofile import (AIOFile, LineReader, Writer, Reader)
import aiosql
import aiosqlite
import httpx

from cfgmgr import CfgMgr


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


class MenuSelector:
    def __init__(self, ifn, hfn, sfn, size=(10, 10)):
        self._ifn = ifn
        self._hfn = hfn
        self._sfn = sfn
        self._size = size

    def create(self):
        size = self._size
        img = Image.new('RGB', size, (255, 0, 0))
        img.save(self._ifn)

        img = Image.new('RGB', size, (0, 255, 0))
        img.save(self._hfn)

        img = Image.new('RGB', size, (0, 0, 255))
        img.save(self._sfn)


class MenuBuilder:
    def add_dvdmenu_fn(self, dvdmenu, pfn):
        self._fns[dvdmenu] = pfn

    async def gen_db_menubreak_rows(self, dvdfilemenu_ary=None):
        # for the labels
        dx = self._label_start_hpercent * self._size[0]
        dy = self._head_vpercent * self._size[1]

        db, queries, dvdnum, renum_menu = (self._db,
                                           self._queries,
                                           self._dvdnum,
                                           self._renum_menu)
        if renum_menu >= 1:
            if dvdfilemenu_ary is not None:
                ary = list(filter(
                    lambda r: r['dvdnum'] == dvdnum and
                    r['renum_menu'] == renum_menu,
                    dvdfilemenu_ary))
            else:
                ary = await queries.get_dvdfilemenu_rows(
                    db, dvdnum=dvdnum, renum_menu=renum_menu)
            labels = list(
                map(lambda r: r['title'] if r['date'] is None else
                    r['date'] + ": " + r['title'], ary))
        elif renum_menu == -1:
            ary = await queries.get_menu_files(db)
            ary = list(filter(lambda r: r['dvdnum'] == dvdnum and
                              r['title'] is not None,
                              ary))
            labels = list(map(lambda r: r['title'], ary))
        else:
            labels = []
        # x1# or y1#: compute top left/bottom right of label area.

        x11, y11 = self._size[0], self._size[1]
        x10, y10 = 0, 0
        x10 += self._label_start_hpercent * self._size[0]
        y10 += self._head_vpercent * self._size[1]
        x11 -= self._label_start_hpercent * self._size[0]
        y11 -= self._head_vpercent * self._size[1]
        y11 -= self._foot_vpercent

        pages = await self.compute_text_pages(self._choice_font,
                                              labels, x11 - x10, y11 - y10,
                                              offset=(x10, y10))
        self._pages = pages
        idx = -1
        ins_rows = []
        for page in pages:
            nextmenu = True
            for ypos, _ in page:
                idx += 1

                # calculate selector position in upper right hand corner
                # from label row height.
                # x0# or y0#: compute top left of selector.
                x00 = int(.5 * (self._label_start_hpercent -
                                self._sel._size[0]))
                y00 = int(.5 * (ypos[0] + ypos[1] - self._sel._size[1]))
                x01 = x00 + self._sel._size[0]
                y01 = y00 + self._sel._size[1]
                x00 += dx
                x01 += dx
                y00 += dy
                y01 += dy
                dvdfile_id = ary[idx]['id']
                h = {'dvdfile_id': dvdfile_id,
                     'nextmenu': nextmenu,
                     'x0': x00, 'y0': y00, 'x1': x01, 'y1': y01}
                ins_rows.append(h)
                nextmenu = False
        await queries.add_menubreak_rows(db, ins_rows)

    async def compute_header_footer(self):
        db, queries = self._db, self._queries
        await self.compute_header_labels(db, queries)
        await self.compute_footer_buttons(db, queries)

    def get_footer_label(self, _, buttonname):
        return buttonname

    async def getmax_font_wordwrapped_msg(self, fontstr, msg, dm):
        fontsize = 1
        jumpsize = 75
        dy, msgs = None, None

        while True:
            oldfontsize, olddy, oldmsgs = fontsize, dy, msgs
            font = ImageFont.truetype(fontstr, fontsize)
            dy, msgs = await self.font_word_wrap(font, msg, dm[0],
                                                 wrap_long_text=False)
            if dy is not None and dy * len(msgs) < dm[1]:
                fontsize += jumpsize
                continue
            if jumpsize <= 1:
                if dy is None:
                    font = ImageFont.truetype(fontstr, oldfontsize)
                    dy = olddy
                    msgs = oldmsgs
                return (font, dy, msgs)
            jumpsize = jumpsize // 2
            fontsize -= jumpsize
        return (font, dy, msgs)

    async def compute_header_labels(self, db, queries):
        ary = await queries.get_header_title_rows(db,
                                                  dvdnum=self._dvdnum,
                                                  renum_menu=self._renum_menu)
        self._header_labels = a = {}
        dm = (int(self._size[0] * (1 - 2 * self._margins_percent[0])),
              int(self._size[1] *
                  (self._head_vpercent - self._margins_percent[0])))
        for r in ary:
            msg = r['title']
            dvdmenu = r['dvdmenu']
            ret = await self.getmax_font_wordwrapped_msg(self._fontname,
                                                         msg, dm)
            a[dvdmenu] = ret

    async def compute_footer_buttons(self, db, queries):
        # Toolbar has 2 elements.  One for right side of footer
        # and one for left. We will get the alignment done later.
        x0, x1 = None, None
        ary = await queries.get_toolbar_rows(db, dvdnum=self._dvdnum,
                                             renum_menu=self._renum_menu)
        mx = self._margins_percent[0] * self._size[0]
        fls = self._footer_lblobj_space_hpercent * self._size[0]
        # y is the centerline.
        y = int(self._size[1] * (1 - self._foot_vpercent / 2))
        fnt = self._footer_font
        print("start loop")
        for row in ary:
            onleft = row['onleft']
            dvdmenu = row['dvdmenu']
            print(dvdmenu)
            if dvdmenu not in self._toolbars:
                x0 = 0
                x1 = self._size[0]
                x0 += mx
                x1 -= mx
                a = self._toolbars[dvdmenu] = []
            print(x1, x0, self._sel._size[0])
            assert (x1 - x0 - self._sel._size[0] -
                    2 * self._size[0] *
                    self._footer_lblobj_space_hpercent > 0)
            lbl = self.get_footer_label(row['act'], row['buttonname'])
            s = sum(fnt.getsize(char)[0] for char in lbl)
            mult = 1 if onleft else -1
            x = x0 if onleft else x1
            aa = []
            if onleft:
                aa.append((int(x), y - int(self._sel._size[1] / 2)))
                x += mult * self._size[0] * self._footer_sel_space_hpercent
                x += mult * self._sel._size[0]

                t = max(fnt.getsize(char)[1] for char in lbl)
                aa.append((int(x), y - int(t / 2)))
                x += mult * s
                x += mult * fls
            else:
                x += mult * s
                lblx = int(x)
                t = max(fnt.getsize(char)[1] for char in lbl)
                lbly = y - int(t / 2)
                x += mult * self._size[0] * self._footer_sel_space_hpercent
                x += mult * self._sel._size[0]
                aa.append((int(x), y - int(self._sel._size[1] / 2)))
                aa.append((lblx, lbly))
                x += mult * fls
            aa.append(lbl)
            a.append((aa[0], aa[1], aa[2]))
            if onleft:
                x0 = x
            else:
                x1 = x

    async def finish_files(self):

        size = self._size
        if len(self._fns) == 0:
            return
        min_dvdmenu = min(self._fns.keys())
        pages = self._pages
        for dvdmenu, page in enumerate(pages, min_dvdmenu):
            x = size[0] * self._label_start_hpercent
            pfn = self._fns[dvdmenu]
            img = Image.new("RGBA", size)
            draw = ImageDraw.Draw(im=img)
            for _, ary in page:
                for lbl in ary:
                    y, msg = lbl
                    draw.text((x, y), msg, fill="black",
                              font=self._choice_font)
            for elem in self._toolbars[dvdmenu]:
                _, lblpos, lbl = elem
                draw.text(lblpos, lbl, fill="black",
                          font=self._footer_font)
            x = int(self._margins_percent[0] * self._size[0])
            y = int(self._margins_percent[1] * self._size[1])
            ary = (self._header_labels if self._header_labels
                   is not None else [])
            if dvdmenu in self._header_labels:
                _, dy, msgs = self._header_labels[dvdmenu]
                if msgs is None:
                    msgs = []
                for msg in msgs:
                    if msg is None:
                        continue
                    draw.text((x, y),
                              msg, fill="black",
                              font=self._footer_font)
                    y += dy
            if Path(pfn).exists():
                Path(pfn).unlink()
            img.save(pfn, "PNG")

    def __init__(self, db, queries, dvdnum, renum_menu,
                 size=(720, 480), sel: MenuSelector = None,
                 head_vpercent=.15, foot_vpercent=.10,
                 label_start_hpercent=.10,
                 footer_sel_space_hpercent=.02,
                 footer_lblobj_space_hpercent=.025,
                 margins_percent=(.01, .01),
                 fontname='DejaVuSans'
                 ):
        self._toolbars = {}
        self._db = db
        self._queries = queries
        self._sel = sel
        self._dvdnum = dvdnum
        self._renum_menu = renum_menu
        self._size = size
        self._pages = None
        self._fontname = fontname
        self._choice_font = ImageFont.truetype(fontname, 40)
        self._footer_font = ImageFont.truetype(fontname, 40)
        self._head_vpercent = head_vpercent
        self._foot_vpercent = foot_vpercent
        self._foot_vpercent = foot_vpercent
        self._label_start_hpercent = label_start_hpercent
        self._footer_sel_space_hpercent = footer_sel_space_hpercent
        self._footer_lblobj_space_hpercent = footer_lblobj_space_hpercent
        self._margins_percent = margins_percent
        self._fns = {}
        self._min_dvdmenu = None

    async def font_word_wrap(self, fnt, msg, width, wrap_long_text=True):
        async def msg_fits_line(fnt, msg, width):
            try:
                s = sum(fnt.getsize(char)[0] for char in msg)
                await asyncio.sleep(0)
                if s <= width:
                    return max(fnt.getsize(char)[1] for char in msg)
            except OSError:
                pass
            return None

        msg.strip()
        ary = []

        dy = pos = oldpos = olddy = None
        sumdy = 0
        while len(msg):
            oldpos = pos
            pos = msg.find(' ', pos + 1 if pos is not None else 0)
            if pos == -1:
                pos = len(msg)
            olddy = dy
            dy = await msg_fits_line(fnt, msg[0:pos].strip(), width)
            if dy is not None and pos != len(msg):
                continue
            if oldpos is None and wrap_long_text:
                # Line is too long... need to break it before the space.
                while pos > 0 and await msg_fits_line(fnt,
                                                      msg[0:pos].strip(),
                                                      width) is None:
                    pos -= 1
                oldpos = pos + 1
                if dy is not None:
                    olddy = dy
                dy = None
            elif oldpos is None and not wrap_long_text:
                return None, None

            if dy is None:
                if olddy is None:
                    olddy = await msg_fits_line(fnt, msg[0:pos].strip(), width)
                assert olddy is not None
                sumdy += olddy
                oldpos = oldpos if oldpos is not None else len(msg)
                ary.append(msg[0:oldpos].strip())
                msg = msg[oldpos:].strip()
                dy = olddy = oldpos = pos = None
            elif len(msg) == pos:
                ary.append(msg.strip())
                break
        n = len(ary)
        if n == 0:
            n = 1
        return int(sumdy / n),  ary

    def vertical_balance_page(self, page, height):
        # Nice to have but can impliment later.
        pass

    def shift_page_selections(self, page_sels, dim):
        # A page is an array of selections here.
        if dim[1] == 0:
            return page_sels
        ret = []
        for sel in page_sels:
            y0, y1 = sel[0]
            ary = sel[1]
            y0 += dim[1]
            y1 += dim[1]
            ary2 = []
            for dy, m in ary:
                dy += dim[1]
                ary2.append((dy, m))
            ret.append(((y0, y1), ary2))
        return ret

    async def compute_text_pages(self, fnt, msgs, width, height, offset=None):
        ''' Given a list of messages, create an aray of pages
          which are a tuple of xy positions and strings.

          Each incoming message has a miniumm of half line width separation.
         '''
        idx = 0
        pages = [[]]
        y1 = y2 = y = 0
        first = True
        dy = None
        while idx < len(msgs):
            msg = msgs[idx]
            if not first:
                y += int(dy / 2)
            dy, splitmsgs = await self.font_word_wrap(fnt, msg, width)
            assert dy is not None
            a = []
            y1 = y
            for m in splitmsgs:
                a.append((y, m))
                y += dy
                y2 = y
            if y2 <= height:
                pages[len(pages) - 1].append(((y1, y2), a))
                idx += 1
                continue
            if len(pages[len(pages) - 1]) == 0:
                return None  # Item too big for page.
            self.vertical_balance_page(pages[len(pages) - 1], height)
            y1 = y2 = y = 0
            first = True
            pages.append([])
        if len(pages[len(pages) - 1]) == 0:
            print("poping extra empty page.")
            pages.pop()
        for i in range(len(pages)):
            pages[i] = self.shift_page_selections(pages[i], offset)
        return pages


class TaskLoader:
    __slots__ = ('_queue', '_afh', '_is_running', '_last_proc_chunk_start_pos',
                 '_last_proc_chunk_end_pos', '_read_from_file_gap_end',
                 '_taskme', '_exec_obj', '_afhset', '_done_file_sz', '_cond',
                 '_finish_up', '_force_terminate', '_source', '_loaders',
                 '_parent')

    def add_TaskLoader(self, obj: "TaskLoader") -> None:
        self.run_task()  # noop if already running.
        obj.run_task()  # noop if already running.
        if obj not in self._loaders:
            self._loaders.append(obj)
        obj._parent = self
        obj._source = self._source
        return self

    def remove_TaskLoader(self, obj: "TaskLoader") -> None:
        self._loaders.remove(obj)
        return self

    def run_task(self):
        if self._taskme is None:
            self._taskme = asyncio.create_task(self.run())
        return self

    async def finish(self, done_file_sz, force=False):
        if not force:
            await self._afh.fsync()
        self._force_terminate = force
        n = 0
        while True:
            if n == 50:
                print("Stream task hung. Force Terminate")
                self._force_terminate = True
            if n > 100:
                print("Could not terminate.. Ignore")
                break
            n += 1
            try:
                self._queue.put_nowait((done_file_sz, None))
                break
            except asyncio.QueueFull:
                await asyncio.sleep(.5)

    def __init__(self, source, max_qsize=20):
        self.max_qsize = max_qsize
        self._is_running = False
        self._taskme = None
        self._cond = asyncio.Condition()
        self._finish_up = False
        self._force_terminate = False
        self._loaders = []
        self._parent = None
        self._source = source
        self.reset()

    def reset(self):
        self._queue = asyncio.Queue(maxsize=self.max_qsize)
        self._last_proc_chunk_start_pos = 0
        self._last_proc_chunk_end_pos = 0
        self._read_from_file_gap_end = None
        self._done_file_sz = None
        if (self._is_running and self._afh is not None):
            self.set_afh(self._afh)

    def set_afh(self, afh: AIOFile):
        self._afh = afh
        if self._source is not None:
            pos = self._source._pos
            pos = pos if pos is not None else 0
            self.add_chunk(pos, bytearray(), block=False)

    async def _send_chunk(self, _):
        assert False  # Must Overide

    async def _proc_chunk(self, web_start_pos, chunk):
        assert web_start_pos is not None

        if chunk is None:
            self._read_from_file_gap_end = web_start_pos
            self._finish_up = True
            print("finish   up")
            return
        web_end_pos = len(chunk) + \
            web_start_pos if chunk is not None else web_start_pos
        # Delare all the sore incoming variables.
        WS, WE = web_start_pos, web_end_pos
        PE = self._last_proc_chunk_end_pos
        GE = self._read_from_file_gap_end
        if WE <= PE and GE != 0:
            #                PE-->
            #       WS       WE
            # State: web_buffer behind (must have been stale in queue), ignore.
            # Gap is filled.
            self._read_from_file_gap_end = None
            return
        if WS <= PE and PE < WE or GE == 0:
            #       PE----->PE)
            #       WS       WE
            # State: Can use but some possible chunk trimming work
            async with self._cond:
                self._last_proc_chunk_start_pos = PE
                self._last_proc_chunk_end_pos = WE
                self._cond.notify_all()
            await self._send_chunk(chunk[PE - WS:])
            if GE is not None and GE <= WE:
                #               WE
                #     <---------GE   (If Gap end exists in range, clear it)
                self._read_from_file_gap_end = None
            return

        if PE < WS:
            # ---->PE)
            #       WS      WE
            # State: Gap... Tell caller to refill and try again
            #  set _read_from file so that the caller will fill from PE
            #    to at least WS.
            self._read_from_file_gap_end = WS
            return
        raise Exception(
            f"Unknown State: {web_start_pos} {web_end_pos} " +
            f"{self._last_proc_chunk_start_pos} " +
            f"{self._last_proc_chunk_end_pos}")  # Bug

    def stop(self):
        self._is_running = False
        self._queue.put_nowait((None, None))

    def is_running(self):
        return self._is_running

    async def _fill_gap_from_file(self):
        pos = self._last_proc_chunk_end_pos
        reader = Reader(self._afh, offset=pos, chunk_size=512 * 1024)
        while self._is_running:
            start = pos
            chunk = await reader.read_chunk()
            if not chunk:
                break

            pos += len(chunk)
            await self._proc_chunk(start, chunk)

            if not self._queue.empty() or self._force_terminate:
                break
        return

    async def handle_task_run_done(self):
        pass

    async def handle_source_eos(self):
        assert False

    async def run(self):
        self._is_running = True
        assert self._source is not None
        try:

            while self._is_running:
                if self._source is not None:
                    self._read_from_file_gap_end = self._source._pos
                    if (self._source._afh is not None and
                            self._read_from_file_gap_end is not None and
                            self._queue.empty()):
                        await self._fill_gap_from_file()

                if self._force_terminate:
                    return

                if not self._is_running:
                    break

                chunk_item = await self._queue.get()

                if self._force_terminate:
                    return
                if chunk_item[0] is None and chunk_item[1] is None:
                    break
                await self._proc_chunk(chunk_item[0], chunk_item[1])
                if self._force_terminate:
                    return
            fin_size = self._source.finished_size()
            assert fin_size is not None
            self._read_from_file_gap_end = self._last_proc_chunk_end_pos
            if self._read_from_file_gap_end == 0:  # boundary case
                await self._proc_chunk(0, bytearray())
            else:
                await self._fill_gap_from_file()

        except asyncio.exceptions.CancelledError:
            print("Got Canceled")
            pass
        finally:

            self._is_running = False

        await self.handle_task_run_done()
        return None

    def truncate(self):
        pass

    def add_chunk(self, start_pos, chunk, block=True):
        try:
            if self._afh is None and block:
                self._queue.put((start_pos, chunk))
                return True
            else:
                self._queue.put_nowait((start_pos, chunk))
                return True
        except asyncio.QueueFull:
            return False


class HashLoader(TaskLoader):
    def __init__(self, ocd, source, fn):
        super().__init__(source)
        self._hashstr = None
        self._computed_hashstr = None
#        self._pos = 0
        self._finish_pos = None
        self._fn = fn
        self._hashalgo = hashlib.blake2b()
        self._ocd = ocd
        self._cond = asyncio.Condition()

    def reset(self):
        super().reset()
        self._hashstr = self._computed_hashstr = None

    async def wait_for_hash(self):
        async with self._cond:
            while (self._computed_hashstr is None):
                await self._cond.wait()

    def hash_match(self):
        if self._hashstr is None:
            return None
        return (self._hashstr is not None and
                self._computed_hashstr == self._hashstr)

    async def handle_source_eos(self):
        self.add_chunk(None, None)

    async def load_db_hash(self, fn):
        async with self._cond:
            self._hashstr = await ocd.get_file_hash(fn)

    async def db_hash_exists(self):
        async with self._cond:
            return self._hashstr is not None

    async def finish_up(self):
        if self._taskme is None:
            return

        ocd, fn = self._ocd, self._fn
        self._source.remove_TaskLoader(self)
        computed_hashstr = self._hashalgo.digest()

        if self._hashstr is None:
            await ocd.replace_hash_fn(computed_hashstr, fn)
        async with self._cond:
            self._computed_hashstr = computed_hashstr
            self._cond.notify_all()

        return

    def update_hash(self, chunk) -> None:
        self._hashalgo.update(chunk)

    async def _send_chunk(self, chunk):
        #        self._pos += len(chunk)
        size = self._source.finished_size()
        self.update_hash(chunk)
        if size is not None and self._last_proc_chunk_end_pos >= size:
            await self.finish_up()


class AsyncProcExecLoader(TaskLoader, ABC):
    def __init__(self, cmd_ary, source,
                 stdout_fname, stderr_fname, cwd):
        super().__init__(source)
        self._cmd_ary = cmd_ary
        self._task = None
        self._proc = None
        self._is_running_apel = False
        self._process_terminated = False
        self._process_terminating = False
        self._cond = asyncio.Condition()
        self._stdout_fname = stdout_fname
        self._stderr_fname = stderr_fname
        self._cwd = cwd

    async def run(self):
        if self._is_running_apel:
            return
        self._is_running_apel = True
        cmd_task = asyncio.create_task(self._start_cmd(
            stdout_fname=self._stdout_fname,
            stderr_fname=self._stderr_fname,
            cwd=self._cwd))
        print("asyncprocexecloader run start", self._taskme)
        try:
            await super().run()
        except AssertionError:
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb)  # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]

            print(f'An error occurred on {filename}:{line} {func}'
                  f' in statement {text}'.format(line, text))
            exit(1)
        except Exception:
            print(traceback.format_exc())
            raise
        print("awaiting cmd task", self._taskme)
        await cmd_task
        print("asyncprocexecloader run done", self._taskme)

    async def _wait_for_cmd_done(self):
        async with self._cond:
            while not self._process_terminated:
                await self._cond.wait()
        return

    async def _start_cmd(
            self, stdout_fname=None, stderr_fname=None,
            cwd=None):
        pid, rc = None, None
        stdout = asyncio.subprocess.DEVNULL
        stderr = asyncio.subprocess.DEVNULL
        CREATE_NO_WINDOW = 0x08000000
        # DETACHED_PROCESS = 0x00000008
        try:
            if stdout_fname is not None:
                stdout = open(stdout_fname, "w+")
            if stderr_fname is not None:
                stderr = open(stderr_fname, "w+")
            p = await asyncio.create_subprocess_exec(
                *self._cmd_ary,
                stdin=asyncio.subprocess.PIPE,
                stdout=stdout,
                stderr=stderr,
                creationflags=CREATE_NO_WINDOW,
                cwd=cwd,
            )
            async with self._cond:
                self._proc = p
                self._cond.notify_all()
                proc = self._proc
            rc = await proc.wait()
            print("process done!!!", rc)
            async with self._cond:
                self._process_terminated = True
                await self._cond.notify_all()
            pid = proc.pid
        except OSError as e:
            return e
        finally:
            if stdout != asyncio.subprocess.DEVNULL:
                stdout.close()
            if stderr != asyncio.subprocess.DEVNULL:
                stderr.close()
            print("proc done", self._taskme)
        return pid, rc

    async def _handle_source_eos_if_self_at_end(self, stdin):
        if self._process_terminated:
            return False
        size = self._source.finished_size()
        if size is None:
            return False
        async with self._cond:
            PE = self._last_proc_chunk_end_pos

        assert PE <= size
        print("Got finished size", PE, size)

        if PE == size:
            self._process_terminated = True
            stdin.write_eof()
            self.stop()

    async def handle_source_eos(self, exc=None):
        await self._send_chunk(None)
        if exc:
            self._force_terminate = True

    async def _send_chunk(self, chunk):
        if chunk is None:
            pass
        elif len(chunk):
            pass
        else:
            return
        while True:
            async with self._cond:
                if self._process_terminated:
                    return None
                if self._proc is None:
                    await self._cond.wait()
                    continue

                stdin = self._proc.stdin
            if stdin is None:
                return None

            if chunk is not None:
                #    print("wrting in", self, len(chunk),
                #          self._last_proc_chunk_start_pos)
                stdin.write(chunk)
            if await self._handle_source_eos_if_self_at_end(stdin):
                return None
            break

        try:
            if chunk:
                await stdin.drain()
        except ConnectionResetError as e:
            await self.handle_source_eos(exc=e)
        except BrokenPipeError as e:
            await self.handle_source_eos(exc=e)
        return


class AsyncStreamTaskMgr:
    __slots__ = ('_afh', '_ahttpclient', '_taskloaders', '_url', '_fname',
                 '_taskme', '_pos', '_taskme', '_running',
                 '_cond', '_request_download', '_download')

    def __init__(self, url: str = None, fname: Path = None,
                 ahttpclient: httpx.AsyncClient = None):
        self._fname = Path(fname)
        self._ahttpclient = ahttpclient
        self._taskloaders = []
        self._url = url
        self._afh = None
        self._pos = None
        self._running = False
        self._taskme = None
        self._request_download = False
        self._download = False
        self._cond = asyncio.Condition()

    async def start_download(self):
        async with self._cond:
            self._request_download = True
            while self._download != self._request_download:
                self._cond.notify_all()
                await self._cond.wait()

    async def stop_download(self):
        async with self._cond:
            self._request_download = False
            while self._download != self._request_download:
                self._cond.notify_all()
                await self._cond.wait()

    async def truncate_file(self):
        self._pos = 0
        for t in self._taskloaders:
            t.reset()
        await self._afh.truncate()

    def is_offline(self):
        return self._url is None or self._afh is None

    def run_task(self):
        if self._taskme is None:
            self._taskme = asyncio.create_task(self.run())
        return self

    async def wait_for_started(self):
        while not self._running:
            await asyncio.sleep(0.01)

    def add_TaskLoader(self, obj: "AsyncProcExecLoader") -> None:
        if obj in self._taskloaders:
            return
        self.run_task()  # noop if already running
        obj.run_task()  # noop if already running
        self._taskloaders.append(obj)
        if self._afh is not None:
            obj.set_afh(self._afh)
        return self

    def remove_TaskLoader(self, obj: "AsyncProcExecLoader") -> None:
        if obj in self._taskloaders:
            self._taskloaders.remove(obj)
        return

    async def proc_chunk(self, f, pos, chunk) -> int:
        if chunk is None:  # end of stream
            for i in self._taskloaders:
                i.add_chunk(None, None, block=False)
            return
        start_pos = pos
        pos += len(chunk)
        await f.write(chunk, offset=start_pos)
        for i in self._taskloaders:
            m = memoryview(chunk)
            m.toreadonly()
            i.add_chunk(start_pos, m, block=True)
        return pos

    async def run(self) -> None:
        url = self._url
        if url and url.startswith("file://"):
            if self._fname is None:
                print("filename does not exist")
                self._fname = url[7:]
                self._url = url = None
            elif Path(self._fname).name == url[7:]:
                self._url = url = None
            else:
                print(self._url, self._fname)
                assert False
        offline = url is None
        web_file_sz = None
        try:
            file_sz = Path(self._fname).stat().st_size
        except FileNotFoundError:
            file_sz = 0

        if self._fname.exists() and not self._fname.is_file():
            raise FileNotFoundError("a non file is found")

        filename = str(self._fname) if self._fname else ''

        firsttime = True
        resume_header = None
        pos = 0
        if filename != '':
            f = AIOFile(self._fname, 'ab+')
            await f.open()
        print("start read")
        try:
            if filename != '':
                self._afh = f

                for i in self._taskloaders:
                    i.set_afh(self._afh)
            self._running = True
            while True:

                if not self._fname.exists():
                    file_sz = 0
                else:
                    self._pos = pos = file_sz
                if offline:
                    self._pos = pos = web_file_sz = file_sz

                resp = None
                if web_file_sz is not None and web_file_sz == file_sz:
                    break

                async with self._cond:
                    while not self._request_download:
                        await self._cond.wait()
                    if self._download != self._request_download:
                        self._download = self._request_download
                        self._cond.notify_all()

                try:
                    if not firsttime and web_file_sz is None:
                        resume_header = {'Range': 'bytes=%d-' % (file_sz)}
                    elif not firsttime:
                        resume_header = {
                            'Range': 'bytes=%d-%d' % (file_sz, web_file_sz - 1)
                        }
                    async with self._ahttpclient.stream('GET', url,
                                                        headers=resume_header
                                                        ) as resp:

                        resp_code = resp.status_code
                        if resp_code == 206:
                            print("Got 206")
                        if (firsttime and resp_code == 200 and
                                resp.headers['Content-Length']):
                            web_file_sz = int(resp.headers['Content-Length'])
                            firsttime = False
                            continue

                        if (resp_code == 200 or resp_code == 206 or
                                firsttime and resp_code == 416):
                            pass
                        else:
                            raise ValueError(
                                f"Bad code {resp_code} with" +
                                f" {filename} for {url}")
                        if (resp_code == 200 and web_file_sz is not None
                                and file_sz > 0):
                            print(f"bad size {web_file_sz} {file_sz}")
                            pos = 0
                            await self.truncate_file()

                        elif resp_code == 206:
                            pos = file_sz
                        if resp_code == 416:
                            print('416 Range: bytes=%d-%d' %
                                  (file_sz, web_file_sz - 1))
                            raise ValueError(
                                f"Bad code {resp_code} with " +
                                f"{filename} for {url}")
                        if web_file_sz == file_sz:
                            pos = file_sz
                        self._pos = pos
                        async for chunk in resp.aiter_bytes():
                            pos = await self.proc_chunk(f, self._pos, chunk)
                            self._pos = pos
                        break
                except httpx.ConnectTimeout:
                    await asyncio.sleep(1)
                    await f.fsync()
                    if resp is not None:
                        await resp.aclose()
                    continue
                except httpx.ReadTimeout:
                    await f.fsync()
                    if resp is not None:
                        await resp.aclose()
                    await asyncio.sleep(10)
                    continue
                except httpx.ConnectError:
                    print("Connection Error: Retry " + str(url))
                    await asyncio.sleep(10)
                    continue
        except asyncio.exceptions.CancelledError:
            pass
        finally:
            self._taskme = None
        for i in self._taskloaders:
            await i.handle_source_eos()

        for i in self._taskloaders:
            while not i._is_running:
                await asyncio.sleep(0)

    def finished_size(self):
        if self._taskme is None or self._url is None:
            return self._pos
        return None


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
                    ffmpeg_exe=CfgMgr.FFMPEG, dvd_mode='ntsc-dvd'
                    ):
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
        self._db_chg = False
        self._dbfn = dbfn
        self._db = None
        self._fdls = {}
        self._finishq = asyncio.Queue()
        self._run_load_args = None
        self._hash_fn_queue = []
        self._bg_task = None
        self._menubreak_ary = None
        self.fn2astm = {}
        self.fn2dvdnum = defaultdict(lambda: set())
        self.replace_hash_on_fail = True
        self.opt_hash = True
        self.tcfn = {}
        self.dvd_tasks = {}
        self.currentfns = set()
        self.dlcnt = 6
        self.passcnt = 4
        self.cross_process_files = True

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

            def get_exception(self):
                if self._taskme is not None:
                    return self._taskme.get_exception()
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

    async def fn_wait(self, fn, start=False, download=False,
                      pass1=False, pass2=False, download_done=False,
                      pass_done=False, fast_hash=False):
        async with self._cond:
            '''
            If you are not the current fn, then delay your excution until
            the current is done for all dvdnums
            '''
            if start:
                if fn not in self.currentfns:
                    while next(
                        filter(lambda x: x in self.currentfns and
                               not(self.tcfn[x][0] == "done"
                                   or self.tcfn[x][0] == "err"),
                               self.currentfns), False):
                        await self._cond.wait()
            if download:
                while (self.dlcnt <= 0 and not fast_hash) or (
                    fn not in self.currentfns and next(
                        filter(lambda x, self=self:
                               x not in self.tcfn or
                               self.tcfn[x][0] not in [
                                   "pass", "wait4pass", "done", "err"],
                               self.currentfns), '') != ''):
                    await self._cond.wait()
                if not fast_hash:  # fast hash does not count.
                    self.dlcnt -= 1
            if download_done:
                assert not fast_hash
                self.dlcnt += 1
                self._cond.notify_all()
            if pass1 or pass2:
                while self.passcnt <= 0 or (fn not in self.currentfns and next(
                    filter(lambda x, self=self:
                           x not in self.tcfn or
                           self.tcfn[x][0] not in [
                               "done", "err"],
                           self.currentfns), '') != ''):
                    await self._cond.wait()
                self.passcnt -= 1
            if pass_done:
                self.passcnt += 1
                self._cond.notify_all()

    async def file_task(self, dvdnum, fn, url, ahttpclient, cmp):
        try:
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

                hl = HashLoader(self, a, fn)
                await hl.load_db_hash(fn)

                a.add_TaskLoader(hl)
                dl = None

                db_has_hash = await hl.db_hash_exists()
                fast_hash = db_has_hash and self.can_load_opt
                if (not db_has_hash and not self.cross_process_files
                        and fname.exists()):
                    fname.unlink()
                    fast_hash = False

                if not fast_hash:
                    class DlLoader(TaskLoader):
                        def __init__(self, me, source, fn):
                            super().__init__(source)
                            self.me = me
                            self.fn = fn

                        async def handle_source_eos(self, exc=None):
                            await self.me.fn_wait(self.fn, download_done=True)
                            exc = exc
                            self._source.remove_TaskLoader(self)

                    dl = DlLoader(self, a, fn)
                    a.add_TaskLoader(dl)
                    dl.run_task()

                self.fn2astm[fn] = a
                can_load_opt = self.opt_hash
                await self.fn_wait(fn, download=True,
                                   fast_hash=fast_hash)
                await a.start_download()
                a.run_task()
                # Sleep to make sure the a.run_task takes effect
                # and runs the
                # task.
                await asyncio.sleep(0)
                tcloader = None
                for tcn in (1, 2):
                    tcloader = self.get_transcode_loader(fn, tcn, a)
                    if tcloader is not None:
                        break

                if (can_load_opt and tcloader is not None and
                        not tcloader.is_running()):
                    await self.fn_wait(fn, pass1=True)
                    tcloader.run_task()
                    async with self._cond:
                        self.tcfn[fn] = ('pass', tcn)
                        self._cond.notify_all()
                    a.add_TaskLoader(tcloader)

                await hl.wait_for_hash()
                m = hl.hash_match()
                if (m is None and fname.exists()
                    and not self.cross_process_files or
                        m is not None and not m):
                    if fname.exists():
                        await a.truncate_file()
                    if self.replace_hash_on_fail:
                        print(f"mismatch hash: {fn}. retry/replace")
                        hl.reset()
                        await hl.wait_for_hash()
                    if not hl.hash_match():
                        async with self._cond:
                            self.tcfn[fn] = ('err', 'dlhash')
                            self._cond.notify_all()
                        return fn, self.tcfn[fn]
                else:
                    if (can_load_opt and tcloader is not None
                            and not tcloader.is_running()):
                        await self.fn_wait(fn, pass1=True)
                        tcloader.run_task()
                        a.add_TaskLoader(tcloader)
                if not tcloader._taskme.done():
                    await tcloader._taskme
                await self.fn_wait(fn, pass_done=True)

                if (tcn == 1 and tcloader is not None and
                        self.get_transcode_loader(fn, tcn + 1) is None):
                    exc = tcloader.get_exception()
                    async with self._cond:
                        if exc is None:
                            self.tcfn[fn] = ('done', tcn)
                        else:
                            self.tcfn[fn] = ('err', tcn, exc)
                        self._cond.notify_all()
                        return fn, self.tcfn[fn]
                self.tcfn[fn] = ('wait4pass', tcn + 1)

                tcn = 2
                async with self._cond:
                    self.tcfn[fn] = ('pass', tcn)
                    self._cond.notify_all()
                tcloader = self.get_transcode_loader(fn, tcn)
                tcloader.run_task()
                await self.fn_wait(fn, pass2=True)
                a.add_TaskLoader(tcloader)
                if not tcloader._taskme.done():
                    await tcloader._taskme

                await self.fn_wait(fn, pass_done=True)
                exc = tcloader.get_exception()
                async with self._cond:
                    if exc is None:
                        self.tcfn[fn] = ('done', tcn)
                    else:
                        self.tcfn[fn] = ('err', tcn, exc)
                    self._cond.notify_all()
                    return fn, self.tcfn[fn]

        finally:
            async with self._cond:
                # Dont have to notify here. Save on signally.
                self.currentfns.remove(fn)

    def create_tc_task(
            self,  dvdnum, fn, url, cmp, ahttpclient):
        return asyncio.create_task(
            self.file_task(
                dvdnum, fn, url, ahttpclient, cmp))

    async def get_file_hash(self, fn):
        queries, db = self._queries, self._db
        r = await queries.get_file_hash(db, fn=fn)
        return r[0]['hashstr'] if r is not None and len(r) > 0 else None

    async def replace_hash_fn(self, hashstr, fn):
        await self.run_bg_task()
        async with self._cond:
            self._hash_fn_queue.append({'fn': fn, 'hashstr': hashstr})
            self._cond.notify_all()

    async def add_menubreak_rows(self, ary):
        await self.run_bg_task()
        async with self._cond:
            while self._menubreak_ary is not None:
                await self._cond.wait()
            self._menubreak_ary = ary
            self._cond.notify_all()
            while self._menubreak_ary is not None:
                await self._cond.wait()

    async def do_replace_hash_fns(self, queries, db, hash_fn_ary):
        await queries.replace_hash_fns(db, hash_fn_ary)

    async def do_delete_all_menubreaks(self, queries, db):
        while True:
            try:
                await queries.delete_all_menubreaks(db)
            except sqlite3.OperationalError:
                continue

    async def run_load(self, create_schema=False, block=False):
        await self.run_bg_task()
        arg = {'create_schema': create_schema}
        async with self._cond:
            while self._run_load_args is not None:
                await self._cond.wait()
            self._run_load_args = arg
            self._cond.notify_all()
            while block and self._run_load_args is not None:
                await self._cond.wait()

    async def do_add_menubreak_rows(self, queries, db, menubreak_ary):
        if len(menubreak_ary) == 0:
            return

        while True:
            try:
                await queries.add_menubreak_rows(db, menubreak_ary)
            except sqlite3.OperationalError:
                continue

    async def do_run_load(self, queries, db, create_schema=False):
        while True:
            try:
                if create_schema:
                    await queries.create_schema(db)
                await queries.delete_all_local_paths(db)
                h = {'DLDIR': str(CfgMgr.DLDIR) + os.path.sep,
                     'MENUDIR': str(CfgMgr.MENUDIR) + os.path.sep,
                     'TRANSCODEDIR': str(CfgMgr.TRANSCODEDIR) + os.path.sep,
                     'PATHSEP': os.path.sep,
                     }
                await queries.add_local_paths(
                    db,
                    map(lambda x: {'key': x, 'dir': str(h[x])},
                        h.keys())
                )

                print("start")
                await queries.start_load(db)
                print("read")
                await self._read_url_config(db)
                print("finish_load")
                await queries.finish_load(db)
                print("create_setup_files")
                await self.create_setup_files(db)
                print("done load")
                break
            except sqlite3.OperationalError:
                print(traceback.format_exc())
                await db.rollback()
                break
            else:
                print("except else")
                await db.rollback()
                continue

    async def _run_bg_task(self):
        async with aiosqlite.connect(self._dbfn) as db:
            await setup_db_conn(db)
            while True:
                async with self._cond:
                    while True:
                        hash_fn_ary = self._hash_fn_queue
                        if len(self._hash_fn_queue):
                            self._hash_fn_queue = []
                        menubreak_ary = self._menubreak_ary
                        run_load_args = self._run_load_args
                        if menubreak_ary is not None:
                            break
                        if run_load_args is not None:
                            break
                        if len(hash_fn_ary):
                            break
                        await self._cond.wait()
                try:
                    queries = self._queries
                    await db.execute("BEGIN TRANSACTION")
                    if run_load_args is not None:
                        print("running load")
                        await self.do_run_load(queries, db, **run_load_args)
                        async with self._cond:
                            self._run_load_args = None
                            self._cond.notify_all()
                    if hash_fn_ary is not None:
                        print("do hash")
                        await self.do_replace_hash_fns(
                            queries, db, hash_fn_ary)
                        print("done hash")
                    if menubreak_ary is not None:
                        print("do menu")
                        await self.do_delete_all_menubreaks(
                            queries, db, menubreak_ary)
                        await self.do_add_menubreak_rows(
                            queries, db, menubreak_ary)
                        async with self._cond:
                            self._menubreak_ary = None
                            self._cond.notify_all()
                    await db.commit()
                except Exception:
                    print(traceback.format_exc())
                    await db.rollback()
                    continue

    async def run_bg_task(self):
        if self._bg_task is not None:
            return
        self._bg_task = CfgMgr.create_task(self._run_bg_task())
        await asyncio.sleep(0)

    async def _parse_streamed_lines(self, resp_lines, db):
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
            await self._queries.add_dvd_menu_row(db, **h)

    async def _read_url_config(self, db):

        if self._dvd_tab_url.startswith("file://"):
            fn = self._dvd_tab_url
            fn = Path(fn[7:])
            if not fn.is_absolute():
                fn = CfgMgr.DLDIR / fn
            async with AIOFile(fn, 'r', encoding="utf-8") as f:
                await self._parse_streamed_lines(LineReader(f), db)
            return
        session = self._session
        while True:
            try:
                async with session.stream('GET', self._dvd_tab_url) as resp:
                    if resp.status_code != 200:
                        return
                    await self._parse_streamed_lines(resp.aiter_lines(), db)
            except httpx.ConnectTimeout:
                await asyncio.sleep(5)
                print("ConnectTimeout")
                continue
            except httpx.ReadTimeout:
                print("ReadTimeout")
                continue

    async def create_setup_files(self, db):
        files = []
        loop = asyncio.get_running_loop()
        queries = self._queries
        ary = None
        fftasks = []
        try:

            def ensure_file(fn):
                file = Path(fn)
                file.parent.mkdir(parents=True, exist_ok=True)
                file.touch()
                return file
            drary, mfary, dvdary = await asyncio.gather(
                queries.get_dvd_files(db),
                queries.get_menu_files(db),
                queries.get_all_dvdfilemenu_rows(db)
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
            await queries.delete_all_menubreaks(db)

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
                mb = MenuBuilder(db, queries, dvdnum,
                                 renum_menu, sel=menusels[dvdnum])
                menubuild_rm[dvdnum][renum_menu] = mb
                await mb.gen_db_menubreak_rows(dvdfilemenu_ary=dvdary)
            print("gather 1")
            daary, dsary = await asyncio.gather(
                queries.get_dvd_files(db),
                queries.get_dvdmenu_files(db)
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
                ary = await queries.get_spumux_rows(
                    db, dvdnum=qq[i][0], dvdmenu=qq[i][1])
                fftasks.append(write_xmlrows2file(xfn, ary))
            # for dvdauthor
            for dvdnum, xfn in map(
                lambda row: (row['dvdnum'],
                             ensure_file(CfgMgr.MENUDIR / row['xfn'])), daary):
                await write_xmlrows2file(
                    xfn, await queries.get_dvdauthor_rows(
                        db, dvdnum=dvdnum))
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
                fftasks.append(loop.run_in_executor(None,
                                                    create_menu_mpg,
                                                    dvdfns[dvdnum][0],
                                                    pfn, mfn))
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
            except AssertionError:
                traceback.print_exc()
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

    async def load_obj(self, db):
        queries = self._queries
        ary = await queries.get_filenames(db)
        fns = set()
        fnst = set()
        for r in ary:
            dvdnum, fn, url, cmp = (r['dvdnum'], r['filename'],
                                    r['dl_link'], r['cmp'])
            if cmp == 0:
                self.currentfns.add(fn)

            t = self.create_tc_task(
                dvdnum, fn, url, cmp, self._session)
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
        self._queries = aiosql.from_path(CfgMgr.SQLFILE, "aiosqlite")
        if not dbexists:
            await self.run_load(create_schema=not dbexists, block=True)
        tt = None
        async with aiosqlite.connect(
                "file:/" + str(self._dbfn) + "?mode=ro", uri=True) as db:
            await setup_db_conn(db)
            self._db = db
            if dbexists:
                await self.run_load(create_schema=False, block=False)
            await self.load_obj(db)
            if tt is not None:
                await tt
                await self.load_obj(db)


async def setup_db_conn(conn):
    # await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA read_uncommitted=ON")
    conn.row_factory = aiosqlite.Row

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
    if DEBUG:
        dbexists = False
        if dbfn.exists():
            dbfn.unlink()

    async with httpx.AsyncClient(
            http2=True, follow_redirects=True) as session:
        global ocd
        ocd = OnlineConfigDbMgr('file://t.tsv', session, dbfn)
        await ocd.run(dbexists=dbexists)
        print("done")
