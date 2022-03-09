'''
Created on Mar 8, 2022

@author: Publishers
'''

import asyncio
from pathlib import Path

from PIL import ImageFont, ImageDraw, Image


class MenuSelector:
    __slots__ = [
        '_ifn', '_hfn', '_sfn', '_size'
    ]

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
    __slots__ = [
        'dbmgr', '_min_dvdmenu', '_fns', '_margins_percent',
        '_footer_lblobj_space_hpercent',
        '_footer_sel_space_hpercent', '_label_start_hpercent',
        '_foot_vpercent', '_head_vpercent', '_footer_font',
        '_choice_font', '_fontname', '_pages', '_size',
        '_renum_menu', '_dvdnum', '_sel', '_toolbars'
    ]

    def __init__(self, dbmgr, dvdnum, renum_menu,
                 size=(720, 480), sel: MenuSelector = None,
                 head_vpercent=.15, foot_vpercent=.10,
                 label_start_hpercent=.10,
                 footer_sel_space_hpercent=.02,
                 footer_lblobj_space_hpercent=.025,
                 margins_percent=(.01, .01),
                 fontname='DejaVuSans'
                 ):
        self._toolbars = {}
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
        self._label_start_hpercent = label_start_hpercent
        self._footer_sel_space_hpercent = footer_sel_space_hpercent
        self._footer_lblobj_space_hpercent = footer_lblobj_space_hpercent
        self._margins_percent = margins_percent
        self._fns = {}
        self._min_dvdmenu = None
        self.dbmgr = dbmgr

    def add_dvdmenu_fn(self, dvdmenu, pfn):
        self._fns[dvdmenu] = pfn

    async def gen_db_menubreak_rows(self, dvdfilemenu_ary=None):
        # for the labels
        dx = self._label_start_hpercent * self._size[0]
        dy = self._head_vpercent * self._size[1]

        dbmgr, dvdnum, renum_menu = (
            self.dbmgr, self._dvdnum, self._renum_menu)
        if renum_menu >= 1:
            if dvdfilemenu_ary is not None:
                ary = list(filter(
                    lambda r: r['dvdnum'] == dvdnum and
                    r['renum_menu'] == renum_menu,
                    dvdfilemenu_ary))
            else:
                ary = await dbmgr.get_dvdfilemenu_rows(
                    dvdnum, renum_menu)
            labels = list(
                map(lambda r: r['title'] if r['date'] is None else
                    r['date'] + ": " + r['title'], ary))
        elif renum_menu == -1:
            ary = await dbmgr.get_menu_files()
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
        await dbmgr.add_menubreak_rows(ins_rows)

    async def compute_header_footer(self):
        dbmgr = self.dbmgr
        await self.compute_header_labels(dbmgr)
        await self.compute_footer_buttons(dbmgr)

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

    async def compute_header_labels(self, dbmgr):
        ary = await dbmgr.get_header_rows(
            dvdnum=self._dvdnum, renum_menu=self._renum_menu)
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

    async def compute_footer_buttons(self, dbmgr):
        # Toolbar has 2 elements.  One for right side of footer
        # and one for left. We will get the alignment done later.
        x0, x1 = None, None
        ary = await dbmgr.get_toolbar_rows(
            self._dvdnum, self._renum_menu)
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
