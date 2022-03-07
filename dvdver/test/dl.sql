 
--  This SQL file is built to python module aiosql 
--  specifications to be loaded by the calling script.
--  (https://github.com/omnilib/aiosqlite).  The database
--  that is created is an sqlite3 lite database.
--  
--  Script is assumed to have the following connections pragmas:
--  - READ uncommited
--  - WAL


-- name: start_load#

DROP TABLE IF EXISTS "dvdfile_incoming";
CREATE TEMP TABLE "dvdfile_incoming" as
select * from dvdfile limit 0;

-- name: finish_load#
insert into makeisoavail(dvdnum)
select distinct dvdnum dvdnum  from dvdfile_incoming dfi
where not exists(select 1 from makeisoavail mia
 where dfi.dvdnum = mia.dvdnum);


insert into dvdfile_summary default values;

insert into dvdfile_history
(
  "summid","action","targetid","dvdnum","mean_bitrate",	"menu","menupart",
	"start_ver","end_ver","cache_bm", "date","title",
	"dl_link","filename",start_secs,end_secs)
with f as (select 'del' act, a.* 
from dvdfile as a left join dvdfile_incoming b
  on a.dvdnum = b.dvdnum and a.filename = b.filename and a.menu = b.menu
where b.dvdnum is null and b.filename is null and b.menu is null and
      a.filename is not null
union all
select 'del' act, a.*
from dvdfile as a left join dvdfile_incoming b
  on a.dvdnum = b.dvdnum and a.menu = b.menu and a.title = b.title
where b.dvdnum is NULL and b.menu is NULL and b.filename is NULL and
      a.filename is null
union all
select 'add' act, a.* 
from dvdfile_incoming as a left join dvdfile b
  on a.dvdnum = b.dvdnum and a.filename = b.filename and a.menu = b.menu
where b.dvdnum is null and b.filename is null and b.menu is null and
      a.filename is not null
union all
select 'add' act, a.*
from dvdfile_incoming as a left join dvdfile b
  on a.dvdnum = b.dvdnum and a.menu = b.menu and a.title = b.title
where b.dvdnum is NULL and b.menu is NULL and b.filename is NULL and
      a.filename is null
)
select (select max(id) from dvdfile_summary), f.* from f;

delete from dvdfile;

insert into dvdfile
	("dvdnum", "mean_bitrate", "menu", "menupart", "start_ver","end_ver",
	"cache_bm", "date", "title", "dl_link", "filename","start_secs",
	end_secs)
select 
	"dvdnum", "mean_bitrate", "menu","menupart", "start_ver", "end_ver",
	"cache_bm", "date", "title", "dl_link",	"filename",start_secs,end_secs
from dvdfile_incoming;


-- name: create_schema#
CREATE TABLE "localpath" (
	"key"	TEXT NOT NULL,
	"dir"	TEXT NOT NULL,
	PRIMARY KEY("key")
);

CREATE TABLE "hash_file" (
	"fn"	TEXT NOT NULL,
	"type"   CHAR NOT NULL CHECK("type" IN ("D","2")),
	"hashstr"	TEXT NOT NULL,
	"pos"	INTEGER NOT NULL,
	"done"	BOOLEAN NOT NULL
	DEFAULT 0 CHECK("done" = 0 OR "done" = 1),
	PRIMARY KEY("fn","type")
);

CREATE TABLE "fn_pass1_done" (
	"fn"	TEXT NOT NULL,
	"done"	BOOLEAN NOT NULL
	DEFAULT 0 CHECK("done" = 0 OR "done" = 1),
	PRIMARY KEY("fn")
);

CREATE TABLE "dvdfile_summary" (
	"id"	INTEGER NOT NULL,
	"timestamp"	TIMESTAMP NOT NULL DEFAULT (datetime('now', 'localtime')),
	"marked_version"	INTEGER,
	PRIMARY KEY("id" AUTOINCREMENT)
);

CREATE TABLE "dvdfile_history" (
	"id"	INTEGER NOT NULL,
	"summid"	INTEGER NOT NULL,
	"action"	TEXT NOT NULL,
	"targetid"	INTEGER,
	"dvdnum"	INTEGER,
	"mean_bitrate"	INTEGER,
	"menu"	INTEGER NOT NULL,
	"menupart"	INTEGER,
	"start_ver"	INTEGER,
	"end_ver"	INTEGER,
	"cache_bm"	INTEGER,
	"date"	TEXT,
	"title"	TEXT,
	"dl_link"	TEXT,
	"filename"	TEXT,
	"start_secs"	REAL CHECK("start_secs" IS null OR "start_secs" >= 0),
	"end_secs"	REAL CHECK("end_secs" IS null OR
	"end_secs" >= 0 OR "start_secs" IS NOT null AND
	"end_secs" IS NOT null AND "start_secs" < "end_secs"),
	FOREIGN KEY("summid") REFERENCES "dvdfile_summary"("id")
	ON DELETE CASCADE,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE "makeisoavail" (
	"dvdnum"	INTEGER NOT NULL,
	"version"	INTEGER NOT NULL DEFAULT 1,
	"bitrate"	INTEGER NOT NULL DEFAULT 2000000,
	"maxisosize"	INTEGER NOT NULL DEFAULT 4700372992,
	"active"	BOOLEAN NOT NULL
	DEFAULT 0 CHECK("active" = 0 OR "active" = 1),
	PRIMARY KEY("dvdnum")
);

CREATE TABLE "isoavailddates" (
	"dvdnum"	INTEGER NOT NULL,
	"start_date"	TEXT NOT NULL,
	"end_date"	TEXT NOT NULL CHECK(end_date is NULL
			or start_date is NULL or end_date >= start_date),
	PRIMARY KEY("dvdnum"),
	FOREIGN KEY("dvdnum") REFERENCES "makeisoavail"("dvdnum")
	ON DELETE CASCADE ON UPDATE CASCADE
);


CREATE TABLE "dvdfile" (
"id"	INTEGER NOT NULL,
"dvdnum"	INTEGER NOT NULL,
"mean_bitrate"	INTEGER,
"menu"	INTEGER NOT NULL,
"menupart"	INTEGER,
"start_ver"	INTEGER,
"end_ver"	INTEGER,
"cache_bm"	INTEGER,
"date"	DATE,
"title"	TEXT,
"dl_link"	TEXT CHECK("filename" IS NOT NULL AND "dl_link"
    IS NOT NULL OR "filename" IS NULL),
"filename"	TEXT,
"start_secs"	REAL NOT NULL DEFAULT 0.0 CHECK(start_secs >=0),
"end_secs"	REAL CHECK(end_secs is null or end_secs >=0 or
                     end_secs is not null and start_secs < end_secs),
PRIMARY KEY("id" AUTOINCREMENT),
FOREIGN KEY("dvdnum") REFERENCES "makeisoavail"("dvdnum")
ON DELETE RESTRICT ON UPDATE CASCADE
);


create view menufn as
with h as (select dfm.dvdnum, dfm.renum_menu,max(dfm.id)id, dfm.menu from dvdfilemenu dfm
group by 1,2),g as (select df.dvdnum,h.renum_menu,df.menu,df.id,df.title from (
select df.* from dvdfile df, makeisoavail mia
where df.dvdnum = mia.dvdnum and
(df.filename is not null or df.cache_bm is NULL) and
df.start_ver <= mia.version and (df.end_ver is null or
mia.version < df.end_ver)
) df, h where cache_bm is  NULL and h.menu = df.menu
order by df.id)
select distinct dfm.dvdnum,dfm.renum_menu,
'menusel'||dfm.dvdnum||'-img.png' sifn,
'menusel'||dfm.dvdnum||'-hil.png' shfn,
'menusel'||dfm.dvdnum||'-sel.png' ssfn,
ifnull(g.title,(select "Disk " || dvdnum || " Version " || version from makeisoavail)) title,g.id
from (
select dvdnum, renum_menu from dvdfilemenu
union ALL
select distinct dvdnum,0  renum_menu from dvdfilemenu
union ALL
select distinct dvdnum,1 from dvdfilemenu
) dfm
left join g
on( g.dvdnum = dfm.dvdnum and g.renum_menu = dfm.renum_menu)
order by id;

CREATE TABLE "menubreak" (
	"id"	INTEGER NOT NULL,
	"dvdfile_id"	INTEGER NOT NULL,
	"ismenu"	BOOLEAN NOT NULL DEFAULT 0,
	"nextmenu"	BOOLEAN NOT NULL DEFAULT 0,
	"x0"	INTEGER,
	"y0"	INTEGER,
	"x1"	INTEGER,
	"y1"	INTEGER,
	FOREIGN KEY("dvdfile_id") REFERENCES "dvdfile"("id") ON DELETE CASCADE,
	PRIMARY KEY("id" AUTOINCREMENT)
);


CREATE VIEW dvdauthor_xml as
select mia.dvdnum,0 tab,'<?xml version="1.0" encoding="utf-8"?>' str
from makeisoavail mia
union ALL
select mia.dvdnum,0,'<dvdauthor jumppad="1">' from makeisoavail mia
union ALL
select mia.dvdnum,1,'<vmgm>' from makeisoavail mia
union ALL
select mia.dvdnum,2,'<fpc>jump vmgm menu 1;</fpc>' from makeisoavail mia
union ALL
select mia.dvdnum,2,'<menus>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<video format="ntsc" aspect="16:9"'||
' widescreen="nopanscan"/>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<audio lang="EN"/>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<subpicture lang="EN">'
from makeisoavail mia
union ALL
select mia.dvdnum,4,'<stream id="0" mode="widescreen"/>'
from makeisoavail mia
union ALL
select mia.dvdnum,4,'<stream id="1" mode="letterbox"/>'
from makeisoavail mia
union ALL
select mia.dvdnum,3,'</subpicture>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<pgc>' from makeisoavail mia
union ALL
select * from (
select DISTINCT but.dvdnum,4,'<button name="'||but.buttonname||
'">g0=2;jump title '||renum_menu||';</button>'
from dvdbutton but where but.dvdmenu = 0
union ALL
select * from (select DISTINCT mia.dvdnum,4,'<vob file="'||
mfn||
'" pause="inf"/>' from dvdmenufn mia where dvdmenu = 0)
union ALL
select DISTINCT mia.dvdnum,4,
'<pre>if (g2 &amp; 0xFFF == 101 and g1 gt 0) button = g1;'||
' else button = 1024;g2=101;</pre>'
from makeisoavail mia
union ALL
select mia.dvdnum,3,'</pgc>' from makeisoavail mia
union ALL
select mia.dvdnum,2,'</menus>' from makeisoavail mia
union ALL
select mia.dvdnum,1,'</vmgm>' from makeisoavail mia
union ALL
select mia.dvdnum,1,'<titleset>' from makeisoavail mia
union ALL
select mia.dvdnum,2,'<menus>' from makeisoavail mia
union ALL
select mia.dvdnum,3,
'<video format="ntsc" aspect="16:9" widescreen="nopanscan"/>'
from makeisoavail mia
union ALL
select mia.dvdnum,3,'<audio lang="EN"/>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<subpicture lang="EN">' from makeisoavail mia
union ALL
select mia.dvdnum,4,'<stream id="0" mode="widescreen"/>' from makeisoavail mia
union ALL
select mia.dvdnum,4,'<stream id="1" mode="letterbox"/>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'</subpicture>' from makeisoavail mia
union ALL
select * from (select f.dvdnum, f.tab,f.str from (
select distinct dvdnum,dvdmenu,3 tab,'<!-- MENU '||dvdmenu||
'-->' str, 0 ordnum from (select dvdnum,dvdmenu from dvdmainmenu
union ALL
select dvdnum,dvdmenu from dvdfilemenu)
union ALL
select distinct dvdnum,dvdmenu,4,'<pgc>', 10 from (
select dvdnum,dvdmenu from dvdmainmenu
union all
select dvdnum,dvdmenu from dvdfilemenu
) where dvdmenu > 1
union ALL
select distinct dvdnum,dvdmenu,4,'<pgc entry="root">', 10 from (
select dvdnum,dvdmenu from dvdmainmenu
union all
select dvdnum,dvdmenu from dvdfilemenu
) where dvdmenu = 1

union ALL
select dvdnum,dvdmenu,4,'<button name="'||buttonname||
'">g0=2;jump title 1;</button>',20 from dvdbutton
where buttonname='playdisc'
union ALL
select d.dvdnum,d.dvdmenu,4,'<button name="'||d.buttonname||
'">g0=1;jump title '||d.renum_menu||
';</button>',20 from dvdbutton d where buttonname='playall'
union ALL
select  dmm.dvdnum,dmm.dvdmenu,4,'<pre>if (g2 &amp; 0x8000 !=0) '||
'{g2^=0x8000;if (g2==101) jump vmgm menu 1;'||
(
select group_concat('if (g2=='||d.dvdmenu||
') jump menu '||d.dvdmenu||';','') from
( select distinct dvdmenu,dvdnum from dvdfilemenu) d
where dmm.dvdnum = d.dvdnum
)||'}g2=1;</pre>', 60 from (select distinct dvdnum, dvdmenu from dvdmainmenu ) dmm
union ALL
select dvdnum,dvdmenu,4,'<button name="'||buttonname||'">g0=0;jump menu 1;</button>',
30 from dvdbutton where dvdmenu > 1
and buttonname='top'
union ALL
select d.dvdnum,d.dvdmenu,4,'<button name="'||d.buttonname||
'">g0=0;jump menu '||(d.dvdmenu-1)||';</button>',
32 from dvdbutton d where d.buttonname='prev'
union ALL
select  d.dvdnum,d.dvdmenu,4,'<button name="'||d.buttonname||
'">g0=0;jump menu '||(d.dvdmenu+1)||';</button>', 34 from dvdbutton d
where d.buttonname='next'
union ALL

select d.dvdnum,dvdmenu,4,'<button name="'||buttonname||
'">g0=0;jump title '||(buttonname)||';</button>',
36 from dvdbutton d where  act='video'

union ALL
select dvdnum,dvdmenu,4,'<button name="'||buttonname||'">g0=0;jump menu '||
chapter||';</button>',38 from dvdbutton where  act in ('menu','topmenu')
and chapter is not null
union ALL
select * from (select DISTINCT dvdnum,dvdmenu,4,'<vob file="'||
(select dir from localpath where key = 'MENUDIR')
||mfn||'" pause="inf"/>',38 from dvdmenufn )
union ALL
select DISTINCT dvdnum,dvdmenu,4,'<pre>g2='||dvdmenu||
'</pre>', 50 from dvdfilemenu
union ALL
select distinct dvdnum,dvdmenu,4,'</pgc>', 999 from (
select dvdnum,dvdmenu from dvdmainmenu
union all
select dvdnum,dvdmenu from dvdfilemenu
)
order by dvdnum, dvdmenu,ordnum
) f
)
union all
select mia.dvdnum,2,'</menus>' from makeisoavail mia
union all
select mia.dvdnum,2,'<titles>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<video format="ntsc" aspect="16:9" widescreen="nopanscan"/>'
from makeisoavail mia
union ALL
select mia.dvdnum,3,'<audio lang="EN"/>' from makeisoavail mia
union ALL
select mia.dvdnum,3,'<subpicture lang="EN">' from makeisoavail mia
union all
select * from (
select f.dvdnum, f.tab,f.str from (
select  dvdnum,dvdtitle_id menu ,3 tab,q.s str, q.t ordnum from dvdfilemenu, (
select '<pgc>' s,20 t
union ALL
select '</pgc>',50
) q
union ALL
select  dvdnum,dvdtitle_id,3 tab,'<!-- TITLE '||dvdtitle_id||'-->', 10 from dvdfilemenu
union ALL
select dvdnum,dvdtitle_id,4 tab,'<vob file="'||
(select dir from localpath where key = 'TRANSCODEDIR')
||mdir||
(select dir from localpath where key = 'PATHSEP')
||mfn||
'" chapters="0:00"/>' str, 30 ordnum from dvdfilemenu
union ALL
select f.dvdnum,f.dvdtitle_id,4 tab,'<post>'||
CASE WHEN (max(f.dvdtitle_id) OVER (PARTITION BY f.dvdnum) )<> f.dvdtitle_id THEN 'if(g0 gt '||
CASE WHEN max(f.dvdtitle_id) OVER (PARTITION BY f.dvdnum,f.renum_menu) = (f.dvdtitle_id) THEN '1' ELSE '0' END
||') jump title '||
(dvdtitle_id+1)||';' ELSE '' END ||'g2|=0x8000; call menu entry root;</post>' str,
40 ordnum from dvdfilemenu f


) f order by f.dvdnum,f.menu, f.ordnum
)
union ALL
select mia.dvdnum,d.ord,d.title from makeisoavail mia,(
select 2 ord,'</titles>' title
union ALL
select 1,'</titleset>'
union ALL
select 0,'</dvdauthor>'
)d
);

CREATE VIEW dvdbutton as
with f as (
select max(f.ord) ord,db.* from
(
select * from (
select dmm.dvdnum, dmm.dvdmenu, 0 renum_menu, dmm.titlenum chapter,
'topmenu' act,dmm.id buttonname,dmm.id id from dvdmainmenu dmm
union ALL
select dvdnum,0,-1,NULL,'menu','continue',NULL from makeisoavail
union ALL
select DISTINCT dmm.dvdnum,dmm.dvdmenu,0 renum_menu , NULL chapter,'menu','playdisc',NULL
from dvdmainmenu dmm , dvdfile df,dvdfilemenu d
where dmm.dvdnum = df.dvdnum  and df.menu = d.menu
group by 1,2
union ALL
select DISTINCT d.dvdnum,d.dvdmenu,d.renum_menu , NULL chapter,'menu','playall',NULL from dvdfilemenu
d, maxmainmenu dmm where dmm.dvdnum = d.dvdnum and dmm.dvdmaxmainmenu < d.dvdmenu
union ALL
select DISTINCT dvdnum,dvdmenu,renum_menu, NULL chapter,'home','top',NULL from dvdfilemenu
union ALL
select DISTINCT d.dvdnum,dvdmenu,renum_menu, NULL chapter,'prev','prev',NULL from dvdfilemenu d
where menutitle_out_of > 1 and menutitle_part > 1
union ALL
select DISTINCT d.dvdnum,dvdmenu,renum_menu, NULL chapter,'next','next',NULL from dvdfilemenu d
where menutitle_part <>  menutitle_out_of
union ALL
select DISTINCT d.dvdnum,d.dvdmenu,1, NULL chapter,'next','next',NULL from dvdmainmenu
d, maxmainmenu dmm where d.dvdmenu < dmm.dvdmaxmainmenu and dmm.dvdnum = d.dvdnum
union ALL
select d.dvdnum,d.dvdmenu,renum_menu, menutitle_id chapter,'video',d.dvdtitle_id,d.id from dvdfilemenu
d, maxmainmenu dmm where dmm.dvdnum = d.dvdnum

) order by dvdnum, dvdmenu,chapter,act,buttonname) db
left join (
select 'menu' act, 'continue' buttonname, 10 ord
union all
select 'menu', 'playdisc', 20
union all
select 'menu', 'playall' , 30
union all
select 'topmenu', NULL, 40
union all
select 'home', 'top', 99999999
union all
select 'prev' , 'prev', 60
union all
select 'next', 'next', 70
union all
select 'video', NULL, 80
) f on (db.act = f.act and db.buttonname = f.buttonname or db.act = f.act and f.buttonname is NULL)
group by db.dvdnum,db.dvdmenu,db.renum_menu,db.chapter,db.act
order by db.dvdnum,db.dvdmenu,max(f.ord)
)
select
ifnull(lag(f.buttonname,1) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord),
last_value(f.buttonname) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord
RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) bup,
ifnull(lead(f.buttonname,1) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord),
first_value(f.buttonname) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord
)) bdown,
ifnull(lag(f.buttonname,1) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord),
last_value(f.buttonname) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord
RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) bright,
ifnull(lead(f.buttonname,1) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord),
first_value(f.buttonname) OVER (PARTITION BY f.dvdnum, f.dvdmenu ORDER BY f.ord
)) bleft,* from f;


CREATE VIEW dvdfilemenu as 
with f as (
select
dense_rank() OVER (PARTITION BY df.dvdnum, df.menu
ORDER BY dvdmenu) menutitle_part,
dense_rank() OVER (PARTITION BY df.dvdnum ORDER BY df.menu,df.id) dvdtitle_id,
dense_rank() OVER (PARTITION BY df.dvdnum,df.menu ORDER BY df.id) menutitle_id,
'entry'||id||'.vob' mfn, id mdir,
mb.dvdmenu,dense_rank() OVER (PARTITION BY df.dvdnum ORDER BY df.menu
GROUPS df.cache_bm is null AND df.filename is null PRECEDING) renum_menu, df.* from dvdfile df,makeisoavail mia
left join mb_dvdmenu mb on (mb.dvdnum = df.dvdnum and mb.dvdfile_id = df.id)
where df.dvdnum = mia.dvdnum and
(df.filename is not null) and
df.start_ver <= mia.version and (df.end_ver is null or
mia.version < df.end_ver)
)
select max(menutitle_part) OVER (PARTITION BY f.dvdnum, f.menu)
menutitle_out_of,* from f;

CREATE VIEW maxmainmenu as
select dmm.dvdnum, max(dvdmenu) dvdmaxmainmenu from dvdmainmenu dmm group by 1;

CREATE VIEW dvdmainmenu as
select f.dvdnum,f.dvdmenu,g.titlenum,f.id,df.title from
(
select dvdnum, renum_menu,min(dvdmenu)  titlenum,min(id) id,min(menu) menu from dvdfilemenu
group by 1,2
) g, (
select f.dvdnum, f.id, f.ok2cnt, dense_rank() OVER (PARTITION BY f.dvdnum ORDER BY f.ok2cnt desc ) dvdmenu from  (
select df.dvdnum,df.id id,0 ok2cnt
from menubreak mb,dvdfile df where mb.dvdfile_id = df.id and mb.ismenu and exists (select 1 from menubreak where ismenu)
) f
union all
select f.dvdnum, f.id, f.ok2cnt, dense_rank() OVER (PARTITION BY f.dvdnum ORDER BY f.ok2cnt desc ) dvdmenu from  (
select df.dvdnum,df.id id,0 ok2cnt
from dvdfile df , makeisoavail mia where df.cache_bm is NULL and
not exists (select 1 from menubreak where ismenu) AND
df.start_ver <= mia.version and (df.end_ver is null or
mia.version < df.end_ver)
) f
)f,dvdfile df
where g.dvdnum = f.dvdnum and df.menu = g.menu and f.id = df.id;

CREATE VIEW mb_dvdmenu as
select df1.dvdnum,mb1.dvdfile_id,count(mb1.nextmenu) dvdmenu 
from menubreak mb1, menubreak mb2, dvdfile df1, dvdfile df2
where mb1.id >= mb2.id and mb2.nextmenu and mb1.dvdfile_id  = df1.id 
and mb2.dvdfile_id = df2.id  and df1.dvdnum = df2.dvdnum
group by df1.dvdnum, mb1.dvdfile_id;

CREATE VIEW dvdfn as
select distinct dfm.dvdnum,
 'dvdauthor'||dfm.dvdnum||'.xml' xfn,
 'background'||dfm.dvdnum||'.png' pfn,
 'background'||dfm.dvdnum||'.mpg' mfn
from dvdfilemenu dfm;

create view dvdmenufn as
select distinct dfm.dvdnum,dfm.dvdmenu,
'dvdmenu'||dfm.dvdnum||'-'||dfm.dvdmenu||'.mpg' mfn,
'dvdmenu'||dfm.dvdnum||'-'||dfm.dvdmenu||'-1'||'.mpg' mfn2,
'dvdmenu'||dfm.dvdnum||'-'||dfm.dvdmenu||'.xml' xfn,
'dvdmenu'||dfm.dvdnum||'-'||dfm.dvdmenu||'.png' pfn,
case when q.renum_menu is not null then 
q.renum_menu else 0 end renum_menu,
case when q.title is not null then q.title ELSE
(select "Disk " || dvdnum || " Version " || version from makeisoavail)
end title
from dvdbutton dfm
left join (
select distinct dfm.menu,
df.dvdnum,dfm.dvdmenu,dfm.renum_menu,
df.title || CASE WHEN dfm.menutitle_out_of >1 THEN ' '|| dfm.menutitle_part ||
' / ' ||  dfm.menutitle_out_of ELSE '' END title
from menufn dmm,dvdfile df,dvdfilemenu dfm
where df.id = dmm.id and dfm.menu = df.menu
union ALL
select 0,
df.dvdnum, 0,0,"Disk " || dvdnum || " Version " || version || " LOCALTIME("|| 
(select max(timestamp) from dvdfile_summary) ||")" from makeisoavail df
) q on (q.dvdnum = dfm.dvdnum and dfm.dvdmenu = q.dvdmenu);

create view menufooter as
select db.dvdnum,db.dvdmenu,db.buttonname,
db.act, ifnull(db.renum_menu,db.dvdmenu) renum_menu,
db.buttonname in ('top') onleft
from dvdbutton db
where db.act not in ('video','topmenu')
order by db.dvdnum,
db.dvdmenu, case when onleft then db.ord
else -db.ord end;


CREATE VIEW spumux_xml as
select distinct d.dvdnum, d.dvdmenu,f.tab,f.str from dvdbutton d,(
   select 0 tab,'<?xml version="1.0" ?>' str
   union all
   select 1,'<subpictures format="NTSC" >'
   union all
   select 2,'<stream>'
) f
union all
select distinct m.dvdnum, d.dvdmenu, 2 ,
'<spu force="yes" start="00:00:00.00" image="'|| 
(select dir from localpath where key = 'MENUDIR')||m.sifn||'" highlight="'
||(select dir from localpath where key = 'MENUDIR')||m.shfn||'" select="'
||(select dir from localpath where key = 'MENUDIR')||m.ssfn||'">' from dvdmenufn d
left join menufn m on (m.dvdnum = d.dvdnum and ifnull(d.renum_menu,0) = m.renum_menu)
union all
select distinct db.dvdnum,db.dvdmenu, 3 , '<button name="'||db.buttonname||'" '
||ifnull('up="'||db.bup||'" ','')
||ifnull('down="'||db.bdown||'" ','')
||ifnull('left="'||db.bleft||'" ','')
||ifnull('right="'||db.bright||'" ','')
||ifnull('x0="'||mb.x0||'" ','')
||ifnull('y0="'|| CASE WHEN mb.y0 %2 THEN mb.y0-1 ELSE 0 END ||'" ','')
||ifnull('x1="'||mb.x1||'" ','')
||ifnull('y1="'|| CASE WHEN mb.y1 %2 THEN mb.y1-1 ELSE 0 END ||'"','')
||'/>' from dvdbutton db
left join menubreak mb on (db.id = mb.dvdfile_id)
union all
select distinct d.dvdnum, d.dvdmenu,f.tab,f.str from dvdmenufn d,(
   select 3 tab,'</spu>' str
   union all
   select 2,'</stream>'
   union all
   select 1,'</subpictures>'
) f;

-- name: delete-all-menubreaks#
DELETE FROM menubreak;
insert into menubreak
(dvdfile_id,ismenu,nextmenu)
SELECT * from (select id dvdfile_id,1 ismenu, 0 nextmenu from dvdfile df 
where cache_bm is NULL and exists (
select 1 from dvdfilemenu dfm 
where df.dvdnum = dfm.dvdnum and df.menu = dfm.menu )
order by df.dvdnum)
union ALL
select * from (
select id,0,0 from dvdfilemenu df order by id);
-- first menubreak item for each dvd is a nextmenu
update menubreak set nextmenu = 1 
where id in (with f as ( select mb.id
from menubreak mb,dvdfile df 
where mb.dvdfile_id = df.id and mb.ismenu
group by dvdnum
having min(dvdfile_id)
)
select f.id from f
);
-- name: add-menubreak-rows*!
update menubreak set nextmenu=:nextmenu,x0=:x0,y0=:y0,x1=:x1,y1=:y1
where dvdfile_id = :dvdfile_id;

-- name: add-dvd-menu-row!
INSERT INTO dvdfile_incoming
(dvdnum,mean_bitrate,menu,start_ver,end_ver,cache_bm,"date",title,
dl_link,filename,start_secs,end_secs)
VALUES
(:dvdnum,:mean_bitrate,:menu,:start_ver,:end_ver,:cache_bm,:date,:title,
:dl_link,:filename,:start_secs,:end_secs);


-- name: get_filenames
WITH F AS (SELECT  df.filename,df.dvdnum,
CASE WHEN ( df.start_ver <= mia.version <=df.end_ver OR
df.start_ver <= mia.version AND df.start_ver is not null and df.end_ver is NULL
 OR
mia.version <=df.end_ver AND df.end_ver is not null and df.start_ver is NULL OR
df.start_ver is NULL and df.end_ver is NULL
 ) THEN 0
 WHEN (mia.version < df.start_ver) THEN 1
ELSE -1 END cmp,df.rowid, df.dl_link
FROM dvdfile df,makeisoavail mia
WHERE df.dvdnum = mia.dvdnum)
SELECT * from F
where rowid NOT IN (SELECT min(rowid) FROM f
GROUP BY f.filename,f.dvdnum
HAVING count(*) > 1 AND MAX(cmp));

-- name: get_menu_files
select * from menufn;

-- name: get_dvdmenu_files
select * from dvdmenufn;

-- name: get_dvd_files
select * from dvdfn;

-- name: get_dvdauthor_rows
select * from dvdauthor_xml where dvdnum = :dvdnum;

-- name: get_all_dvdfilemenu_rows
select * from dvdfilemenu order by id;

-- name: get_spumux_rows
select * from spumux_xml where dvdnum = :dvdnum and dvdmenu = :dvdmenu;

-- name: get_toolbar_rows
select * from menufooter db where db.dvdnum=:dvdnum and db.renum_menu=:renum_menu

-- name: get_header_title_rows
select distinct dfm.menu,
df.dvdnum,dfm.dvdmenu,
df.title || CASE WHEN dfm.menutitle_out_of >1 THEN ' '|| dfm.menutitle_part || 
' / ' ||  dfm.menutitle_out_of ELSE '' END title 
from dvdmainmenu dmm,dvdfile df,dvdfilemenu dfm
where df.id = dmm.id and dfm.menu = df.menu and
dfm.renum_menu = :renum_menu and dfm.dvdnum = :dvdnum;

-- name: get_min_dvdmenus
select distinct dvdnum,ifnull(renum_menu,0) renum_menu,min_dvdmenu from dvdmenufn;

-- name: delete_hash_fn!
delete from hash_file where fn = :fn and type = :fntype;

-- name: get_file_hash
select * from hash_file where fn = :fn and type = :fntype;

-- name: replace_hash_fns*!
replace into hash_file(fn,type,hashstr,pos,done)
VALUES(:fn,:fntype,:hashstr,:pos,:done);

-- name: delete_fn_pass1_done!
delete from fn_pass1_done where fn = :fn;

-- name: get_fn_pass1_done
select * from fn_pass1_done where fn = :fn;

-- name: replace_p1_fns_done*!
replace into fn_pass1_done(fn,done)
VALUES(:fn,:done);

-- name: get_dvdmainmenu
-- this query has the property if the menubreak is removed,
-- the fields dvdmenu and titlenum are not correct.
-- However, once all the records for the dvd files are added
-- into the menubreak, this query will have all fields correct,
-- including dvdmenu and titlenum
select * from dvdmainmenu;

-- name: delete_all_local_paths!
delete from localpath;

-- name: add_local_paths!
insert into localpath(key,dir)
values(:key,:dir);
