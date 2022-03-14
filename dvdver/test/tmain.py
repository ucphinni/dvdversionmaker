'''
Created on Mar 13, 2022

@author: Cote Phinnizee
'''

import trio
from triofrw import TrioFileRW


async def test(nm, w):
    r = await w.new_reader()
    try:
        while True:
            buff = await r.read()
            if buff is not None:
                print(f"{nm}: ", buff)
                continue
            if r.reset_needed():
                print(f"{nm}: reset")
            else:
                print(f"{nm}: done")
                break
    finally:
        await r.delete()


async def trio_main2():
    async with trio.open_nursery() as nursery:
        w = TrioFileRW("test")
        nursery.start_soon(test, "s1", w)
        nursery.start_soon(test, "s2", w)
        for i in range(1000):
            s = str(i) + "\n"
            await w.write(s.encode('utf-8'))
        print("reseting")
        await w.reset_file()
        print("reset")
        for i in range(2000):
            await w.write(str(i).encode('utf-8'))
        print("marking done")
        await w.mark_done()
        print("done")


async def trio_main():
    async with trio.open_nursery() as nursery:
        w = TrioFileRW("test")
        nursery.start_soon(test, "s1", w)
        nursery.start_soon(test, "s2", w)
        for i in range(1000):
            s = str(i) + "\n"
            await w.write(s.encode('utf-8'))
        print("reseting")
        await w.reset_file()
        print("reset")
        for i in range(2000):
            await w.write(str(i).encode('utf-8'))
        print("marking done")
        await w.mark_done()
        print("done")
trio.run(trio_main)
