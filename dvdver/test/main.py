#!/usr/local/bin/python2.7
# encoding: utf-8
'''
main -- shortdesc

main is a dvd versioning test

It defines classes_and_methods

@author:     Cote Phinnizee

@copyright:  2022 Lone Programmer. All rights reserved.

@license:    license

@contact:    ucphinni@gmail.com
@deffield    updated: Updated
'''

from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
import asyncio
import os
from pathlib import Path
import sys

from cfgmgr import CfgMgr
import dbmgr
from s import main_async_func, ocd


CfgMgr.set_paths(Path(__file__).parent / 'download_dir',
                 Path(__file__).parent / 'dl.sql')


__all__ = []
__version__ = 0.1
__date__ = '2022-02-28'
__updated__ = '2022-03-07'

DEBUG = 1
TESTRUN = 0
PROFILE = 0


class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''

    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg

    def __str__(self):
        return self.msg

    def __unicode__(self):
        return self.msg


def main(argv=None):  # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (
        program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by Cote Phinnizee on %s.
  Copyright 2022 Lone Programmer. All rights reserved.

  Licensed under the Apache License 2.0
  http://www.apache.org/licenses/LICENSE-2.0

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

USAGE
''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(
            description=program_license,
            formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument("-v", "--verbose", dest="verbose", action="count",
                            help="set verbosity level [default: %(default)s]")
        parser.add_argument('-V', '--version', action='version',
                            version=program_version_message)
        parser.add_argument("-d", "--dldir",
                            dest="dldir", help="DL to folder(s) with source file(s) "
                            "[default: %(default)s]", metavar="path")

        async def main():
            CfgMgr._sgton = CfgMgr()

            await main_async_func()
            await CfgMgr.quit()

            # loop.stop()

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(main())
        except KeyboardInterrupt:
            loop.run_until_complete(ocd.stop())
        return 0

        args = parser.parse_args()

        paths = args.dldir
        verbose = args.verbose

        if verbose > 0:
            print("Verbose mode on")

        # if inpat and expat and inpat == expat:
        #    raise CLIError(
        #        "include and exclude pattern "
        # "are equal! Nothing will be processed.")
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception as e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2


if __name__ == "__main__":
    if DEBUG:
        sys.argv.append("-h")
        sys.argv.append("-v")
        sys.argv.append("-r")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'main_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())
