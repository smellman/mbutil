#!/usr/bin/env python

# MBUtil: a tool for MBTiles files
# Supports importing, exporting, and more
# 
# (c) Development Seed 2012
# (c) 2016 ePi Rational, Inc.
# Licensed under BSD

import logging
import os
import sys
from optparse import OptionParser

from mbutil.util import mbtiles_to_disk, disk_to_mbtiles, mbtiles_metadata_to_disk

def main():

    logging.basicConfig(level=logging.DEBUG)

    parser = OptionParser(usage="""usage: %prog [options] input output
    
    Examples:

    Export an mbtiles file to a directory of files:
    $ mb-util world.mbtiles dumps # when the 2nd argument is "dumps", then dumps the metatdata.json

    Export an mbtiles file to a directory of files:
    $ mb-util world.mbtiles tiles # tiles must not already exist
    
    Import a directory of tiles into an mbtiles file:
    $ mb-util tiles world.mbtiles # mbtiles file must not already exist""")
    
    parser.add_option('--scheme', dest='scheme',
        help='''Tiling scheme of the tiles. Default is "xyz" (z/x/y), other options '''
        + '''are "tms" which is also z/x/y but uses a flipped y coordinate, and "wms" '''
        + '''which replicates the MapServer WMS TileCache directory structure '''
        + '''"z/000/000/x/000/000/y.png"''',
        type='choice',
        choices=['wms', 'tms', 'xyz', 'zyx', 'gwc','ags'],
        default='xyz')
        
    parser.add_option('--image_format', dest='format',
        help='''The format of the image tiles, either png, jpg, webp, pbf or mvt''',
        choices=['png', 'jpg', 'pbf', 'webp', 'mvt'],
        default='png')

    parser.add_option('--grid_callback', dest='callback',
        help='''Option to control JSONP callback for UTFGrid tiles. If grids are not '''
        + '''used as JSONP, you can remove callbacks specifying --grid_callback="" ''',
        default='grid')

    parser.add_option('--do_compression', dest='compression',
        help='''Do mbtiles compression''',
        action="store_true",
        default=False)

    parser.add_option('--silent', dest='silent',
        help='''Dictate whether the operations should run silently''',
        action="store_true",
        default=False)

    (options, args) = parser.parse_args()

    # Transfer operations
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)

    if os.path.isfile(args[0]) and os.path.exists(args[1]):
        sys.stderr.write('To export MBTiles to disk, specify a directory that does not yet exist\n')
        sys.exit(1)
    
    # to disk
    if os.path.isfile(args[0]) and args[1]=="dumps":
        mbtiles_file, dumps = args
        mbtiles_metadata_to_disk(mbtiles_file, **options.__dict__)
        sys.exit(1)

    if os.path.isfile(args[0]) and not os.path.exists(args[1]):
        mbtiles_file, directory_path = args
        mbtiles_to_disk(mbtiles_file, directory_path, **options.__dict__)

    if os.path.isdir(args[0]) and os.path.isfile(args[1]):
        sys.stderr.write('Importing tiles into already-existing MBTiles is not yet supported\n')
        sys.exit(1)
    
    # to mbtiles
    if os.path.isdir(args[0]) and not os.path.isfile(args[0]):
        directory_path, mbtiles_file = args
        disk_to_mbtiles(directory_path, mbtiles_file, **options.__dict__)
