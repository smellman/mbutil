# MBUtil

MBUtil is a utility for importing and exporting the [MBTiles](http://mbtiles.org/) format,
typically created with [Mapbox](http://mapbox.com/) [TileMill](http://mapbox.com/tilemill/).

Before exporting tiles to disk, see if there's a [Mapbox Hosting plan](http://mapbox.com/plans/)
or an open source [MBTiles server implementation](https://github.com/mapbox/mbtiles-spec/wiki/Implementations)
that works for you - tiles on disk are notoriously difficult to manage.

[![CI](https://github.com/smellman/mbutil/actions/workflows/ci.yml/badge.svg)](https://github.com/smellman/mbutil/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/smellman-mbutil.svg)](https://pypi.org/project/smellman-mbutil/)

**Note well**: this project is no longer actively developed. Issues and pull requests will be attended to when possible, but delays should be expected.

## Installation

Install with pip:

```
pip3 install smellman-mbutil
```

or use sudo if you want to install it globally:

```bash
sudo pip3 install smellman-mbutil --break-system-packages
```

or clone the repository and install it manually:

```bash
git clone https://github.com/smellman/mbutil.git
cd mbutil
uv venv
source .venv/bin/activate
uv pip install .
```

## Usage

```
Usage: mb-util [options] input output

    Examples:

    Export an mbtiles file to a directory of files:
    $ mb-util world.mbtiles dumps # when the 2nd argument is "dumps", then dumps the metatdata.json

    Export an mbtiles file to a directory of files:
    $ mb-util world.mbtiles tiles # tiles must not already exist

    Import a directory of tiles into an mbtiles file:
    $ mb-util tiles world.mbtiles # mbtiles file must not already exist

Options:
  -h, --help            show this help message and exit
  --scheme=SCHEME       Tiling scheme of the tiles. Default is "xyz" (z/x/y),
                        other options are "tms" which is also z/x/y but uses a
                        flipped y coordinate, and "wms" which replicates the
                        MapServer WMS TileCache directory structure
                        "z/000/000/x/000/000/y.png"
  --image_format=FORMAT
                        The format of the image tiles, either png, jpg, webp,
                        pbf or mvt
  --grid_callback=CALLBACK
                        Option to control JSONP callback for UTFGrid tiles. If
                        grids are not used as JSONP, you can remove callbacks
                        specifying --grid_callback=""
  --do_compression      Do mbtiles compression
  --silent              Dictate whether the operations should run silently
```

Export an `mbtiles` file to files on the filesystem:

```bash
mb-util World_Light.mbtiles adirectory
```

Import a directory into a `mbtiles` file

```bash
mb-util directory World_Light.mbtiles
```

## Requirements

* Python `>= 3.8`

## Metadata

MBUtil imports and exports metadata as JSON, in the root of the tile directory, as a file named `metadata.json`.

```javascript
{
    "name": "World Light",
    "description": "A Test Metadata",
    "version": "3"
}
```

## Testing

This project uses pytest for testing. Install pytest:

```bash
uv venv
source .venv/bin/activate
uv pip install hatch
hatch env create
hatch test
```

## Building from Source

To build from source, clone the repository and run:

```bash
hatch build
```

## See Also

* [node-mbtiles provides mbpipe](https://github.com/mapbox/node-mbtiles/wiki/Post-processing-MBTiles-with-MBPipe), a useful utility.
* [mbliberator](https://github.com/calvinmetcalf/mbliberator) a similar program but in node.

## License

BSD - see LICENSE.md

## Authors

- Tom MacWright (tmcw)
- Dane Springmeyer (springmeyer)
- Mathieu Leplatre (leplatrem)
