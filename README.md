# FIT Tools

[![CI](https://github.com/neri14/fitt/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/neri14/fitt/actions/workflows/ci.yml)
[![Coverage Status](https://codecov.io/gh/neri14/fitt/branch/master/graph/badge.svg)](https://codecov.io/gh/neri14/fitt)
[![PyPI - Version](https://img.shields.io/pypi/v/fitt)](https://pypi.org/project/fitt/)

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

**FIT Tools** - A collection of tools to work with FIT files.


## Usage

```
$ fitt -h
usage: fitt [-h] [--version] tool ...

FIT Tools - A collection of tools to work with FIT files.

positional arguments:
  tool        Available tools:
    convert   Convert the FIT file to GPX.
    plot      Plot data from the fit file.
    print     Print all messages in the fit file.
    verify    Verify the fit file.

options:
  -h, --help  show this help message and exit
  --version   show program's version number and exit
```


### Convert tool

```
$ fitt convert -h
usage: fitt convert [-h] [-o OUTPUT] fit_file

positional arguments:
  fit_file              Path to the FIT file.

options:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Path to the output file. If not provided, uses the same name as the input file changed extension.
```


### Plot tool

```
$ fitt plot -h
usage: fitt plot [-h] -x X_AXIS -y Y_AXIS [Y_AXIS ...] [--y-right Y_AXIS_RIGHT [Y_AXIS_RIGHT ...]] [-t {line,scatter}] [--type-right {line,scatter}] [-o OUTPUT] fit_file

positional arguments:
  fit_file              Path to the fit file.

options:
  -h, --help            show this help message and exit
  -x X_AXIS, --x-axis X_AXIS
                        Field to use for the x-axis.
  -y Y_AXIS [Y_AXIS ...], --y-axis Y_AXIS [Y_AXIS ...]
                        Field to use for the y-axis.
  --y-right Y_AXIS_RIGHT [Y_AXIS_RIGHT ...]
                        Field to use for the y-axis on the right side.
  -t {line,scatter}, --type {line,scatter}
                        Plot type: line, scatter. Default is line.
  --type-right {line,scatter}
                        Plot type for right y-axis: line, scatter. Default is line.
  -o OUTPUT, --output OUTPUT
                        Path to the output image file. If not provided, shows the plot interactively.
```


### Print tool

```
$ fitt print -h
usage: fitt print [-h] fit_file

positional arguments:
  fit_file    Path to the fit file.

options:
  -h, --help  show this help message and exit
```


### Verify tool
```
$ fitt verify -h
usage: fitt verify [-h] fit_file

positional arguments:
  fit_file    Path to the fit file.

options:
  -h, --help  show this help message and exit
```
