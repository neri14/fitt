# FIT Tools

[![CI](https://github.com/neri14/fitt/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/neri14/fitt/actions/workflows/ci.yml)
[![Coverage Status](https://codecov.io/gh/neri14/fitt/branch/master/graph/badge.svg)](https://codecov.io/gh/neri14/fitt)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)

**FIT Tools** - A collection of tools to work with FIT files.


## Usage

```
$ fitt -h
usage: fitt [-h] [--version] tool ...

FIT Tools - A collection of tools to work with FIT files.

positional arguments:
  tool        Available tools
    verify    Verify the fit file.

options:
  -h, --help  show this help message and exit
  --version   show program's version number and exit
```

```
$ fitt verify -h
usage: fitt verify [-h] fit_file

positional arguments:
  fit_file    Path to the fit file.

options:
  -h, --help  show this help message and exit
```
