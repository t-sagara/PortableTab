# PortableTab

A Python package for serializing tables in portable format
using [Cap'n Proto](https://capnproto.org/).

This package is a kind of KVS that outputs data tables
as a set of page files and provides the ability
to quickly retrieve records with a specified number.

## Main Features

- The format of records is defined in Cap'n Proto's schema.
- Once output to page files, tables are essentially read-only.
- When retrieving records, only the necessary portion of
  the page files is opened with mmap, thus saving speed and memory.
- Page files are serialized by Cap'n Proto and are therefore
  architecture independent.

## How to use

Documentation has not yet been created. Please see [sample code](sample.py).

## Development status

Unstable alpha version.

## License

-This package is available according to the MIT license.
