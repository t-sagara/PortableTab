# PortableTab

![Python 3.7](https://github.com/t-sagara/PortableTab/actions/workflows/python-3.7.yml/badge.svg)
![Python 3.8](https://github.com/t-sagara/PortableTab/actions/workflows/python-3.8.yml/badge.svg)
![Python 3.9](https://github.com/t-sagara/PortableTab/actions/workflows/python-3.9.yml/badge.svg)
![Python 3.10](https://github.com/t-sagara/PortableTab/actions/workflows/python-3.10.yml/badge.svg)
![Python 3.11](https://github.com/t-sagara/PortableTab/actions/workflows/python-3.11.yml/badge.svg)
![Python 3.12](https://github.com/t-sagara/PortableTab/actions/workflows/python-3.12.yml/badge.svg)

*PortableTab* is a Python library that allows for serialization of 
typed tables into a set of files, as well as deserialization of
specific rows extracted from the files.

## Features

The serialized files are independent of OS and CPU architecture, so it can
be used to create portable table which is useful when working with large
datasets that need to be shared between different systems or environments.

It also allows fast deserialization of only specified rows without loading
the entire table into memory, so it does not take time to load and
deserialize the table on the first access, nor consume memory during execution.

- [Capn' Proto](https://capnproto.org/) is used for serialization,
  making the file format portable.
- Since *PortableTab* uses mmap for file access, it does not consume
  much memory even when handling large tables.
- Indexes on strings are created using
  [Marisa-trie](https://github.com/pytries/marisa-trie),
  the output files are also portable and accessible using mmap.

## Limitations

The tables are serialized into compact files so they cannot be dynamically
modified.

- Rows can only be retrieved at their specified position. If you want to
  access by an attribute such as *id*, you must create an index on that attribute.
- Updating records in serialized files is possible but very slow.
- It is not possible to insert rows in the middle of a serialized file. If you
  want to insert rows in the middle, the only way is to deserialize
  the entire table and recreate another table.

## How to use

Please refer to the documentation at
[PortableTab Document](https://portabletab.readthedocs.io/en/latest/).

## Development status

Unstable alpha version.

## License

This package is available according to the MIT license.

## Author

Takeshi SAGARA <sagara@info-proto.com>
