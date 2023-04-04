.. _overview:

What is PortableTab
===================

PortableTab is a library that provides the ability to serialize
a table with types into a file, and deserialize rows extracted
from the specified positions in the file.

Features
--------

- The file format is portable by using `Capn' Proto <https://capnproto.org/>`_ 
  for serialization.
- Using mmap for file access, it does not consume much memory
  even when using large tables.

Limitations
-----------

- Rows can only be retrieved at that position.
- It is possible to update records in a serialized file,
  but it is very slow.
- It is not possible to insert rows in the middle of a serialized file.
- It does not have any data frame functions such as searching,
  index generation, or sorting.

Comparison with others
----------------------

- JSON

    JSON is easily used for serialization and is portable.
    However, since it encodes data as strings, the file size becomes larger,
    and it is not suitable for serializing large tables.
    It is also not possible to deserialize only certain lines.

- Pickle

    Pickle encodes data into byte strings, so the file size is smaller than JSON.
    It can serialize not only simple strings and numbers,
    but also complex python objects.
    But Pickle reads the entire file at once when deserializing,
    which consumes memory when dealing with large tables.

- RDBMS

    RDMBS is the best option for random access to huge tables.
    It also has advanced search capabilities and data updates.
    In particular, since SQLite manages databases as portable files,
    it can be another candidate in most cases where PortableTab is used.
    The advantages of PortableTab over SQLite are that PortableTab is faster
    when accessing rows only by position, and it can handle records
    containing variable-length lists.
