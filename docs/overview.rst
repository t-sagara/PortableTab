.. _overview:

What is PortableTab
===================

*PortableTab* is a library that allows for serialization of typed tables
into a set of files, as well as deserialization of specific rows
extracted from the files.

Features
--------

- `Capn' Proto <https://capnproto.org/>`_ is used for serialization,
  making the file format portable.
- Since *PortableTab* uses mmap for file access, it does not consume
  much memory even when handling large tables.
- Indexes on strings can be created using Marisa-trie.
  This index also uses mmap to save memory.

Limitations
-----------

- Rows can only be retrieved at their specified position.
- Updating records in a serialized file is possible but very slow.
- It is not possible to insert rows in the middle of a serialized file.

Comparison with others
----------------------

- JSON

   JSON is a widely-used format for serialization due to its portability
   and ease of use. However, because it encodes data as strings,
   it can result in larger file sizes and is not well-suited for serializing
   large tables.
   
   Additionally, JSON does not support selective deserialization of specific
   rows, which can be a limitation for certain use cases.

- Pickle

   Pickle is another format for serialization that encodes data into byte strings,
   resulting in smaller file sizes than JSON. Unlike JSON, it can serialize
   not only simple strings and numbers but also complex Python objects.
   
   However, one limitation of Pickle is that it reads the entire file at once
   during deserialization, which can consume a significant amount of memory
   when dealing with large tables.

- RDBMS

   RDBMS is often the best option for random access to large tables due to
   its advanced search capabilities and ability to perform data updates.
   In particular, SQLite is a good alternative to *PortableTab* in many cases
   since it manages databases as portable files.
   
   However, *PortableTab* has certain advantages over SQLite: it is faster
   when accessing rows only by position, and it can handle records containing
   variable-length lists.
