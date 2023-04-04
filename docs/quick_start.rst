.. _quick_start:

Quick Start
===========

Here is a brief description of basic usage of PortableTab.

Create Table
------------

First, define the table with the table name and schema.

- The table schema must follow the Struct format of
  `Capn' Proto Schema Language <https://capnproto.org/language.html>`_ .

.. code-block:: python

    >>> from PortableTab import BaseTable
    >>> class SampleTable(BaseTable):
    ...     __tablename__ = "sample"
    ...     __schema__ = """
    ...         struct SampleRecord {
    ...             name @0 :Text;
    ...             email @1 :Text;
    ...         }
    ...     """
    ...     __record_type__ = "SampleRecord"
    ...

Next, create this table under the specified directory.

.. code-block:: python

    >>> sample_table = SampleTable(db_dir="./db")
    >>> sample_table.create()
    PosixPath('db/sample')

The `create()` method creates a subdirectory with
the table name (`sample`) under the specified directory `./db`.
It also places the table definition and other files in it.

Finally, prepare a set of records and register them into the table.

.. code-block:: python

    >>> sample_table.append_records([
    ...     {"name": "Bob", "email": "bob@example.com"},
    ...     {"name": "Alice", "email": "alice@foo.org"},
    ... ])


Read Table
----------

To access the created table, first open the table by specifying the directory.

.. code-block:: python

    >>> sample_table = SampleTable(db_dir="./db")

If the code that reads the table does not define a `SampleTable` class,
the base `CapnpTable` class can be used instead.

.. code-block:: python

    >>> from PortableTab import CapnpTable
    >>> sample_table = CapnpTable(tablename="sample", db_dir="./db")

The records in the created table can be retrieved
by specifying the position of rows (0 origin).

.. code-block:: python

    >>> sample_table = SampleTable(db_dir="./db")
    >>> for i in range(sample_table.count_records()):
    ...     print(sample_table.get_record(pos=i, as_dict=True))
    ...
    {'name': 'Bob', 'email': 'bob@example.com'}
    {'name': 'Alice', 'email': 'alice@foo.org'}
