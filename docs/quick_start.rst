.. _quick_start:

Quick Start
===========

Here is a brief description of basic usage of *PortableTab*.

Install
-------

PortableTab requires Python 3.7 or later.

It can be installed using pip.

.. code-block:: bash

    $ pip install PortableTab


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

The :py:meth:`~PortableTab.base_table.BaseTable.create` method creates
a subdirectory with the table name ``sample`` under the specified
directory ``./db``.
It also places the table definition and other files in it.

Finally, prepare a set of records and register them into the table.

.. code-block:: python

    >>> sample_table.append_records([
    ...     {"name": "Bob", "email": "bob@example.com"},
    ...     {"name": "Alice", "email": "alice@foo.org"},
    ...     {"name": "Alexa", "email": "alexa@bar.net"},
    ... ])


Read Table
----------

Open the table by specifying the directory.

.. code-block:: python

    >>> sample_table = SampleTable(db_dir="./db")

If the code that wants to read the table does not have the definition of
``SampleTable`` class, the base :py:class:`~PortableTab.capnp_table.CapnpTable`
class can be used instead.

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
    {'name': 'Alexa', 'email': 'alexa@bar.net'}


TRIE index
----------

You can create a TRIE index for any attribute using
:py:meth:`~PortableTab.capnp_table.CapnpTable.create_trie_on`.

.. code-block:: python

    >>> sample_table.create_trie_on('name')

Once an index is created, records can be searched by their attributes using
:py:meth:`~PortableTab.capnp_table.CapnpTable.search_records_on`.

.. code-block:: python

    >>> sample_table.search_records_on(attr='name', value='Alice')
    [<sample.capnp:SampleRecord reader (name = "Alice", email = "alice@foo.org")>]

You can also search for records that begin with a specified string,
or match the leading portion of a specified string, specifying
``funcname`` parameter of that method.

.. code-block:: python

    >>> sample_table.search_records_on(attr='name', value='A', funcname='keys')
    [<sample.capnp:SampleRecord reader (name = "Alice", email = "alice@foo.org")>, <sample.capnp:SampleRecord reader (name = "Alexa", email = "alexa@bar.net")>]
    >>> sample_table.search_records_on(attr='name', value='Bobson', funcname='prefixes')
    [<sample.capnp:SampleRecord reader (name = "Bob", email = "bob@example.com")>]

Attributes that have not been indexed are not searchable.

.. note::

    Indexes that search by arithmetic relationship of values (e.g., b-tree)
    are not yet implemented.

Delete index and tables
-----------------------

You can delete unnecessary indexes using
:py:meth:`~PortableTab.capnp_table.CapnpTable.delete_trie_on`.

.. code-block:: python

    >>> sample_table.delete_trie_on('name')

The table can be deleted using
:py:meth:`~PortableTab.capnp_table.CapnpTable.delete`.

.. code-block:: python

    >>> sample_table.delete()
    >>> del sample_table
