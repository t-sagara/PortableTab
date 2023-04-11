.. _cli:

Command Line Interface
======================

The command line interface can be used to check table contents.

To display the online help, run ``portabletab --help``.

List tables
-----------

Show list of tables in the specified directory.

.. code-block:: sh

    portabletab list [--db-dir=<dir>]

If ``--db-dir`` is not specified, the program will search for tables
under the current directory.


Dump table
----------

Dump records in the specified table.

.. code-block:: sh

    portabletab dump [--db-dir=<dir>] <tablename>

Optionally, you can use ```-f N`` to specify the starting line to dump.
You can also specify the number of lines to dump with ``-n N`` or
the end line with ``-t N``.


Search records
--------------

Search for records in the specified table using TRIE index.

The following command will list the records whose `<attr>`
matches `<value>` from `<tablename>`.

.. code-block:: sh

    portabletab search [--db-dir=<dir>] <tablename> <attr> <value>

Optionally, you can use ``--keys`` to search for records that
start with ``<value>``, or ``--prefixes`` for records with
common prefixes.
