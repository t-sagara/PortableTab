.. _cli:

Command Line Interface
======================

Command line interface is available to check table contents.

Run ``portabletab --help`` to show the online help.

List tables
-----------

Show list of tables in the specified directory.

.. code-block:: sh

    portabletab list [--db-dir=<dir>]

If ``--db-dir`` is not specified, search for tables under
the current directory.


Dump table
----------

Dump records in the specified table.

.. code-block:: sh

    portabletab dump [--db-dir=<dir>] <tablename>

Optionally, ``-f N`` can be used to specify the starting line to dump.
You can also specify the number of lines to dump with ``-n N`` or
the end line with ``-t N``.
