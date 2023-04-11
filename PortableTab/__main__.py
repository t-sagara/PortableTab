import csv
from pathlib import Path
import sys
from typing import Optional

from docopt import docopt

import PortableTab
from PortableTab.capnp_table import CapnpTable

HELP = r"""
'PortableTab' is a Python package for serializing tables
in portable format using Cap'n Proto.

Usage:
  {p} -h
  {p} -v
  {p} dump [--db-dir=<dir>] [-f <from>] ([-n <lines>] | [-t <to>]) <tablename>
  {p} list [--db-dir=<dir>]
  {p} search [--db-dir=<dir>] ([--keys | --prefixes]) <tablename> <attr> <value>

Options:
  -h --help        Show this help.
  -v --version     Show version number.
  --db-dir=<dir>   Specify dictionary directory.
  -f <from>        The first line number to dump. [default: 0]
  -n <lines>       The number of lines to dump.  [default: 10]
  -t <to>          The last line number + 1 to dump.
  --keys           Search for records that start with value.
  --prefixes       Search for records with common prefixes to value.

Examples:

- List tables

  {p} list --db-dir=testdb

- Dump 20 rows from 10 of the customer table.

  {p} dump --db-dir=testdb -f 10 -n 20 customer

- Search for "customer" records whose "name" starts with "Griffith"

  {p} search --db-dir=testdb --keys customer name Griffith

""".format(p='portabletab')  # noqa: E501


def dump_table(
    tablename: str,
    f: Optional[int],
    n: Optional[int],
    t: Optional[int] = None,
    db_dir: Optional[Path] = None,
) -> None:
    """
    Dump table.
    """
    if db_dir is None:
        db_dir = Path.cwd()
    else:
        db_dir = Path(db_dir)

    table = CapnpTable(
        db_dir=db_dir,
        tablename=tablename
    )

    if t is None:
        t = f + n

    t = min(t, table.count_records())

    writer = csv.writer(sys.stdout)
    labels = None
    for pos in range(f, t):
        record = table.get_record(pos=pos, as_dict=True)
        if labels is None:
            labels = record.keys()
            writer.writerow(labels)

        writer.writerow(record.values())


def list_tables(
    db_dir: Path
) -> None:
    if db_dir is None:
        db_dir = Path.cwd()
    else:
        db_dir = Path(db_dir)

    for confpath in db_dir.glob('*/config.json'):
        tablepath = confpath.parent
        try:
            table = CapnpTable(
                db_dir=db_dir,
                tablename=tablepath.name
            )
            nrecords = table.count_records()
            print(f"{tablepath.name}, {nrecords} records.")

            indexes = []
            for triepath in tablepath.glob("*.trie"):
                indexes.append(triepath.name)

            if len(indexes) > 0:
                print("  index: {}".format(
                    ", ".join(indexes)
                ))
        except FileNotFoundError:
            pass


def search_records(
    tablename: str,
    attr: str,
    value: str,
    db_dir: Optional[Path] = None,
    funcname: str = "get"
) -> None:
    """
    Search for records.

    Parameters
    ----------
    tablename: str
        Name of the target table.
    attr: str
        Target attribute.
    value: str
        Search value.
    db_dir: Path, optional
        Database directory where the table exists.
    funcname: str, optional ["get"]
        Search method.
    """
    if db_dir is None:
        db_dir = Path.cwd()
    else:
        db_dir = Path(db_dir)

    table = CapnpTable(
        db_dir=db_dir,
        tablename=tablename
    )

    writer = csv.writer(sys.stdout)
    labels = None
    try:
        for record in table.search_records_on(
                attr=attr,
                value=value,
                funcname=funcname):

            record = record.to_dict()
            if labels is None:
                labels = record.keys()
                writer.writerow(labels)

            writer.writerow(record.values())

    except PortableTab.exceptions.NoIndexError:
        print("No index has been created.", file=sys.stderr)
        exit(1)


def main():
    args = docopt(HELP)

    if args['--version']:
        print(PortableTab.__version__)
        exit(0)

    if args['dump']:
        dump_table(
            tablename=args["<tablename>"],
            f=int(args["-f"]),
            n=int(args["-n"]),
            t=int(args["-t"]) if args["-t"] else None,
            db_dir=args["--db-dir"],
        )
        exit(0)

    if args['list']:
        list_tables(
            db_dir=args["--db-dir"]
        )
        exit(0)

    if args['search']:
        funcname = "get"
        if args["--keys"] is True:
            funcname = "keys"
        elif args["--prefixes"] is True:
            funcname = "prefixes"

        search_records(
            tablename=args["<tablename>"],
            attr=args["<attr>"],
            value=args["<value>"],
            funcname=funcname,
            db_dir=args["--db-dir"],
        )


if __name__ == "__main__":
    main()
