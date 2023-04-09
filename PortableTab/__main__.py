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

Options:
  -h --help        Show this help.
  -v --version     Show version number.
  --db-dir=<dir>   Specify dictionary directory. [default: "."]
  -f <from>        The first line number to dump. [default: 0]
  -n <lines>       The number of lines to dump.  [default: 10]
  -t <to>          The last line number + 1 to dump.

Examples:

- List tables

  {p} list --db-dir=testdb

- Dump 20 rows from 10 of the customer table.

  {p} dump --db-dir=testdb -f 10 -n 20 customer

""".format(p='portabletab')  # noqa: E501

def dump_table(
    db_dir: Path,
    tablename: str,
    f: Optional[int],
    n: Optional[int],
    t: Optional[int] = None
) -> None:
    """
    Dump table.
    """    
    writer = csv.writer(sys.stdout)
    labels = None
    table = CapnpTable(
        db_dir=db_dir,
        tablename=tablename
    )

    if t is None:
        t = f + n

    for pos in range(f, t):
        record = table.get_record(pos=pos, as_dict=True)
        if labels is None:
            labels = record.keys()
            writer.writerow(labels)

        writer.writerow(record.values())

def list_tables(
    db_dir: Path
) -> None:
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


def main():
    args = docopt(HELP)

    if args['--version']:
        print(PortableTab.__version__)
        exit(0)

    if args['dump']:
        dump_table(
            db_dir=args["--db-dir"],
            tablename=args["<tablename>"],
            f=int(args["-f"]),
            n=int(args["-n"]),
            t=int(args["-t"]) if args["-t"] else None 
        )
        exit(0)
    
    if args['list']:
        list_tables(
            db_dir=args["--db-dir"]
        )
        exit(0)


if __name__ == "__main__":
    main()
