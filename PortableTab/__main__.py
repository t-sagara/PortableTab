import csv
import sys

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

Options:
  -h --help        Show this help.
  -v --version     Show version number.
  --db-dir=<dir>   Specify dictionary directory. [default: "."]
  -f <from>        The first line number to dump. [default: 0]
  -n <lines>       The number of lines to dump.  [default: 10]
  -t <to>          The last line number + 1 to dump.

Examples:

- Dump 20 rows from 10 of the customer table.

  {p} dump --db-dir=testdb -f 10 -n 20 customer

""".format(p='portabletab')  # noqa: E501


def main():
    args = docopt(HELP)

    if args['--version']:
        print(PortableTab.__version__)
        exit(0)

    if args['dump']:
        writer = csv.writer(sys.stdout)
        labels = None
        table = CapnpTable(
            db_dir=args["--db-dir"],
            tablename=args["<tablename>"])

        pos0 = int(args["-f"])
        if args["-t"]:
            pos1 = int(args["-t"])
        else:
            pos1 = pos0 + int(args["-n"])

        for pos in range(pos0, pos1):
            record = table.get_record(pos=pos, as_dict=True)
            if labels is None:
                labels = record.keys()
                writer.writerow(labels)

            writer.writerow(record.values())


if __name__ == "__main__":
    main()
