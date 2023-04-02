import csv
from pathlib import Path

from PortableTab import CapnpTable


def prepare_table(
        db_dir: Path,
        datafile: Path) -> CapnpTable:

    # Declare table
    table = CapnpTable(
        db_dir=db_dir,        # Directory where tables are located
        tablename="customer"  # Name of the table
    )

    # Create table
    schema = """
struct Customer {
  index @0 :UInt32;
  organizationId @1 :Text;
  name @2 :Text;
  website @3 :Text;
  country @4 :Text;
  description @5 :Text;
  founded @6 :Int16;
  industry @7 :Text;
  numberOfEmployees @8 :UInt32;
}"""  # The schema of the record

    table.create(
        capnp_schema=schema,
        record_type="Customer",  # Record type name
    )

    # Load data
    records = []
    with open(datafile, "r", newline="") as f:
        dictreader = csv.DictReader(f)
        for row in dictreader:
            record = {
                "index": int(row["Index"]),
                "organizationId": row["Organization Id"],
                "name": row["Name"],
                "website": row["Website"],
                "country": row["Country"],
                "description": row["Description"],
                "founded": int(row["Founded"]),
                "industry": row["Industry"],
                "numberOfEmployees": int(row["Number of employees"]),
            }
            records.append(record)

    table.append_records(records)
    return table


if __name__ == '__main__':
    db_dir = Path.cwd() / "testdb"
    datafile = db_dir / "organizations-1000000.csv"
    if not datafile.exists():
        print("Download csv from \"https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-1000000.zip\"")  # noqa E501
        print(" and extract \"organizations-1000000.csv\" to {}".format(
            datafile))
        exit(1)

    customer_table = prepare_table(db_dir, datafile)
