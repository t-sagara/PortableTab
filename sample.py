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


def random_read(table: CapnpTable):
    import random
    random.seed()
    for _ in range(10):
        record = table.get_record(
            pos=random.randrange(table.count_records()),
            as_dict=True)
        print(record)


if __name__ == '__main__':
    datafile = Path.cwd() / "organizations-1000000.csv"
    if not datafile.exists():
        print("Download csv from \"https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-1000000.zip\"")
        print(" and extract \"organizations-1000000.csv\" to {}".format(
            datafile))
        exit(1)

    db_dir = Path.cwd() / "testdb"
    customer_table = prepare_table(db_dir, datafile)
    random_read(customer_table)
