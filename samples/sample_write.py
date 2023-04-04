import csv
from pathlib import Path
from typing import NoReturn

from PortableTab import CapnpTable


def prepare_tables(
        db_dir: Path,
        datafile: Path) -> NoReturn:

    # Declare table
    customers = CapnpTable(
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

    customers.create(
        capnp_schema=schema,
        record_type="Customer",  # Record type name
    )

    # Load data
    records = []
    by_country = {}
    with open(datafile, "r", newline="") as f:
        dictreader = csv.DictReader(f)
        for i, row in enumerate(dictreader):
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
            country = row["Country"]
            if country in by_country:
                by_country[country].append(i)
            else:
                by_country[country] = [i]

            records.append(record)

    customers.append_records(records)

    # Declare second table
    idx_country = CapnpTable(
        db_dir=db_dir,         # Directory where tables are located
        tablename="IdxCountry"  # Name of the table
    )

    # Create table
    schema = """
struct IdxCountry {
  country @0 :Text;
  idList @1 :List(UInt32);
}"""  # The schema of the record

    idx_country.create(
        capnp_schema=schema,
        record_type="IdxCountry",  # Record type name
    )

    # Load data
    records = []
    for country, indexes in by_country.items():
        record = {
            "country": country,
            "idList": indexes,
        }
        records.append(record)

    idx_country.append_records(records)


if __name__ == '__main__':
    db_dir = Path.cwd() / "testdb"
    datafile = db_dir / "organizations-1000000.csv"
    if not datafile.exists():
        print("Download csv from \"https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-1000000.zip\"")  # noqa E501
        print(" and extract \"organizations-1000000.csv\" to {}".format(
            datafile))
        exit(0)

    customer_table = prepare_tables(db_dir, datafile)
