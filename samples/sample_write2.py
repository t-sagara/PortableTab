import csv
from pathlib import Path
from typing import Any, Dict

from PortableTab import BaseTable


class CustomerTable(BaseTable):

    __tablename__ = "customer"
    __schema__ = """
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
}"""
    __record_type__ = "Customer"

    def csv2record(self, row: Dict[str, str]) -> Dict[str, Any]:
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
        return record


class IdxCountryTable(BaseTable):

    __tablename__ = "IdxCountry"
    __schema__ = """
struct IdxCountry {
  country @0 :Text;
  idList @1 :List(UInt32);
}"""
    __record_type__ = "IdxCountry"


def prepare_tables(
        db_dir: Path,
        datafile: Path) -> None:

    # Declare table
    customers = CustomerTable(
        db_dir=db_dir,        # Directory where tables are located
    )
    customers.create()

    # Load data
    by_country = {}

    def csv2record_generator() -> None:
        with open(datafile, "r", newline="") as f:
            dictreader = csv.DictReader(f)
            for i, row in enumerate(dictreader):
                record = customers.csv2record(row)
                country = row["Country"]
                if country in by_country:
                    by_country[country].append(i)
                else:
                    by_country[country] = [i]

                yield record

    customers.append_records(csv2record_generator())

    # Declare second table
    idx_country = IdxCountryTable(
        db_dir=db_dir,         # Directory where tables are located
    )

    # Create table
    idx_country.create()

    # Load data
    records = []
    for country, id_list in by_country.items():
        record = {
            "country": country,
            "idList": id_list,
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
