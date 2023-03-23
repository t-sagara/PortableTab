import csv
from pathlib import Path

from PortableTab.capnp_manager import CapnpTable


def declare_table(db_dir, tablename):
    table = CapnpTable(
        db_dir=db_dir,
        tablename=tablename
    )
    return table


def create_table(table):
    # Index,Organization Id,Name,Website,Country,Description,Founded,
    # Industry,Number of employees
    # Capnp schema must be in camelCase.
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
}"""

    table.create(
        capnp_schema=schema,
        record_type="Customer",
    )


def load_data(table: CapnpTable, zip_url: str):
    from zipfile import ZipFile
    from io import TextIOWrapper
    db_dir = table.get_dir().parent
    zipname = Path(zip_url).name
    csvname = zipname[:-4] + ".csv"
    datapath = Path(db_dir) / zipname
    if not datapath.exists():
        import urllib.request
        with urllib.request.urlopen(zip_url) as response, \
                open(datapath, "w") as fout:
            fout.write(response.read())

    record_type = table.get_record_type()
    with ZipFile(datapath) as zipf:
        with zipf.open(csvname, "r") as f:
            dictreader = csv.DictReader(
                TextIOWrapper(f, encoding='UTF-8', newline=''))
            records = []
            for row in dictreader:
                record = record_type.new_message(
                    index=int(row["Index"]),
                    organizationId=row["Organization Id"],
                    name=row["Name"],
                    website=row["Website"],
                    country=row["Country"],
                    description=row["Description"],
                    founded=int(row["Founded"]),
                    industry=row["Industry"],
                    numberOfEmployees=int(row["Number of employees"]),
                )
                records.append(record)

            table.append_records(records)


def random_read(table: CapnpTable):
    import random
    random.seed()
    for _ in range(10):
        record = table.get_record(
            pos=random.randrange(table.count_records()))
        print(record)


if __name__ == '__main__':
    db_dir = Path(__file__).parent / "testdb"
    customer_table = declare_table(db_dir, "customer")
    create_table(customer_table)
    load_data(
        customer_table,
        "https://github.com/datablist/sample-csv-files/raw/main/files/organizations/organizations-1000000.zip")  # noqa: E501
    random_read(customer_table)
