import csv
from io import TextIOWrapper
from pathlib import Path
import shutil
from zipfile import ZipFile

from PortableTab import CapnpTable


data = []


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


def load_data(table: CapnpTable, zippath: Path, csvname: str):
    global data
    data = []

    def record_generator(datapath):
        with ZipFile(datapath) as zipf:
            with zipf.open(csvname, "r") as f:
                dictreader = csv.DictReader(
                    TextIOWrapper(f, encoding='UTF-8', newline=''))
                records = []
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
                    data.append(record)
                    yield record

    table.append_records(record_generator(zippath))
    return data


def random_read(table: CapnpTable):
    import random
    random.seed()
    for _ in range(10):
        r = random.randrange(table.count_records())
        record = table.get_record(
            pos=r,
            as_dict=True)
        assert record["name"] == data[r]["name"]


def update_records(table: CapnpTable):
    updates = {
        0: {
            "name": "Info-Proto",
            "website": "https://www.info-proto.com/",
            "country": "Japan",
            "description": "Software development",
            "founded": 2012,
            "industry": "Computer Software / Engineering",
            "numberOfEmployees": 1,
        },
    }
    table.update_records(updates)


def read_record(table: CapnpTable, pos: int):
    record = table.get_record(pos, as_dict=True)
    return record


def test_all():
    db_dir = Path(__file__).parent / "testdb"
    customer_table = declare_table(db_dir, "customer")
    create_table(customer_table)
    load_data(
        customer_table,
        Path(__file__).parent / "organizations-1000.zip",
        "organizations-1000.csv"
    )
    random_read(customer_table)
    update_records(customer_table)
    record = customer_table.get_record(pos=0, as_dict=False)
    assert record.name == "Info-Proto"

    # Clean up
    shutil.rmtree(db_dir)
