from pathlib import Path
import random

from PortableTab import CapnpTable


if __name__ == '__main__':
    db_dir = Path.cwd() / "testdb"
    table = CapnpTable(tablename="customer", db_dir=db_dir)

    random.seed()
    for _ in range(10):
        record = table.get_record(
            pos=random.randrange(table.count_records()),
            as_dict=True)
        print(record)
