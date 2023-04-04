from pathlib import Path
import random

from PortableTab import CapnpTable


if __name__ == '__main__':
    db_dir = Path.cwd() / "testdb"
    customers = CapnpTable(tablename="customer", db_dir=db_dir)
    idx_country = CapnpTable(tablename="IdxCountry", db_dir=db_dir)
    for i in range(idx_country.count_records()):
        record = idx_country.get_record(pos=i, as_dict=True)
        print(f"[{i:03d}]{record['country']}")
        for id in record["idList"]:
            customer = customers.get_record(pos=id, as_dict=True)
            print(f"\t[{id}]{customer['name']}")

        print("\tTotal: {}".format(len(record["idList"])))
