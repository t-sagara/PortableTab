from pathlib import Path
import re
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


if __name__ == '__main__':
    db_dir = Path.cwd() / "testdb"
    customers = CustomerTable(db_dir=db_dir)

    # Create TRIE index on "name"
    # customers.create_trie_on("name")
    # print(customers.search_records_on(attr="name", value="Griffin"))
    # print(customers.search_records_on(
    #     attr="name", value="Griffin", funcname="keys"))

    # Create TRIE index on "industry" with function
    customers.create_trie_on(
        attr="industry",
        func=lambda x: re.split(r'\s*/\s*', x)
    )
    print(customers.search_records_on("industry", "Family Services"))
