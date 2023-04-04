from PortableTab import BaseTable


class SampleTable(BaseTable):
    __tablename__ = "sample"
    __schema__ = """
        struct SampleRecord {
            name @0 :Text;
            email @1 :Text;
        }
    """
    __record_type__ = "SampleRecord"


sample_table = SampleTable(db_dir="./db")
sample_table.create()
sample_table.append_records([
    {"name": "Bob", "email": "bob@example.com"},
    {"name": "Alice", "email": "alice@foo.org"},
])


sample_table = SampleTable(db_dir="./db")
for i in range(sample_table.count_records()):
    print(sample_table.get_record(pos=i, as_dict=True))
