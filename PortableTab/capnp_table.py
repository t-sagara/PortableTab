from functools import lru_cache
import json
from logging import getLogger
import math
import mmap
from pathlib import Path
from typing import Any, Callable, Iterator, Iterable, Optional

import marisa_trie

from .exceptions import NoIndexError
from .capnp_manager import CapnpManager

logger = getLogger(__name__)


class CapnpTable(CapnpManager):
    """
    Class that represents a table containing Capnp records.

    Parameters
    ----------
    tablename: str
        Name of the table.
    db_dir: Path-like, optional
        Base directory where the schema and tables are placed.

    Notes
    -----
    - "db_dir" specifies the base directory where other tables will also be placed.
      Tables are placed in subdirectories with their tablename under the base directory.
    """

    def __init__(
            self,
            tablename: str,
            db_dir=None):
        super().__init__(db_dir)
        self.tablename = tablename
        self.readers = {}
        self.trie_indexes = {}

    def __del__(self):
        self.unload()

    def unload(self) -> None:
        """
        Release loaded resources manually,
        including the capnp module corresponding to this table.
        """
        super().unload()
        self.__class__.unload_schema(self.tablename)

    @lru_cache(maxsize=128)
    def get_dir(self) -> Path:
        """
        Get the directory where the table are placed.
        """
        return self.db_dir / self.tablename

    def _get_config_path(self) -> Path:
        """
        Get path to the config file of the table.
        """
        return self.get_dir() / "config.json"

    @lru_cache(maxsize=128)
    def get_config(self) -> Path:
        """
        Get the contents of the config file of the table.
        """
        with open(self._get_config_path(), "r") as f:
            config = json.load(f)

        return config

    def set_config(self, config: dict) -> None:
        """
        Set the contents of the config file of the table.

        Notes
        -----
        - This method overwrite the config file completery.
        """
        with open(self._get_config_path(), "w") as f:
            json.dump(config, f)

    def _load_capnp_file(self) -> None:
        """
        Load the capnp schema assigned to the table.

        Notes
        -----
        - This method is automatically called when needed.
          There is no need to call it explicitly from the user program.
        """
        config = self.get_config()
        module_name = config["module_name"]
        if module_name not in CapnpManager.modules or \
                CapnpManager.modules[module_name] is None:
            CapnpManager.load_schema(
                self.get_dir() / config["capnp_file"],
                module_name)

    @lru_cache(maxsize=128)
    def get_record_type(self) -> Any:
        """
        Get the record type.

        Returns
        -------
        Any
            Capnp type name corresponding to a record.

        Notes
        -----
        - The type is defined by the schema file name.
        """
        config = self.get_config()
        self._load_capnp_file()
        record_type = getattr(
            CapnpManager.modules[config["module_name"]],
            config["record_type"])
        return record_type

    @lru_cache(maxsize=128)
    def get_list_type(self) -> Any:
        """
        Get the list type.

        Returns
        -------
        Any
            Capnp type name corresponding to a list of records.

        Notes
        -----
        - The type is defined by the schema file name.
        """
        config = self.get_config()
        self._load_capnp_file()
        list_type = getattr(
            CapnpManager.modules[config["module_name"]],
            config["list_type"])
        return list_type

    def count_records(self) -> int:
        """
        Count the number of records in the table.

        Returns
        -------
        int
            The number of records.

        Notes
        -----
        - This method actually just reads the configuration file.
        """
        config = self.get_config()
        return config["count"]

    def _get_page_path(self, pos: int) -> Path:
        """
        Get the path to the page file.

        Paramaters
        ----------
        pos: int
            Position number of the record contained in the page file.

        Returns
        -------
        Path
            Path to the page file.
        """
        page_number = math.floor(pos / self.PAGE_SIZE)
        table_dir = self.get_dir()
        return table_dir / f"page_{page_number:03d}.bin"

    def _write_page(
            self,
            page: int,
            records: list) -> None:
        """
        Write a page file.

        Parameters
        ----------
        page: int
            Page number starting from 0.
        records: list
            List of records to output in Capnp record type.

        Notes
        -----
        - If records is larger than the page size, only the records in the page size
          are output and the rest of them are ignored.
        """
        target_records = records[0:self.PAGE_SIZE]
        list_obj = self.get_list_type().new_message()
        records_prop = list_obj.init('records', len(target_records))
        for i, node in enumerate(target_records):
            records_prop[i] = node

        page_path = self._get_page_path(page * self.PAGE_SIZE)
        with open(page_path, "wb") as f:
            list_obj.write(f)

    def delete(self) -> None:
        """
        Delete the table.

        Notes
        -----
        - This method actually deletes the subtree containing the table.
        """
        table_dir = self.get_dir()
        if table_dir.exists():
            import shutil
            shutil.rmtree(table_dir)  # remove directory with its contents

    def create(
            self,
            capnp_schema: str,
            record_type: str,
    ) -> Path:
        """
        Create table.

        Parameters
        ----------
        capnp_schema: str
            Record definition written in Capnp schema format.
        record_type: str
            Type name of the record (struct) defined in the schema.

        Returns
        -------
        Path
            Directory path where the created tables and schema will be stored.

        Notes
        -----
        - Even if multiple structure types are defined in the schema,
          the type passed as record_type is used as the record type of this table.
        """
        table_dir = self.get_dir()
        if table_dir.exists():
            import shutil
            shutil.rmtree(table_dir)  # remove directory with its contents

        list_type = f"{record_type}List"
        capnp_schema = capnp_schema + \
            "struct {} {{\n".format(list_type) + \
            "  records @0 :List({});\n".format(record_type) + \
            "}\n"

        table_dir.mkdir()
        # Copy capnp file
        copied = table_dir / f"{self.tablename}.capnp"
        with open(copied, "w") as fout:
            fout.write(capnp_schema)

        with open(self._get_config_path(), "w") as f:
            json.dump(obj={
                "capnp_file": copied.name,
                "module_name": self.tablename,
                "record_type": record_type,
                "list_type": list_type,
                "count": 0
            }, fp=f)

        # Load this schema once and generate the ID automatically.
        CapnpManager.load_schema(copied, self.tablename)

        return table_dir

    def get_record(
            self,
            pos: int,
            as_dict: bool = False) -> Any:
        """
        Get a record from the table at pos.

        Parameters
        ----------
        pos: int
            Position of the target record.
        as_dict: bool [False]
            Specifies whether records are returned in dict format.

        Returns
        -------
        Any
            When "as_dict" is set to True, it returns a dict object.
            Otherwise, it returns a record_type object.
        """
        page_path = self._get_page_path(pos=pos)
        mmap = self.get_page_mmap(page_path)
        with self.get_list_type().from_bytes(
                buf=mmap, traversal_limit_in_words=2**64-1) as list_obj:
            record = list_obj.records[pos % self.PAGE_SIZE]
            if as_dict:
                return record.to_dict()

            return record

    def retrieve_records(
            self,
            limit: Optional[int] = None,
            offset: Optional[int] = None,
            as_dict: bool = False) -> Iterator[Any]:
        """
        Get a iterator that retrieves records from the table.

        Paramaters
        ----------
        limit: int, optional
            Max number of records to be retrieved.
            If omitted, all records are retrieved.
        offset: int, optional
            Specifies the number of records to be retrieved from.
            If omitted, the retrieval is performed from the beginning.
        as_dict: bool [False]
            Specifies whether records are returned in dict format.

        Returns
        -------
        Iterator[Any]
            When "as_dict" is set to True, it returns a iterator of dict.
            Otherwise, it returns a iterator of record_type object.
        """
        if limit is None:
            limit = self.count_records()

        offset = 0 if offset is None else offset

        f = None
        mm = None
        list_obj = None

        pos = offset
        while pos < offset + limit:
            page_path = self._get_page_path(pos)
            current_path = page_path
            f = open(current_path, "rb")
            mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
            with self.get_list_type().from_bytes(
                    buf=mm, traversal_limit_in_words=2**64-1) as list_obj:

                while pos < offset + limit:
                    page_path = self._get_page_path(pos)
                    if page_path != current_path:
                        break

                    record = list_obj.records[pos % self.PAGE_SIZE]
                    if as_dict:
                        record = record.to_dict()

                    yield record
                    pos += 1

            mm.close()
            f.close()

    def append_records(
            self,
            records: Iterable[dict]) -> None:
        """
        Appends a set of record to the end of the table.

        Paramaters
        ----------
        records: Iterable[dict]
            Iterable that returns the records in order.
        """
        cur_pos = self.count_records()
        buffer = []
        record_type = self.get_record_type()

        # If the last page file is not full, read its contents
        # into the buffer.
        page = math.floor(cur_pos / self.PAGE_SIZE)
        pos = page * self.PAGE_SIZE
        if cur_pos - pos > 0:
            buffer = list(
                self.retrieve_records(limit=cur_pos - pos, offset=pos)
            )

        # Add records one by one and output when the number of records
        # reaches the page size.
        for record in records:
            buffer.append(record_type.new_message(**record))
            cur_pos += 1
            if len(buffer) == self.PAGE_SIZE:
                self._write_page(page=page, records=buffer)
                buffer.clear()
                page += 1

        # Outputs the records remaining in the buffer.
        if len(buffer) > 0:
            self._write_page(page=page, records=buffer)

        # Adjust record count.
        config = self.get_config()
        config["count"] = cur_pos
        self.set_config(config)

    def update_records(
            self,
            updates: dict) -> bool:
        """
        Updates records in the table that has already been output to a file.

        Paramaters
        ----------
        updates: dict
            A dict whose keys are the positions of records to be updated and
            whose values are the contents to be updated.

            The format of the values are a dict of field name/value pairs
            to be updated.

        Examples
        --------

            >>> table.update_records({
                15: {
                        "name": "Bernardo de la Paz",
                        "job": "Professor",
                },
            })

        Notes
        -----
        - This process is very slow and should not be called if possible.
        """
        current_page = None
        records = None
        updates = dict(sorted(updates.items()))

        for pos, new_value in updates.items():
            page = math.floor(pos / self.PAGE_SIZE)
            if page != current_page:
                if current_page is not None:
                    # Write the page to file
                    self._write_page(
                        page=current_page,
                        records=records
                    )

                # Read the page into memory
                page_path = self._get_page_path(pos)
                with open(page_path, "rb") as f:
                    current_page = page
                    list_obj = self.get_list_type().read(
                        f, traversal_limit_in_words=2**64 - 1)
                    records = [r.as_builder() for r in list_obj.records]

            pos_in_page = pos % self.PAGE_SIZE
            for key, value in new_value.items():
                setattr(
                    records[pos_in_page],
                    key, value)

        if current_page is not None:
            # Write the page to file
            self._write_page(
                page=current_page,
                records=records
            )

    def create_trie_on(
        self,
        attr: str,
        key_func: Optional[Callable] = None,
        filter_func: Optional[Callable] = None,
    ) -> None:
        """
        Create TRIE index on the specified attribute.

        Paramters
        ---------
        attr: str
            The name of target attribute.
        key_func: Callable, optional
            A function that takes the attribute value as argument
            used to generate a set of index key strings from each record.
            If not specified, the 'str' function will be used.
        filter_func: Callable, optional
            A function that takes the record as argument to determine
            if the record should be added to the index or not.
            When the function returns true (in the context of Boolean
            operation), the record will be indexed.
            If not specified, all records will be indexed.

        Notes
        -----
        - The created index is saved in the same directory as
          the page files with the file name "<attr>.trie".
        """

        def kvgen():
            # Generator function to enumerate sets of
            # attribute value and position.
            for pos in range(self.count_records()):
                record = CapnpTable.get_record(
                    self, pos=pos)  # Call base class method
                if pos == 0 and not hasattr(record, attr):
                    raise ValueError(f"Attribute '{attr}' doesn't exist.")

                if filter_func and not filter_func(record):
                    continue

                if key_func is None:
                    strings = str(getattr(record, attr))
                else:
                    strings = key_func(getattr(record, attr))

                if isinstance(strings, str) and strings != "":
                    yield (strings, (pos,))
                else:
                    for string in strings:
                        if string != "":
                            yield (string, (pos,))

        # Create RecordTrie
        # https://marisa-trie.readthedocs.io/en/latest/tutorial.html#marisa-trie-recordtrie  # noqa: E501
        trie = marisa_trie.RecordTrie("<L", kvgen())
        path = self.get_dir() / f"{attr}.trie"
        trie.save(str(path))

        # Open the trie using mmap.
        self.open_trie_on(attr)

    def open_trie_on(self, attr: str) -> marisa_trie.RecordTrie:
        """
        Open TRIE index on the specified attribute.

        Paramters
        ---------
        attr: str
            The name of target attribute.

        Returns
        -------
        RecordTrie
            The TRIE index.

        Notes
        -----
        - The index is mmapped from the file name "<attr>.trie",
          in the same directory as the page files.
        """
        if attr in self.trie_indexes:
            return self.trie_indexes[attr]

        path = self.get_dir() / f"{attr}.trie"
        if not path.exists():
            raise NoIndexError(f"Index '{path}' doesn't exist.")

        trie = marisa_trie.RecordTrie("<L").mmap(str(path))
        self.trie_indexes[attr] = trie
        return trie

    def delete_trie_on(self, attr: str):
        """
        Delete TRIE index on the specified attribute.

        Paramters
        ---------
        attr: str
            The name of target attribute.

        Notes
        -----
        - Delete any file named "<attr>.trie" in the same directory
          as the page files.
        - If the index is already loaded, unload it.
        """
        path = self.get_dir() / f"{attr}.trie"
        if path.exists():
            path.unlink()

        if attr in self.trie_indexes:
            del self.trie_indexes[attr]

    def search_records_on(
            self,
            attr: str,
            value: str,
            funcname: str = "get") -> list:
        """
        Search value from the table on the specified attribute.

        Paramters
        ---------
        attr: str
            The name of target attribute.
        value: str
            The target value.
        funcname: str
            The name of search method.
            - "get" searches for records that exactly match the value.
            - "prefixes" searches for records that contained in the value.
            - "keys" searches for records that containing the value.

        Returns
        -------
        List[Record]
            List of records.

        Notes
        -----
        - TRIE index must be created on the column before searching.
        - The TRIE index file will be automatically opened if it exists.
        """
        if funcname not in ("get", "prefixes", "keys"):
            raise ValueError("'func' must be 'get', 'prefixes' or 'keys'.")

        trie = self.open_trie_on(attr)
        positions = []
        if funcname == "get":
            positions = trie.get(value, [])
        elif funcname == "prefixes":
            for v in trie.prefixes(value):
                positions += trie.get(v, [])

        elif funcname == "keys":
            for v in trie.keys(value):
                positions += trie.get(v, [])

        records = []
        pos_tuples = set([p[0] for p in positions])
        for pos in pos_tuples:
            records.append(self.get_record(pos=pos))

        return records
