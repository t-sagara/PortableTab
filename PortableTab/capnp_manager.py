from collections import OrderedDict
from functools import cache
import hashlib
import json
from logging import getLogger
import math
import mmap
from pathlib import Path
from typing import List, Optional

import capnp

capnp.remove_import_hook()
logger = getLogger(__name__)


class PageReader(object):
    """
    Class that manages a mmapped page file.

    Paramaters
    ----------
    page_path: Pathlike
        Path to the page file.

    Attributes
    ----------
    f: File object
        File object to read the file contents.
    mm: Memory-map object
        Mmap object to map the file to memory.
    """

    def __init__(self, page_path: Path):
        self.page_path = page_path
        self.f = open(self.page_path, "rb")
        self.mm = mmap.mmap(self.f.fileno(), length=0, access=mmap.ACCESS_READ)
        logger.debug("Assign mmap for '{}'.".format(self.page_path))

    def __del__(self):
        logger.debug("Release mmap for '{}'.".format(self.page_path))
        if self.mm:
            self.mm.close()

        if self.f:
            self.f.close()


class CapnpManager(object):
    """
    Class that manages the Capnp schema.

    Parameters
    ----------
    db_dir: Path-like
        Base directory where the schema and tables are placed.

    Attributes
    ----------
    PAGE_SIZE: int [500000]

    page_cache: OrderedDict
        PageReader cache. Up to 10 caches are retained.
    """

    PAGE_SIZE = 500000
    modules = {}

    def __init__(self, db_dir):
        if isinstance(db_dir, str):
            db_dir = Path(db_dir)

        if not db_dir.exists():
            db_dir.mkdir(exist_ok=True)

        self.db_dir = db_dir

        self.page_cache = OrderedDict()

    def __del__(self):
        self.unload()

    def unload(self):
        """
        Release loaded resources manually,
        including cached PageReaders.
        """
        for page_path, reader in self.page_cache.items():
            self.page_cache[page_path] = None
            del reader

    @classmethod
    def load_schema(cls, path: str, as_module: str = None):
        """
        Load capnp schema into the name space.

        Parameters
        ----------
        path: str
            Path to the schema file.
        as_module: str, optional
            Name of the module assinged to the schema.

        Returns
        -------
        pycapnp.module
            The assigned module.
        """
        if as_module is None:
            as_module = Path(path).name.replace(".", "_")

        cls.modules[as_module] = capnp.load(f"{path}")
        return cls.modules[as_module]

    @classmethod
    def unload_schema(cls, names: Optional[List[str]] = None):
        """
        Unload capnp schema from the name space.

        Parameters
        ----------
        names: [str], optional
            List of assigned names for the modules to be unloaded.
        """
        for name, module in cls.modules.items():
            if names is None or name in names:
                cls.modules[name] = None
                del module

        cls.modules = {n: m for n, m in cls.modules.items() if m}

    def get_page_mmap(self, page_path: Path):
        """
        Get mmap object assigned to the page file.

        Parameters
        ----------
        page_path: Path-like
            Path to the page file.

        Notes
        -----
        - This method uses cache mechanism.
        """
        if page_path in self.page_cache:
            logger.debug("{} is in the cache.".format(page_path))
            self.page_cache.move_to_end(page_path)
            return self.page_cache[page_path].mm

        new_page_reader = PageReader(page_path)
        self.page_cache[page_path] = new_page_reader
        self.page_cache.move_to_end(page_path)
        logger.debug("Added {} to the cache.".format(page_path))

        if len(self.page_cache) > 10:
            k, v = self.page_cache.popitem(0)
            del v
            logger.debug("{} had been deleted from the cache.".format(k))

        return self.page_cache[page_path].mm


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
      Tables are placed in subdirectories with tablename in the base directory.
    """

    def __init__(
            self,
            tablename: str,
            db_dir=None):
        super().__init__(db_dir)
        self.tablename = tablename
        self.readers = {}

    def __del__(self):
        self.unload()

    def unload(self):
        """
        Release loaded resources manually,
        including the capnp module corresponding to this table.
        """
        super().unload()
        self.__class__.unload_schema(self.tablename)

    @cache
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

    @cache
    def get_config(self) -> Path:
        """
        Get the contents of the config file of the table.
        """
        with open(self._get_config_path(), "r") as f:
            config = json.load(f)

        return config

    def set_config(self, config: dict):
        """
        Set the contents of the config file of the table.

        Notes
        -----
        - This method overwrite the config file completery.
        """
        with open(self._get_config_path(), "w") as f:
            json.dump(config, f)

    def load_capnp_file(self):
        config = self.get_config()
        module_name = config["module_name"]
        if module_name not in CapnpManager.modules:
            CapnpManager.load_schema(
                self.get_dir() / config["capnp_file"],
                module_name)

    @cache
    def get_record_type(self):
        config = self.get_config()
        self.load_capnp_file()
        record_type = getattr(
            CapnpManager.modules[config["module_name"]],
            config["record_type"])
        return record_type

    @cache
    def get_list_type(self):
        config = self.get_config()
        self.load_capnp_file()
        list_type = getattr(
            CapnpManager.modules[config["module_name"]],
            config["list_type"])
        return list_type

    def count_records(self):
        config = self.get_config()
        return config["length"]

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
            records: list):
        target_records = records[0:self.PAGE_SIZE]
        list_obj = self.get_list_type().new_message()
        records_prop = list_obj.init('records', len(target_records))
        for i, node in enumerate(target_records):
            records_prop[i] = node

        page_path = self._get_page_path(page * self.PAGE_SIZE)
        with open(page_path, "wb") as f:
            list_obj.write(f)

    def delete(self):
        """
        Delete this table with records.
        """
        table_dir = self.get_dir()
        if table_dir.exists():
            import shutil
            shutil.rmtree(table_dir)  # remove directory with its contents

    def create(
            self,
            capnp_schema: str,
            record_type: str):
        """
        Create table.
        """
        table_dir = self.get_dir()
        if table_dir.exists():
            import shutil
            shutil.rmtree(table_dir)  # remove directory with its contents

        list_type = f"{record_type}List"
        capnp_id = hashlib.md5(capnp_schema.encode('utf-8')).hexdigest()
        capnp_schema = "@0x{};\n".format(capnp_id[0:16]) + capnp_schema + \
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
                "length": 0
            }, fp=f)

        return table_dir

    def get_record(
            self,
            pos: int):
        """
        Get a record from the table at pos.

        Parameters
        ----------
        pos: int
            Position of the target record.
        """
        page_path = self._get_page_path(pos=pos)
        mmap = self.get_page_mmap(page_path)
        with self.get_list_type().from_bytes(
                buf=mmap, traversal_limit_in_words=2**64-1) as list_obj:
            return list_obj.records[pos % self.PAGE_SIZE]

    def _get_page_reader(self, pos: int):
        return PageReader(table=self, pos=pos)

    def retrieve_records(
            self,
            limit: Optional[int] = None,
            offset: Optional[int] = None) -> list:
        """
        Get a generator that retrieves records from the table.

        Paramaters
        ----------
        limit: int, optional
            Max number of records to be retrieved.
            If omitted, all records are retrieved.
        offset: int, optional
            Specifies the number of records to be retrieved from.
            If omitted, the retrieval is performed from the beginning.

        Returns
        -------
        A record object of the table.
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

                    yield list_obj.records[pos % self.PAGE_SIZE]
                    pos += 1

            mm.close()
            f.close()

    def append_records(
            self,
            records: list) -> bool:
        """
        Appends a record to the end of the table.

        Paramaters
        ----------
        records: list
            The list of record.
        """
        new_pos = self.count_records()

        page_path = self._get_page_path(new_pos)
        page = math.floor(new_pos / self.PAGE_SIZE)
        pos = page * self.PAGE_SIZE
        if new_pos - pos > 0:
            with open(page_path, "rb") as f:
                list_obj = self.get_list_type().read(f)

            self._write_page(
                page=page,
                records=(list_obj.records + records)[:self.PAGE_SIZE])

            new_records = records[self.PAGE_SIZE - len(list_obj.records):]
            page += 1
        else:
            new_records = records[:]

        while len(new_records) > 0:
            self._write_page(
                page=page,
                records=new_records[0:self.PAGE_SIZE])

            new_records = new_records[self.PAGE_SIZE:]
            page += 1

        config = self.get_config()
        config["length"] += len(records)
        self.set_config(config)

    def update_records(
            self,
            updates: dict) -> bool:
        """
        Updates records in a table that has already been output to a file.

        Paramaters
        ----------
        updates: dict
            A dict whose keys are the positions of records to be updated and
            whose values are the contents to be updated.

            The format of the valuse are a dict of field name/value pairs
            to be updated.

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
                    # records = list_obj.records

            for key, value in new_value.items():
                setattr(
                    records[pos % self.PAGE_SIZE],
                    key, value)

        if current_page is not None:
            # Write the page to file
            self._write_page(
                page=current_page,
                records=records
            )