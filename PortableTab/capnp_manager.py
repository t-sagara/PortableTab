from collections import OrderedDict
from logging import getLogger
import mmap
from pathlib import Path
import re
from typing import Any, List, Optional

import capnp
from capnp import KjException

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
    fp: File object
        File object to read the file contents.
    mm: Memory-map object
        Mmap object to map the file to memory.
    """

    def __init__(self, page_path: Path):
        self.page_path = page_path
        self.fp = open(self.page_path, "rb")
        self.mm = mmap.mmap(self.fp.fileno(), length=0,
                            access=mmap.ACCESS_READ)
        logger.debug("Assign mmap for '{}'.".format(self.page_path))

    def __del__(self):
        logger.debug("Release mmap for '{}'.".format(self.page_path))
        if self.mm:
            self.mm.close()

        if self.fp:
            self.fp.close()


class CapnpManager(object):
    """
    Class that manages the Capnp schema.

    Parameters
    ----------
    db_dir: Path-like
        Base directory where the schema and tables are placed.

    Attributes
    ----------
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

    def unload(self) -> None:
        """
        Release loaded resources manually,
        including cached PageReaders.
        """
        for page_path, reader in self.page_cache.items():
            self.page_cache[page_path] = None
            del reader

    @classmethod
    def load_schema(cls, path: str, as_module: str = None) -> Any:
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

        Notes
        -----
        - The type of the assigned module is dynamically determined from the file name.
        """
        if as_module is None:
            as_module = Path(path).name.replace(".", "_")

        try:
            cls.modules[as_module] = capnp.load(f"{path}")
        except KjException as e:
            m = re.search(r"@0x[0-9a-f]{16};", e.description)
            if m is None:
                raise e

            with open(path, "r") as f:
                content = m.group(0) + "\n" + f.read()

            with open(path, "w") as f:
                f.write(content)

            return cls.load_schema(path, as_module)

        return cls.modules[as_module]

    @classmethod
    def unload_schema(cls, names: Optional[List[str]] = None) -> None:
        """
        Unload capnp schema from the name space.

        Parameters
        ----------
        names: [str], optional
            List of assigned names for the modules to be unloaded.
        """
        new_modules = {}
        for name, module in cls.modules.items():
            if names is None or name in names:
                del module
            else:
                new_modules[name] = cls.modules[name]

        cls.modules = new_modules

    def get_page_mmap(self, page_path: Path) -> mmap.mmap:
        """
        Get mmap object assigned to the page file.

        Parameters
        ----------
        page_path: Path-like
            Path to the page file.

        Notes
        -----
        - This method uses cache mechanism.
          If more than 10 objects are opened, the oldest one is released.
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
