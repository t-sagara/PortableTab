from functools import lru_cache

from abc import ABC
import json
from logging import getLogger
import math
import mmap
from pathlib import Path
from typing import Any, Iterator, Iterable, List, Optional

from .capnp_table import CapnpTable

logger = getLogger(__name__)


class BaseTable(CapnpTable, ABC):
    """
    Class that represents a table assigned schema.

    Attributes
    ----------
    __tablename__: str
        The name of the table.
    __schema__: str
        The schema of the table.
        It must be defined in the struct of Capnp schema.
        ref: https://capnproto.org/language.html
    __record_type__: str
        The record type of the table.

    Parameters
    ----------
    db_dir: Path-like, optional
        Base directory where the schema and tables are placed.

    Notes
    -----
    - "db_dir" specifies the base directory where other tables will also be placed.
      Tables are placed in subdirectories with tablename in the base directory.
    """

    __tablename__ = None
    __schema__ = None
    __record_type__ = None

    def __init__(
            self,
            db_dir=None):
        self.tablename = self.__class__.__tablename__
        self.schema = self.__class__.__schema__
        self.record_type = self.__class__.__record_type__
        self.db_dir = db_dir

        super().__init__(
            tablename=self.tablename,
            db_dir=db_dir)

    def __del__(self):
        self.unload()

    def create(self) -> Path:
        """
        Create table.

        Returns
        -------
        Path
            Directory path where the created tables and schema will be stored.

        Notes
        -----
        - Even if multiple types are defined, only the type specified
          as __record_type__ is used.
        """
        return super().create(
            capnp_schema=self.schema,
            record_type=self.record_type,
        )
