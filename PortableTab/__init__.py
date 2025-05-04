from importlib.metadata import version

from .base_table import BaseTable
from .capnp_manager import CapnpManager
from .capnp_table import CapnpTable

__version__ = version("PortableTab")

__all__ = [
    "BaseTable",
    "CapnpManager",
    "CapnpTable",
]
