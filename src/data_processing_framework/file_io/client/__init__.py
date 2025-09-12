"""
Módulo core do framework
"""

from .hdfs_client import HDFSClient
from .local_file_client import LocalFileClient
from .fabric_client import FabricLakehouseClient

__all__ = ["HDFSClient", "LocalFileClient", "FabricLakehouseClient"]