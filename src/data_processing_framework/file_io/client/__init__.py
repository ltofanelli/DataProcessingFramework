"""
Módulo core do framework
"""
from .base_io_client import BaseIOClient
from .hdfs_client import HDFSClient
from .local_file_client import LocalFileClient
from .onelake_client import OneLakeClient

__all__ = ["BaseIOClient", "HDFSClient", "LocalFileClient", "OneLakeClient"]