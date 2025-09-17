"""
Módulo core do framework
"""
from .hdfs_client import HDFSClient
from .local_file_client import LocalFileClient
from .onelake_client import OneLakeClient

__all__ = ["HDFSClient", "LocalFileClient", "OneLakeClient"]