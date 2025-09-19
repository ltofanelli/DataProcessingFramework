"""
Módulo file_io.clients do framework
"""
from .hdfs_client import HDFSClient
from .local_file_client import LocalFileClient
from .onelake_client import OneLakeClient
from .sftp_client import SFTPClient

__all__ = ["HDFSClient", "LocalFileClient", "OneLakeClient", "SFTPClient"]