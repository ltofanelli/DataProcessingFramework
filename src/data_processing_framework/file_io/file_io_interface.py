import logging
from typing import Union, Optional, Dict, Any, List
import pandas as pd
from .client import HDFSClient
from .client import LocalFileClient
from .client import OneLakeClient
from data_processing_framework.config.enums import FileInterfaceType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileIOInterface:
    """Classe unificada para operações de leitura e escrita com interface única"""
    
    def __init__(self, io_credentials: dict = {}, client_type: FileInterfaceType = FileInterfaceType.LOCAL, **kwargs):
        """
        Inicializa o cliente unificado de arquivos
        
        Args:
            io_credentials (dict): Credenciais de conexão para o client especificaco no 'client_type'
            client_type (str): Tipo de cliente ('local', 'hdfs', 'fabric')
            **kwargs: Argumentos específicos para cada tipo de cliente
        """
        self.credentials = io_credentials
        self.client_type = client_type
        
        if client_type == FileInterfaceType.LOCAL:
            self.client = LocalFileClient(**kwargs)

        elif client_type == FileInterfaceType.HDFS:
            self.client = HDFSClient(self.credentials)

        elif client_type == FileInterfaceType.FABRIC:
            self.client = OneLakeClient(self.credentials)
            
        else:
            raise ValueError(f"Tipo de cliente não suportado: {self.client_type}")
        
        logger.info(f"FileIOInterface inicializado com tipo: {self.client_type}")
    
    # Métodos primitivos - delegação simples
    def read_file(self, path: str) -> Optional[bytes]:
        """Lê um arquivo e retorna bytes"""
        return self.client.read_file(path)
    
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Salva um arquivo"""
        return self.client.save_file(path, content, overwrite)
    
    def list_files(self, path: str, file_pattern: Optional[str] = None, 
                   max_depth: Optional[int] = None, exclude_patterns: Optional[list] = None,
                   recursive: bool = True) -> Optional[list]:
        """Lista arquivos de um diretório"""
        files = self.client.list_files(path, file_pattern, max_depth, exclude_patterns, recursive)
        return sorted(files) if files else files
    
    # Métodos de alto nível - delegação simples (lógica está na BaseIOClient)
    def read_text(self, path: str, encoding: str = 'utf-8') -> Optional[str]:
        """Lê um arquivo de texto"""
        return self.client.read_text(path, encoding)
    
    def save_text(self, path: str, text: str, encoding: str = 'utf-8', overwrite: bool = True) -> bool:
        """Salva um arquivo de texto"""
        return self.client.save_text(path, text, encoding, overwrite)
    
    def read_json(self, path: str) -> Optional[Union[Dict, list]]:
        """Lê um arquivo JSON"""
        return self.client.read_json(path)
    
    def save_json(self, path: str, data: Any, indent: int = 2, overwrite: bool = True) -> bool:
        """Salva dados como JSON"""
        return self.client.save_json(path, data, indent, overwrite)
    
    def read_csv(self, path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV como DataFrame"""
        return self.client.read_csv(path, **kwargs)
    
    def save_csv(self, path: str, dataframe: pd.DataFrame, index: bool = False, overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como CSV"""
        return self.client.save_csv(path, dataframe, index, overwrite, **kwargs)
    
    def read_parquet(self, path: str, columns: Optional[List[str]] = None, use_pandas_metadata: bool = True, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo Parquet como DataFrame"""
        return self.client.read_parquet(path, columns, use_pandas_metadata, **kwargs)

    def save_parquet(self, path: str, dataframe: pd.DataFrame, compression: str = 'snappy', index: bool = False, 
                     partition_cols: Optional[List[str]] = None, overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como Parquet"""
        return self.client.save_parquet(path, dataframe, compression, index, partition_cols, overwrite, **kwargs)

    # Métodos de conveniência
    def file_exists(self, path: str) -> bool:
        """Verifica se um arquivo existe"""
        return self.client.file_exists(path)
    
    def find_files(self, path: str, filename_pattern: str, case_sensitive: bool = True,
                   exclude_patterns: Optional[list] = None, recursive: bool = True) -> Optional[list]:
        """Busca arquivos por padrão no nome"""
        return self.client.find_files(path, filename_pattern, case_sensitive, exclude_patterns, recursive)
    
    def get_client_type(self) -> str:
        """Retorna o tipo de cliente em uso"""
        return self.client_type.value