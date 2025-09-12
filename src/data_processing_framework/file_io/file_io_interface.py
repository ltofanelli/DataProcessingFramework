import pandas as pd
import logging
from typing import Union, Optional, Dict, Any
from data_processing_framework.config.enums import FileInterfaceType
from data_processing_framework.file_io.client.hdfs_client import HDFSClient
from data_processing_framework.file_io.client.local_file_client import LocalFileClient
from data_processing_framework.file_io.client.fabric_client import FabricLakehouseClient

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileIOInterface:
    """Classe unificada para operações de leitura e escrita com interface única"""
    
    def __init__(self, client_type: FileInterfaceType = FileInterfaceType.LOCAL, **kwargs):
        """
        Inicializa o cliente unificado de arquivos
        
        Args:
            client_type (str): Tipo de cliente ('local' ou 'hdfs')
            **kwargs: Argumentos específicos para cada tipo de cliente
        """
        self.client_type = client_type

        if client_type == FileInterfaceType.LOCAL:
            self.client = LocalFileClient(**kwargs)

        elif client_type == FileInterfaceType.HDFS:
            self.client = HDFSClient(**kwargs)

        elif client_type == FileInterfaceType.FABRIC:
            self.client = FabricLakehouseClient(**kwargs)
            
        else:
            raise ValueError(f"Tipo de cliente não suportado: {self.client_type}")
        
        logger.info(f"FileIOInterface inicializado com tipo: {self.client_type}")
    
    def read_file(self, path: str) -> Optional[bytes]:
        """Lê um arquivo e retorna bytes"""
        return self.client.read_file(path)
    
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Salva um arquivo"""
        return self.client.save_file(path, content, overwrite)
    
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
    
    def list_files(self, path: str, file_pattern: Optional[str] = None, 
               max_depth: Optional[int] = None, exclude_patterns: Optional[list] = None,
               recursive: bool = True) -> Optional[list]:
        """
        Lista apenas os caminhos dos arquivos de um diretório
        
        Args:
            path: Caminho do diretório a ser listado
            file_pattern: Padrão regex para filtrar arquivos (ex: r'\.csv$' para apenas CSVs)
            max_depth: Profundidade máxima da busca (None = sem limite, só funciona se recursive=True)
            exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos (ex: ['tracking', r'\.tmp$'])
            recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
        Returns:
            list: Lista simples com os caminhos completos dos arquivos encontrados
                Retorna None em caso de erro
        """
        files = self.client.list_files(path, file_pattern, max_depth, exclude_patterns, recursive)
        return files.sort()
    
    def get_client_type(self) -> str:
        """Retorna o tipo de cliente em uso"""
        return self.client_type.value