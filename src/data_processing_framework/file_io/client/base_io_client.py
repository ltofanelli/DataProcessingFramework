import pandas as pd
from abc import ABC, abstractmethod
from typing import Union, Optional, Dict, Any

class BaseIOClient(ABC):
    """Interface abstrata para operações de leitura e escrita de arquivos"""
    
    @abstractmethod
    def read_file(self, path: str) -> Optional[bytes]:
        """Lê um arquivo e retorna bytes"""
        pass
    
    @abstractmethod
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Salva um arquivo"""
        pass
    
    @abstractmethod
    def read_text(self, path: str, encoding: str = 'utf-8') -> Optional[str]:
        """Lê um arquivo de texto"""
        pass
    
    @abstractmethod
    def save_text(self, path: str, text: str, encoding: str = 'utf-8', overwrite: bool = True) -> bool:
        """Salva um arquivo de texto"""
        pass
    
    @abstractmethod
    def read_json(self, path: str) -> Optional[Union[Dict, list]]:
        """Lê um arquivo JSON"""
        pass
    
    @abstractmethod
    def save_json(self, path: str, data: Any, indent: int = 2, overwrite: bool = True) -> bool:
        """Salva dados como JSON"""
        pass
    
    @abstractmethod
    def read_csv(self, path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV como DataFrame"""
        pass
    
    @abstractmethod
    def save_csv(self, path: str, dataframe: pd.DataFrame, index: bool = False, overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como CSV"""
        pass