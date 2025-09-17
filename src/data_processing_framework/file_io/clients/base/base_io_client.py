import pandas as pd
import json
import logging
from abc import ABC, abstractmethod
from typing import Union, Optional, Dict, Any, List
import io

logger = logging.getLogger(__name__)

class BaseIOClient(ABC):
    """Interface abstrata para operações de leitura e escrita de arquivos"""
    
    # Métodos primitivos abstratos - devem ser implementados pelos clientes específicos
    @abstractmethod
    def read_file(self, path: str) -> Optional[bytes]:
        """Lê um arquivo e retorna bytes"""
        pass
    
    @abstractmethod
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Salva um arquivo"""
        pass
    
    @abstractmethod
    def list_files(self, path: str, file_pattern: Optional[str] = None, 
                   max_depth: Optional[int] = None, exclude_patterns: Optional[list] = None,
                   recursive: bool = True) -> Optional[list]:
        """Lista arquivos de um diretório"""
        pass
    
    # Métodos de alto nível - implementados aqui usando os primitivos
    def read_text(self, path: str, encoding: str = 'utf-8') -> Optional[str]:
        """Lê um arquivo de texto"""
        content = self.read_file(path)
        if content is not None:
            try:
                return content.decode(encoding)
            except UnicodeDecodeError as e:
                logger.error(f"Erro de encoding ao ler texto {path}: {e}")
                return None
        return None
    
    def save_text(self, path: str, text: str, encoding: str = 'utf-8', overwrite: bool = True) -> bool:
        """Salva um arquivo de texto"""
        try:
            content = text.encode(encoding)
            return self.save_file(path, content, overwrite)
        except UnicodeEncodeError as e:
            logger.error(f"Erro de encoding ao salvar texto {path}: {e}")
            return False
    
    def read_json(self, path: str) -> Optional[Union[Dict, List]]:
        """Lê um arquivo JSON"""
        text = self.read_text(path)
        if text is not None:
            try:
                return json.loads(text)
            except json.JSONDecodeError as e:
                logger.error(f"Erro ao decodificar JSON {path}: {e}")
                return None
        return None
    
    def save_json(self, path: str, data: Any, indent: int = 2, overwrite: bool = True) -> bool:
        """Salva dados como JSON"""
        try:
            text = json.dumps(data, indent=indent, ensure_ascii=False)
            return self.save_text(path, text, overwrite=overwrite)
        except (TypeError, ValueError) as e:
            logger.error(f"Erro ao serializar JSON {path}: {e}")
            return False
    
    def read_csv(self, path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV como DataFrame"""
        content = self.read_file(path)
        if content is not None:
            try:
                return pd.read_csv(io.BytesIO(content), **kwargs)
            except Exception as e:
                logger.error(f"Erro ao ler CSV {path}: {e}")
                return None
        return None
    
    def save_csv(self, path: str, dataframe: pd.DataFrame, index: bool = False, 
                 overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como CSV"""
        try:
            buffer = io.StringIO()
            dataframe.to_csv(buffer, index=index, **kwargs)
            content = buffer.getvalue()
            return self.save_text(path, content, overwrite=overwrite)
        except Exception as e:
            logger.error(f"Erro ao salvar CSV {path}: {e}")
            return False
    
    def read_parquet(self, path: str, columns: Optional[List[str]] = None, 
                     use_pandas_metadata: bool = True, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo Parquet como DataFrame"""
        content = self.read_file(path)
        if content is not None:
            try:
                return pd.read_parquet(io.BytesIO(content), columns=columns, **kwargs)
            except Exception as e:
                logger.error(f"Erro ao ler Parquet {path}: {e}")
                return None
        return None
    
    def save_parquet(self, path: str, dataframe: pd.DataFrame, compression: str = 'snappy', 
                     index: bool = False, partition_cols: Optional[List[str]] = None, 
                     overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como Parquet"""
        try:
            buffer = io.BytesIO()
            dataframe.to_parquet(buffer, compression=compression, index=index, **kwargs)
            content = buffer.getvalue()
            return self.save_file(path, content, overwrite=overwrite)
        except Exception as e:
            logger.error(f"Erro ao salvar Parquet {path}: {e}")
            return False
    
    # Métodos de conveniência que podem ser sobrescritos se necessário
    def file_exists(self, path: str) -> bool:
        """Verifica se um arquivo existe (implementação padrão usando read_file)"""
        try:
            content = self.read_file(path)
            return content is not None
        except Exception:
            return False
    
    def find_files(self, path: str, filename_pattern: str, case_sensitive: bool = True,
                   exclude_patterns: Optional[list] = None, recursive: bool = True) -> Optional[list]:
        """Busca arquivos por padrão no nome"""
        import re
        
        try:
            flags = 0 if case_sensitive else re.IGNORECASE
            pattern = re.compile(filename_pattern, flags)
            
            all_files = self.list_files(
                path,
                exclude_patterns=exclude_patterns,
                recursive=recursive
            )
            
            if all_files is None:
                return None
            
            matching_files = []
            for file_path in all_files:
                filename = file_path.split('/')[-1]
                if pattern.search(filename):
                    matching_files.append(file_path)
            
            return matching_files
            
        except Exception as e:
            logger.error(f"Erro na busca de arquivos em {path}: {e}")
            return None