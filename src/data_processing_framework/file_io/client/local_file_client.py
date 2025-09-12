import json
import pandas as pd
import logging
from pathlib import Path
from typing import Union, Optional, Dict, Any
from data_processing_framework.file_io.client.base_io_client import BaseIOClient

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalFileClient(BaseIOClient):
    """Cliente para operações de leitura e escrita no sistema de arquivos local"""
    
    def __init__(self):
        """Inicializa o cliente local"""
        logger.info("LocalFileClient inicializado")
    
    def _ensure_directory(self, file_path: Union[str, Path]) -> Path:
        """Garante que o diretório pai do arquivo existe e retorna o Path"""
        path = Path(file_path).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        return path
    
    def read_file(self, path: str) -> Optional[bytes]:
        """Lê um arquivo local e retorna bytes"""
        try:
            file_path = Path(path).resolve()
            
            if not file_path.exists():
                logger.error(f"Arquivo não encontrado: {file_path}")
                return None
                
            with open(file_path, 'rb') as f:
                content = f.read()
                logger.info(f"Arquivo lido: {file_path}")
                return content
                
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {path}: {e}")
            return None
    
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Salva um arquivo local"""
        try:
            file_path = self._ensure_directory(path)
            
            # Verificar se arquivo existe e se deve sobrescrever
            if file_path.exists() and not overwrite:
                logger.error(f"Arquivo já existe e overwrite=False: {file_path}")
                return False
            
            # Converter string para bytes se necessário
            if isinstance(content, str):
                content_bytes = content.encode('utf-8')
            else:
                content_bytes = content
            
            with open(file_path, 'wb') as f:
                f.write(content_bytes)
                logger.info(f"Arquivo salvo: {file_path}")
                return True
                
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo {path}: {e}")
            return False
    
    def read_text(self, path: str, encoding: str = 'utf-8') -> Optional[str]:
        """Lê um arquivo de texto local"""
        content = self.read_file(path)
        if content:
            try:
                return content.decode(encoding)
            except Exception as e:
                logger.error(f"Erro ao decodificar texto de {path}: {e}")
                return None
        return None
    
    def save_text(self, path: str, text: str, encoding: str = 'utf-8', overwrite: bool = True) -> bool:
        """Salva um arquivo de texto local"""
        return self.save_file(path, text.encode(encoding), overwrite)
    
    def read_json(self, path: str) -> Optional[Union[Dict, list]]:
        """Lê um arquivo JSON local"""
        text = self.read_text(path)
        if text:
            try:
                return json.loads(text)
            except Exception as e:
                logger.error(f"Erro ao decodificar JSON de {path}: {e}")
                return None
        return None
    
    def save_json(self, path: str, data: Any, indent: int = 2, overwrite: bool = True) -> bool:
        """Salva dados como JSON local"""
        try:
            json_text = json.dumps(data, ensure_ascii=False, indent=indent)
            return self.save_text(path, json_text, overwrite=overwrite)
        except Exception as e:
            logger.error(f"Erro ao converter dados para JSON para {path}: {e}")
            return False
    
    def read_csv(self, path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV local como DataFrame"""
        try:
            file_path = Path(path).resolve()
            
            if not file_path.exists():
                logger.error(f"Arquivo CSV não encontrado: {file_path}")
                return None
                
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"CSV lido: {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao ler CSV de {path}: {e}")
            return None
    
    def save_csv(self, path: str, dataframe: pd.DataFrame, index: bool = False, overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como CSV local"""
        try:
            file_path = self._ensure_directory(path)
            
            # Verificar se arquivo existe e se deve sobrescrever
            if file_path.exists() and not overwrite:
                logger.error(f"Arquivo CSV já existe e overwrite=False: {file_path}")
                return False
            
            dataframe.to_csv(file_path, index=index, **kwargs)
            logger.info(f"CSV salvo: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar CSV em {path}: {e}")
            return False