import logging
import re
from pathlib import Path
from typing import Union, Optional, List
from .base import BaseIOClient

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
        """Implementação específica para arquivos locais"""
        try:
            file_path = Path(path).resolve()
            
            if not file_path.exists():
                logger.warning(f"Arquivo não encontrado: {file_path}")
                return None
                
            with open(file_path, 'rb') as f:
                content = f.read()
                logger.info(f"Arquivo lido: {file_path}")
                return content
                
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {path}: {e}")
            return None
    
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Implementação específica para arquivos locais"""
        try:
            file_path = self._ensure_directory(path)
            
            # Verificar se arquivo existe e se deve sobrescrever
            if file_path.exists() and not overwrite:
                logger.warning(f"Arquivo já existe e overwrite=False: {file_path}")
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
    
    def list_files(self, path: str, file_pattern: Optional[str] = None, 
                   max_depth: Optional[int] = None, exclude_patterns: Optional[List[str]] = None,
                   recursive: bool = True) -> Optional[List[str]]:
        """Implementação específica para sistema de arquivos local"""
        try:
            base_path = Path(path).resolve()
            
            if not base_path.exists():
                logger.error(f"Diretório não encontrado: {base_path}")
                return None
            
            if not base_path.is_dir():
                logger.error(f"Caminho não é um diretório: {base_path}")
                return None
            
            files = []
            
            def _collect_files(current_path: Path, current_depth: int = 0):
                """Função recursiva para coletar arquivos"""
                
                # Verificar limite de profundidade
                if max_depth is not None and current_depth > max_depth:
                    return
                
                try:
                    # Listar itens no diretório atual
                    for item in current_path.iterdir():
                        # Verificar padrões de exclusão
                        should_exclude = False
                        if exclude_patterns:
                            for exclude_pattern in exclude_patterns:
                                if re.search(exclude_pattern, item.name):
                                    should_exclude = True
                                    logger.debug(f"Excluindo {item.name} (padrão: {exclude_pattern})")
                                    break
                        
                        if should_exclude:
                            continue
                        
                        if item.is_file():
                            # Aplicar filtro de padrão se fornecido
                            if file_pattern is None or re.search(file_pattern, item.name):
                                files.append(str(item))
                        
                        elif item.is_dir() and recursive:
                            # Recursivamente processar subdiretório
                            _collect_files(item, current_depth + 1)
                            
                except PermissionError:
                    logger.warning(f"Permissão negada para acessar: {current_path}")
                except Exception as e:
                    logger.error(f"Erro ao processar diretório {current_path}: {e}")
            
            # Iniciar coleta
            _collect_files(base_path)
            
            search_type = "recursiva" if recursive else "não-recursiva"
            logger.info(f"Encontrados {len(files)} arquivos em {path} (busca {search_type})")
            return sorted(files)
            
        except Exception as e:
            logger.error(f"Erro na listagem de {path}: {e}")
            return None
    
    # Sobrescrever file_exists para usar implementação mais eficiente do sistema operacional
    def file_exists(self, path: str) -> bool:
        """Verifica se um arquivo existe usando Path.exists()"""
        try:
            return Path(path).resolve().exists()
        except Exception:
            return False
    
    # Sobrescrever métodos que podem ser otimizados para o sistema local
    def read_csv(self, path: str, **kwargs) -> Optional['pd.DataFrame']:
        """Implementação otimizada para CSV local usando pandas diretamente"""
        try:
            import pandas as pd
            file_path = Path(path).resolve()
            
            if not file_path.exists():
                logger.warning(f"Arquivo CSV não encontrado: {file_path}")
                return None
                
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"CSV lido diretamente: {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao ler CSV de {path}: {e}")
            # Fallback para implementação da classe base
            return super().read_csv(path, **kwargs)
    
    def save_csv(self, path: str, dataframe: 'pd.DataFrame', index: bool = False, 
                 overwrite: bool = True, **kwargs) -> bool:
        """Implementação otimizada para CSV local usando pandas diretamente"""
        try:
            file_path = self._ensure_directory(path)
            
            # Verificar se arquivo existe e se deve sobrescrever
            if file_path.exists() and not overwrite:
                logger.warning(f"Arquivo CSV já existe e overwrite=False: {file_path}")
                return False
            
            dataframe.to_csv(file_path, index=index, **kwargs)
            logger.info(f"CSV salvo diretamente: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao salvar CSV em {path}: {e}")
            # Fallback para implementação da classe base
            return super().save_csv(path, dataframe, index, overwrite, **kwargs)
    
    # Métodos específicos do sistema local que podem ser úteis
    def get_file_info(self, path: str) -> Optional[dict]:
        """Obtém informações detalhadas sobre um arquivo local"""
        try:
            file_path = Path(path).resolve()
            
            if not file_path.exists():
                return None
            
            stat = file_path.stat()
            
            return {
                'path': str(file_path),
                'name': file_path.name,
                'size': stat.st_size,
                'modified': stat.st_mtime,
                'created': stat.st_ctime,
                'is_file': file_path.is_file(),
                'is_dir': file_path.is_dir(),
                'permissions': oct(stat.st_mode)[-3:],
                'owner': stat.st_uid,
                'group': stat.st_gid
            }
            
        except Exception as e:
            logger.error(f"Erro ao obter informações do arquivo {path}: {e}")
            return None
    
    def list_files_detailed(self, path: str, file_pattern: Optional[str] = None,
                           include_directories: bool = False, max_depth: Optional[int] = None,
                           exclude_patterns: Optional[List[str]] = None, 
                           recursive: bool = True) -> Optional[List[dict]]:
        """Lista arquivos com informações detalhadas"""
        try:
            base_path = Path(path).resolve()
            
            if not base_path.exists():
                logger.error(f"Diretório não encontrado: {base_path}")
                return None
            
            files = []
            
            def _collect_detailed_files(current_path: Path, current_depth: int = 0):
                """Função recursiva para coletar arquivos com detalhes"""
                
                if max_depth is not None and current_depth > max_depth:
                    return
                
                try:
                    for item in current_path.iterdir():
                        # Verificar padrões de exclusão
                        should_exclude = False
                        if exclude_patterns:
                            for exclude_pattern in exclude_patterns:
                                if re.search(exclude_pattern, item.name):
                                    should_exclude = True
                                    break
                        
                        if should_exclude:
                            continue
                        
                        # Obter informações do item
                        item_info = self.get_file_info(str(item))
                        if item_info:
                            item_info['depth'] = current_depth
                        
                        if item.is_file():
                            # Aplicar filtro de padrão
                            if file_pattern is None or re.search(file_pattern, item.name):
                                files.append(item_info)
                        
                        elif item.is_dir():
                            # Incluir diretório se solicitado
                            if include_directories:
                                if file_pattern is None or re.search(file_pattern, item.name):
                                    files.append(item_info)
                            
                            # Recursivamente processar subdiretório
                            if recursive:
                                _collect_detailed_files(item, current_depth + 1)
                                
                except PermissionError:
                    logger.warning(f"Permissão negada para acessar: {current_path}")
                except Exception as e:
                    logger.error(f"Erro ao processar diretório {current_path}: {e}")
            
            _collect_detailed_files(base_path)
            
            logger.info(f"Encontrados {len(files)} itens com detalhes em {path}")
            return files
            
        except Exception as e:
            logger.error(f"Erro na listagem detalhada de {path}: {e}")
            return None
    
    def create_directory(self, path: str, parents: bool = True, exist_ok: bool = True) -> bool:
        """Cria um diretório local"""
        try:
            dir_path = Path(path).resolve()
            dir_path.mkdir(parents=parents, exist_ok=exist_ok)
            logger.info(f"Diretório criado: {dir_path}")
            return True
        except Exception as e:
            logger.error(f"Erro ao criar diretório {path}: {e}")
            return False
    
    def delete_file(self, path: str) -> bool:
        """Deleta um arquivo local"""
        try:
            file_path = Path(path).resolve()
            
            if not file_path.exists():
                logger.warning(f"Arquivo não existe: {file_path}")
                return False
            
            if file_path.is_file():
                file_path.unlink()
                logger.info(f"Arquivo deletado: {file_path}")
                return True
            else:
                logger.error(f"Caminho não é um arquivo: {file_path}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao deletar arquivo {path}: {e}")
            return False
    
    def delete_directory(self, path: str, recursive: bool = False) -> bool:
        """Deleta um diretório local"""
        try:
            dir_path = Path(path).resolve()
            
            if not dir_path.exists():
                logger.warning(f"Diretório não existe: {dir_path}")
                return False
            
            if not dir_path.is_dir():
                logger.error(f"Caminho não é um diretório: {dir_path}")
                return False
            
            if recursive:
                # Deletar recursivamente
                import shutil
                shutil.rmtree(dir_path)
                logger.info(f"Diretório deletado recursivamente: {dir_path}")
            else:
                # Deletar apenas se vazio
                dir_path.rmdir()
                logger.info(f"Diretório vazio deletado: {dir_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao deletar diretório {path}: {e}")
            return False