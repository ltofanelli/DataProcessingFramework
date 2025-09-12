import json
import pandas as pd
import os
import logging
import time
import re
from typing import Union, Optional, Dict, Any, List
from data_processing_framework.file_io.client.base_io_client import BaseIOClient

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Importações específicas do Microsoft Fabric
# import notebookutils as nbu
# from notebookutils import mssparkutils


class FabricLakehouseClient(BaseIOClient):
    """Cliente para operações no Microsoft Fabric Lakehouse"""
    
    def __init__(self, lakehouse_path: str = ''):
        """
        Inicializa o cliente do Fabric Lakehouse
        
        Args:
            lakehouse_path: Path completo do lakehouse (ex: "/lakehouse/default/Files")
        """
        self.lakehouse_path = lakehouse_path.rstrip('/')
        logger.info(f"FabricLakehouseClient iniciado com path: {self.lakehouse_path}")
    
    # def _get_full_path(self, relative_path: str) -> str:
    #     """Converte caminho relativo para caminho completo no lakehouse"""
    #     if not relative_path:
    #         return self.lakehouse_path
        
    #     # Remove barras iniciais do path relativo
    #     clean_path = relative_path.lstrip('/')
    #     return f"{self.lakehouse_path}/{clean_path}".lstrip('/')
    
    # def _ensure_directory(self, file_path: str):
    #     """Garante que o diretório do arquivo existe"""
    #     dir_path = os.path.dirname(file_path)
    #     try:
    #         mssparkutils.fs.mkdirs(dir_path)
    #     except Exception as e:
    #         # Ignorar erros - diretório pode já existir
    #         logger.debug(f"Aviso ao criar diretório {dir_path}: {e}")
    
    # def _file_exists(self, full_path: str) -> bool:
    #     """Verifica se um arquivo existe"""
    #     try:
    #         parent_dir = os.path.dirname(full_path)
    #         filename = os.path.basename(full_path)
            
    #         files = mssparkutils.fs.ls(parent_dir)
    #         return any(f.name == filename for f in files)
    #     except Exception:
    #         return False
    
    # def read_file(self, relative_path: str, retries: int = 3) -> Optional[bytes]:
    #     """Lê um arquivo do Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     for attempt in range(retries):
    #         try:
    #             # Usar mssparkutils para ler arquivo binário
    #             content_str = mssparkutils.fs.head(full_path)
                
    #             if content_str is None:
    #                 logger.info(f"Arquivo não encontrado ou vazio: {relative_path}")
    #                 return None
                
    #             # Se for string, converter para bytes
    #             if isinstance(content_str, str):
    #                 return content_str.encode('utf-8')
    #             else:
    #                 return content_str
                
    #         except Exception as e:
    #             logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao ler {relative_path}: {e}")
    #             if attempt < retries - 1:
    #                 time.sleep(2)
        
    #     logger.error(f"Falha ao ler arquivo {relative_path} após {retries} tentativas")
    #     return None
    
    # def save_file(self, relative_path: str, content: Union[str, bytes], overwrite: bool = True, retries: int = 3) -> bool:
    #     """Salva um arquivo no Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     for attempt in range(retries):
    #         try:
    #             # Garantir que o diretório existe
    #             self._ensure_directory(full_path)
                
    #             # Verificar se arquivo existe e overwrite
    #             if not overwrite and self._file_exists(full_path):
    #                 logger.warning(f"Arquivo já existe e overwrite=False: {relative_path}")
    #                 return False
                
    #             # Converter para string se for bytes (mssparkutils trabalha melhor com strings)
    #             if isinstance(content, bytes):
    #                 try:
    #                     content_str = content.decode('utf-8')
    #                 except UnicodeDecodeError:
    #                     # Se não conseguir decodificar como UTF-8, usar método alternativo
    #                     import tempfile
    #                     with tempfile.NamedTemporaryFile(mode='wb', delete=False) as tmp_file:
    #                         tmp_file.write(content)
    #                         tmp_path = tmp_file.name
                        
    #                     # Copiar arquivo temporário para o lakehouse
    #                     mssparkutils.fs.cp(f"file://{tmp_path}", full_path, recurse=False)
    #                     os.unlink(tmp_path)
                        
    #                     logger.info(f"Arquivo binário salvo em: {relative_path}")
    #                     return True
    #             else:
    #                 content_str = content
                
    #             # Usar put para escrever string diretamente
    #             mssparkutils.fs.put(full_path, content_str, overwrite=True)
                
    #             logger.info(f"Arquivo salvo em: {relative_path}")
    #             return True
                
    #         except Exception as e:
    #             logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao salvar {relative_path}: {e}")
    #             if attempt < retries - 1:
    #                 time.sleep(3)
        
    #     logger.error(f"Falha ao salvar arquivo {relative_path} após {retries} tentativas")
    #     return False
    
    # def read_text(self, relative_path: str, encoding: str = 'utf-8') -> Optional[str]:
    #     """Lê um arquivo de texto do Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         # Usar head diretamente para arquivos de texto
    #         content = mssparkutils.fs.head(full_path)
    #         if content is not None:
    #             return str(content)
    #         else:
    #             logger.info(f"Arquivo não encontrado: {relative_path}")
    #             return None
                
    #     except Exception as e:
    #         logger.error(f"Erro ao ler texto de {relative_path}: {e}")
    #         return None
    
    # def save_text(self, relative_path: str, text: str, encoding: str = 'utf-8', overwrite: bool = True) -> bool:
    #     """Salva um arquivo de texto no Lakehouse"""
    #     return self.save_file(relative_path, text, overwrite)
    
    # def read_json(self, relative_path: str) -> Optional[Union[Dict, list]]:
    #     """Lê um arquivo JSON do Lakehouse"""
    #     text = self.read_text(relative_path)
    #     if text:
    #         try:
    #             return json.loads(text)
    #         except Exception as e:
    #             logger.error(f"Erro ao decodificar JSON de {relative_path}: {e}")
    #             return None
    #     return None
    
    # def save_json(self, relative_path: str, data: Any, indent: int = 2, overwrite: bool = True) -> bool:
    #     """Salva dados como JSON no Lakehouse"""
    #     try:
    #         json_text = json.dumps(data, ensure_ascii=False, indent=indent)
    #         return self.save_text(relative_path, json_text, overwrite=overwrite)
    #     except Exception as e:
    #         logger.error(f"Erro ao converter dados para JSON para {relative_path}: {e}")
    #         return False
    
    # def read_csv(self, relative_path: str, **kwargs) -> Optional[pd.DataFrame]:
    #     """Lê um arquivo CSV do Lakehouse como DataFrame"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         # Usar pandas diretamente com o path do lakehouse
    #         return pd.read_csv(full_path, **kwargs)
                
    #     except Exception as e:
    #         logger.error(f"Erro ao ler CSV de {relative_path}: {e}")
    #         return None
    
    # def save_csv(self, relative_path: str, dataframe: pd.DataFrame, index: bool = False, overwrite: bool = True, **kwargs) -> bool:
    #     """Salva um DataFrame como CSV no Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         if not overwrite and self._file_exists(full_path):
    #             logger.warning(f"Arquivo já existe e overwrite=False: {relative_path}")
    #             return False
            
    #         # Garantir que o diretório existe
    #         self._ensure_directory(full_path)
            
    #         # Salvar CSV diretamente
    #         dataframe.to_csv(full_path, index=index, **kwargs)
            
    #         logger.info(f"CSV salvo em: {relative_path}")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"Erro ao salvar CSV em {relative_path}: {e}")
    #         return False
    
    # def read_parquet(self, relative_path: str, **kwargs) -> Optional[pd.DataFrame]:
    #     """Lê um arquivo Parquet do Lakehouse como DataFrame"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         return pd.read_parquet(full_path, **kwargs)
    #     except Exception as e:
    #         logger.error(f"Erro ao ler Parquet de {relative_path}: {e}")
    #         return None
    
    # def save_parquet(self, relative_path: str, dataframe: pd.DataFrame, overwrite: bool = True, **kwargs) -> bool:
    #     """Salva um DataFrame como Parquet no Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         if not overwrite and self._file_exists(full_path):
    #             logger.warning(f"Arquivo já existe e overwrite=False: {relative_path}")
    #             return False
            
    #         # Garantir que o diretório existe
    #         self._ensure_directory(full_path)
            
    #         # Salvar Parquet diretamente
    #         dataframe.to_parquet(full_path, **kwargs)
            
    #         logger.info(f"Parquet salvo em: {relative_path}")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"Erro ao salvar Parquet em {relative_path}: {e}")
    #         return False
    
    # def list_directory(self, relative_path: str = "") -> Optional[list]:
    #     """Lista o conteúdo de um diretório no Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         files = mssparkutils.fs.ls(full_path)
    #         # Converter para formato similar ao HDFS
    #         result = []
    #         for file_info in files:
    #             result.append({
    #                 'pathSuffix': file_info.name,
    #                 'type': 'DIRECTORY' if file_info.isDir else 'FILE',
    #                 'length': file_info.size,
    #                 'modificationTime': file_info.modifyTime
    #             })
    #         return result
                
    #     except Exception as e:
    #         if "Path does not exist" in str(e) or "No such file or directory" in str(e):
    #             logger.info(f"Diretório não encontrado: {relative_path}")
    #             return None
    #         else:
    #             logger.error(f"Erro ao listar diretório {relative_path}: {e}")
    #             return None
    
    # def delete_recursive(self, relative_path: str, retries: int = 3) -> bool:
    #     """Deleta um diretório e todo seu conteúdo recursivamente"""
    #     try:
    #         # Primeiro, listar o conteúdo do diretório
    #         contents = self.list_directory(relative_path)
            
    #         if contents is None:
    #             # Se não conseguiu listar, tentar deletar diretamente (pode ser um arquivo)
    #             result = self.delete_file(relative_path, recursive=False, retries=retries)
    #             return result is True
            
    #         # Deletar cada item no diretório
    #         for item in contents:
    #             item_name = item.get('pathSuffix', '')
    #             item_type = item.get('type', 'FILE')
    #             item_path = f"{relative_path.rstrip('/')}/{item_name}" if relative_path else item_name
                
    #             if item_type == 'DIRECTORY':
    #                 # Recursivamente deletar subdiretório
    #                 if not self.delete_recursive(item_path, retries):
    #                     logger.error(f"Falha ao deletar subdiretório: {item_path}")
    #                     return False
    #             else:
    #                 # Deletar arquivo
    #                 result = self.delete_file(item_path, recursive=False, retries=retries)
    #                 if result is not True:
    #                     logger.error(f"Falha ao deletar arquivo: {item_path}")
    #                     return False
            
    #         # Após deletar todo o conteúdo, deletar o diretório vazio
    #         result = self.delete_file(relative_path, recursive=False, retries=retries)
    #         return result is True
            
    #     except Exception as e:
    #         logger.error(f"Erro na deleção recursiva de {relative_path}: {e}")
    #         return False

    # def delete_file(self, relative_path: str, recursive: bool = False, retries: int = 3) -> Optional[bool]:
    #     """Deleta um arquivo ou diretório do Lakehouse"""
    #     full_path = self._get_full_path(relative_path)
        
    #     for attempt in range(retries):
    #         try:
    #             # Verificar se existe
    #             if not self._file_exists(full_path):
    #                 logger.info(f"Arquivo/diretório não encontrado: {relative_path}")
    #                 return None
                
    #             # Deletar usando mssparkutils
    #             mssparkutils.fs.rm(full_path, recurse=recursive)
                
    #             logger.info(f"Arquivo/diretório deletado com sucesso: {relative_path}")
    #             return True
                
    #         except Exception as e:
    #             logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao deletar {relative_path}: {e}")
    #             if attempt < retries - 1:
    #                 time.sleep(2)
        
    #     logger.error(f"Falha ao deletar arquivo/diretório {relative_path} após {retries} tentativas")
    #     return False

    # def delete_directory(self, relative_path: str, force_recursive: bool = True, retries: int = 3) -> bool:
    #     """
    #     Método conveniente para deletar diretórios
        
    #     Args:
    #         relative_path: Caminho relativo do diretório no Lakehouse
    #         force_recursive: Se True, usa deleção recursiva manual. Se False, tenta usar o parâmetro recursive
    #         retries: Número de tentativas
        
    #     Returns:
    #         bool: True se deletado com sucesso, False caso contrário
    #     """
    #     if force_recursive:
    #         # Usa nossa implementação recursiva manual
    #         return self.delete_recursive(relative_path, retries)
    #     else:
    #         # Tenta usar o parâmetro recursive do mssparkutils
    #         result = self.delete_file(relative_path, recursive=True, retries=retries)
    #         return result is True
        
    # def list_files(self, relative_path: str = "", file_pattern: Optional[str] = None, 
    #            max_depth: Optional[int] = None, exclude_patterns: Optional[list] = None,
    #            recursive: bool = True) -> Optional[list]:
    #     """
    #     Lista apenas os caminhos dos arquivos de um diretório
        
    #     Args:
    #         relative_path: Caminho relativo do diretório no Lakehouse
    #         file_pattern: Padrão regex para filtrar arquivos (ex: r'\.csv$' para apenas CSVs)
    #         max_depth: Profundidade máxima da busca (None = sem limite, só funciona se recursive=True)
    #         exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos (ex: ['tracking', r'\.tmp$'])
    #         recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
    #     Returns:
    #         list: Lista simples com os caminhos relativos dos arquivos encontrados
    #             Retorna None em caso de erro
    #     """
    #     try:
    #         def _list_recursive(path: str, current_depth: int = 0) -> list:
    #             file_paths = []
                
    #             # Verificar limite de profundidade (apenas se recursivo)
    #             if recursive and max_depth is not None and current_depth > max_depth:
    #                 return file_paths
                
    #             # Listar conteúdo do diretório atual
    #             contents = self.list_directory(path)
                
    #             if contents is None:
    #                 logger.warning(f"Não foi possível listar diretório: {path}")
    #                 return file_paths
                
    #             for item in contents:
    #                 item_name = item.get('pathSuffix', '')
    #                 item_type = item.get('type', 'FILE')
    #                 item_path = f"{path.rstrip('/')}/{item_name}" if path else item_name
                    
    #                 # Verificar se deve ser excluído
    #                 should_exclude = False
    #                 if exclude_patterns:
    #                     for exclude_pattern in exclude_patterns:
    #                         if re.search(exclude_pattern, item_name):
    #                             should_exclude = True
    #                             logger.debug(f"Excluindo {item_name} (padrão: {exclude_pattern})")
    #                             break
                    
    #                 if should_exclude:
    #                     continue
                    
    #                 if item_type == 'DIRECTORY':
    #                     # Recursivamente listar subdiretório apenas se recursive=True
    #                     if recursive:
    #                         try:
    #                             subfiles = _list_recursive(item_path, current_depth + 1)
    #                             file_paths.extend(subfiles)
    #                         except Exception as e:
    #                             logger.error(f"Erro ao listar subdiretório {item_path}: {e}")
    #                             continue
                            
    #                 else:  # É um arquivo
    #                     # Aplicar filtro de padrão se fornecido
    #                     if file_pattern is None or re.search(file_pattern, item_name):
    #                         file_paths.append(item_path)
                
    #             return file_paths
            
    #         # Verificar se o caminho inicial existe
    #         initial_contents = self.list_directory(relative_path)
    #         if initial_contents is None:
    #             logger.error(f"Diretório não encontrado ou inacessível: {relative_path}")
    #             return None
            
    #         # Executar busca
    #         all_files = _list_recursive(relative_path)
            
    #         search_type = "recursiva" if recursive else "não-recursiva"
    #         logger.info(f"Encontrados {len(all_files)} arquivos em {relative_path or '/'} (busca {search_type})")
    #         return all_files
            
    #     except Exception as e:
    #         logger.error(f"Erro na listagem de {relative_path}: {e}")
    #         return None

    # def list_files_detailed(self, relative_path: str = "", file_pattern: Optional[str] = None, 
    #                     include_directories: bool = False, max_depth: Optional[int] = None,
    #                     exclude_patterns: Optional[list] = None, recursive: bool = True) -> Optional[list]:
    #     """
    #     Lista todos os arquivos de um diretório com informações detalhadas
        
    #     Args:
    #         relative_path: Caminho relativo do diretório no Lakehouse
    #         file_pattern: Padrão regex para filtrar arquivos (ex: r'\.csv$' para apenas CSVs)
    #         include_directories: Se True, inclui diretórios na listagem
    #         max_depth: Profundidade máxima da busca (None = sem limite, só funciona se recursive=True)
    #         exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos (ex: ['tracking', r'\.tmp$'])
    #         recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
    #     Returns:
    #         list: Lista de dicionários com informações detalhadas dos arquivos encontrados
    #             Cada item contém: path, name, type, size, modification_time, permissions, etc.
    #             Retorna None em caso de erro
    #     """
    #     try:
    #         def _list_recursive(path: str, current_depth: int = 0) -> list:
    #             files = []
                
    #             # Verificar limite de profundidade (apenas se recursivo)
    #             if recursive and max_depth is not None and current_depth > max_depth:
    #                 return files
                
    #             # Listar conteúdo do diretório atual
    #             contents = self.list_directory(path)
                
    #             if contents is None:
    #                 logger.warning(f"Não foi possível listar diretório: {path}")
    #                 return files
                
    #             for item in contents:
    #                 item_name = item.get('pathSuffix', '')
    #                 item_type = item.get('type', 'FILE')
    #                 item_path = f"{path.rstrip('/')}/{item_name}" if path else item_name
                    
    #                 # Verificar se deve ser excluído
    #                 should_exclude = False
    #                 if exclude_patterns:
    #                     for exclude_pattern in exclude_patterns:
    #                         if re.search(exclude_pattern, item_name):
    #                             should_exclude = True
    #                             logger.debug(f"Excluindo {item_name} (padrão: {exclude_pattern})")
    #                             break
                    
    #                 if should_exclude:
    #                     continue
                    
    #                 # Adicionar informações extras ao item
    #                 enhanced_item = item.copy()
    #                 enhanced_item['full_path'] = item_path
    #                 enhanced_item['depth'] = current_depth
                    
    #                 if item_type == 'DIRECTORY':
    #                     # Incluir diretório se solicitado
    #                     if include_directories:
    #                         # Aplicar filtro de padrão se fornecido
    #                         if file_pattern is None or re.search(file_pattern, item_name):
    #                             files.append(enhanced_item)
                        
    #                     # Recursivamente listar subdiretório apenas se recursive=True
    #                     if recursive:
    #                         try:
    #                             subfiles = _list_recursive(item_path, current_depth + 1)
    #                             files.extend(subfiles)
    #                         except Exception as e:
    #                             logger.error(f"Erro ao listar subdiretório {item_path}: {e}")
    #                             continue
                            
    #                 else:  # É um arquivo
    #                     # Aplicar filtro de padrão se fornecido
    #                     if file_pattern is None or re.search(file_pattern, item_name):
    #                         files.append(enhanced_item)
                
    #             return files
            
    #         # Verificar se o caminho inicial existe
    #         initial_contents = self.list_directory(relative_path)
    #         if initial_contents is None:
    #             logger.error(f"Diretório não encontrado ou inacessível: {relative_path}")
    #             return None
            
    #         # Executar busca
    #         all_files = _list_recursive(relative_path)
            
    #         search_type = "recursiva" if recursive else "não-recursiva"
    #         logger.info(f"Encontrados {len(all_files)} itens em {relative_path or '/'} (busca {search_type})")
    #         return all_files
            
    #     except Exception as e:
    #         logger.error(f"Erro na listagem detalhada de {relative_path}: {e}")
    #         return None

    # def get_file_tree(self, relative_path: str = "", max_depth: Optional[int] = 0, 
    #                 exclude_patterns: Optional[list] = None) -> Optional[dict]:
    #     """
    #     Retorna a estrutura de arquivos em formato de árvore (similar ao comando 'ls')
        
    #     Args:
    #         relative_path: Caminho relativo do diretório no Lakehouse
    #         max_depth: Profundidade máxima da busca:
    #                 - 0 = apenas o diretório atual (como 'ls')
    #                 - 1 = atual + 1 nível de subdiretórios (como 'ls -R' limitado)
    #                 - None = sem limite (busca completa)
    #         exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos
        
    #     Returns:
    #         dict: Estrutura hierárquica dos arquivos e diretórios
    #     """
    #     try:
    #         def _build_tree(path: str, current_depth: int = 0) -> dict:
    #             # Verificar limite de profundidade
    #             if max_depth is not None and current_depth > max_depth:
    #                 return {}
                
    #             contents = self.list_directory(path)
    #             if contents is None:
    #                 return {}
                
    #             tree = {}
                
    #             for item in contents:
    #                 item_name = item.get('pathSuffix', '')
    #                 item_type = item.get('type', 'FILE')
    #                 item_path = f"{path.rstrip('/')}/{item_name}" if path else item_name
                    
    #                 # Verificar se deve ser excluído
    #                 should_exclude = False
    #                 if exclude_patterns:
    #                     for exclude_pattern in exclude_patterns:
    #                         if re.search(exclude_pattern, item_name):
    #                             should_exclude = True
    #                             break
                    
    #                 if should_exclude:
    #                     continue
                    
    #                 if item_type == 'DIRECTORY':
    #                     # Para diretórios, sempre adicionar a entrada, mas só expandir se tiver profundidade
    #                     if max_depth is None or current_depth < max_depth:
    #                         # Expandir subdiretório
    #                         subtree = _build_tree(item_path, current_depth + 1)
    #                         tree[item_name] = {
    #                             'type': 'DIRECTORY',
    #                             'info': item,
    #                             'children': subtree
    #                         }
    #                     else:
    #                         # Apenas mostrar que é um diretório, sem expandir
    #                         tree[item_name] = {
    #                             'type': 'DIRECTORY',
    #                             'info': item,
    #                             'children': {}  # Vazio indica que não foi expandido
    #                         }
    #                 else:
    #                     # Adicionar arquivo à árvore
    #                     tree[item_name] = {
    #                         'type': 'FILE',
    #                         'info': item
    #                     }
                
    #             return tree
            
    #         return _build_tree(relative_path)
            
    #     except Exception as e:
    #         logger.error(f"Erro ao construir árvore de arquivos para {relative_path}: {e}")
    #         return None

    # def find_files(self, relative_path: str = "", filename_pattern: str = ".*", case_sensitive: bool = True,
    #             exclude_patterns: Optional[list] = None, recursive: bool = True) -> Optional[list]:
    #     """
    #     Busca arquivos por padrão no nome
        
    #     Args:
    #         relative_path: Diretório raiz relativo para busca
    #         filename_pattern: Padrão regex para buscar no nome do arquivo
    #         case_sensitive: Se a busca deve ser case-sensitive
    #         exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos
    #         recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
    #     Returns:
    #         list: Lista de caminhos relativos dos arquivos encontrados
    #     """
    #     try:
    #         flags = 0 if case_sensitive else re.IGNORECASE
    #         pattern = re.compile(filename_pattern, flags)
            
    #         all_files = self.list_files(
    #             relative_path,
    #             exclude_patterns=exclude_patterns,
    #             recursive=recursive
    #         )
            
    #         if all_files is None:
    #             return None
            
    #         matching_files = []
    #         for file_path in all_files:
    #             filename = file_path.split('/')[-1]  # Extrair apenas o nome do arquivo
    #             if pattern.search(filename):
    #                 matching_files.append(file_path)
            
    #         search_type = "recursiva" if recursive else "não-recursiva"
    #         logger.info(f"Encontrados {len(matching_files)} arquivos matching '{filename_pattern}' (busca {search_type})")
    #         return matching_files
            
    #     except Exception as e:
    #         logger.error(f"Erro na busca de arquivos em {relative_path}: {e}")
    #         return None

    # def copy_file(self, source_path: str, dest_path: str, overwrite: bool = True) -> bool:
    #     """Copia um arquivo dentro do Lakehouse"""
    #     full_source = self._get_full_path(source_path)
    #     full_dest = self._get_full_path(dest_path)
        
    #     try:
    #         if not overwrite and self._file_exists(full_dest):
    #             logger.warning(f"Arquivo destino já existe e overwrite=False: {dest_path}")
    #             return False
            
    #         self._ensure_directory(full_dest)
    #         mssparkutils.fs.cp(full_source, full_dest, recurse=False)
            
    #         logger.info(f"Arquivo copiado de {source_path} para {dest_path}")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"Erro ao copiar arquivo de {source_path} para {dest_path}: {e}")
    #         return False
    
    # def move_file(self, source_path: str, dest_path: str, overwrite: bool = True) -> bool:
    #     """Move um arquivo dentro do Lakehouse"""
    #     if self.copy_file(source_path, dest_path, overwrite):
    #         if self.delete_file(source_path):
    #             logger.info(f"Arquivo movido de {source_path} para {dest_path}")
    #             return True
    #         else:
    #             logger.error(f"Falha ao deletar arquivo origem após cópia: {source_path}")
    #             return False
    #     return False
    
    # def get_file_info(self, relative_path: str) -> Optional[dict]:
    #     """Obtém informações detalhadas de um arquivo"""
    #     full_path = self._get_full_path(relative_path)
        
    #     try:
    #         parent_dir = os.path.dirname(full_path)
    #         filename = os.path.basename(full_path)
            
    #         files = mssparkutils.fs.ls(parent_dir)
    #         for file_info in files:
    #             if file_info.name == filename:
    #                 return {
    #                     'name': file_info.name,
    #                     'path': full_path,
    #                     'relative_path': relative_path,
    #                     'size': file_info.size,
    #                     'isDir': file_info.isDir,
    #                     'modifyTime': file_info.modifyTime
    #                 }
            
    #         return None
            
    #     except Exception as e:
    #         logger.error(f"Erro ao obter informações de {relative_path}: {e}")
    #         return None