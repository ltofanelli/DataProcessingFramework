import requests
import json
import pandas as pd
from io import BytesIO
import re
import logging
import time
from typing import Union, Optional, Dict, Any
from data_processing_framework.file_io.client.base_io_client import BaseIOClient

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HDFSClient(BaseIOClient):
    """Cliente para operações no HDFS via WebHDFS"""

    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        """
        Args:
            credentials: dicionário com as credenciais necessárias.
                Exemplo:
                {
                    "host": "meu-namenode",
                    "port": 9870,
                    "user": "airflow"
                }
        """
        credentials = credentials or {}
        self.namenode_host = credentials.get("host", "namenode")
        self.namenode_port = credentials.get("port", 9870)
        self.user = next(
            (credentials[k] for k in ("login", "user", "username") if k in credentials),
            "hdfs"
        )
        self.base_url = f"http://{self.namenode_host}:{self.namenode_port}/webhdfs/v1"
    
    def _fix_datanode_url(self, url: str): 
        """Corrige URLs de DataNode para usar localhost"""
        if not url:
            return url
        
        # Substituir 0.0.0.0:9870 por localhost:9870
        url = re.sub(r'://0\.0\.0\.0:9870', f'://{self.namenode_host}:{self.namenode_port}', url)
        
        # Substituir qualquer hostname que termine com :9870 por localhost:9870
        url = re.sub(r'://[^:/]+:9870', f'://{self.namenode_host}:{self.namenode_port}', url)
        
        return url
    
    def read_file(self, hdfs_path: str, retries: int = 3) -> Optional[bytes]:
        """Lê um arquivo do HDFS"""
        for attempt in range(retries):
            try:
                read_url = f"{self.base_url}{hdfs_path}?op=OPEN&user.name={self.user}"
                
                response = requests.get(read_url, allow_redirects=False, timeout=30)

                if response.status_code == 200:
                    return response.content
                
                elif response.status_code == 404:
                    logger.info(f"O arquivo não foi encontrado ou não existe: {hdfs_path}")
                    return None
                
                elif response.status_code == 307:
                    # Arquivo está em um DataNode, precisamos seguir o redirecionamento
                    redirect_url = response.headers.get('Location')
                    
                    # Corrigir URL de redirecionamento
                    redirect_url = self._fix_datanode_url(redirect_url)
                    
                    # Fazer a requisição para o DataNode
                    data_response = requests.get(redirect_url, timeout=45)
                    
                    if data_response.status_code == 200:
                        return data_response.content
                    else:
                        logger.error(f"Erro ao ler do DataNode (tentativa {attempt + 1}/{retries}): {data_response.status_code} - {data_response.text}")

                else:
                    print(f"status_code: {response.status_code} - reason: {response.reason}")
                    logger.error(f"Erro ao acessar arquivo {hdfs_path} (tentativa {attempt + 1}/{retries}): {response.status_code} - {response.text}")
                
                # Aguardar antes da próxima tentativa
                if attempt < retries - 1:
                    time.sleep(3)
                    
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao ler {hdfs_path}: {e}")
                if attempt < retries - 1:
                    time.sleep(3)
        
        logger.error(f"Falha ao ler arquivo {hdfs_path} após {retries} tentativas")
        return None
    
    def save_file(self, hdfs_path: str, content: Union[str, bytes], overwrite: bool = True, retries: int = 3) -> bool:
        """Salva um arquivo no HDFS"""
        for attempt in range(retries):
            try:
                # Converter string para bytes se necessário
                if isinstance(content, str):
                    content_bytes = content.encode('utf-8')
                else:
                    content_bytes = content
                
                # Primeira requisição para obter URL de redirecionamento
                create_url = f"{self.base_url}{hdfs_path}?op=CREATE&overwrite={str(overwrite).lower()}&user.name={self.user}&permission=777"
                
                response = requests.put(create_url, allow_redirects=False, timeout=15)
                
                if response.status_code == 307:
                    # Corrigir URL de redirecionamento
                    redirect_url = self._fix_datanode_url(response.headers['Location'])
                    
                    # Escrever o arquivo
                    write_response = requests.put(
                        redirect_url, 
                        data=content_bytes, 
                        headers={'Content-Type': 'application/octet-stream'},
                        timeout=45
                    )
                    
                    if write_response.status_code == 201:
                        logger.info(f"Arquivo salvo em: {hdfs_path}")
                        return True
                    else:
                        logger.error(f"Erro ao escrever arquivo {hdfs_path} (tentativa {attempt + 1}/{retries}): {write_response.status_code} - {write_response.text}")
                        
                        # Se for erro 403, pode ser problema de DataNode
                        if write_response.status_code == 403:
                            logger.warning("Erro 403 - possível problema de conectividade com DataNode")
                            
                elif response.status_code == 403:
                    error_text = response.text
                    logger.error(f"Erro 403 ao criar arquivo {hdfs_path} (tentativa {attempt + 1}/{retries}): {error_text}")
                    
                    # Verificar se é problema específico de DataNode
                    if "Failed to find datanode" in error_text:
                        logger.warning("DataNode não encontrado - aguardando e tentando novamente...")
                        if attempt < retries - 1:
                            time.sleep(10)
                            continue
                else:
                    logger.error(f"Erro ao criar arquivo {hdfs_path} (tentativa {attempt + 1}/{retries}): {response.status_code} - {response.text}")
                
                # Se chegou aqui, houve erro - aguardar antes da próxima tentativa
                if attempt < retries - 1:
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao salvar {hdfs_path}: {e}")
                if attempt < retries - 1:
                    time.sleep(5)
        
        logger.error(f"Falha ao salvar arquivo {hdfs_path} após {retries} tentativas")
        return False
    
    def read_text(self, hdfs_path: str, encoding: str = 'utf-8') -> Optional[str]:
        """Lê um arquivo de texto do HDFS"""
        content = self.read_file(hdfs_path)
        if content:
            try:
                return content.decode(encoding)
            except Exception as e:
                logger.error(f"Erro ao decodificar texto de {hdfs_path}: {e}")
                return None
        return None
    
    def save_text(self, hdfs_path: str, text: str, encoding: str = 'utf-8', overwrite: bool = True) -> bool:
        """Salva um arquivo de texto no HDFS"""
        return self.save_file(hdfs_path, text.encode(encoding), overwrite)
    
    def read_json(self, hdfs_path: str) -> Optional[Union[Dict, list]]:
        """Lê um arquivo JSON do HDFS"""
        text = self.read_text(hdfs_path)
        if text:
            try:
                return json.loads(text)
            except Exception as e:
                logger.error(f"Erro ao decodificar JSON de {hdfs_path}: {e}")
                return None
        return None
    
    def save_json(self, hdfs_path: str, data: Any, indent: int = 2, overwrite: bool = True) -> bool:
        """Salva dados como JSON no HDFS"""
        try:
            json_text = json.dumps(data, ensure_ascii=False, indent=indent)
            return self.save_text(hdfs_path, json_text, overwrite=overwrite)
        except Exception as e:
            logger.error(f"Erro ao converter dados para JSON para {hdfs_path}: {e}")
            return False
    
    def read_csv(self, hdfs_path: str, **kwargs) -> Optional[pd.DataFrame]:
        """Lê um arquivo CSV do HDFS como DataFrame"""
        content = self.read_file(hdfs_path)
        if content:
            try:
                return pd.read_csv(BytesIO(content), **kwargs)
            except Exception as e:
                logger.error(f"Erro ao ler CSV de {hdfs_path}: {e}")
                return None
        return None
    
    def save_csv(self, hdfs_path: str, dataframe: pd.DataFrame, index: bool = False, overwrite: bool = True, **kwargs) -> bool:
        """Salva um DataFrame como CSV no HDFS"""
        try:
            csv_text = dataframe.to_csv(index=index, **kwargs)
            return self.save_text(hdfs_path, csv_text, overwrite=overwrite)
        except Exception as e:
            logger.error(f"Erro ao converter DataFrame para CSV para {hdfs_path}: {e}")
            return False
    
    def list_directory(self, hdfs_path: str) -> Optional[list]:
        """Lista o conteúdo de um diretório no HDFS"""
        try:
            list_url = f"{self.base_url}{hdfs_path}?op=LISTSTATUS&user.name={self.user}"
            response = requests.get(list_url, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result.get('FileStatuses', {}).get('FileStatus', [])
            elif response.status_code == 404:
                logger.info(f"Diretório não encontrado: {hdfs_path}")
                return None
            else:
                logger.error(f"Erro ao listar diretório {hdfs_path}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Erro ao listar diretório {hdfs_path}: {e}")
            return None

    def delete_recursive(self, hdfs_path: str, retries: int = 3) -> bool:
        """Deleta um diretório e todo seu conteúdo recursivamente"""
        try:
            # Primeiro, listar o conteúdo do diretório
            contents = self.list_directory(hdfs_path)
            
            if contents is None:
                # Se não conseguiu listar, tentar deletar diretamente (pode ser um arquivo)
                result = self.delete_file(hdfs_path, recursive=False, retries=retries)
                return result is True
            
            # Deletar cada item no diretório
            for item in contents:
                item_name = item.get('pathSuffix', '')
                item_type = item.get('type', 'FILE')
                item_path = f"{hdfs_path.rstrip('/')}/{item_name}"
                
                if item_type == 'DIRECTORY':
                    # Recursivamente deletar subdiretório
                    if not self.delete_recursive(item_path, retries):
                        logger.error(f"Falha ao deletar subdiretório: {item_path}")
                        return False
                else:
                    # Deletar arquivo
                    result = self.delete_file(item_path, recursive=False, retries=retries)
                    if result is not True:
                        logger.error(f"Falha ao deletar arquivo: {item_path}")
                        return False
            
            # Após deletar todo o conteúdo, deletar o diretório vazio
            result = self.delete_file(hdfs_path, recursive=False, retries=retries)
            return result is True
            
        except Exception as e:
            logger.error(f"Erro na deleção recursiva de {hdfs_path}: {e}")
            return False

    def delete_file(self, hdfs_path: str, recursive: bool = False, retries: int = 3) -> Optional[bool]:
        """Deleta um arquivo ou diretório do HDFS"""
        for attempt in range(retries):
            try:
                # URL correta para operação DELETE no WebHDFS
                delete_url = f"{self.base_url}{hdfs_path}?op=DELETE&user.name={self.user}&recursive={str(recursive).lower()}"
                
                response = requests.delete(delete_url, timeout=30)

                if response.status_code == 200:
                    # Verificar o conteúdo da resposta para confirmar se foi deletado
                    try:
                        result = response.json()
                        print(f"result: {result}")
                        if result.get('boolean'):
                            logger.info(f"Arquivo/diretório deletado com sucesso: {hdfs_path}")
                            return True
                        else:
                            logger.warning(f"Falha ao deletar {hdfs_path}: operação retornou false")
                            return False
                    except json.JSONDecodeError:
                        # Se não conseguir parsear JSON, mas status é 200, assumir sucesso
                        logger.info(f"Arquivo/diretório deletado com sucesso: {hdfs_path}")
                        return True
                
                elif response.status_code == 404:
                    logger.info(f"Arquivo/diretório não encontrado: {hdfs_path}")
                    return None
                    
                elif response.status_code == 403:
                    logger.error(f"Permissão negada para deletar {hdfs_path}")
                    return False
                    
                else:
                    logger.error(f"Erro ao deletar {hdfs_path} (tentativa {attempt + 1}/{retries}): {response.status_code} - {response.text}")
                
                # Aguardar antes da próxima tentativa
                if attempt < retries - 1:
                    time.sleep(2)
                    
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao deletar {hdfs_path}: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        
        logger.error(f"Falha ao deletar arquivo/diretório {hdfs_path} após {retries} tentativas")
        return False

    def delete_directory(self, hdfs_path: str, force_recursive: bool = True, retries: int = 3) -> bool:
        """
        Método conveniente para deletar diretórios
        
        Args:
            hdfs_path: Caminho do diretório no HDFS
            force_recursive: Se True, usa deleção recursiva manual. Se False, tenta usar o parâmetro recursive do WebHDFS
            retries: Número de tentativas
        
        Returns:
            bool: True se deletado com sucesso, False caso contrário
        """
        if force_recursive:
            # Usa nossa implementação recursiva manual
            return self.delete_recursive(hdfs_path, retries)
        else:
            # Tenta usar o parâmetro recursive do WebHDFS (pode não funcionar em alguns clusters)
            result = self.delete_file(hdfs_path, recursive=True, retries=retries)
            return result is True
        
    def list_files(self, hdfs_path: str, file_pattern: Optional[str] = None, 
               max_depth: Optional[int] = None, exclude_patterns: Optional[list] = None,
               recursive: bool = True) -> Optional[list]:
        """
        Lista apenas os caminhos dos arquivos de um diretório
        
        Args:
            hdfs_path: Caminho do diretório no HDFS
            file_pattern: Padrão regex para filtrar arquivos (ex: r'\.csv$' para apenas CSVs)
            max_depth: Profundidade máxima da busca (None = sem limite, só funciona se recursive=True)
            exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos (ex: ['tracking', r'\.tmp$'])
            recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
        Returns:
            list: Lista simples com os caminhos completos dos arquivos encontrados
                Retorna None em caso de erro
        """
        try:
            def _list_recursive(path: str, current_depth: int = 0) -> list:
                file_paths = []
                
                # Verificar limite de profundidade (apenas se recursivo)
                if recursive and max_depth is not None and current_depth > max_depth:
                    return file_paths
                
                # Listar conteúdo do diretório atual
                contents = self.list_directory(path)
                
                if contents is None:
                    logger.warning(f"Não foi possível listar diretório: {path}")
                    return file_paths
                
                for item in contents:
                    item_name = item.get('pathSuffix', '')
                    item_type = item.get('type', 'FILE')
                    item_path = f"{path.rstrip('/')}/{item_name}"
                    
                    # Verificar se deve ser excluído
                    should_exclude = False
                    if exclude_patterns:
                        for exclude_pattern in exclude_patterns:
                            if re.search(exclude_pattern, item_name):
                                should_exclude = True
                                logger.debug(f"Excluindo {item_name} (padrão: {exclude_pattern})")
                                break
                    
                    if should_exclude:
                        continue
                    
                    if item_type == 'DIRECTORY':
                        # Recursivamente listar subdiretório apenas se recursive=True
                        if recursive:
                            try:
                                subfiles = _list_recursive(item_path, current_depth + 1)
                                file_paths.extend(subfiles)
                            except Exception as e:
                                logger.error(f"Erro ao listar subdiretório {item_path}: {e}")
                                continue
                            
                    else:  # É um arquivo
                        # Aplicar filtro de padrão se fornecido
                        if file_pattern is None or re.search(file_pattern, item_name):
                            file_paths.append(item_path)
                
                return file_paths
            
            # Verificar se o caminho inicial existe
            initial_contents = self.list_directory(hdfs_path)
            if initial_contents is None:
                logger.error(f"Diretório não encontrado ou inacessível: {hdfs_path}")
                return None
            
            # Executar busca
            all_files = _list_recursive(hdfs_path)
            
            search_type = "recursiva" if recursive else "não-recursiva"
            logger.info(f"Encontrados {len(all_files)} arquivos em {hdfs_path} (busca {search_type})")
            return all_files
            
        except Exception as e:
            logger.error(f"Erro na listagem de {hdfs_path}: {e}")
            return None

    def list_files_detailed(self, hdfs_path: str, file_pattern: Optional[str] = None, 
                        include_directories: bool = False, max_depth: Optional[int] = None,
                        exclude_patterns: Optional[list] = None, recursive: bool = True) -> Optional[list]:
        """
        Lista todos os arquivos de um diretório com informações detalhadas
        
        Args:
            hdfs_path: Caminho do diretório no HDFS
            file_pattern: Padrão regex para filtrar arquivos (ex: r'\.csv$' para apenas CSVs)
            include_directories: Se True, inclui diretórios na listagem
            max_depth: Profundidade máxima da busca (None = sem limite, só funciona se recursive=True)
            exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos (ex: ['tracking', r'\.tmp$'])
            recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
        Returns:
            list: Lista de dicionários com informações detalhadas dos arquivos encontrados
                Cada item contém: path, name, type, size, modification_time, permissions, etc.
                Retorna None em caso de erro
        """
        try:
            def _list_recursive(path: str, current_depth: int = 0) -> list:
                files = []
                
                # Verificar limite de profundidade (apenas se recursivo)
                if recursive and max_depth is not None and current_depth > max_depth:
                    return files
                
                # Listar conteúdo do diretório atual
                contents = self.list_directory(path)
                
                if contents is None:
                    logger.warning(f"Não foi possível listar diretório: {path}")
                    return files
                
                for item in contents:
                    item_name = item.get('pathSuffix', '')
                    item_type = item.get('type', 'FILE')
                    item_path = f"{path.rstrip('/')}/{item_name}"
                    
                    # Verificar se deve ser excluído
                    should_exclude = False
                    if exclude_patterns:
                        for exclude_pattern in exclude_patterns:
                            if re.search(exclude_pattern, item_name):
                                should_exclude = True
                                logger.debug(f"Excluindo {item_name} (padrão: {exclude_pattern})")
                                break
                    
                    if should_exclude:
                        continue
                    
                    # Adicionar informações extras ao item
                    enhanced_item = item.copy()
                    enhanced_item['full_path'] = item_path
                    enhanced_item['depth'] = current_depth
                    
                    if item_type == 'DIRECTORY':
                        # Incluir diretório se solicitado
                        if include_directories:
                            # Aplicar filtro de padrão se fornecido
                            if file_pattern is None or re.search(file_pattern, item_name):
                                files.append(enhanced_item)
                        
                        # Recursivamente listar subdiretório apenas se recursive=True
                        if recursive:
                            try:
                                subfiles = _list_recursive(item_path, current_depth + 1)
                                files.extend(subfiles)
                            except Exception as e:
                                logger.error(f"Erro ao listar subdiretório {item_path}: {e}")
                                continue
                            
                    else:  # É um arquivo
                        # Aplicar filtro de padrão se fornecido
                        if file_pattern is None or re.search(file_pattern, item_name):
                            files.append(enhanced_item)
                
                return files
            
            # Verificar se o caminho inicial existe
            initial_contents = self.list_directory(hdfs_path)
            if initial_contents is None:
                logger.error(f"Diretório não encontrado ou inacessível: {hdfs_path}")
                return None
            
            # Executar busca
            all_files = _list_recursive(hdfs_path)
            
            search_type = "recursiva" if recursive else "não-recursiva"
            logger.info(f"Encontrados {len(all_files)} itens em {hdfs_path} (busca {search_type})")
            return all_files
            
        except Exception as e:
            logger.error(f"Erro na listagem detalhada de {hdfs_path}: {e}")
            return None

    def get_file_tree(self, hdfs_path: str, max_depth: Optional[int] = 0, 
                    exclude_patterns: Optional[list] = None) -> Optional[dict]:
        """
        Retorna a estrutura de arquivos em formato de árvore (similar ao comando 'ls')
        
        Args:
            hdfs_path: Caminho do diretório no HDFS
            max_depth: Profundidade máxima da busca:
                    - 0 = apenas o diretório atual (como 'ls')
                    - 1 = atual + 1 nível de subdiretórios (como 'ls -R' limitado)
                    - None = sem limite (busca completa)
            exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos
        
        Returns:
            dict: Estrutura hierárquica dos arquivos e diretórios
        """
        try:
            def _build_tree(path: str, current_depth: int = 0) -> dict:
                # Verificar limite de profundidade
                if max_depth is not None and current_depth > max_depth:
                    return {}
                
                contents = self.list_directory(path)
                if contents is None:
                    return {}
                
                tree = {}
                
                for item in contents:
                    item_name = item.get('pathSuffix', '')
                    item_type = item.get('type', 'FILE')
                    item_path = f"{path.rstrip('/')}/{item_name}"
                    
                    # Verificar se deve ser excluído
                    should_exclude = False
                    if exclude_patterns:
                        for exclude_pattern in exclude_patterns:
                            if re.search(exclude_pattern, item_name):
                                should_exclude = True
                                break
                    
                    if should_exclude:
                        continue
                    
                    if item_type == 'DIRECTORY':
                        # Para diretórios, sempre adicionar a entrada, mas só expandir se tiver profundidade
                        if max_depth is None or current_depth < max_depth:
                            # Expandir subdiretório
                            subtree = _build_tree(item_path, current_depth + 1)
                            tree[item_name] = {
                                'type': 'DIRECTORY',
                                'info': item,
                                'children': subtree
                            }
                        else:
                            # Apenas mostrar que é um diretório, sem expandir
                            tree[item_name] = {
                                'type': 'DIRECTORY',
                                'info': item,
                                'children': {}  # Vazio indica que não foi expandido
                            }
                    else:
                        # Adicionar arquivo à árvore
                        tree[item_name] = {
                            'type': 'FILE',
                            'info': item
                        }
                
                return tree
            
            return _build_tree(hdfs_path)
            
        except Exception as e:
            logger.error(f"Erro ao construir árvore de arquivos para {hdfs_path}: {e}")
            return None

    def find_files(self, hdfs_path: str, filename_pattern: str, case_sensitive: bool = True,
                exclude_patterns: Optional[list] = None, recursive: bool = True) -> Optional[list]:
        """
        Busca arquivos por padrão no nome
        
        Args:
            hdfs_path: Diretório raiz para busca
            filename_pattern: Padrão regex para buscar no nome do arquivo
            case_sensitive: Se a busca deve ser case-sensitive
            exclude_patterns: Lista de padrões regex para excluir diretórios/arquivos
            recursive: Se True, busca recursivamente em subdiretórios. Se False, apenas no diretório atual
        
        Returns:
            list: Lista de caminhos completos dos arquivos encontrados
        """
        try:
            flags = 0 if case_sensitive else re.IGNORECASE
            pattern = re.compile(filename_pattern, flags)
            
            all_files = self.list_files(
                hdfs_path,
                exclude_patterns=exclude_patterns,
                recursive=recursive
            )
            
            if all_files is None:
                return None
            
            matching_files = []
            for file_path in all_files:
                filename = file_path.split('/')[-1]  # Extrair apenas o nome do arquivo
                if pattern.search(filename):
                    matching_files.append(file_path)
            
            search_type = "recursiva" if recursive else "não-recursiva"
            logger.info(f"Encontrados {len(matching_files)} arquivos matching '{filename_pattern}' (busca {search_type})")
            return matching_files
            
        except Exception as e:
            logger.error(f"Erro na busca de arquivos em {hdfs_path}: {e}")
            return None