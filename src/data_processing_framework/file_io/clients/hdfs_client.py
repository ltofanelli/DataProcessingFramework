import requests
import re
import logging
import time
from typing import Union, Optional, Dict, Any
from urllib.parse import urlparse, urlunparse
from .base import BaseIOClient

logger = logging.getLogger(__name__)

class HDFSClient(BaseIOClient):
    """Cliente para operações no HDFS via WebHDFS, com suporte a mapeamento de DataNodes."""

    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        """
        Args:
            credentials: dicionário com as credenciais necessárias.
                Exemplo:
                {
                    "host": "172.26.13.147",          # NameNode
                    "port": 9870,
                    "user": "hadoop.usr",
                    "datanodes": {                     # Mapeamento hostname -> IP + porta
                        "archdata-p01.intradesk": {"host": "172.26.13.147", "port": 50075},
                        "archdata-p02.intradesk": {"host": "172.26.13.148", "port": 50075},
                    }
                }
        """
        credentials = credentials or {}
        self.host = credentials.get("host", "namenode")
        self.port = credentials.get("port", 9870)
        self.user = next(
            (credentials[k] for k in ("login", "user", "username") if k in credentials),
            "hdfs"
        )
        self.datanodes = credentials.get("datanodes", {})
        self.base_url = f"http://{self.host}:{self.port}/webhdfs/v1"
        logger.info("HDFSClient inicializado")

    def _fix_datanode_url(self, url: str) -> str:
        """Substitui hostname/porta do DataNode pelo IP/porta configurados em credentials."""
        if not url:
            return url

        parsed = urlparse(url)
        hostname = parsed.hostname

        if hostname in self.datanodes:
            dn_info = self.datanodes[hostname]
            new_host = dn_info.get("host", hostname)
            new_port = dn_info.get("port", parsed.port)
            new_netloc = f"{new_host}:{new_port}"
            return urlunparse(parsed._replace(netloc=new_netloc))
        else:
            # fallback: mantém a URL original
            return url

    def read_file(self, hdfs_path: str, retries: int = 3) -> Optional[bytes]:
        """Lê um arquivo do HDFS via WebHDFS"""
        for attempt in range(retries):
            try:
                read_url = f"{self.base_url}{hdfs_path}?op=OPEN&user.name={self.user}"
                response = requests.get(read_url, allow_redirects=False, timeout=30)

                if response.status_code == 200:
                    return response.content
                elif response.status_code == 404:
                    logger.info(f"Arquivo não encontrado: {hdfs_path}")
                    return None
                elif response.status_code in (307, 302):
                    redirect_url = self._fix_datanode_url(response.headers.get('Location'))
                    data_response = requests.get(redirect_url, timeout=45)
                    if data_response.status_code == 200:
                        return data_response.content
                    else:
                        logger.error(f"Erro ao ler do DataNode (tentativa {attempt + 1}/{retries}): {data_response.status_code}")
                else:
                    logger.error(f"Erro ao acessar arquivo {hdfs_path} (tentativa {attempt + 1}/{retries}): {response.status_code}")

                if attempt < retries - 1:
                    time.sleep(3)
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao ler {hdfs_path}: {e}")
                if attempt < retries - 1:
                    time.sleep(3)

        logger.error(f"Falha ao ler arquivo {hdfs_path} após {retries} tentativas")
        return None

    def save_file(self, hdfs_path: str, content: Union[str, bytes], overwrite: bool = True, retries: int = 3) -> bool:
        """Salva um arquivo no HDFS via WebHDFS"""
        for attempt in range(retries):
            try:
                content_bytes = content.encode('utf-8') if isinstance(content, str) else content
                create_url = f"{self.base_url}{hdfs_path}?op=CREATE&overwrite={str(overwrite).lower()}&user.name={self.user}&permission=777"
                response = requests.put(create_url, allow_redirects=False, timeout=15)

                if response.status_code in (307, 302):
                    redirect_url = self._fix_datanode_url(response.headers.get('Location'))
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
                        logger.error(f"Erro ao escrever arquivo {hdfs_path} (tentativa {attempt + 1}/{retries})")
                else:
                    logger.error(f"Erro ao criar arquivo {hdfs_path} (tentativa {attempt + 1}/{retries}): {response.status_code}")

                if attempt < retries - 1:
                    time.sleep(5)
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao salvar {hdfs_path}: {e}")
                if attempt < retries - 1:
                    time.sleep(5)

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
                logger.error(f"Erro ao listar diretório {hdfs_path}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Erro ao listar diretório {hdfs_path}: {e}")
            return None
    
    def list_files(self, hdfs_path: str, file_pattern: Optional[str] = None, 
               max_depth: Optional[int] = None, exclude_patterns: Optional[list] = None,
               recursive: bool = True) -> Optional[list]:
        """Implementação específica para HDFS"""
        try:
            def _list_recursive(path: str, current_depth: int = 0) -> list:
                file_paths = []
                
                if recursive and max_depth is not None and current_depth > max_depth:
                    return file_paths
                
                contents = self.list_directory(path)
                
                if contents is None:
                    logger.warning(f"Não foi possível listar diretório: {path}")
                    return file_paths
                
                for item in contents:
                    item_name = item.get('pathSuffix', '')
                    item_type = item.get('type', 'FILE')
                    item_path = f"{path.rstrip('/')}/{item_name}"
                    
                    # Verificar exclusões
                    should_exclude = False
                    if exclude_patterns:
                        for exclude_pattern in exclude_patterns:
                            if re.search(exclude_pattern, item_name):
                                should_exclude = True
                                break
                    
                    if should_exclude:
                        continue
                    
                    if item_type == 'DIRECTORY':
                        if recursive:
                            try:
                                subfiles = _list_recursive(item_path, current_depth + 1)
                                file_paths.extend(subfiles)
                            except Exception as e:
                                logger.error(f"Erro ao listar subdiretório {item_path}: {e}")
                                continue
                            
                    else:
                        if file_pattern is None or re.search(file_pattern, item_name):
                            file_paths.append(item_path)
                
                return file_paths
            
            initial_contents = self.list_directory(hdfs_path)
            if initial_contents is None:
                logger.error(f"Diretório não encontrado ou inacessível: {hdfs_path}")
                return None
            
            all_files = _list_recursive(hdfs_path)
            logger.info(f"Encontrados {len(all_files)} arquivos em {hdfs_path}")
            return all_files
            
        except Exception as e:
            logger.error(f"Erro na listagem de {hdfs_path}: {e}")
            return None
    
    # Sobrescrever file_exists para usar HEAD request mais eficiente
    def file_exists(self, path: str) -> bool:
        """Verifica se um arquivo existe usando operação GETFILESTATUS"""
        try:
            status_url = f"{self.base_url}{path}?op=GETFILESTATUS&user.name={self.user}"
            response = requests.get(status_url, timeout=15)
            return response.status_code == 200
        except Exception as e:
            raise e