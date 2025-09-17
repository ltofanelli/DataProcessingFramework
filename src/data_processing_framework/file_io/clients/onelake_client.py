import msal
import requests
import re
import logging
from typing import Union, Optional, List
from urllib.parse import quote
from .base import BaseIOClient

logger = logging.getLogger(__name__)

class OneLakeClient(BaseIOClient):
    """Cliente para operações de leitura e escrita no OneLake do Microsoft Fabric"""
    
    def __init__(self, credentials: dict):
        """
        Inicializa o cliente do Fabric Lakehouse
        
        Args:
            credentials (dict): Deve conter 'tenant_id', 'client_id' e 'client_secret'
        """
        required_keys = ['tenant_id', 'client_id', 'client_secret']
        for key in required_keys:
            if key not in credentials:
                raise ValueError(f"Credencial obrigatória '{key}' não encontrada")
        
        self.tenant_id = credentials['tenant_id']
        self.client_id = credentials['client_id']
        self.client_secret = credentials['client_secret']
        
        # Configuração do MSAL
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.storage_scope = "https://storage.azure.com/.default"
        
        self.app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            authority=self.authority,
            client_credential=self.client_secret,
        )
        
        self._token = None
        logger.info("FabricLakehouseClient inicializado")
    
    def _get_token(self) -> str:
        """Obtém ou renova o token de acesso"""
        if self._token is None:
            res = self.app.acquire_token_for_client(scopes=[self.storage_scope])
            if "access_token" not in res:
                raise Exception(f"Falha ao obter token: {res}")
            self._token = res["access_token"]
        return self._token
    
    def _parse_path(self, path: str) -> tuple:
        """
        Extrai workspace e o resto do caminho
        
        Args:
            path: Formato esperado: "workspace/lakehouse/Files/file_path"
        
        Returns:
            tuple: (workspace, rest_of_path)
        """
        parts = path.strip('/').split('/', 1)
        if len(parts) < 2:
            raise ValueError(f"Path deve ter formato 'workspace/lakehouse/Files/file_path', recebido: {path}")
        
        workspace = parts[0]
        rest_of_path = parts[1]
        
        return workspace, rest_of_path
    
    def _build_url(self, path: str, operation: str = "read") -> str:
        """Constrói a URL do OneLake"""
        workspace, rest_of_path = self._parse_path(path)
        encoded_rest_path = quote(rest_of_path, safe='/')
        
        if operation == "list":
            return f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{encoded_rest_path}?resource=filesystem&recursive=true"
        else:
            return f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{encoded_rest_path}"
    
    def _make_request(self, url: str, method: str = "GET", data: bytes = None, headers: dict = None) -> requests.Response:
        """Faz uma requisição HTTP com o token de autenticação"""
        token = self._get_token()
        
        request_headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/octet-stream"
        }
        
        if headers:
            request_headers.update(headers)
        
        try:
            response = requests.request(method, url, headers=request_headers, data=data, timeout=300)
            
            # Se token expirado, tenta renovar
            if response.status_code == 401:
                self._token = None
                token = self._get_token()
                request_headers["Authorization"] = f"Bearer {token}"
                response = requests.request(method, url, headers=request_headers, data=data, timeout=300)
            
            return response
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição para {url}: {e}")
            raise
    
    def read_file(self, path: str) -> Optional[bytes]:
        """Implementação específica para Fabric Lakehouse"""
        try:
            url = self._build_url(path, "read")
            response = self._make_request(url)
            
            if response.status_code == 200:
                logger.info(f"Arquivo lido com sucesso: {path}")
                return response.content
            elif response.status_code == 404:
                logger.warning(f"Arquivo não encontrado: {path}")
                return None
            else:
                logger.error(f"Erro ao ler arquivo {path}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Erro ao ler arquivo {path}: {e}")
            return None
    
    def save_file(self, path: str, content: Union[str, bytes], overwrite: bool = True) -> bool:
        """Implementação específica para Fabric Lakehouse"""
        try:
            if isinstance(content, str):
                content = content.encode('utf-8')
            
            url = self._build_url(path, "write")
            
            # Verifica se arquivo existe se overwrite=False
            if not overwrite and self.file_exists(path):
                logger.warning(f"Arquivo já existe e overwrite=False: {path}")
                return False
            
            response = self._make_request(url, method="PUT", data=content)
            
            if response.status_code in [200, 201]:
                logger.info(f"Arquivo salvo com sucesso: {path}")
                return True
            else:
                logger.error(f"Erro ao salvar arquivo {path}: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo {path}: {e}")
            return False
    
    def file_exists(self, path: str) -> bool:
        """Sobrescreve o método da classe base para usar HEAD request mais eficiente"""
        try:
            url = self._build_url(path, "read")
            response = self._make_request(url, method="HEAD")
            
            if response.status_code == 200:
                return True
            elif response.status_code == 404:
                return False
            else:
                logger.warning(f"Erro ao verificar existência do arquivo {path}: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            logger.warning(f"Erro ao verificar existência do arquivo {path}: {e}")
            return False
    
    def list_files(self, path: str, file_pattern: Optional[str] = None, 
                   max_depth: Optional[int] = None, exclude_patterns: Optional[List[str]] = None,
                   recursive: bool = True) -> Optional[List[str]]:
        """Implementação específica para Fabric Lakehouse"""
        try:
            workspace, rest_of_path = self._parse_path(path.rstrip('/') + '/')
            
            # URL da API do Data Lake Storage Gen2 para listagem
            base_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}"
            
            # Parâmetros otimizados para listagem
            params = {
                'resource': 'filesystem',
                'recursive': str(recursive).lower()
            }
            
            # Se há um diretório específico, adiciona aos parâmetros
            if rest_of_path and rest_of_path != '/':
                params['directory'] = rest_of_path.strip('/')
            
            # Monta a URL com parâmetros
            param_str = "&".join([f"{k}={quote(str(v))}" for k, v in params.items()])
            list_url = f"{base_url}?{param_str}"
            
            # Headers específicos para listagem
            headers = {
                'x-ms-version': '2020-06-12'
            }
            
            response = self._make_request(list_url, headers=headers)
            
            if response.status_code != 200:
                logger.error(f"Erro ao listar arquivos {path}: HTTP {response.status_code}")
                return None
            
            files = []
            content = response.text.strip()
            
            # Parse da resposta
            if content:
                if content.startswith('<?xml') or '<' in content:
                    files = self._parse_xml_listing(content, workspace, rest_of_path)
                else:
                    files = self._parse_text_listing(content, workspace, rest_of_path)
            
            # Aplica filtros
            filtered_files = []
            for file_path in files:
                relative_path = file_path.split('/', 1)[-1] if len(file_path.split('/')) > 1 else file_path
                
                # Filtro por padrão
                if file_pattern and not re.search(file_pattern, relative_path):
                    continue
                
                # Filtros de exclusão
                if exclude_patterns:
                    skip = False
                    for pattern in exclude_patterns:
                        if re.search(pattern, relative_path):
                            skip = True
                            break
                    if skip:
                        continue
                
                # Filtro por profundidade máxima
                if max_depth is not None and recursive:
                    files_part = relative_path.split('Files/', 1)[-1] if 'Files/' in relative_path else relative_path
                    depth = files_part.count('/')
                    if depth > max_depth:
                        continue
                
                filtered_files.append(file_path)
            
            logger.info(f"Listagem concluída: {len(filtered_files)} arquivos encontrados em {path}")
            return sorted(filtered_files)
            
        except Exception as e:
            logger.error(f"Erro ao listar arquivos {path}: {e}")
            return None
    
    def _parse_xml_listing(self, xml_content: str, workspace: str, rest_of_path: str) -> List[str]:
        """Parse de resposta XML da API de listagem"""
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_content)
            
            files = []
            
            # Busca por elementos de arquivo
            for item in root.findall('.//Name') or root.findall('.//name'):
                if item.text and not item.text.endswith('/'):  # Exclui diretórios
                    full_path = f"{workspace}/{item.text}"
                    files.append(full_path)
            
            return files
        except Exception as e:
            logger.warning(f"Erro no parse XML: {e}")
            return []
    
    def _parse_text_listing(self, content: str, workspace: str, rest_of_path: str) -> List[str]:
        """Parse de resposta em texto simples"""
        files = []
        for line in content.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and not line.endswith('/'):
                # Parse baseado no formato da linha
                if ',' in line:
                    filename = line.split(',')[0].strip()
                elif '\t' in line:
                    filename = line.split('\t')[-1].strip()
                else:
                    filename = line
                
                if filename and not filename.startswith('.'):
                    full_path = f"{workspace}/{filename}"
                    files.append(full_path)
        
        return files