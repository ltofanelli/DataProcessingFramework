import msal
import requests
import re
import logging
from typing import Union, Optional, List, Dict, Any
from urllib.parse import quote
from .base import BaseIOClient

logger = logging.getLogger(__name__)

class OneLakeClient(BaseIOClient):
    """Cliente para operações de leitura e escrita no OneLake do Microsoft Fabric"""
    
    def __init__(self,  credentials: Dict[str, Any]):
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
        elif operation == "write":
            # Para escrita, usa a API do Data Lake Storage Gen2
            return f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{encoded_rest_path}"
        else:
            return f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{encoded_rest_path}"
    
    def _make_request(self, url: str, method: str = "GET", data: bytes = None, headers: dict = None) -> requests.Response:
        """Faz uma requisição HTTP com o token de autenticação - versão corrigida"""
        token = self._get_token()
        
        # Headers base (sem Content-Type por padrão para evitar conflitos)
        request_headers = {
            "Authorization": f"Bearer {token}"
        }
        
        # Adiciona headers específicos se fornecidos
        if headers:
            request_headers.update(headers)
        
        # Log da requisição para debug
        logger.debug(f"Fazendo requisição: {method} {url}")
        logger.debug(f"Headers: {request_headers}")
        if data:
            logger.debug(f"Data size: {len(data)} bytes")
        
        try:
            response = requests.request(method, url, headers=request_headers, data=data, timeout=300)
            
            logger.debug(f"Resposta recebida: {response.status_code}")
            
            # Se token expirado, tenta renovar
            if response.status_code == 401:
                logger.info("Token expirado, renovando...")
                self._token = None
                token = self._get_token()
                request_headers["Authorization"] = f"Bearer {token}"
                response = requests.request(method, url, headers=request_headers, data=data, timeout=300)
                logger.debug(f"Resposta após renovação do token: {response.status_code}")
            
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
        """Implementação corrigida para Fabric Lakehouse usando Azure Data Lake Storage Gen2 API"""
        try:
            if isinstance(content, str):
                content = content.encode('utf-8')
            
            # Verifica se arquivo existe se overwrite=False
            if not overwrite and self.file_exists(path):
                logger.warning(f"Arquivo já existe e overwrite=False: {path}")
                return False
            
            workspace, rest_of_path = self._parse_path(path)
            base_url = f"https://onelake.dfs.fabric.microsoft.com/{workspace}/{quote(rest_of_path, safe='/')}"
            
            # Headers base para todas as operações
            base_headers = {
                "x-ms-version": "2020-06-12"
            }
            
            # ETAPA 1: Criar o arquivo (PUT com resource=file)
            create_url = f"{base_url}?resource=file"
            logger.debug(f"Criando arquivo: {create_url}")
            
            create_response = self._make_request(
                create_url, 
                method="PUT", 
                headers=base_headers
            )
            
            if create_response.status_code not in [200, 201]:
                logger.error(f"Erro ao criar arquivo {path}: HTTP {create_response.status_code}")
                logger.error(f"Resposta: {create_response.text}")
                return False
            
            logger.debug(f"Arquivo criado com sucesso: {create_response.status_code}")
            
            # ETAPA 2: Escrever conteúdo (PATCH com action=append)
            if len(content) > 0:  # Só escreve se há conteúdo
                append_url = f"{base_url}?action=append&position=0"
                append_headers = {
                    **base_headers,
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(len(content))
                }
                
                logger.debug(f"Escrevendo conteúdo: {append_url}, tamanho: {len(content)}")
                
                append_response = self._make_request(
                    append_url, 
                    method="PATCH", 
                    data=content,
                    headers=append_headers
                )
                
                if append_response.status_code not in [200, 202]:
                    logger.error(f"Erro ao escrever conteúdo {path}: HTTP {append_response.status_code}")
                    logger.error(f"Resposta: {append_response.text}")
                    return False
                
                logger.debug(f"Conteúdo escrito com sucesso: {append_response.status_code}")
            
            # ETAPA 3: Finalizar o arquivo (PATCH com action=flush)
            flush_url = f"{base_url}?action=flush&position={len(content)}"
            
            logger.debug(f"Finalizando arquivo: {flush_url}")
            
            flush_response = self._make_request(
                flush_url, 
                method="PATCH",
                headers=base_headers
            )
            
            if flush_response.status_code in [200, 201]:
                logger.info(f"Arquivo salvo com sucesso: {path}")
                return True
            else:
                logger.error(f"Erro ao finalizar arquivo {path}: HTTP {flush_response.status_code}")
                logger.error(f"Resposta: {flush_response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo {path}: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
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
            
            # Parse da resposta JSON
            if content:
                files = self._parse_json_listing(content, workspace, rest_of_path)
            
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

    def _parse_json_listing(self, content: str, workspace: str, base_path: str) -> List[str]:
        """Parse da resposta JSON da API do Fabric Lakehouse"""
        files = []
        try:
            import json
            
            # Parse do JSON
            data = json.loads(content)
            
            # Verifica se existe a chave 'paths'
            if 'paths' not in data:
                logger.warning("Resposta JSON não contém a chave 'paths'")
                return files
            
            paths = data['paths']
            logger.debug(f"Encontrados {len(paths)} items no JSON")
            
            for item in paths:
                # Pega o nome do arquivo/diretório
                name = item.get('name', '')
                
                if not name:
                    continue
                
                # Verifica se é um diretório
                is_directory = item.get('isDirectory')
                
                # Se isDirectory não existe na resposta, assumimos que é um arquivo
                # Se isDirectory existe e é "true", é um diretório - pula
                if is_directory and is_directory.lower() == 'true':
                    logger.debug(f"Pulando diretório: {name}")
                    continue
                
                # É um arquivo - adiciona à lista
                # Se o nome já inclui o workspace, usa como está
                # Senão, adiciona o workspace
                if name.startswith(workspace):
                    full_path = name
                else:
                    full_path = f"{workspace}/{name}" if not name.startswith('/') else f"{workspace}{name}"
                
                files.append(full_path)
                logger.debug(f"Arquivo encontrado: {full_path}")
            
            logger.info(f"Parse JSON concluído: {len(files)} arquivos")
            return files
            
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao fazer parse do JSON: {e}")
            logger.debug(f"Conteúdo que causou erro: {content[:500]}...")
            return []
        except Exception as e:
            logger.error(f"Erro inesperado no parse JSON: {e}")
            return []