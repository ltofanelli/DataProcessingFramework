import paramiko
import re
import logging
import time
import stat
from typing import Union, Optional, List, Dict, Any
from .base import BaseIOClient

logger = logging.getLogger(__name__)

class SFTPClient(BaseIOClient):
    """Cliente para operações SFTP"""

    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        """
        Args:
            credentials: dicionário com as credenciais necessárias.
                Exemplo:
                {
                    "host": "sftp.exemplo.com",
                    "port": 22,
                    "username": "usuario",
                    "password": "senha",
                    "private_key_path": "/path/to/key.pem",  # opcional
                    "passphrase": "passphrase_da_chave",     # opcional
                    "timeout": 30                            # opcional
                }
        """
        credentials = credentials or {}
        self.host = credentials.get("host")
        self.port = credentials.get("port", 22)
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        self.private_key_path = credentials.get("private_key_path")
        self.passphrase = credentials.get("passphrase")
        self.timeout = credentials.get("timeout", 30)
        
        if not self.host:
            raise ValueError("Host é obrigatório nas credenciais")
        if not self.username:
            raise ValueError("Username é obrigatório nas credenciais")
        if not self.password and not self.private_key_path:
            raise ValueError("Password ou private_key_path deve ser fornecido")
        
        self._ssh_client = None
        self._sftp_client = None
        logger.info(f"SFTPClient inicializado para {self.host}:{self.port}")

    def _get_connection(self) -> paramiko.SFTPClient:
        """Obtém ou cria uma conexão SFTP"""
        if self._sftp_client is None or self._ssh_client is None:
            try:
                # Criar cliente SSH
                self._ssh_client = paramiko.SSHClient()
                self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                
                # Preparar autenticação
                connect_kwargs = {
                    'hostname': self.host,
                    'port': self.port,
                    'username': self.username,
                    'timeout': self.timeout
                }
                
                # Autenticação por chave privada
                if self.private_key_path:
                    try:
                        # Tenta diferentes tipos de chave
                        key = None
                        for key_class in [paramiko.RSAKey, paramiko.DSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]:
                            try:
                                key = key_class.from_private_key_file(
                                    self.private_key_path, 
                                    password=self.passphrase
                                )
                                break
                            except paramiko.PasswordRequiredException:
                                if self.passphrase:
                                    continue
                                else:
                                    raise ValueError("Chave privada requer passphrase")
                            except Exception:
                                continue
                        
                        if key:
                            connect_kwargs['pkey'] = key
                        else:
                            raise ValueError("Não foi possível carregar a chave privada")
                            
                    except Exception as e:
                        logger.error(f"Erro ao carregar chave privada: {e}")
                        raise
                
                # Autenticação por senha (se não tiver chave ou como fallback)
                if self.password and 'pkey' not in connect_kwargs:
                    connect_kwargs['password'] = self.password
                
                # Conectar
                self._ssh_client.connect(**connect_kwargs)
                self._sftp_client = self._ssh_client.open_sftp()
                
                logger.info(f"Conexão SFTP estabelecida com {self.host}")
                
            except Exception as e:
                logger.error(f"Erro ao conectar SFTP: {e}")
                self._cleanup_connection()
                raise
        
        return self._sftp_client

    def _cleanup_connection(self):
        """Limpa as conexões"""
        if self._sftp_client:
            try:
                self._sftp_client.close()
            except:
                pass
            self._sftp_client = None
        
        if self._ssh_client:
            try:
                self._ssh_client.close()
            except:
                pass
            self._ssh_client = None

    def _reconnect_if_needed(self, retries: int = 3) -> paramiko.SFTPClient:
        """Reconecta se necessário"""
        for attempt in range(retries):
            try:
                sftp = self._get_connection()
                # Testa a conexão
                sftp.listdir('.')
                return sftp
            except Exception as e:
                logger.warning(f"Conexão perdida (tentativa {attempt + 1}/{retries}): {e}")
                self._cleanup_connection()
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
                else:
                    raise

    def read_file(self, remote_path: str, retries: int = 3) -> Optional[bytes]:
        """Implementação específica para SFTP"""
        for attempt in range(retries):
            try:
                sftp = self._reconnect_if_needed()
                
                with sftp.open(remote_path, 'rb') as remote_file:
                    content = remote_file.read()
                    logger.info(f"Arquivo lido com sucesso: {remote_path} ({len(content)} bytes)")
                    return content
                    
            except FileNotFoundError:
                logger.info(f"Arquivo não encontrado: {remote_path}")
                return None
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao ler {remote_path}: {e}")
                self._cleanup_connection()
                if attempt < retries - 1:
                    time.sleep(3)
        
        logger.error(f"Falha ao ler arquivo {remote_path} após {retries} tentativas")
        return None

    def save_file(self, remote_path: str, content: Union[str, bytes], overwrite: bool = True, retries: int = 3) -> bool:
        """Implementação específica para SFTP"""
        for attempt in range(retries):
            try:
                sftp = self._reconnect_if_needed()
                
                # Verifica se arquivo existe e overwrite=False
                if not overwrite and self._file_exists_sftp(sftp, remote_path):
                    logger.warning(f"Arquivo já existe e overwrite=False: {remote_path}")
                    return False
                
                # Cria diretórios pai se necessário
                remote_dir = '/'.join(remote_path.split('/')[:-1])
                if remote_dir:
                    self._ensure_remote_dir(sftp, remote_dir)
                
                # Converte string para bytes se necessário
                if isinstance(content, str):
                    content_bytes = content.encode('utf-8')
                else:
                    content_bytes = content
                
                # Escreve o arquivo
                with sftp.open(remote_path, 'wb') as remote_file:
                    remote_file.write(content_bytes)
                
                logger.info(f"Arquivo salvo com sucesso: {remote_path} ({len(content_bytes)} bytes)")
                return True
                
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}/{retries} ao salvar {remote_path}: {e}")
                self._cleanup_connection()
                if attempt < retries - 1:
                    time.sleep(3)
        
        logger.error(f"Falha ao salvar arquivo {remote_path} após {retries} tentativas")
        return False

    def _file_exists_sftp(self, sftp: paramiko.SFTPClient, path: str) -> bool:
        """Verifica se arquivo existe usando conexão SFTP existente"""
        try:
            sftp.stat(path)
            return True
        except FileNotFoundError:
            return False
        except Exception:
            return False

    def _ensure_remote_dir(self, sftp: paramiko.SFTPClient, remote_dir: str):
        """Cria diretórios remotos recursivamente se necessário"""
        if not remote_dir or remote_dir == '/':
            return
        
        try:
            sftp.stat(remote_dir)
        except FileNotFoundError:
            # Diretório não existe, cria recursivamente
            parent_dir = '/'.join(remote_dir.split('/')[:-1])
            if parent_dir:
                self._ensure_remote_dir(sftp, parent_dir)
            
            try:
                sftp.mkdir(remote_dir)
                logger.debug(f"Diretório criado: {remote_dir}")
            except Exception as e:
                logger.warning(f"Erro ao criar diretório {remote_dir}: {e}")

    def list_files(self, remote_path: str, file_pattern: Optional[str] = None, 
                   max_depth: Optional[int] = None, exclude_patterns: Optional[List[str]] = None,
                   recursive: bool = True) -> Optional[List[str]]:
        """Implementação específica para SFTP"""
        try:
            sftp = self._reconnect_if_needed()
            
            def _list_recursive(path: str, current_depth: int = 0) -> List[str]:
                file_paths = []
                
                if recursive and max_depth is not None and current_depth > max_depth:
                    return file_paths
                
                try:
                    items = sftp.listdir_attr(path)
                except Exception as e:
                    logger.warning(f"Erro ao listar diretório {path}: {e}")
                    return file_paths
                
                for item in items:
                    item_name = item.filename
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
                    
                    # Verifica se é diretório
                    if stat.S_ISDIR(item.st_mode):
                        if recursive:
                            try:
                                subfiles = _list_recursive(item_path, current_depth + 1)
                                file_paths.extend(subfiles)
                            except Exception as e:
                                logger.error(f"Erro ao listar subdiretório {item_path}: {e}")
                                continue
                    else:
                        # É um arquivo
                        if file_pattern is None or re.search(file_pattern, item_name):
                            file_paths.append(item_path)
                
                return file_paths
            
            # Verifica se o diretório existe
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                logger.error(f"Diretório não encontrado: {remote_path}")
                return None
            
            all_files = _list_recursive(remote_path)
            logger.info(f"Encontrados {len(all_files)} arquivos em {remote_path}")
            return sorted(all_files)
            
        except Exception as e:
            logger.error(f"Erro na listagem de {remote_path}: {e}")
            return None

    def file_exists(self, path: str) -> bool:
        """Verifica se um arquivo existe"""
        try:
            sftp = self._reconnect_if_needed()
            return self._file_exists_sftp(sftp, path)
        except Exception as e:
            logger.error(f"Erro ao verificar existência do arquivo {path}: {e}")
            return False

    def list_directory(self, remote_path: str) -> Optional[List[Dict[str, Any]]]:
        """Lista o conteúdo de um diretório - método específico do SFTP"""
        try:
            sftp = self._reconnect_if_needed()
            items = sftp.listdir_attr(remote_path)
            
            result = []
            for item in items:
                item_info = {
                    'name': item.filename,
                    'size': item.st_size,
                    'modified': item.st_mtime,
                    'is_directory': stat.S_ISDIR(item.st_mode),
                    'permissions': oct(item.st_mode)[-3:],
                    'mode': item.st_mode
                }
                result.append(item_info)
            
            logger.info(f"Listado diretório {remote_path}: {len(result)} itens")
            return result
            
        except FileNotFoundError:
            logger.info(f"Diretório não encontrado: {remote_path}")
            return None
        except Exception as e:
            logger.error(f"Erro ao listar diretório {remote_path}: {e}")
            return None

    def create_directory(self, remote_path: str, recursive: bool = True) -> bool:
        """Cria um diretório no servidor SFTP"""
        try:
            sftp = self._reconnect_if_needed()
            
            if recursive:
                self._ensure_remote_dir(sftp, remote_path)
            else:
                sftp.mkdir(remote_path)
            
            logger.info(f"Diretório criado: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao criar diretório {remote_path}: {e}")
            return False

    def delete_file(self, remote_path: str) -> bool:
        """Remove um arquivo do servidor SFTP"""
        try:
            sftp = self._reconnect_if_needed()
            sftp.remove(remote_path)
            logger.info(f"Arquivo removido: {remote_path}")
            return True
            
        except FileNotFoundError:
            logger.warning(f"Arquivo não encontrado para remoção: {remote_path}")
            return False
        except Exception as e:
            logger.error(f"Erro ao remover arquivo {remote_path}: {e}")
            return False

    def delete_directory(self, remote_path: str, recursive: bool = False) -> bool:
        """Remove um diretório do servidor SFTP"""
        try:
            sftp = self._reconnect_if_needed()
            
            if recursive:
                # Remove recursivamente
                self._remove_directory_recursive(sftp, remote_path)
            else:
                sftp.rmdir(remote_path)
            
            logger.info(f"Diretório removido: {remote_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao remover diretório {remote_path}: {e}")
            return False

    def _remove_directory_recursive(self, sftp: paramiko.SFTPClient, path: str):
        """Remove diretório recursivamente"""
        items = sftp.listdir_attr(path)
        
        for item in items:
            item_path = f"{path.rstrip('/')}/{item.filename}"
            
            if stat.S_ISDIR(item.st_mode):
                self._remove_directory_recursive(sftp, item_path)
            else:
                sftp.remove(item_path)
        
        sftp.rmdir(path)

    def close(self):
        """Fecha as conexões explicitamente"""
        self._cleanup_connection()
        logger.info("Conexões SFTP fechadas")

    def __enter__(self):
        """Suporte a context manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Suporte a context manager"""
        self.close()