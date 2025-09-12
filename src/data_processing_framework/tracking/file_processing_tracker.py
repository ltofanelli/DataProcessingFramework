import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from data_processing_framework.config import FileInterfaceType
from data_processing_framework.file_io.file_io_interface import FileIOInterface

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FileProcessingTracker:
    """
    Classe para rastreamento de arquivos processados, similar ao checkpoint do Spark Streaming.
    Mantém registro dos arquivos já processados em um arquivo JSON persistente.
    """
    
    def __init__(
        self, 
        tracking_path: str, 
        process_name: str = "default_process",
        file_interface_type: FileInterfaceType = FileInterfaceType.HDFS,
        auto_create: bool = True
    ):
        """
        Inicializa o tracker de arquivos processados.
        
        Args:
            tracking_path (str): Caminho do arquivo JSON de tracking
            process_name (str): Nome ou ID do processo (usado no process_run)
            file_interface_type: tipo de interface de arquivo
            auto_create (bool): Se True, cria arquivo de tracking se não existir
        """
        self.file_io = FileIOInterface(file_interface_type)
        self.process_name = process_name
        self.file_name = "file.json"
        self.tracking_path = self._validate_tracking_path(tracking_path)
        self.tracking_data = self._initialize_tracking(auto_create)
        
        logger.info(f"FileProcessingTracker inicializado: {self.tracking_path}")
        logger.info(f"Total de arquivos rastreados: {self.tracking_data.get('total_files', 0)}")

    def _validate_tracking_path(self, tracking_path) -> str:
        if tracking_path:
            return tracking_path + "/" + self.file_name
        raise ValueError("'PipelineConfig.tracking_path' não pode ser Nulo")
    
    def _initialize_tracking(self, auto_create: bool) -> Dict[str, Any]:
        """
        Inicializa ou carrega o arquivo de tracking.
        
        Args:
            auto_create (bool): Se True, cria arquivo vazio se não existir
            
        Returns:
            Dict com dados de tracking
        """
        try:
            data = self.file_io.read_json(self.tracking_path)
            if data is None:
                if auto_create:
                    logger.info(f"Criando novo arquivo de tracking: {self.tracking_path}")
                    return self._create_empty_tracking()
                else:
                    raise FileNotFoundError(f"Arquivo de tracking não encontrado: {self.tracking_path}")
            
            # Validar estrutura do JSON
            self._validate_tracking_structure(data)
            return data
            
        except Exception as e:
            logger.error(f"Erro ao inicializar tracking: {e}")
            if auto_create:
                logger.info("Criando novo arquivo de tracking devido a erro")
                return self._create_empty_tracking()
            raise

    def _create_empty_tracking(self) -> Dict[str, Any]:
        """Cria estrutura vazia de tracking."""
        empty_data = {
            "files": [],
            "last_file": None,
            "last_incremental_datetime": None,
            "last_record_datetime": None,
            "total_files": 0,
            "metadata": {
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "process_name": self.process_name
            }
        }
        
        # Salvar arquivo vazio
        self._save_tracking(empty_data)
        return empty_data
    
    def _validate_tracking_structure(self, data: Dict[str, Any]) -> None:
        """
        Valida a estrutura do JSON de tracking.
        
        Args:
            data (Dict): Dados do tracking para validar
            
        Raises:
            ValueError: Se estrutura estiver incorreta
        """
        required_keys = ["files", "total_files"]
        for key in required_keys:
            if key not in data:
                raise ValueError(f"Chave obrigatória '{key}' ausente no arquivo de tracking")
        
        if not isinstance(data["files"], list):
            raise ValueError("'files' deve ser uma lista")
    
    def _save_tracking(self, data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Salva dados de tracking no arquivo JSON.
        
        Args:
            data (Dict): Dados para salvar (se None, usa self.tracking_data)
            
        Returns:
            bool: True se salvou com sucesso
        """
        if data is None:
            data = self.tracking_data
        
        # Atualizar timestamp de última modificação
        if "metadata" not in data:
            data["metadata"] = {}
        data["metadata"]["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            success = self.file_io.save_json(self.tracking_path, data, indent=2)
            if success:
                logger.debug(f"Tracking salvo com sucesso: {self.tracking_path}")
            return success
        except Exception as e:
            logger.error(f"Erro ao salvar tracking: {e}")
            return False
    
    def get_unprocessed_files(self, file_paths: List[str]) -> List[Dict[str, Any]]:
        """
        Retorna lista de arquivos que ainda não foram processados.
        
        Args:
            file_paths (List[str]): Lista de caminhos de arquivos para verificar
            
        Returns:
            List[Dict[str, Any]]: Lista com informações dos arquivos não processados
                                no formato padronizado: [{"file_path": str, "incremental_datetime": str, "record_datetime": str}, ...]
        """
        if not file_paths:
            return []
        
        # Criar set com arquivos já processados para busca O(1)
        processed_files = {
            record["file_path"] 
            for record in self.tracking_data.get("files", [])
        }
        
        # Filtrar apenas arquivos não processados e criar estrutura padronizada
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        unprocessed = []
        
        for file_path in file_paths:
            if file_path not in processed_files:
                unprocessed.append({
                    "file_path": file_path,
                    "incremental_datetime": current_datetime,
                    "record_datetime": current_datetime
                })
        
        logger.info(f"Total de arquivos verificados: {len(file_paths)}")
        logger.info(f"Arquivos não processados: {len(unprocessed)}")
        
        return unprocessed


    def mark_file_processed(self, file_path: str, incremental_datetime: Optional[str], process_run: Optional[str] = None) -> bool:
        """
        Marca um arquivo como processado usando apenas o caminho.
        Ideal para uso fora do framework em códigos de ingestão.
        
        Args:
            file_path (str): Caminho do arquivo processado
            process_run (str): ID do processo (opcional)
            
        Returns:
            bool: True se marcou com sucesso
        """
        try:
            if self.is_file_processed(file_path):
                logger.warning(f"Arquivo já processado: {file_path}")
                return False
            
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            file_record = {
                "file_path": file_path,
                "incremental_datetime": current_datetime,
                "record_datetime": incremental_datetime or current_datetime,
                "process_run": process_run or self.process_name
            }
            
            self.tracking_data["files"].append(file_record)
            
            # Atualizar estatísticas
            self.tracking_data["last_file"] = file_path
            self.tracking_data["last_incremental_datetime"] = current_datetime
            self.tracking_data["last_record_datetime"] = current_datetime
            self.tracking_data["total_files"] = len(self.tracking_data["files"])
            
            success = self._save_tracking()
            if success:
                logger.info(f"Arquivo marcado como processado: {file_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"Erro ao marcar arquivo: {e}")
            return False

    def mark_file_dict_processed(self, file_dict: Dict[str, Any]) -> bool:
        """
        Marca um arquivo como processado usando dicionário completo.
        Preserva incremental_datetime, atualiza apenas record_datetime.
        
        Args:
            file_dict (Dict): {"file_path": str, "incremental_datetime": str, 
                            "record_datetime": str, "process_run": str}
            
        Returns:
            bool: True se marcou com sucesso
        """
        try:
            file_path = file_dict.get("file_path")
            if not file_path:
                logger.error("Dicionário deve conter 'file_path'")
                return False
                
            if self.is_file_processed(file_path):
                logger.warning(f"Arquivo já processado: {file_path}")
                return False
            
            current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            file_record = {
                "file_path": file_path,
                "incremental_datetime": file_dict.get("incremental_datetime", current_datetime),
                "record_datetime": current_datetime,  # Sempre atualizado
                "process_run": file_dict.get("process_run") or self.process_name
            }
            
            self.tracking_data["files"].append(file_record)
            
            # Atualizar estatísticas
            self.tracking_data["last_file"] = file_path
            self.tracking_data["last_incremental_datetime"] = file_record["incremental_datetime"]
            self.tracking_data["last_record_datetime"] = current_datetime
            self.tracking_data["total_files"] = len(self.tracking_data["files"])
            
            success = self._save_tracking()
            if success:
                logger.info(f"Arquivo marcado como processado: {file_path}")
            
            return success
            
        except Exception as e:
            logger.error(f"Erro ao marcar arquivo: {e}")
            return False

    def mark_batch_processed(self, batch_data: Union[List[str], List[Dict[str, Any]]], process_run: Optional[str] = None) -> Dict[str, bool]:
        """
        Marca múltiplos arquivos como processados em lote.
        Aceita lista de strings (paths) ou lista de dicionários.
        
        Args:
            batch_data: Lista de strings (paths) ou lista de dicionários
            process_run (str): ID do processo (aplicado apenas para strings)
            
        Returns:
            Dict[str, bool]: Mapa de arquivo -> sucesso
        """
        if not batch_data:
            return {}
        
        results = {}
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Detectar tipo da lista
        is_string_list = isinstance(batch_data[0], str)
        
        for item in batch_data:
            if is_string_list:
                # Lista de strings (paths)
                file_path = item
                if self.is_file_processed(file_path):
                    logger.warning(f"Arquivo já processado: {file_path}")
                    results[file_path] = False
                    continue
                    
                file_record = {
                    "file_path": file_path,
                    "incremental_datetime": current_datetime,
                    "record_datetime": current_datetime,
                    "process_run": process_run or self.process_name
                }
            else:
                # Lista de dicionários
                file_path = item.get("file_path")
                if not file_path:
                    logger.warning(f"Dicionário sem file_path ignorado: {item}")
                    results["arquivo_sem_path"] = False
                    continue
                    
                if self.is_file_processed(file_path):
                    logger.warning(f"Arquivo já processado: {file_path}")
                    results[file_path] = False
                    continue
                    
                file_record = {
                    "file_path": file_path,
                    "incremental_datetime": item.get("incremental_datetime", current_datetime),
                    "record_datetime": current_datetime,  # Sempre atualizado
                    "process_run": item.get("process_run") or self.process_name
                }
            
            self.tracking_data["files"].append(file_record)
            results[file_path] = True
        
        # Atualizar estatísticas com último arquivo processado
        successful_files = [fp for fp, success in results.items() if success and fp != "arquivo_sem_path"]
        
        if successful_files:
            last_file = successful_files[-1]
            
            # Encontrar incremental_datetime do último arquivo
            if is_string_list:
                last_incremental = current_datetime
            else:
                last_item = next((item for item in batch_data if item.get("file_path") == last_file), {})
                last_incremental = last_item.get("incremental_datetime", current_datetime)
            
            self.tracking_data["last_file"] = last_file
            self.tracking_data["last_incremental_datetime"] = last_incremental
            self.tracking_data["last_record_datetime"] = current_datetime
            self.tracking_data["total_files"] = len(self.tracking_data["files"])
            
            # Salvar uma vez após todos os registros
            success = self._save_tracking()
            if not success:
                logger.error("Falha ao salvar tracking do lote")
                return {fp: False for fp in results.keys()}
        
        success_count = sum(1 for success in results.values() if success)
        logger.info(f"Lote processado: {success_count}/{len(batch_data)} arquivos")
        return results
    
    def is_file_processed(self, file_path: str) -> bool:
        """
        Verifica se um arquivo já foi processado.
        
        Args:
            file_path (str): Caminho do arquivo
            
        Returns:
            bool: True se já foi processado
        """
        for record in self.tracking_data.get("files", []):
            if record["file_path"] == file_path:
                return True
        return False
    
    def get_processed_files_count(self) -> int:
        """Retorna total de arquivos processados."""
        return self.tracking_data.get("total_files", 0)
    
    def get_last_processed_info(self) -> Dict[str, Any]:
        """
        Retorna informações do último arquivo processado.
        
        Returns:
            Dict com informações do último processamento
        """
        return {
            "last_file": self.tracking_data.get("last_file"),
            "last_incremental_datetime": self.tracking_data.get("last_incremental_datetime"),
            "last_record_datetime": self.tracking_data.get("last_record_datetime")
        }
    
    def get_files_by_date_range(
        self, 
        start_date: str, 
        end_date: str,
        date_field: str = "record_datetime"
    ) -> List[Dict[str, Any]]:
        """
        Retorna arquivos processados em um intervalo de datas.
        
        Args:
            start_date (str): Data inicial (formato: YYYY-MM-DD)
            end_date (str): Data final (formato: YYYY-MM-DD)
            date_field (str): Campo de data para filtrar ('record_datetime' ou 'incremental_datetime')
            
        Returns:
            List[Dict]: Lista de registros no intervalo
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            filtered_files = []
            for record in self.tracking_data.get("files", []):
                date_str = record.get(date_field, "")
                if date_str:
                    # Extrair apenas data para comparação
                    record_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                    if start_dt <= record_date <= end_dt:
                        filtered_files.append(record)
            
            return filtered_files
            
        except ValueError as e:
            logger.error(f"Erro ao filtrar por data: {e}")
            return []
    
    def remove_file_from_tracking(self, file_path: str) -> bool:
        """
        Remove um arquivo do tracking (útil para reprocessamento).
        
        Args:
            file_path (str): Caminho do arquivo
            
        Returns:
            bool: True se removeu com sucesso
        """
        initial_count = len(self.tracking_data["files"])
        
        self.tracking_data["files"] = [
            record for record in self.tracking_data["files"]
            if record["file_path"] != file_path
        ]
        
        if len(self.tracking_data["files"]) < initial_count:
            self.tracking_data["total_files"] = len(self.tracking_data["files"])
            success = self._save_tracking()
            if success:
                logger.info(f"Arquivo removido do tracking: {file_path}")
            return success
        
        logger.warning(f"Arquivo não encontrado no tracking: {file_path}")
        return False
    
    def clear_tracking(self, confirm: bool = False) -> bool:
        """
        Limpa todo o histórico de tracking.
        
        Args:
            confirm (bool): Confirmação de segurança
            
        Returns:
            bool: True se limpou com sucesso
        """
        if not confirm:
            logger.warning("Operação de limpeza não confirmada")
            return False
        
        self.tracking_data = self._create_empty_tracking()
        logger.info("Tracking limpo completamente")
        return True
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Retorna estatísticas do tracking.
        
        Returns:
            Dict com estatísticas
        """
        files = self.tracking_data.get("files", [])
        
        if not files:
            return {
                "total_files": 0,
                "first_processed": None,
                "last_processed": None,
                "process_runs": []
            }
        
        # Coletar process_runs únicos
        process_runs = list(set(f.get("process_run", "") for f in files if f.get("process_run")))
        
        return {
            "total_files": len(files),
            "first_processed": files[0].get("record_datetime") if files else None,
            "last_processed": files[-1].get("record_datetime") if files else None,
            "process_runs": process_runs,
            "metadata": self.tracking_data.get("metadata", {})
        }
    
    def reload_tracking(self) -> None:
        """Recarrega dados do arquivo de tracking."""
        self.tracking_data = self._initialize_tracking(auto_create=False)
        logger.info("Tracking recarregado do arquivo")
    
    # ============================================================================
    # MÉTODOS PARA COMPARAÇÃO COM TRACKING DE ORIGEM
    # ============================================================================
    
    def compare_with_source_tracking(
        self, 
        source_tracking_path: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        process_run_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Método principal para comparar com tracking de origem e retornar arquivos não processados.
        
        Args:
            source_tracking_path (str): Caminho do tracking de origem
            start_date (str): Data inicial para filtrar (formato: YYYY-MM-DD) - opcional
            end_date (str): Data final para filtrar (formato: YYYY-MM-DD) - opcional
            process_run_filter (str): Filtrar por process_run específico - opcional
            
        Returns:
            List[Dict[str, Any]]: Lista de arquivos não processados com informações da origem
            
        Raises:
            FileNotFoundError: Se tracking de origem não existir
            ValueError: Se tracking de origem for inválido
        """
        logger.info(f"Iniciando comparação com tracking de origem: {source_tracking_path}")
        
        # Carregar e validar tracking de origem
        source_data = self._load_tracking_from_path(source_tracking_path)
        
        # Obter arquivos não processados
        unprocessed_files = self._get_unprocessed_from_source(source_data)
        
        # Aplicar filtros se especificados
        if start_date or end_date or process_run_filter:
            unprocessed_files = self._apply_filters(
                unprocessed_files, 
                start_date, 
                end_date, 
                process_run_filter
            )
        
        logger.info(f"Comparação concluída: {len(unprocessed_files)} arquivos não processados encontrados")
        
        return unprocessed_files
    
    def _load_tracking_from_path(self, source_tracking_path: str) -> Dict[str, Any]:
        """
        Carrega e valida dados do tracking de origem.
        
        Args:
            source_tracking_path (str): Caminho do arquivo de tracking de origem
            
        Returns:
            Dict com dados do tracking de origem
            
        Raises:
            FileNotFoundError: Se arquivo não existir
            ValueError: Se estrutura for inválida
        """
        try:
            # Construir caminho completo se necessário
            if not source_tracking_path.endswith('.json'):
                full_source_path = source_tracking_path + "/" + self.file_name
            else:
                full_source_path = source_tracking_path
            
            logger.info(f"Carregando tracking de origem: {full_source_path}")
            
            # Carregar arquivo
            source_data = self.file_io.read_json(full_source_path)
            
            if source_data is None:
                raise FileNotFoundError(f"Tracking de origem não encontrado: {full_source_path}")
            
            # Validar estrutura
            self._validate_tracking_structure(source_data)
            
            logger.info(f"Tracking de origem carregado com sucesso: {source_data.get('total_files', 0)} arquivos")
            
            return source_data
            
        except Exception as e:
            if isinstance(e, (FileNotFoundError, ValueError)):
                raise
            else:
                raise ValueError(f"Erro ao carregar tracking de origem: {e}")
    
    def _get_unprocessed_from_source(self, source_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Compara dados de origem com tracking atual e retorna arquivos não processados.
        
        Args:
            source_data (Dict): Dados do tracking de origem
            
        Returns:
            List[Dict]: Lista de arquivos não processados com informações da origem
        """
        # Criar set com arquivos já processados para busca O(1)
        processed_files = {
            record["file_path"] 
            for record in self.tracking_data.get("files", [])
        }
        
        ### Com datetimes originais
        # Filtrar arquivos não processados do tracking de origem
        unprocessed_files = []
        
        for source_record in source_data.get("files", []):
            file_path = source_record.get("file_path")
            
            if file_path and file_path not in processed_files:
                unprocessed_info = {
                    "file_path": file_path,
                    "incremental_datetime": source_record.get("incremental_datetime"),
                    "source_record_datetime": source_record.get("record_datetime"),
                    "source_process_run": source_record.get("process_run")
                }
                
                ## # Incluir metadados adicionais se existirem
                ## if "metadata" in source_record:
                ##     unprocessed_info["source_metadata"] = source_record["metadata"]
                
                unprocessed_files.append(unprocessed_info)

        ### Com datetimes já usando um current
        # Filtrar arquivos não processados do tracking de origem
        # unprocessed_files = []
        # current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # for source_record in source_data.get("files", []):
        #     file_path = source_record.get("file_path")
            
        #     if file_path and file_path not in processed_files:
        #         unprocessed_info = {
        #             "file_path": file_path,
        #             "incremental_datetime": source_record.get("incremental_datetime", current_datetime),
        #             "record_datetime": current_datetime  # Sempre usar datetime atual para record_datetime
        #         }
                
        #         ## # Incluir metadados adicionais se existirem
        #         ## if "metadata" in source_record:
        #         ##     unprocessed_info["source_metadata"] = source_record["metadata"]

        #         unprocessed_files.append(unprocessed_info)

        logger.info(f"Arquivos na origem: {source_data.get('total_files', 0)}")
        logger.info(f"Arquivos já processados: {len(processed_files)}")
        logger.info(f"Arquivos não processados: {len(unprocessed_files)}")
        
        return unprocessed_files
    
    def _apply_filters(
        self,
        unprocessed_files: List[Dict[str, Any]],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        process_run_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Aplica filtros de data e process_run aos arquivos não processados.
        
        Args:
            unprocessed_files (List[Dict]): Lista de arquivos para filtrar
            start_date (str): Data inicial (YYYY-MM-DD)
            end_date (str): Data final (YYYY-MM-DD)
            process_run_filter (str): Process run para filtrar
            
        Returns:
            List[Dict]: Lista filtrada
        """
        filtered_files = unprocessed_files
        
        # Filtrar por data se especificado
        if start_date or end_date:
            filtered_files = self._filter_by_date(filtered_files, start_date, end_date)
        
        # Filtrar por process_run se especificado
        if process_run_filter:
            filtered_files = self._filter_by_process_run(filtered_files, process_run_filter)
        
        return filtered_files
    
    def _filter_by_date(
        self, 
        files: List[Dict[str, Any]], 
        start_date: Optional[str], 
        end_date: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Filtra arquivos por intervalo de datas."""
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None
            
            filtered = []
            for file_info in files:
                incremental_dt_str = file_info.get("incremental_datetime", "")
                if incremental_dt_str:
                    try:
                        file_date = datetime.strptime(incremental_dt_str[:10], "%Y-%m-%d")
                        
                        if start_dt and file_date < start_dt:
                            continue
                        if end_dt and file_date > end_dt:
                            continue
                        
                        filtered.append(file_info)
                    except ValueError:
                        logger.warning(f"Data inválida ignorada: {incremental_dt_str}")
                        continue
            
            logger.info(f"Filtro por data aplicado: {len(filtered)} de {len(files)} arquivos")
            return filtered
            
        except ValueError as e:
            logger.error(f"Erro no filtro de data: {e}")
            return files
    
    def _filter_by_process_run(
        self, 
        files: List[Dict[str, Any]], 
        process_run_filter: str
    ) -> List[Dict[str, Any]]:
        """Filtra arquivos por process_run."""
        filtered = [
            file_info for file_info in files
            if file_info.get("source_process_run") == process_run_filter
        ]
        
        logger.info(f"Filtro por process_run '{process_run_filter}' aplicado: {len(filtered)} de {len(files)} arquivos")
        return filtered