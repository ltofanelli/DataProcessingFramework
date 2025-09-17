"""
Processador para fontes de arquivo
"""

import os
from typing import List, Union, Dict, Any
from pyspark.sql import DataFrame

from .base import BasePipeline
from data_processing_framework.config import PipelineConfig
from data_processing_framework.config.enums import FileProcessingMode
from data_processing_framework.tracking import FileProcessingTracker
from data_processing_framework.file_io import FileIOInterface
from data_processing_framework.read import Reader
from data_processing_framework.transformation import TransformationRegistry
from data_processing_framework.conformance import ConformanceFactory
from data_processing_framework.write import Writer


class FilePipeline(BasePipeline):
    """Processador para fontes de arquivo (camada Raw)"""
    def __init__(self, spark, config: PipelineConfig):
        super().__init__(spark, config)
        self.processed_count = 0
        self.total_records_processed = 0
        self.tracker = FileProcessingTracker(
            tracking_path = self.config.tracking_path,
            process_name = self.config.pipeline_name, 
            io_credentials = self.config.tracking_io_credentials,
            file_interface_type = self.config.file_interface_type
        )

    def process(self) -> None:
        try:
            self.logger.info(f"Iniciando processamento: {self.config.pipeline_name}.")

            files_to_process = self._get_files_to_process()

            if not files_to_process:
                self.logger.info("Nenhum arquivo para processar.")
                return
            
            self._log_processing_plan(files_to_process)

            iterable = self._create_iterable(files_to_process)
            for idx, value in enumerate(iterable):

                self._log_processing_progress(idx, value, iterable)

                # Read process
                df = self.read_data(value)
                record_count = df.count()

                if record_count == 0:
                    self.logger.warning(f"Arquivo vazio: {value}.")
                    continue
                
                self.logger.info(f"Lidos {record_count} registros.")

                # Transform process
                df = self.transform_data(df)
                self.logger.info(f"Transformados {df.count()} registros.")
                
                # Conformance process
                if self.config.conform_data:
                    conformance_strategy = ConformanceFactory.create_strategy(self.spark, self.config)
                    df = conformance_strategy.process(df)
                    self.logger.info(f"Conformados {df.count()} registros.")
                
                # Write process
                self.write_data(df)

                # Marca o arquivo/lote como processado
                self._update_tracking(value)

                self.processed_count += 1 if FileProcessingMode.SEQUENTIAL else len(value)
                self.total_records_processed += record_count

            # Manutenção pós-processamento (apenas uma vez no final)
            self.post_write_maintenance()
            
            self.logger.info(f"Processamento concluído: {self.processed_count} arquivos, "
              f"{self.total_records_processed} registros totais.")
            
        except Exception as e:
            self.logger.error(f"Erro no processamento: {str(e)}")
            raise

    def _get_files_to_process(self) -> List[Dict[str, Any]]:
        """Obtém lista de arquivos para processar, ordenados"""

        if self.config.source_tracking_path:
            return self.tracker.compare_with_source_tracking(
                self.config.source_tracking_path)
        
        file_io = FileIOInterface(self.config.tracking_io_credentials, self.config.file_interface_type)
        file_pattern = rf'\.{self.config.source_file_format.value}$'
        all_files_from_source_path = file_io.list_files(
            path=self.config.source_path, 
            file_pattern=file_pattern, 
            exclude_patterns=["tracking"]
        )

        unprocessed_files = self.tracker.get_unprocessed_files(all_files_from_source_path)
        return unprocessed_files
    
    def _update_tracking(self, file: Union[dict, List[Dict[str, Any]]]) -> bool:
        if isinstance(file, dict):
            self.tracker.mark_file_dict_processed(file)
        elif isinstance(file, list):
            self.tracker.mark_batch_processed(file)
        
        return True
        
    def _log_processing_plan(self, files: List[str]) -> None:
        """Log do plano de processamento"""
        if self.config.file_processing_mode == FileProcessingMode.SEQUENTIAL:
            self.logger.info(f"Plano: {len(files)} iterações (1 arquivo por vez).")
        elif self.config.file_processing_mode == FileProcessingMode.BATCH:
            self.logger.info(f"Plano: 1 iteração ({len(files)} arquivos).")
        elif self.config.file_processing_mode == FileProcessingMode.CHUNKED:
            chunk_size = self.config.chunk_size or self.config.max_files_per_chunk or 10
            num_chunks = (len(files) + chunk_size - 1) // chunk_size
            self.logger.info(f"Plano: {num_chunks} iterações (chunks de até {chunk_size} arquivos).")
        else:
            self.logger.info(f"[FileProcessor._log_processing_plan] FileProcessingMode.{self.config.file_processing_mode} não mapeado.\nMensagem de plano não gerada.")
            
    def _create_iterable(self, files: List[Dict[str, Any]]):
        """Cria iterável baseado no modo de processamento."""
        if self.config.file_processing_mode == FileProcessingMode.CHUNKED:
            chunk_size = self.config.chunk_size or self.config.max_files_per_chunk or 5

            # Se o chunk é maior que o total, processa como batch
            if chunk_size >= len(files):
                return [files]
            else:
                return [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
        elif self.config.file_processing_mode == FileProcessingMode.BATCH:
            return [files]
        else:
            return files
    
    def _log_processing_progress(self, idx, value, iterable):
        """Registra progresso do processamento."""
        if self.config.file_processing_mode == FileProcessingMode.SEQUENTIAL:
            # value é um dicionário com file_path
            file_name = os.path.basename(value.get("file_path", "arquivo_desconhecido"))
            self.logger.info(f"[{idx+1}/{len(iterable)}] Processando: {file_name}")

        elif self.config.file_processing_mode == FileProcessingMode.BATCH:
            # value é uma lista de dicionários
            self.logger.info(f"Modo BATCH: Processando {len(value)} arquivos de uma vez.")

        elif self.config.file_processing_mode == FileProcessingMode.CHUNKED:
            # value é uma lista de dicionários (chunk)
            self.logger.info(f"[Chunk {idx+1}/{len(iterable)}] Processando {len(value)} arquivos.")

        else:
            self.logger.info(f"[FileProcessor._log_processing_progress] FileProcessingMode.{self.config.file_processing_mode} não mapeado.\nMensagem de progresso não gerada.")
    
    def read_data(self, files_to_process: Union[List[Dict[str, Any]], Dict[str, Any]]) -> DataFrame:
        """Lê dados dos arquivos."""
        reader = Reader(self.spark, self.config)
        
        # Extrair file_paths dos dicionários
        if isinstance(files_to_process, dict):
            # Modo sequential - um único dicionário
            file_paths = files_to_process.get("file_path")
        else:
            # Modo batch/chunked - lista de dicionários
            file_paths = [file_info.get("file_path") for file_info in files_to_process]
        
        df = reader.read(file_paths)
        return df

    def transform_data(self, df: DataFrame) -> DataFrame:
        """Aplica transformações nos dados"""
        if df.isEmpty():
            return df
            
        # Aplicar transformações customizadas
        if self.config.custom_transformations:
            df = TransformationRegistry.apply_transformations(df, self.config.custom_transformations)
        return df
    
    def write_data(self, df: DataFrame) -> bool:
        """Chamna a classe que faz a escrita dos dados na tabela de destino"""
        writer = Writer(self.spark, self.config)
        writer.write(df)
        return True