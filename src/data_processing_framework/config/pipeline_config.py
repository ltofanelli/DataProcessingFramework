"""
Configuração principal do framework
"""

from dataclasses import dataclass
from typing import List, Optional, Any
from data_processing_framework.config import SourceType, LoadType, WriteMode, FileFormat, FileProcessingMode, FileInterfaceType, SCD2Columns

@dataclass
class PipelineConfig:
    """Configuração principal do processamento de dados"""
    
    # Configurações obrigatórias
    pipeline_name: str
    source_type: SourceType
    load_type: LoadType
    write_mode: WriteMode
    target_table: str
    target_path: str
    tracking_path: str

    # Configurações de fonte
    source_path: Optional[str] = None
    source_query: Optional[str] = None
    source_file_format: FileFormat = FileFormat.PARQUET

    # Configurações do tracking 
    tracking_path: Optional[str] = None
    source_tracking_path: Optional[str] = None
    file_interface_type: FileInterfaceType = FileInterfaceType.HDFS # Tipo de Interface de I/O usando para o tracking de arquivos

    # Configurações de esquema
    primary_keys: Optional[List[str]] = None
    partition_columns: Optional[List[str]] = None
    
    # Configurações SCD2
    apply_scd2: bool = False
    scd2_columns: Optional[List[str]] = None
    
    # Configurações incrementais
    incremental_column: str = None
    incremental_column_format: str = None
    watermark_table: Optional[str] = None
    
    # Configurações de processamento de arquivos
    file_processing_mode: FileProcessingMode = FileProcessingMode.SEQUENTIAL
    chunk_size: Optional[int] = None
    max_files_per_chunk: Optional[int] = None
    read_options: dict = None

    # Configurações de otimização Delta
    auto_optimize: bool = False
    auto_vacuum: bool = False
    z_order_columns: Optional[List[str]] = None
    vacuum_retention_hours: int = 168
    optimize_after_write: bool = True
    vacuum_schedule: str = "end"
    
    # Configurações de transformação
    custom_transformations: Optional[List[str]] = None
    conform_data: bool = True

    # Configurações de soft delete
    soft_delete_column: Optional[str] = None
    soft_delete_true_value: Optional[Any] = None

    # Configurações de merge
    custom_merge_condition: Optional[str] = None

    
    # def get_tracking_path(self):
    #     if not self.tracking_path:
    #         head, _, tail = self.target_path.rpartition("/")
    #         return f"{head}/_tracking/{tail}"
    #     return self.tracking_path
    
    def get_z_order_columns(self) -> List[str]:
        """Retorna colunas para Z-Order com fallback inteligente"""
        if self.z_order_columns:
            return self.z_order_columns
        
        fallback_columns = []
        if self.primary_keys:
            fallback_columns.extend(self.primary_keys)
        
        if self.incremental_column and self.incremental_column not in fallback_columns:
            fallback_columns.append(self.incremental_column)
        
        if self.apply_scd2:
            fallback_columns.append(SCD2Columns.VALID_FROM)
            fallback_columns.append(SCD2Columns.VALID_TO)
            fallback_columns.append(SCD2Columns.VERSION)

        return fallback_columns
        
    def should_optimize(self) -> bool:
        """Determina se deve executar OPTIMIZE"""
        return self.auto_optimize and self.optimize_after_write
    
    def should_vacuum(self) -> bool:
        """Determina se deve executar VACUUM"""
        return self.auto_vacuum and self.vacuum_schedule in ["end", "after_optimize"]
    
    def validate(self) -> List[str]:
        """Valida a configuração e retorna lista de erros"""
        errors = []
        
        if not self.pipeline_name:
            errors.append("pipeline_name é obrigatório")
        
        if not self.target_path:
            errors.append("target_path é obrigatório")
        
        if self.source_type == SourceType.FILE and not self.source_path:
            errors.append("source_path é obrigatório para fontes de arquivo")

        elif self.source_type == SourceType.QUERY and not self.source_query:
            errors.append("source_query é obrigatório para fontes de query")
        
        if self.write_mode == WriteMode.UPDATE and not self.primary_keys:
            errors.append("primary_keys é obrigatório para write_mode UPDATE")
        
        if (self.file_processing_mode == FileProcessingMode.CHUNKED and 
            not self.chunk_size and not self.max_files_per_chunk):
            errors.append("chunk_size ou max_files_per_chunk é obrigatório para modo CHUNKED")
        
        return errors
    
    def is_valid(self) -> bool:
        """Verifica se a configuração é válida"""
        return len(self.validate()) == 0