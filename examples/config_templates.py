"""
Configuração principal do framework
"""

from dataclasses import dataclass
from typing import List, Optional, Any
from ..src.data_processing_framework.config import SourceType, LoadType, WriteMode, FileFormat, FileProcessingMode, FileInterfaceType, SCD2Columns
from ..src.data_processing_framework.config import PipelineConfig

class ConfigTemplates:
    """Templates de configuração para casos comuns"""
    
    @staticmethod
    def raw_to_bronze_incremental(pipeline_name: str, source_path: str, target_path: str, 
                                 target_table: str, file_format: FileFormat = FileFormat.PARQUET) -> PipelineConfig:
        """Template para Raw → Bronze incremental"""
        return PipelineConfig(
            pipeline_name=pipeline_name,
            source_type=SourceType.FILE,
            load_type=LoadType.INCREMENTAL,
            write_mode=WriteMode.APPEND,
            source_path=source_path,
            target_path=target_path,
            target_table=target_table,
            file_format=file_format,
            file_processing_mode=FileProcessingMode.CHUNKED,
            chunk_size=10,
            custom_transformations=["add_audit_columns", "clean_nulls"]
        )
    
    @staticmethod
    def bronze_to_silver_merge(pipeline_name: str, source_query: str, target_path: str,
                              target_table: str, primary_keys: List[str],
                              incremental_column: str = "modified_date") -> PipelineConfig:
        """Template para Bronze → Silver com merge"""
        return PipelineConfig(
            pipeline_name=pipeline_name,
            source_type=SourceType.QUERY,
            load_type=LoadType.INCREMENTAL,
            write_mode=WriteMode.UPDATE,
            source_query=source_query,
            target_path=target_path,
            target_table=target_table,
            primary_keys=primary_keys,
            incremental_column=incremental_column,
            watermark_table=target_table,
            custom_transformations=["add_audit_columns"]
        )
    
    @staticmethod
    def dimension_scd2(pipeline_name: str, source_query: str, target_path: str,
                      target_table: str, primary_keys: List[str], scd2_columns: List[str],
                      incremental_column: str = "modified_date") -> PipelineConfig:
        """Template para dimensões SCD2"""
        return PipelineConfig(
            pipeline_name=pipeline_name,
            source_type=SourceType.QUERY,
            load_type=LoadType.INCREMENTAL,
            write_mode=WriteMode.UPDATE,
            source_query=source_query,
            target_path=target_path,
            target_table=target_table,
            primary_keys=primary_keys,
            apply_scd2=True,
            scd2_columns=scd2_columns,
            incremental_column=incremental_column,
            watermark_table=target_table,
            z_order_columns=primary_keys + [incremental_column]
        )