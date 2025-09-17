"""
Factory para criação de processadores
"""

from pyspark.sql import SparkSession
from data_processing_framework.config import PipelineConfig
from data_processing_framework.config.enums import SourceType
from .base_pipeline import BasePipeline

class PipelineFactory:
    """Factory para criar processadores baseado na configuração"""
    
    @staticmethod
    def create_processor(spark: SparkSession, config: PipelineConfig) -> BasePipeline:
        """Cria processador baseado no tipo de fonte"""
        
        # Importações locais para evitar circular imports
        from .file_pipeline import FilePipeline
        from .query_pipeline import QueryPipeline
        
        if config.source_type == SourceType.FILE:
            return FilePipeline(spark, config)
        elif config.source_type == SourceType.QUERY:
            return QueryPipeline(spark, config)
        else:
            raise ValueError(f"Tipo de fonte não suportado: {config.source_type}")
    
    @staticmethod
    def get_supported_source_types():
        """Retorna tipos de fonte suportados"""
        return [SourceType.FILE, SourceType.QUERY]