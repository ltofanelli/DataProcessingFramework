from pyspark.sql import SparkSession
from data_processing_framework.config.pipeline_config import PipelineConfig
from data_processing_framework.config.enums import LoadType, WriteMode
from data_processing_framework.conformance.conformance import *
   
class ConformanceFactory:
    """Factory para criar estratégias de processamento"""
    
    _strategies = {
        (LoadType.INCREMENTAL, WriteMode.UPDATE, False): IncrementalUpdate,
        (LoadType.INCREMENTAL, WriteMode.UPDATE, True): IncrementalUpdateWithSCD2,
        (LoadType.INCREMENTAL, WriteMode.APPEND, False): IncrementalAppend,
        (LoadType.INCREMENTAL, WriteMode.OVERWRITE, False): IncrementalOverwrite,
        #(LoadType.FULL, WriteMode.UPDATE, False): FullUpdate,
        #(LoadType.FULL, WriteMode.UPDATE, True): FullUpdateWithSCD2,
        (LoadType.FULL, WriteMode.OVERWRITE, False): FullOverwrite,
    }
    
    @classmethod
    def create_strategy(cls, spark: SparkSession, config: PipelineConfig) -> Conformance:
        """Cria estratégia baseada na configuração"""
        cls.spark = spark
        strategy_key = (
            config.load_type, 
            config.write_mode, 
            config.apply_scd2
        )
        
        if strategy_key not in cls._strategies:
            raise ValueError(f"Combinação não suportada: {strategy_key}")
        
        strategy_class = cls._strategies[strategy_key]
        return strategy_class(cls.spark, config)