"""
Processador para fontes de query
"""

import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable

from .base_pipeline import BasePipeline
from data_processing_framework.config.pipeline_config import PipelineConfig
from data_processing_framework.config.enums import LoadType, WriteMode
# from data_framework.tracking_antigo.watermark_manager import WatermarkManager
from data_processing_framework.transformation.registry import TransformationRegistry

class QueryPipeline(BasePipeline):
    """Processador para fontes de query (camadas após Raw)"""
    
    def __init__(self, spark, config: PipelineConfig):
        super().__init__(spark, config)
        # self.watermark_manager = WatermarkManager(spark)
    
    def read_data(self) -> DataFrame:
        """Executa query para ler dados"""
        if self.config.load_type == LoadType.INCREMENTAL:
            query = self._build_incremental_query()
        else:
            query = self.config.source_query
        
        self.logger.info(f"Executando query: {query[:200]}...") # type: ignore
        return self.spark.sql(query) # type: ignore
    
    def _build_incremental_query(self) -> str:
        """Constrói query incremental baseada em watermark"""
        base_query = self.config.source_query
        
        last_watermark = None
        if self.config.watermark_table:
            last_watermark = self.watermark_manager.get_last_watermark(
                self.config.watermark_table, 
                self.config.incremental_column
            )
        
        if last_watermark:
            if "WHERE" in base_query.upper(): # type: ignore
                incremental_query = f"{base_query} AND {self.config.incremental_column} > '{last_watermark}'"
            else:
                incremental_query = f"{base_query} WHERE {self.config.incremental_column} > '{last_watermark}'"
        else:
            incremental_query = base_query
            
        return incremental_query # type: ignore
    
    def transform_data(self, df: DataFrame) -> DataFrame:
        """Aplica transformações nos dados"""
        if df.isEmpty():
            return df
            
        if self.config.custom_transformations:
            df = TransformationRegistry.apply_transformations(df, self.config.custom_transformations)
        
        if self.config.soft_delete_column and self.config.soft_delete_column in df.columns:
            df = df.filter(col(self.config.soft_delete_column) != True)
        
        if self.config.incremental_column in df.columns:
            df = df.filter(col(self.config.incremental_column).isNotNull())
        
        if self.config.apply_scd2:
            df = self._add_scd2_columns(df)
        
        return df
    
    def _add_scd2_columns(self, df: DataFrame) -> DataFrame:
        """Adiciona colunas SCD2"""
        return df.withColumn("valid_from", current_timestamp()) \
                .withColumn("valid_to", lit(None).cast(TimestampType())) \
                .withColumn("is_current", lit(True))
    
    def write_data(self, df: DataFrame) -> None:
        """Escreve dados no destino"""
        if df.isEmpty():
            self.logger.info("DataFrame vazio, pulando escrita")
            return
        
        if self.config.write_mode == WriteMode.APPEND:
            self._write_append(df)
        elif self.config.write_mode == WriteMode.OVERWRITE:
            self._write_overwrite(df)
        elif self.config.write_mode == WriteMode.UPDATE:
            self._write_merge(df)
    
    def _write_append(self, df: DataFrame) -> None:
        """Escreve em modo append"""
        writer = df.write.format("delta")
        
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        writer.mode("append").save(self.config.target_path)
    
    def _write_overwrite(self, df: DataFrame) -> None:
        """Escreve em modo overwrite"""
        writer = df.write.format("delta")
        
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        writer.mode("overwrite").save(self.config.target_path)
    
    def _write_merge(self, df: DataFrame) -> None:
        """Escreve usando merge"""
        if not os.path.exists(self.config.target_path):
            self._write_overwrite(df)
            return
        
        # from ..merge.default_strategy import DefaultMergeStrategy
        
        delta_table = DeltaTable.forPath(self.spark, self.config.target_path)
        # merge_strategy = DefaultMergeStrategy()
        
        # merge_condition = merge_strategy.get_merge_condition(self.config)
        # update_set = merge_strategy.get_update_set(self.config)
        # insert_values = merge_strategy.get_insert_values(self.config)
        
        # (delta_table.alias("target")
        #  .merge(df.alias("source"), merge_condition)
        #  .whenMatchedUpdate(set=update_set) # type: ignore
        #  .whenNotMatchedInsert(values=insert_values) # type: ignore
        #  .execute())