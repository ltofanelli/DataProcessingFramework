"""
Framework para processamento de dados com estratégias baseadas em configurações
"""
from pyspark.sql import DataFrame

# Framework dependencies
from .base import BaseConformance
from data_processing_framework.config.enums import SCD2Columns, FileProcessingMode

import logging
logger = logging.getLogger(__name__)

class IncrementalUpdate(BaseConformance):
    """INCREMENTAL + UPDATE + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + UPDATE sem SCD2")

        # Adicionar colunas de auditoria
        df_with_audit = self._add_audit_columns(new_data)
        
        # 1. Se não for processamento sequencial, manter apenas a última versão por primary_key
        if self.config.file_processing_mode != FileProcessingMode.SEQUENTIAL:
            df_with_audit = self._newer_by_source_file(df_with_audit)
        
        return df_with_audit

class IncrementalUpdateWithSCD2(BaseConformance):
    """INCREMENTAL + UPDATE + SCD2 = True"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + UPDATE + SCD2")

        # Ler dados atuais
        current_data = self._read_current_data()

        # Se for a primeira carga e sequential, retorna com valores default
        if current_data is None:
            return self._add_default_scd2_columns(self._add_audit_columns(new_data))


        # 2. Fazer pruning das colunas necessárias para SCD2
        pruning_columns = [SCD2Columns.VERSION.value, SCD2Columns.VALID_FROM.value]
        current_pruned = self._pruning_data_frame(current_data, pruning_columns)

        # Renomeia as colunas dos dados atuais
        current_renamed = current_pruned.withColumnsRenamed(
            {
                SCD2Columns.VERSION.value: SCD2Columns.VERSION.value+"_current",
                SCD2Columns.VALID_FROM.value: SCD2Columns.VALID_FROM.value+"_current"
            }
        )

        # 3. Filtrar apenas as primary keys que estão no new_data
        new_keys = new_data.select(*self.config.primary_keys).distinct()
        current_filtered = current_renamed.join(new_keys, self.config.primary_keys, "inner")
        
        return None

class IncrementalAppend(BaseConformance):
    """INCREMENTAL + APPEND + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + APPEND")
        
        # Para APPEND, sempre trata como novos registros
        return self._add_audit_columns(new_data)

class IncrementalOverwrite(BaseConformance):
    """INCREMENTAL + OVERWRITE + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + OVERWRITE")

        return self._add_audit_columns(new_data)

# class FullUpdate(BaseConformance):
#     """FULL + UPDATE + SCD2 = False"""
    
#     def process(self, new_data: DataFrame) -> DataFrame:
#         logger.info("Executando: FULL + UPDATE + SCD2 = False")
#         return new_data

# class FullUpdateWithSCD2(BaseConformance):
#     """FULL + UPDATE + SCD2 = True"""
    
#     def process(self, new_data: DataFrame) -> DataFrame:
#         logger.info("Executando: FULL + UPDATE + SCD2 = True")

#         # Ler dados atuais
#         current_data = self._read_current_data()

#         # Se for a primeira carga e sequential, retorna com valores default
#         if current_data is None and self.config.file_processing_mode.SEQUENTIAL:
#             logger.info("Primeira carga - tratando como inserção completa")
#             return self._add_default_scd2_columns(self._add_audit_columns(new_data))
        
#         return new_data

class FullOverwrite(BaseConformance):
    """FULL + OVERWRITE + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: FULL + OVERWRITE")

        # 1. Adicionar colunas de auditoria
        df_with_audit = self._add_audit_columns(new_data)
        
        # 2. Se não for processamento sequencial, fazer manter somente versão mais recente
        if self.config.file_processing_mode != FileProcessingMode.SEQUENTIAL:
            df_with_audit = self._newer_by_source_file(df_with_audit)
        
        return df_with_audit