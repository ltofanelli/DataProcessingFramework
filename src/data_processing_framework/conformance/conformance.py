"""
Framework para processamento de dados com estratégias baseadas em configurações
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, row_number, when,
    hash, concat_ws, last, input_file_name,
    regexp_extract, to_timestamp, desc, lead
)
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, BooleanType
from delta.tables import DeltaTable

# Framework dependencies
from data_processing_framework.config import PipelineConfig
from data_processing_framework.config import AuditColumns, SCD2Columns, FileProcessingMode

import logging
logger = logging.getLogger(__name__)

class Conformance(ABC):
    """Classe base para estratégias de processamento"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.business_fields = None
    
    @abstractmethod
    def process(self, new_data: DataFrame, files_path: List[list]) -> DataFrame:
        """Processa os dados conforme a estratégia"""
        pass
    
    def _read_current_data(self) -> Optional[DataFrame]:
        """
        Lê dados atuais da Delta table com otimizações baseadas no tipo de carga
        
        Args:
            incremental_keys: Lista de chaves para carga incremental (None = full load)
            columns: Colunas específicas a serem lidas (None = todas)
            
        Returns:
            DataFrame com dados atuais ou None se tabela não existir
        """
        try:
            # Verifica se tabela existe
            if not self._table_exists():
                logger.info(f"Tabela {self.config.target_path} não existe. Primeira carga.")
                return None
            
            # Constrói query base
            df = self.spark.read.format("delta").load(self.config.target_path)
                    
            # Filtro principal: apenas registros atuais
            df = df.filter(col(SCD2Columns.CURRENT.value) == True)
            
            return df
            
        except Exception as e:
            logger.error(f"Erro ao ler tabela {self.config.target_path}: {str(e)}")
            return None
    
    def _table_exists(self) -> bool:
        """Verifica se a Delta table existe"""
        try:
            DeltaTable.forPath(self.spark, self.config.target_path)
            return True
        except:
            return False
    
    def _pruning_data_frame(self, df: DataFrame, pruning_columns: List[str]) -> DataFrame:
        """ Aplica column pruning no data frame"""

        # Cria uma lista da/das Primary keys e colunas enviadas para pruning
        required_cols = self.config.primary_keys + pruning_columns

        # Mantém apenas as colunas desejadas no data frame
        df = df.select(*required_cols)
        return df

    def _newer_by_source_file(self, df: DataFrame) -> DataFrame:
        """Mantem somente a versão mais recente baseada no __source_file"""
        window_spec = Window.partitionBy(*self.config.primary_keys).orderBy(desc(AuditColumns.SOURCE_FILE.value))
        
        df = df.withColumn("__row_number", row_number().over(window_spec))
        
        # Mantém apenas a primeira linha de cada grupo (mais recente)
        df = df.filter(col("__row_number") == 1).drop("__row_number")
        
        return df
    
    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        """Adiciona colunas de auditoria para estratégias sem SCD2"""
        now = current_timestamp()

        df = self._add_modified_date(df)

        df = self._add_deleted_column(df)

        df = df.withColumn(AuditColumns.CREATED_AT.value, now.cast(TimestampType())) \
                    .withColumn(AuditColumns.UPDATED_AT.value, now.cast(TimestampType())) \
                    .withColumn(AuditColumns.SOURCE_FILE.value, regexp_extract(input_file_name(),
                            r"^(.*?)(\?|$)",  # Padrão regex
                            1  # Captura o primeiro grupo (tudo antes do ? ou fim da string)
                    )
                )
        return df

    def _add_modified_date(self, df: DataFrame) -> DataFrame:
        """Adiciona coluna modified_date baseada na configuração"""
        if self.config.incremental_column:
            return self._modified_date_from_config(df)
        else:
            return self._modified_date_from_file_path(df)

    def _modified_date_from_config(self, df: DataFrame) -> DataFrame:
        try:
            # Verifica se a coluna existe no DataFrame
            if self.config.incremental_column not in df.columns:
                raise ValueError(f"Coluna '{self.config.incremental_column}' não encontrada no DataFrame")
            
            # Obtém o formato da coluna (padrão se não especificado)
            if not self.config.incremental_column_format:
                raise ValueError(f"Formato da coluna '{self.config.incremental_column}' para criar a coluna '{AuditColumns.MODIFIED_DATE.value}', não foi especificado")

            print(f"Formato da coluna '{self.config.incremental_column}' para criar a coluna '{AuditColumns.MODIFIED_DATE.value}': {self.config.incremental_column_format}")
            
            # Converte a coluna para timestamp no formato padrão
            df = df.withColumn(
                AuditColumns.MODIFIED_DATE.value,
                to_timestamp(col(self.config.incremental_column), self.config.incremental_column_format).cast(TimestampType())
            )
            return df
        
        except Exception as e:
            logger.error(f"Erro ao tentar montar '{AuditColumns.MODIFIED_DATE.value}' através do PipelineConfig.incremental_column = {self.config.incremental_column} ou PipelineConfig.incremental_column_format = {self.config.incremental_column_format} - error messagem: {str(e)}")
            raise
    
    def _modified_date_from_file_path(self, df: DataFrame) -> DataFrame:
        try:
            # Adiciona coluna com o caminho do arquivo
            df_with_path = df.withColumn("__file_path", input_file_name())
            
            # Padrão regex para extrair /YYYY/MM/DD/HH_MM_SS_
            # Este padrão procura por /4digitos/2digitos/2digitos/2digitos_2digitos_2digitos_
            date_pattern = r".*/(\d{4})/(\d{2})/(\d{2})/(\d{2})_(\d{2})_(\d{2})_.*"
            
            # Extrai os componentes da data do caminho
            df_with_extracted = df_with_path.withColumn(
                "__year", regexp_extract(col("__file_path"), date_pattern, 1)
            ).withColumn(
                "__month", regexp_extract(col("__file_path"), date_pattern, 2)
            ).withColumn(
                "__day", regexp_extract(col("__file_path"), date_pattern, 3)
            ).withColumn(
                "__hour", regexp_extract(col("__file_path"), date_pattern, 4)
            ).withColumn(
                "__minute", regexp_extract(col("__file_path"), date_pattern, 5)
            ).withColumn(
                "__second", regexp_extract(col("__file_path"), date_pattern, 6)
            )

            # Constrói a string de data/hora no formato yyyy-MM-dd HH:mm:ss
            df_with_date_string = df_with_extracted.withColumn(
                "__date_string",
                concat_ws(" ",
                    concat_ws("-", col("__year"), col("__month"), col("__day")),
                    concat_ws(":", col("__hour"), col("__minute"), col("__second"))
                )
            )

            # Converte para timestamp, deixando nulo se não conseguir extrair a data
            
            df_with_modified = df_with_date_string.withColumn(
                AuditColumns.MODIFIED_DATE.value,
                when(
                    (col("__year") != "") & 
                    (col("__month") != "") & 
                    (col("__day") != "") &
                    (col("__hour") != "") & 
                    (col("__minute") != "") & 
                    (col("__second") != ""),
                    to_timestamp(col("__date_string"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType())
                ).otherwise(to_timestamp(lit("1900-01-01 00:00:00"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType()))
            )

            # Remove colunas auxiliares
            columns_to_drop = ["__file_path", "__year", "__month", "__day", 
                            "__hour", "__minute", "__second", "__date_string"]
            
            for col_name in columns_to_drop:
                df_with_modified = df_with_modified.drop(col_name)

            return df_with_modified
        
        except Exception as e:
            logger.error(f"Erro ao tentar montar '{AuditColumns.MODIFIED_DATE.value}' através do file_path: {str(e)}")
            raise
    
    def _add_deleted_column(self, df: DataFrame) -> DataFrame:
        """Adiciona a coluna de deletado quando configurado"""
        try:
            if self.config.soft_delete_column:
                if self.config.soft_delete_true_value:
                    df = df.withColumn(AuditColumns.DELETED.value, when(col(self.config.soft_delete_column) == self.config.soft_delete_true_value, lit(True).cast(BooleanType())).otherwise(lit(False)))
                else:
                    raise ValueError(f"O valor da coluna '{self.config.soft_delete_column}' para criar a coluna '{AuditColumns.DELETED.value}', não foi especificado na varíavel de configuração 'soft_delete_true_value'")
            else:
                df.withColumn(AuditColumns.DELETED.value, lit(False))
            
            return df
        
        except Exception as e:
            logger.error(f"Erro ao tentar montar '{AuditColumns.DELETED.value}': {str(e)}")
            raise
    
    def _add_default_scd2_columns(self, df: DataFrame, ) -> DataFrame:
        """Adiciona colunas padrão SCD2"""
        if self.config.file_processing_mode == FileProcessingMode.SEQUENTIAL:
            result = df.withColumn(SCD2Columns.CURRENT.value, lit(True).cast(BooleanType())) \
                    .withColumn(SCD2Columns.VERSION.value, lit(1)) \
                    .withColumn(SCD2Columns.VALID_FROM.value, lit("1900-01-01 00:00:00").cast(TimestampType())) \
                    .withColumn(SCD2Columns.VALID_TO.value, lit("3000-01-01 00:00:00").cast(TimestampType()))
        else:
            window_spec = Window.partitionBy(*self.config.primary_keys).orderBy(AuditColumns.MODIFIED_DATE.value)

            result = df.withColumn("row_number", row_number().over(window_spec)) \
                .withColumn("max_row_number", last("row_number").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) \
                .withColumn("last_modified_date", lead(AuditColumns.MODIFIED_DATE.value).over(window_spec)) \
                .withColumn(SCD2Columns.CURRENT.value, 
                    when(col("row_number") == col("max_row_number"),
                        lit(True)
                    )
                    .otherwise(
                        lit(False)
                    ).cast(BooleanType())
                ) \
                .withColumn(SCD2Columns.VERSION.value, col("row_number").cast(IntegerType())) \
                .withColumn(SCD2Columns.VALID_FROM.value,
                    when(col(SCD2Columns.VERSION.value) == 1,
                        lit("1900-01-01 00:00:00.000000")
                    )
                    .otherwise(
                        col(AuditColumns.MODIFIED_DATE.value)
                    ).cast(TimestampType())
                ) \
                .withColumn(SCD2Columns.VALID_FROM.value,
                    when(col(SCD2Columns.CURRENT.value) == True,
                        lit("3000-01-01 00:00:00.000000")
                    )
                    .otherwise(
                        col("last_modified_date")
                    ).cast(TimestampType())
                ).drop("row_number").drop("max_row_number").drop("last_modified_date")
            
        return result
    
    def _create_row_hash(self, df: DataFrame) -> DataFrame:
        """Cria hash da linha para detectar mudanças"""
        # Exclui colunas de controle do hash
        business_columns = [col(c) for c in df.columns 
                          if not c.startswith('__') and c not in self.config.primary_keys]
        return df.withColumn("__row_hash", hash(concat_ws("_",*business_columns)))

class IncrementalUpdate(Conformance):
    """INCREMENTAL + UPDATE + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + UPDATE sem SCD2")

        # Adicionar colunas de auditoria
        df_with_audit = self._add_audit_columns(new_data)
        
        # 1. Se não for processamento sequencial, manter apenas a última versão por primary_key
        if self.config.file_processing_mode != FileProcessingMode.SEQUENTIAL:
            df_with_audit = self._newer_by_source_file(df_with_audit)
        
        return df_with_audit

class IncrementalUpdateWithSCD2(Conformance):
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

class IncrementalAppend(Conformance):
    """INCREMENTAL + APPEND + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + APPEND")
        
        # Para APPEND, sempre trata como novos registros
        return self._add_audit_columns(new_data)

class IncrementalOverwrite(Conformance):
    """INCREMENTAL + OVERWRITE + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: INCREMENTAL + OVERWRITE")

        return self._add_audit_columns(new_data)

# class FullUpdate(Conformance):
#     """FULL + UPDATE + SCD2 = False"""
    
#     def process(self, new_data: DataFrame) -> DataFrame:
#         logger.info("Executando: FULL + UPDATE + SCD2 = False")
#         return new_data

# class FullUpdateWithSCD2(Conformance):
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

class FullOverwrite(Conformance):
    """FULL + OVERWRITE + SCD2 = False"""
    
    def process(self, new_data: DataFrame) -> DataFrame:
        logger.info("Executando: FULL + OVERWRITE")

        # 1. Adicionar colunas de auditoria
        df_with_audit = self._add_audit_columns(new_data)
        
        # 2. Se não for processamento sequencial, fazer manter somente versão mais recente
        if self.config.file_processing_mode != FileProcessingMode.SEQUENTIAL:
            df_with_audit = self._newer_by_source_file(df_with_audit)
        
        return df_with_audit