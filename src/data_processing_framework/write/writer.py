"""
Classe Writer para gerenciar escrita de dados no framework
"""

import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from delta import DeltaTable

from data_processing_framework.config.enums import (
    WriteMode, 
    AuditColumns, 
    SCD2Columns
)
from data_processing_framework.config.pipeline_config import PipelineConfig
from data_processing_framework.util.utils import table_exists

logger = logging.getLogger(__name__)


class Writer:
    """
    Classe responsável por gerenciar toda a lógica de escrita de dados
    incluindo merge, SCD2 e diferentes modos de escrita
    """
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        """
        Inicializa o Writer
        
        Args:
            spark: Sessão Spark ativa
            config: Classe PipelineConfig com as configurações do pipeline
        """
        self.spark = spark
        self.config = config
        
    def write(self, df: DataFrame) -> bool:
        """
        Método principal de escrita que delega para a estratégia apropriada
        
        Args:
            df: DataFrame a ser escrito
            config: Configurações do pipeline
        """
        logger.info(f"Iniciando escrita para tabela {self.config.target_table}")
        logger.info(f"Modo de escrita: {self.config.write_mode.value}")
        logger.info(f"SCD2 habilitado: {self.config.apply_scd2}")
        
        # Validações básicas
        self._validate_config()

        # Executa a estratégia de escrita apropriada
        if self.config.write_mode == WriteMode.OVERWRITE:
            self._write_overwrite(df)
        elif self.config.write_mode == WriteMode.APPEND:
            self._write_append(df)
        elif self.config.write_mode == WriteMode.UPDATE:
            self._write_update(df)
        else:
            raise ValueError(f"Modo de escrita não suportado: {self.config.write_mode}")
            
        logger.info(f"Escrita concluída para tabela {self.config.target_table}")
        return True
    
    def _validate_config(self) -> None:
        """
        Valida as configurações necessárias para escrita
        
        Args:
            config: Configurações do pipeline
        """            
        if self.config.write_mode == WriteMode.UPDATE and not self.config.primary_keys:
            raise ValueError("primary_keys é obrigatório para modo UPDATE")
            
        if self.config.apply_scd2 and not self.config.primary_keys:
            raise ValueError("primary_keys é obrigatório quando SCD2 está habilitado")
    
    def _write_overwrite(self, df: DataFrame) -> None:
        """
        Escreve dados sobrescrevendo a tabela existente
        
        Args:
            df: DataFrame a ser escrito
            config: Configurações do pipeline
        """
        logger.info(f"Executando OVERWRITE em {self.config.target_path}")
        
        writer = df.write.mode("overwrite")
        
        # Adiciona particionamento se configurado
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        # Escreve em formato Delta
        writer.format("delta").save(self.config.target_path)
        
        # Registra tabela no metastore se necessário
        self._register_table()
    
    def _write_append(self, df: DataFrame) -> None:
        """
        Escreve dados anexando à tabela existente
        
        Args:
            df: DataFrame a ser escrito
            config: Configurações do pipeline
        """
        logger.info(f"Executando APPEND em {self.config.target_path}")
        
        writer = df.write.mode("append")
        
        # Adiciona particionamento se configurado
        if self.config.partition_columns:
            writer = writer.partitionBy(*self.config.partition_columns)
        
        # Escreve em formato Delta
        writer.format("delta").save(self.config.target_path)
        
        # Registra tabela no metastore se necessário
        self._register_table()
    
    def _write_update(self, df: DataFrame) -> None:
        """
        Escreve dados usando merge (upsert)
        
        Args:
            df: DataFrame a ser escrito
            config: Configurações do pipeline
        """
        logger.info(f"Executando UPDATE em {self.config.target_path}")
        
        # Verifica se a tabela existe
        if not table_exists(self.spark, self.config.target_path):
            logger.info("Tabela não existe, executando escrita inicial")
            self._write_overwrite(df)
            return
        
        # Executa merge apropriado
        if self.config.apply_scd2:
            self._merge_scd2(df)
        else:
            self._merge_standard(df)
    
    def _merge_standard(self, df: DataFrame) -> None:
        """
        Executa merge padrão (sem SCD2)
        
        Args:
            df: DataFrame com novos dados
            config: Configurações do pipeline
        """
        logger.info("Executando merge padrão")
        
        # Carrega tabela Delta existente
        delta_table = DeltaTable.forPath(self.spark, self.config.target_path)
        
        # Constrói condição de merge baseada nas primary keys
        merge_condition = " AND ".join([
            f"target.{pk} = source.{pk}" 
            for pk in self.config.primary_keys
        ])
        
        # Prepara valores para update (exclui CREATED_AT)
        update_values = {
            col: f"source.{col}"
            for col in df.columns
            if col != AuditColumns.CREATED_AT.value
        }
        
        # Prepara valores para insert (todas as colunas)
        insert_values = {
            col: f"source.{col}"
            for col in df.columns
        }
        
        # Executa merge
        (delta_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdate(set=update_values)
         .whenNotMatchedInsert(values=insert_values)
         .execute())
        
        logger.info("Merge padrão concluído")
    
    def _merge_scd2(self, df: DataFrame) -> None:
        """
        Executa merge com lógica SCD2
        
        Args:
            df: DataFrame com novos dados (já com colunas SCD2)
            config: Configurações do pipeline
        """
        logger.info("Executando merge SCD2")
        
        # Carrega tabela Delta existente
        delta_table = DeltaTable.forPath(self.spark, self.config.target_path)
        existing_df = delta_table.toDF()
        
        # Identifica chaves que precisam ser versionadas
        keys_to_update = df.select(*self.config.primary_keys).distinct()
        
        # Marca registros existentes como históricos para chaves que serão atualizadas
        historical_updates = (
            existing_df.alias("existing")
            .join(
                keys_to_update.alias("keys"),
                on=[existing_df[pk] == keys_to_update[pk] for pk in self.config.primary_keys],
                how="inner"
            )
            .where(F.col(f"existing.{SCD2Columns.CURRENT.value}") == True)
            .select("existing.*")
        )
        
        if historical_updates.count() > 0:
            # Pega a menor data dos novos registros para cada chave
            min_dates = df.groupBy(*self.config.primary_keys).agg(
                F.min(SCD2Columns.VALID_FROM.value).alias("min_valid_from")
            )
            
            # Constrói condição de update para registros históricos
            update_condition = " AND ".join([
                f"target.{pk} = source.{pk}" 
                for pk in self.config.primary_keys
            ] + [f"target.{SCD2Columns.CURRENT.value} = true"])
            
            # Atualiza registros existentes para marcar como históricos
            (delta_table.alias("target")
             .merge(min_dates.alias("source"), update_condition)
             .whenMatchedUpdate(set={
                 SCD2Columns.CURRENT.value: "false",
                 SCD2Columns.VALID_TO.value: "source.min_valid_from"
             })
             .execute())
        
        # Insere todos os novos registros (incluindo novas versões)
        df.write.mode("append").format("delta").save(self.config.target_path)
        
        logger.info("Merge SCD2 concluído")

    def _register_table(self) -> None:
        """
        Registra tabela no metastore do Spark se ainda não estiver registrada
        
        Args:
            config: Configurações do pipeline
        """
        try:
            # Tenta criar tabela se não existir
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.config.target_table}
                USING DELTA
                LOCATION '{self.config.target_path}'
            """)
            logger.info(f"Tabela {self.config.target_table} registrada no metastore")
        except Exception as e:
            logger.warning(f"Não foi possível registrar tabela no metastore: {e}")