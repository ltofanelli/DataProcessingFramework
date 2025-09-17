"""
Classe base abstrata para processadores
"""
import sys
import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from data_processing_framework.config import PipelineConfig

# Configuração global do logging
logging.basicConfig(
    level=logging.INFO,  # INFO ou DEBUG dependendo do que você quer ver
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class BasePipeline(ABC):
    """Classe base abstrata para processadores de dados"""
    
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    # Opt 1
    @abstractmethod
    def process(self) -> None:
        """Método principal de processamento"""
        pass

    # Opt 2
    def process(self) -> None:
        raise NotImplementedError("Subclasses devem implementar este método")
    
    def optimize_table(self) -> None:
        """Executa otimização na tabela"""
        if not self.config.should_optimize():
            self.logger.info("Otimização desabilitada")
            return
            
        try:
            z_order_cols = self.config.get_z_order_columns()
            
            if z_order_cols:
                self.logger.info(f"Executando OPTIMIZE com Z-Order: {z_order_cols}")
                optimize_sql = f"OPTIMIZE delta.`{self.config.target_path}` ZORDER BY ({', '.join(z_order_cols)})"
            else:
                self.logger.info("Executando OPTIMIZE sem Z-Order")
                optimize_sql = f"OPTIMIZE delta.`{self.config.target_path}`"
            
            self.spark.sql(optimize_sql)
            self.logger.info("OPTIMIZE executado com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao executar OPTIMIZE: {str(e)}")
            
    def vacuum_table(self) -> None:
        """Executa VACUUM na tabela"""
        if not self.config.should_vacuum():
            self.logger.info("VACUUM desabilitado")
            return
            
        try:
            retention_hours = self.config.vacuum_retention_hours
            self.logger.info(f"Executando VACUUM com retenção de {retention_hours} horas")
            
            vacuum_sql = f"VACUUM delta.`{self.config.target_path}` RETAIN {retention_hours} HOURS"
            self.spark.sql(vacuum_sql)
            self.logger.info("VACUUM executado com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao executar VACUUM: {str(e)}")
    
    def post_write_maintenance(self) -> None:
        """Executa rotinas de manutenção após escrita"""
        self.logger.info("Iniciando manutenção pós-escrita")
        
        if self.config.should_optimize():
            self.optimize_table()
            
        if self.config.should_vacuum():
            self.vacuum_table()
    
    # @abstractmethod
    # def read_data(self, files_to_process: Union[List[str], str]) -> DataFrame:
    #     """Lê dados da fonte"""
    #     pass
    
    # @abstractmethod
    # def transform_data(self, df: DataFrame) -> DataFrame:
    #     """Aplica transformações nos dados"""
    #     pass
    
    # @abstractmethod
    # def write_data(self, df: DataFrame) -> None:
    #     """Escreve dados no destino"""
    #     pass