from data_processing_framework.config.enums import *
from data_processing_framework.config import PipelineConfig
from data_processing_framework.pipeline import PipelineFactory
from pyspark.sql import SparkSession

from data_processing_framework.transformation.registry import TransformationRegistry

def exemplo_arquivo_csv(spark):
    """Exemplo: Processamento de arquivos CSV"""
    
    @TransformationRegistry.register("teste_registry")
    def teste_registry(df):
        from pyspark.sql.functions import when, col, lit
        return df.withColumn("new", when(col("status") == "A", lit(True)).otherwise(lit(False)))
    
    try:
        config = PipelineConfig(
            pipeline_name="clientes_csv",
            source_type=SourceType.FILE,
            load_type=LoadType.INCREMENTAL,
            write_mode=WriteMode.UPDATE,
            primary_keys=["id"],
            target_table="clientes",
            target_path="/stage/clientes",
            source_path="/raw/clientes",
            source_file_format=FileFormat.CSV,
            read_options={"header": "true", "multiline": "true", "quote": "\"", "delimiter": ","},
            tracking_path = "/stage/tracking/clientes",
            source_tracking_path = "/raw/clientes/tracking",
            tracking_io_credentials = {"host": "namenode", "port": "9870", "username": "hdfs"},
            soft_delete_column="status",
            soft_delete_true_value="C",
            custom_transformations=["teste_registry"],
            file_interface_type=FileInterfaceType.HDFS,
            file_processing_mode=FileProcessingMode.CHUNKED,
            chunk_size=2
        )

        processor = PipelineFactory.create_processor(spark, config)
        processor.process()

        print("Processamento CSV concluído!")
        
    finally:
        print("finally")
        spark.stop()


if __name__ == "__main__":

    spark = (SparkSession.builder
        .appName("ExemploClienteCSV")
        .master("local[*]")
        .config("spark.sql.debug.maxToStringFields", 1000)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    exemplo_arquivo_csv(spark)