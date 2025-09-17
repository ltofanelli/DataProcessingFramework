from pyspark.sql import SparkSession, DataFrame
from typing import List, Union

from data_processing_framework.config import PipelineConfig
from data_processing_framework.config.enums import FileProcessingMode, FileFormat

class Reader:
    def __init__(self, spark: SparkSession, config: PipelineConfig):
        self.spark = spark
        self.config = config

    def read(self, files_to_process: Union[List[str], str]) -> DataFrame:
        if self.config.file_processing_mode == FileProcessingMode.SEQUENTIAL:
            return self._read_single_file(files_to_process)
        elif self.config.file_processing_mode == FileProcessingMode.BATCH or self.config.file_processing_mode == FileProcessingMode.CHUNKED:
            return self._read_multiple_files(files_to_process)
        else:
            # Fallback para sequential
            return self._read_single_file(files_to_process)

    def _read_single_file(self, file_path: str) -> DataFrame:
        """Lê um único arquivo"""
        if self.config.source_file_format == FileFormat.PARQUET:
            return self.spark.read.parquet(file_path)
        
        elif self.config.source_file_format == FileFormat.DELTA:
            return self.spark.read.format("delta").load(file_path)
        
        elif self.config.source_file_format == FileFormat.CSV:
            if self.config.read_options:
                return self.spark.read.options(**self.config.read_options).csv(file_path)
            return self.spark.read.csv(file_path)
        
        elif self.config.source_file_format == FileFormat.JSON:
            return self.spark.read.json(file_path)
        
        else:
            raise ValueError(f"Formato não suportado: {self.config.source_file_format}.")

    def _read_multiple_files(self, files: List[str]) -> DataFrame:
        """Lê múltiplos arquivos"""
        if self.config.source_file_format == FileFormat.PARQUET:
            return self.spark.read.parquet(*files)
        
        elif self.config.source_file_format == FileFormat.CSV:
            if self.config.read_options:
                return self.spark.read.options(**self.config.read_options).csv(files)
            return self.spark.read.csv(*files)
        
        elif self.config.source_file_format == FileFormat.JSON:
            return self.spark.read.json(*files)
        
        else:
            # Para formatos que não suportam múltiplos arquivos, une os DataFrames
            dfs = [self._read_single_file(f) for f in files]
            return dfs[0].unionAll(*dfs[1:]) if len(dfs) > 1 else dfs[0]