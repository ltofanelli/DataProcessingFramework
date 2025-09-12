"""
Módulo de configuração do framework
"""

from .enums import (
    SourceType, LoadType, WriteMode, FileFormat, FileProcessingMode,
    OptimizationSchedule,TableType, CompressionType, ValidationLevel,
    ProcessingStatus, MergeStrategy, FileInterfaceType, AuditColumns, SCD2Columns
)
from .pipeline_config import PipelineConfig
from .validation import ConfigValidator

__all__ = [
    "SourceType", "LoadType", "WriteMode", "FileFormat", "FileProcessingMode",
    "OptimizationSchedule", "TableType", "CompressionType", "ValidationLevel",
    "ProcessingStatus", "MergeStrategy", "FileInterfaceType", "AuditColumns", "SCD2Columns",
    "PipelineConfig", "ConfigValidator"
]

