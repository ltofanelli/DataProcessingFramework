"""
Enumerações usadas pelo framework
"""

from enum import Enum

## Enums de configuração
class SourceType(Enum):
    """Tipo de fonte de dados"""
    FILE = "file"
    QUERY = "query"

class LoadType(Enum):
    """Tipo de carga de dados"""
    FULL = "full"
    INCREMENTAL = "incremental"

class WriteMode(Enum):
    """Modo de escrita de dados"""
    APPEND = "append"
    OVERWRITE = "overwrite"
    UPDATE = "update"

class FileFormat(Enum):
    """Formato de arquivo suportado"""
    PARQUET = "parquet"
    DELTA = "delta"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"

class FileProcessingMode(Enum):
    """Modo de processamento de arquivos"""
    SEQUENTIAL = "sequential"
    BATCH = "batch"
    CHUNKED = "chunked"

class OptimizationSchedule(Enum):
    """Agendamento de otimização"""
    NEVER = "never"
    END = "end"
    AFTER_OPTIMIZE = "after_optimize"
    SCHEDULED = "scheduled"

class TableType(Enum):
    """Tipo de tabela"""
    FACT = "fact"
    DIMENSION = "dimension"
    STAGING = "staging"
    LOG = "log"
    TEMP = "temp"

class CompressionType(Enum):
    """Tipo de compressão"""
    NONE = "none"
    SNAPPY = "snappy"
    GZIP = "gzip"
    LZ4 = "lz4"
    ZSTD = "zstd"

class ValidationLevel(Enum):
    """Nível de validação"""
    NONE = "none"
    BASIC = "basic"
    STRICT = "strict"
    CUSTOM = "custom"

class ProcessingStatus(Enum):
    """Status do processamento"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"

class MergeStrategy(Enum):
    """Estratégia de merge"""
    DEFAULT = "default"
    CUSTOM = "custom"
    UPSERT_ONLY = "upsert_only"
    DELETE_INSERT = "delete_insert"
    SCD2 = "scd2"

class FileInterfaceType(Enum):
    LOCAL = 'local'
    HDFS = 'hdfs'
    FABRIC = 'fabric'
    SFTP = 'sftp'

## Enums do processamento
class AuditColumns(Enum):
    MODIFIED_DATE = "__modified_date"
    DELETED = "__deleted"
    CREATED_AT = "__created_at"
    UPDATED_AT = "__updated_at"
    SOURCE_FILE = "__source_file"
    
class SCD2Columns(Enum):
    CURRENT = "__current"
    VERSION = "__version"
    VALID_FROM = "__valid_from"
    VALID_TO = "__valid_to"

# Constantes úteis
DEFAULT_CHUNK_SIZE = 10
DEFAULT_VACUUM_RETENTION_HOURS = 168
DEFAULT_INCREMENTAL_COLUMN = "_modified_date"