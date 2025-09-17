"""
Transformações pré-definidas do framework
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, current_timestamp, current_date, concat, monotonically_increasing_id, row_number, regexp_replace, hash
from pyspark.sql.types import StringType
from data_processing_framework.transformation import TransformationRegistry

@TransformationRegistry.register("add_audit_columns")
def add_audit_columns(df: DataFrame) -> DataFrame:
    """Adiciona colunas de auditoria"""
    return df.withColumn("processed_at", current_timestamp()) \
             .withColumn("processed_date", current_date())

@TransformationRegistry.register("handle_soft_deletes")
def handle_soft_deletes(df: DataFrame) -> DataFrame:
    """Filtra registros com soft delete"""
    if "is_deleted" in df.columns:
        return df.filter(col("is_deleted") != True)
    return df

@TransformationRegistry.register("clean_nulls")
def clean_nulls(df: DataFrame) -> DataFrame:
    """Remove registros com todas as colunas nulas"""
    return df.dropna(how="all")

@TransformationRegistry.register("standardize_columns")
def standardize_columns(df: DataFrame) -> DataFrame:
    """Padroniza nomes de colunas (lowercase, underscore)"""
    new_columns = []
    for col_name in df.columns:
        new_name = col_name.lower().replace(" ", "_").replace("-", "_")
        new_columns.append(new_name)
    
    for old_name, new_name in zip(df.columns, new_columns):
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df

@TransformationRegistry.register("remove_duplicates")
def remove_duplicates(df: DataFrame) -> DataFrame:
    """Remove registros duplicados"""
    return df.dropDuplicates()

@TransformationRegistry.register("trim_strings")
def trim_strings(df: DataFrame) -> DataFrame:
    """Remove espaços em branco de colunas string"""
    string_columns = [field.name for field in df.schema.fields if field.dataType == StringType()]
    
    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    
    return df

@TransformationRegistry.register("add_row_hash")
def add_row_hash(df: DataFrame) -> DataFrame:
    """Adiciona hash da linha para detecção de mudanças"""
    columns_to_hash = [col(c) for c in df.columns if c not in ["processed_at", "processed_date"]]
    return df.withColumn("row_hash", hash(concat(*columns_to_hash)))

@TransformationRegistry.register("add_surrogate_key")
def add_surrogate_key(df: DataFrame) -> DataFrame:
    """Adiciona chave surrogate incremental"""
    from pyspark.sql.window import Window
    
    window_spec = Window.orderBy(monotonically_increasing_id())
    return df.withColumn("surrogate_key", row_number().over(window_spec))

@TransformationRegistry.register("mask_sensitive_data")
def mask_sensitive_data(df: DataFrame) -> DataFrame:
    """Mascara dados sensíveis comuns"""
    sensitive_columns = ["cpf", "cnpj", "email", "telefone", "phone"]
    
    for col_name in df.columns:
        if any(sensitive in col_name.lower() for sensitive in sensitive_columns):
            if "email" in col_name.lower():
                # Mascarar email: exemplo@domain.com -> ex****@domain.com
                df = df.withColumn(col_name, 
                    regexp_replace(col(col_name), r"(?<=.{2}).(?=.*@)", "*"))
            elif any(doc in col_name.lower() for doc in ["cpf", "cnpj"]):
                # Mascarar documentos: manter apenas primeiros e últimos dígitos
                df = df.withColumn(col_name,
                    regexp_replace(col(col_name), r"(?<=.{3}).(?=.{2})", "*"))
            elif any(phone in col_name.lower() for phone in ["telefone", "phone"]):
                # Mascarar telefone: manter apenas primeiros e últimos dígitos
                df = df.withColumn(col_name,
                    regexp_replace(col(col_name), r"(?<=.{2}).(?=.{2})", "*"))
    
    return df

@TransformationRegistry.register("validate_required_columns")
def validate_required_columns(df: DataFrame) -> DataFrame:
    """Remove registros com campos obrigatórios nulos"""
    required_columns = ["id", "primary_key", "key"]  # Configurável
    
    for col_name in df.columns:
        if any(req in col_name.lower() for req in required_columns):
            df = df.filter(col(col_name).isNotNull())
    
    return df