"""
Classe base para transformações customizadas
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from data_processing_framework.transformation import TransformationRegistry

class CustomTransformation(ABC):
    """Classe base para transformações customizadas complexas"""
    
    def __init__(self, **kwargs):
        self.config = kwargs
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Método principal de transformação"""
        pass
    
    def register(self, name: str):
        """Registra a transformação no registry"""
        @TransformationRegistry.register(name)
        def wrapper(df: DataFrame) -> DataFrame:
            return self.transform(df)
        return wrapper

class ParameterizedTransformation(CustomTransformation):
    """Base para transformações que recebem parâmetros"""
    
    def __init__(self, **params):
        super().__init__(**params)
        self.params = params
    
    def get_param(self, key: str, default=None):
        """Obtém parâmetro da configuração"""
        return self.params.get(key, default)

# Exemplo de transformação parametrizada
class FilterByDateRange(ParameterizedTransformation):
    """Filtra dados por range de datas"""
    
    def transform(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import col
        
        date_column = self.get_param("date_column", "created_date")
        start_date = self.get_param("start_date")
        end_date = self.get_param("end_date")
        
        if start_date:
            df = df.filter(col(date_column) >= start_date) # type: ignore
        if end_date:
            df = df.filter(col(date_column) <= end_date) # type: ignore
            
        return df

# class AggregateByWindow(ParameterizedTransformation):
#     """Agrega dados por janela temporal"""
    
#     def transform(self, df: DataFrame) -> DataFrame:
#         from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max
        
#         group_columns = self.get_param("group_columns", [])
#         agg_columns = self.get_param("agg_columns", {})  # {"column": "function"}
        
#         if not group_columns:
#             return df
            
#         agg_exprs = []
#         for col_name, func_name in agg_columns.items():
#             if func_name == "sum":
#                 agg_exprs.append(spark_sum(col_name).alias(f"{col_name}_sum"))
#             elif func_name == "count":
#                 agg_exprs.append(count(col_name).alias(f"{col_name}_count"))
#             elif func_name == "avg":
#                 agg_exprs.append(avg(col_name).alias(f"{col_name}_avg"))
#             elif func_name == "max":
#                 agg_exprs.append(spark_max(col_name).alias(f"{col_name}_max"))
        
#         if agg_exprs:
#             return df.groupBy(*group_columns).agg(*agg_exprs)
#         else:
#             return df.groupBy(*group_columns).count()