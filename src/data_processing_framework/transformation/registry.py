"""
Registry para transformações customizadas
"""

import logging
from typing import List, Optional, Callable
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

class TransformationRegistry:
    """Registry para transformações customizadas"""
    
    _transformations = {}
    
    @classmethod
    def register(cls, name: str):
        """Decorator para registrar transformações"""
        def decorator(func):
            cls._transformations[name] = func
            logger.info(f"Transformação '{name}' registrada")
            return func
        return decorator
    
    @classmethod
    def get_transformation(cls, name: str) -> Optional[Callable[[DataFrame], DataFrame]]:
        """Obtém transformação registrada"""
        return cls._transformations.get(name)
    
    @classmethod
    def apply_transformations(cls, df: DataFrame, transformation_names: List[str]) -> DataFrame:
        """Aplica múltiplas transformações"""
        result_df = df
        for name in transformation_names or []:
            transformation = cls.get_transformation(name)
            if transformation:
                logger.info(f"Aplicando transformação: {name}")
                result_df = transformation(result_df)
            else:
                logger.warning(f"Transformação '{name}' não encontrada")
        return result_df
    
    @classmethod
    def list_transformations(cls) -> List[str]:
        """Lista todas as transformações registradas"""
        return list(cls._transformations.keys())
    
    @classmethod
    def clear_transformations(cls):
        """Limpa todas as transformações (útil para testes)"""
        cls._transformations.clear()
    
    @classmethod
    def unregister(cls, name: str) -> bool:
        """Remove uma transformação do registry"""
        if name in cls._transformations:
            del cls._transformations[name]
            logger.info(f"Transformação '{name}' removida")
            return True
        return False