"""
Módulo de estratégias de merge
"""

from .base_strategy import MergeStrategy
from .default_strategy import DefaultMergeStrategy
from .custom_strategy import CustomMergeStrategy, SCD2MergeStrategy, UpsertOnlyStrategy

__all__ = [
    "MergeStrategy", 
    "DefaultMergeStrategy", 
    "CustomMergeStrategy",
    "SCD2MergeStrategy",
    "UpsertOnlyStrategy"
]