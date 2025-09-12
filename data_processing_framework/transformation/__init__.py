"""
Módulo de transformações
"""

from .registry import TransformationRegistry
from . import builtin  # Importa transformações pré-definidas

__all__ = ["TransformationRegistry"]