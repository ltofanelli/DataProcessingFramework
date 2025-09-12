"""
Módulo core do framework
"""

from .base_pipeline import BasePipeline
from .pipeline_factory import PipelineFactory
from .file_pipeline import FilePipeline
from .query_pipeline import QueryPipeline

__all__ = ["BasePipeline", "PipelineFactory", "FilePipeline"]