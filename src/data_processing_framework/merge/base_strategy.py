"""
Classe base para estratégias de merge
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class MergeStrategy(ABC):
    """Estratégia base para operações de merge Delta"""
    
    def __init__(self):
        self.logger = logger
    
    @abstractmethod
    def get_merge_condition(self, config) -> str:
        """
        Retorna condição SQL para merge
        
        Args:
            config: PipelineConfig com configurações do pipeline
            
        Returns:
            String com condição SQL (ex: "target.id = source.id")
        """
        pass
    
    @abstractmethod
    def get_update_set(self, config) -> Dict[str, str]:
        """
        Retorna conjunto de campos para update quando há match
        
        Args:
            config: PipelineConfig com configurações do pipeline
            
        Returns:
            Dict com mapeamento coluna -> valor (ex: {"nome": "source.nome"})
        """
        pass
    
    @abstractmethod
    def get_insert_values(self, config) -> Dict[str, str]:
        """
        Retorna valores para insert quando não há match
        
        Args:
            config: PipelineConfig com configurações do pipeline
            
        Returns:
            Dict com mapeamento coluna -> valor (ex: {"*": "source.*"})
        """
        pass
    
    def validate_merge_config(self, config) -> bool:
        """
        Valida se a configuração é válida para merge
        
        Args:
            config: PipelineConfig para validar
            
        Returns:
            True se válida, False caso contrário
        """
        if not hasattr(config, 'primary_keys') or not config.primary_keys:
            self.logger.error("primary_keys é obrigatório para operações de merge")
            return False
        return True
    
    def get_delete_condition(self, config) -> Optional[str]:
        """
        Retorna condição para delete (opcional)
        
        Args:
            config: PipelineConfig com configurações do pipeline
            
        Returns:
            String com condição SQL para delete ou None
        """
        return None
    
    def supports_delete(self) -> bool:
        """Indica se a estratégia suporta operações de delete"""
        return False
    
    def get_strategy_name(self) -> str:
        """Retorna nome da estratégia para logging"""
        return self.__class__.__name__
    
    def log_merge_operation(self, config, operation_type: str):
        """
        Log da operação de merge
        
        Args:
            config: PipelineConfig
            operation_type: Tipo da operação (MERGE, UPSERT, etc.)
        """
        self.logger.info(
            f"Executando {operation_type} com estratégia {self.get_strategy_name()} "
            f"para tabela {config.target_table}"
        )