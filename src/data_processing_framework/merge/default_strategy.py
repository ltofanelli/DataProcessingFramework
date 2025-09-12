"""
Estratégia padrão de merge baseada em primary keys
"""

import logging
from typing import Dict, List
from .base_strategy import MergeStrategy

logger = logging.getLogger(__name__)

class DefaultMergeStrategy(MergeStrategy):
    """
    Estratégia padrão de merge baseada em primary keys
    
    Comportamento:
    - Merge baseado nas primary_keys da configuração
    - Update de todas as colunas quando há match
    - Insert de todas as colunas quando não há match
    """
    
    def get_merge_condition(self, config) -> str:
        """Constrói condição de merge baseada nas PKs"""
        
        # Se há condição customizada, usa ela
        if hasattr(config, 'custom_merge_condition') and config.custom_merge_condition:
            self.logger.debug(f"Usando condição customizada: {config.custom_merge_condition}")
            return config.custom_merge_condition
            
        # Valida se primary_keys existem
        if not hasattr(config, 'primary_keys') or not config.primary_keys:
            raise ValueError("primary_keys é obrigatório para merge padrão")
        
        # Constrói condição baseada nas PKs
        conditions = []
        for pk in config.primary_keys:
            # Remove espaços e caracteres especiais do nome da coluna
            clean_pk = pk.strip()
            conditions.append(f"target.{clean_pk} = source.{clean_pk}")
        
        merge_condition = " AND ".join(conditions)
        self.logger.debug(f"Condição de merge gerada: {merge_condition}")
        return merge_condition
    
    def get_update_set(self, config) -> Dict[str, str]:
        """
        Define campos para update - atualiza todas as colunas exceto PKs
        
        Por padrão, usa "*" para atualizar todas as colunas do source.
        Para mais controle, pode-se sobrescrever este método.
        """
        # Configuração simples: atualizar tudo
        update_set = {"*": "source.*"}
        
        # Se há configuração específica de update, usa ela
        if hasattr(config, 'update_columns') and config.update_columns:
            update_set = {}
            for col in config.update_columns:
                update_set[col] = f"source.{col}"
        
        self.logger.debug(f"Update set: {update_set}")
        return update_set
    
    def get_insert_values(self, config) -> Dict[str, str]:
        """
        Define valores para insert - insere todas as colunas
        """
        # Configuração simples: inserir tudo
        insert_values = {"*": "source.*"}
        
        # Se há configuração específica de insert, usa ela
        if hasattr(config, 'insert_columns') and config.insert_columns:
            insert_values = {}
            for col in config.insert_columns:
                insert_values[col] = f"source.{col}"
        
        self.logger.debug(f"Insert values: {insert_values}")
        return insert_values
    
    def validate_merge_config(self, config) -> bool:
        """Valida configuração para merge padrão"""
        
        if not super().validate_merge_config(config):
            return False
        
        # Validações específicas do merge padrão
        if hasattr(config, 'primary_keys'):
            for pk in config.primary_keys:
                if not pk or not pk.strip():
                    self.logger.error(f"Primary key vazia ou inválida: '{pk}'")
                    return False
        
        # Log de sucesso
        self.logger.debug(
            f"Configuração válida para merge padrão. PKs: {config.primary_keys}"
        )
        return True
    
    def get_strategy_name(self) -> str:
        return "DefaultMerge"

class SimpleUpsertStrategy(DefaultMergeStrategy):
    """
    Estratégia simplificada para upsert básico
    Herda do DefaultMergeStrategy mas com validações mais simples
    """
    
    def __init__(self, primary_keys: List[str]):
        """
        Inicializa com primary keys específicas
        
        Args:
            primary_keys: Lista de colunas que servem como chave primária
        """
        super().__init__()
        self.primary_keys = primary_keys
    
    def get_merge_condition(self, config) -> str:
        """Usa as PKs definidas na inicialização"""
        conditions = []
        for pk in self.primary_keys:
            conditions.append(f"target.{pk} = source.{pk}")
        
        merge_condition = " AND ".join(conditions)
        self.logger.debug(f"Simple upsert condition: {merge_condition}")
        return merge_condition
    
    def validate_merge_config(self, config) -> bool:
        """Validação simplificada usando PKs da inicialização"""
        if not self.primary_keys:
            self.logger.error("Primary keys não definidas na estratégia")
            return False
        
        return True
    
    def get_strategy_name(self) -> str:
        return f"SimpleUpsert({','.join(self.primary_keys)})"