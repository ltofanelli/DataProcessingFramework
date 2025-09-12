"""
Estratégias customizadas de merge para casos específicos
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
from .base_strategy import MergeStrategy

logger = logging.getLogger(__name__)

class CustomMergeStrategy(MergeStrategy):
    """
    Estratégia de merge completamente customizada
    
    Permite definir condições, updates e inserts específicos
    """
    
    def __init__(self, 
                 merge_condition: str, 
                 update_set: Optional[Dict[str, str]] = None, 
                 insert_values: Optional[Dict[str, str]] = None,
                 delete_condition: Optional[str] = None):
        """
        Inicializa estratégia customizada
        
        Args:
            merge_condition: Condição SQL para merge (ex: "target.id = source.id")
            update_set: Campos para update (ex: {"nome": "source.nome"})
            insert_values: Valores para insert (ex: {"*": "source.*"})
            delete_condition: Condição para delete (opcional)
        """
        super().__init__()
        self.merge_condition = merge_condition
        self.update_set = update_set or {"*": "source.*"}
        self.insert_values = insert_values or {"*": "source.*"}
        self.delete_condition = delete_condition
    
    def get_merge_condition(self, config) -> str:
        """Retorna condição customizada"""
        return self.merge_condition
    
    def get_update_set(self, config) -> Dict[str, str]:
        """Retorna campos customizados para update"""
        return self.update_set
    
    def get_insert_values(self, config) -> Dict[str, str]:
        """Retorna valores customizados para insert"""
        return self.insert_values
    
    def get_delete_condition(self, config) -> Optional[str]:
        """Retorna condição para delete se configurada"""
        return self.delete_condition
    
    def supports_delete(self) -> bool:
        """Suporta delete se condição foi definida"""
        return self.delete_condition is not None
    
    def validate_merge_config(self, config) -> bool:
        """Validação customizada"""
        if not self.merge_condition:
            self.logger.error("Condição de merge não pode ser vazia")
            return False
        
        return True
    
    def get_strategy_name(self) -> str:
        return "CustomMerge"

class SCD2MergeStrategy(MergeStrategy):
    """
    Estratégia de merge para SCD2 (Slowly Changing Dimensions Type 2)
    
    Implementa o padrão SCD2 onde:
    - Registros que mudaram são marcados como não-atuais (is_current = false)
    - Novos registros são inseridos como atuais (is_current = true)
    - Campos de versionamento são gerenciados automaticamente
    """
    
    def __init__(self, business_keys: List[str], tracked_columns: List[str],
                 version_column: str = "version",
                 valid_from_column: str = "valid_from",
                 valid_to_column: str = "valid_to",
                 is_current_column: str = "is_current"):
        """
        Inicializa estratégia SCD2
        
        Args:
            business_keys: Chaves de negócio (não mudam)
            tracked_columns: Colunas rastreadas para mudanças
            version_column: Nome da coluna de versão
            valid_from_column: Nome da coluna de data início
            valid_to_column: Nome da coluna de data fim
            is_current_column: Nome da coluna de registro atual
        """
        super().__init__()
        self.business_keys = business_keys
        self.tracked_columns = tracked_columns
        self.version_column = version_column
        self.valid_from_column = valid_from_column
        self.valid_to_column = valid_to_column
        self.is_current_column = is_current_column
    
    def get_merge_condition(self, config) -> str:
        """Condição para SCD2: match por business key e registro atual"""
        conditions = []
        
        # Condições das business keys
        for key in self.business_keys:
            conditions.append(f"target.{key} = source.{key}")
        
        # Só considera registros atuais no target
        conditions.append(f"target.{self.is_current_column} = true")
        
        return " AND ".join(conditions)
    
    def get_update_set(self, config) -> Dict[str, str]:
        """
        Update para SCD2: fecha registro atual se houve mudança
        """
        # Constrói condição de mudança para colunas rastreadas
        change_conditions = []
        for col in self.tracked_columns:
            change_conditions.append(f"target.{col} != source.{col}")
        
        change_condition = " OR ".join(change_conditions)
        
        # Se houve mudança, fecha o registro atual
        update_set = {
            self.is_current_column: f"CASE WHEN ({change_condition}) THEN false ELSE target.{self.is_current_column} END",
            self.valid_to_column: f"CASE WHEN ({change_condition}) THEN current_timestamp() ELSE target.{self.valid_to_column} END"
        }
        
        # Mantém outros campos inalterados quando há mudança
        for col in self.tracked_columns:
            update_set[col] = f"target.{col}"
        
        return update_set
    
    def get_insert_values(self, config) -> Dict[str, str]:
        """
        Insert para SCD2: sempre insere novos registros como atuais
        """
        insert_values = {"*": "source.*"}
        
        # Sobrescreve campos de controle SCD2
        insert_values.update({
            self.valid_from_column: "current_timestamp()",
            self.valid_to_column: "null",
            self.is_current_column: "true"
        })
        
        # Se há coluna de versão, inicializa com 1
        if hasattr(config, 'columns') and self.version_column in config.columns:
            insert_values[self.version_column] = "1"
        
        return insert_values
    
    def validate_merge_config(self, config) -> bool:
        """Validação específica para SCD2"""
        
        if not self.business_keys:
            self.logger.error("Business keys são obrigatórias para SCD2")
            return False
        
        if not self.tracked_columns:
            self.logger.error("Tracked columns são obrigatórias para SCD2")
            return False
        
        return True
    
    def get_strategy_name(self) -> str:
        return f"SCD2({','.join(self.business_keys)})"

class UpsertOnlyStrategy(MergeStrategy):
    """
    Estratégia que só faz upsert, sem deletes
    Ideal para tabelas onde registros nunca são removidos
    """
    
    def __init__(self, primary_keys: List[str], 
                 update_columns: Optional[List[str]] = None,
                 exclude_columns: Optional[List[str]] = None):
        """
        Inicializa estratégia upsert-only
        
        Args:
            primary_keys: Chaves primárias para merge
            update_columns: Colunas específicas para update (None = todas)
            exclude_columns: Colunas para excluir do update
        """
        super().__init__()
        self.primary_keys = primary_keys
        self.update_columns = update_columns
        self.exclude_columns = exclude_columns or []
    
    def get_merge_condition(self, config) -> str:
        """Condição baseada em PKs"""
        conditions = []
        for pk in self.primary_keys:
            conditions.append(f"target.{pk} = source.{pk}")
        return " AND ".join(conditions)
    
    def get_update_set(self, config) -> Dict[str, str]:
        """Update apenas colunas específicas ou todas exceto exclusões"""
        
        if self.update_columns:
            # Update apenas colunas específicas
            update_set = {}
            for col in self.update_columns:
                if col not in self.exclude_columns:
                    update_set[col] = f"source.{col}"
        else:
            # Update todas as colunas exceto exclusões e PKs
            update_set = {"*": "source.*"}
            
            # Se há exclusões, precisa ser mais específico
            if self.exclude_columns:
                update_set = {}
                # Aqui seria necessário conhecer todas as colunas da tabela
                # Para simplificar, mantemos o "*" e logamos a limitação
                self.logger.warning(
                    "exclude_columns não suportado com update '*'. "
                    "Use update_columns para controle específico."
                )
                update_set = {"*": "source.*"}
        
        return update_set
    
    def get_insert_values(self, config) -> Dict[str, str]:
        """Insert todas as colunas"""
        return {"*": "source.*"}
    
    def validate_merge_config(self, config) -> bool:
        """Validação para upsert-only"""
        
        if not self.primary_keys:
            self.logger.error("Primary keys são obrigatórias para UpsertOnly")
            return False
        
        return True
    
    def get_strategy_name(self) -> str:
        return f"UpsertOnly({','.join(self.primary_keys)})"

class TimestampBasedMergeStrategy(MergeStrategy):
    """
    Estratégia de merge baseada em timestamp
    Só atualiza se o registro source é mais recente que o target
    """
    
    def __init__(self, primary_keys: List[str], timestamp_column: str = "modified_date"):
        """
        Inicializa estratégia baseada em timestamp
        
        Args:
            primary_keys: Chaves primárias para merge
            timestamp_column: Coluna de timestamp para comparação
        """
        super().__init__()
        self.primary_keys = primary_keys
        self.timestamp_column = timestamp_column
    
    def get_merge_condition(self, config) -> str:
        """Condição baseada em PKs"""
        conditions = []
        for pk in self.primary_keys:
            conditions.append(f"target.{pk} = source.{pk}")
        return " AND ".join(conditions)
    
    def get_update_set(self, config) -> Dict[str, str]:
        """Update apenas se source é mais recente"""
        
        # Condição: só atualiza se source é mais recente
        timestamp_condition = f"source.{self.timestamp_column} > target.{self.timestamp_column}"
        
        update_set = {}
        
        # Atualiza timestamp sempre
        update_set[self.timestamp_column] = f"source.{self.timestamp_column}"
        
        # Para outras colunas, só atualiza se mais recente
        # Simplificado: atualiza tudo se condição for verdadeira
        update_set["*"] = f"CASE WHEN ({timestamp_condition}) THEN source.* ELSE target.* END"
        
        return update_set
    
    def get_insert_values(self, config) -> Dict[str, str]:
        """Insert todas as colunas"""
        return {"*": "source.*"}
    
    def validate_merge_config(self, config) -> bool:
        """Validação para timestamp-based merge"""
        
        if not self.primary_keys:
            self.logger.error("Primary keys são obrigatórias")
            return False
        
        if not self.timestamp_column:
            self.logger.error("Timestamp column é obrigatória")
            return False
        
        return True
    
    def get_strategy_name(self) -> str:
        return f"TimestampBased({self.timestamp_column})"

# Funções utilitárias para criação rápida de estratégias
def create_simple_upsert(primary_keys: List[str]) -> CustomMergeStrategy:
    """Cria estratégia de upsert simples"""
    condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_keys])
    return CustomMergeStrategy(
        merge_condition=condition,
        update_set={"*": "source.*"},
        insert_values={"*": "source.*"}
    )

def create_scd2_strategy(business_keys: List[str], 
                        tracked_columns: List[str]) -> SCD2MergeStrategy:
    """Cria estratégia SCD2 com configurações padrão"""
    return SCD2MergeStrategy(
        business_keys=business_keys,
        tracked_columns=tracked_columns
    )

def create_timestamp_strategy(primary_keys: List[str], 
                            timestamp_column: str = "modified_date") -> TimestampBasedMergeStrategy:
    """Cria estratégia baseada em timestamp"""
    return TimestampBasedMergeStrategy(
        primary_keys=primary_keys,
        timestamp_column=timestamp_column
    )