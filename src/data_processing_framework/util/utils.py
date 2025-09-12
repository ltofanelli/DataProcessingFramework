def table_exists(spark, path: str) -> bool:
        """
        Verifica se a tabela Delta existe
        
        Args:
            path: Caminho da tabela
            
        Returns:
            True se a tabela existe, False caso contrário
        """
        from delta import DeltaTable

        try:
            DeltaTable.forPath(spark, path)
            return True
        except:
            return False