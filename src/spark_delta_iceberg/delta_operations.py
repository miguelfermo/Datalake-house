# -*- coding: utf-8 -*-
"""
Módulo para operações com Delta Lake.
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List, Dict, Any, Optional

class DeltaLakeOperations:
    """
    Classe para operações com Delta Lake.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa a classe com uma sessão Spark.
        
        Args:
            spark (SparkSession): Sessão Spark configurada
        """
        self.spark = spark
    
    def create_table(self, df: DataFrame, table_path: str, mode: str = "overwrite", partition_by: Optional[List[str]] = None) -> None:
        """
        Cria uma tabela Delta Lake.
        
        Args:
            df (DataFrame): DataFrame com os dados
            table_path (str): Caminho para a tabela
            mode (str): Modo de escrita (overwrite, append, etc.)
            partition_by (List[str], optional): Colunas para particionamento
        """
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.save(table_path)
        
        print(f"Tabela Delta Lake criada em: {table_path}")
    
    def read_table(self, table_path: str) -> DataFrame:
        """
        Lê uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            
        Returns:
            DataFrame: DataFrame com os dados da tabela
        """
        return self.spark.read.format("delta").load(table_path)
    
    def insert_data(self, table_path: str, new_data: DataFrame, mode: str = "append") -> None:
        """
        Insere dados em uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            new_data (DataFrame): DataFrame com os novos dados
            mode (str): Modo de escrita (append, overwrite)
        """
        new_data.write.format("delta").mode(mode).save(table_path)
        print(f"Dados inseridos na tabela Delta Lake: {table_path}")
    
    def update_data(self, table_path: str, condition: str, update_expr: Dict[str, str]) -> None:
        """
        Atualiza dados em uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            condition (str): Condição para atualização
            update_expr (Dict[str, str]): Expressões de atualização
        """
        # Criar a expressão de atualização
        set_expr = ", ".join([f"{col} = {expr}" for col, expr in update_expr.items()])
        
        # Executar a atualização
        self.spark.sql(f"""
        UPDATE delta.`{table_path}`
        SET {set_expr}
        WHERE {condition}
        """)
        
        print(f"Dados atualizados na tabela Delta Lake: {table_path}")
    
    def delete_data(self, table_path: str, condition: str) -> None:
        """
        Exclui dados de uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            condition (str): Condição para exclusão
        """
        self.spark.sql(f"""
        DELETE FROM delta.`{table_path}`
        WHERE {condition}
        """)
        
        print(f"Dados excluídos da tabela Delta Lake: {table_path}")
    
    def merge_data(self, table_path: str, source_df: DataFrame, merge_condition: str, 
                  matched_update: Optional[Dict[str, str]] = None, 
                  not_matched_insert: bool = True) -> None:
        """
        Realiza operação MERGE em uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            source_df (DataFrame): DataFrame com os dados de origem
            merge_condition (str): Condição para o MERGE
            matched_update (Dict[str, str], optional): Expressões de atualização para registros correspondentes
            not_matched_insert (bool): Se deve inserir registros não correspondentes
        """
        # Registrar o DataFrame de origem como uma view temporária
        source_view = "source_data_temp"
        source_df.createOrReplaceTempView(source_view)
        
        # Construir a consulta MERGE
        merge_query = f"""
        MERGE INTO delta.`{table_path}` target
        USING {source_view} source
        ON {merge_condition}
        """
        
        # Adicionar cláusula WHEN MATCHED
        if matched_update:
            set_expr = ", ".join([f"target.{col} = {expr}" for col, expr in matched_update.items()])
            merge_query += f"""
            WHEN MATCHED THEN UPDATE SET
            {set_expr}
            """
        
        # Adicionar cláusula WHEN NOT MATCHED
        if not_matched_insert:
            merge_query += """
            WHEN NOT MATCHED THEN INSERT *
            """
        
        # Executar o MERGE
        self.spark.sql(merge_query)
        
        print(f"Operação MERGE concluída na tabela Delta Lake: {table_path}")
    
    def time_travel(self, table_path: str, version: Optional[int] = None, timestamp: Optional[str] = None) -> DataFrame:
        """
        Realiza time travel em uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            version (int, optional): Versão específica
            timestamp (str, optional): Timestamp específico (formato: 'yyyy-MM-dd HH:mm:ss')
            
        Returns:
            DataFrame: DataFrame com os dados da versão especificada
        """
        reader = self.spark.read.format("delta")
        
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp is not None:
            reader = reader.option("timestampAsOf", timestamp)
        
        return reader.load(table_path)
    
    def get_history(self, table_path: str) -> DataFrame:
        """
        Obtém o histórico de uma tabela Delta Lake.
        
        Args:
            table_path (str): Caminho para a tabela
            
        Returns:
            DataFrame: DataFrame com o histórico da tabela
        """
        return self.spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`")
