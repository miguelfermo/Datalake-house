# -*- coding: utf-8 -*-
"""
Módulo para operações com Apache Iceberg.
"""

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from typing import List, Dict, Any, Optional

class IcebergOperations:
    """
    Classe para operações com Apache Iceberg.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Inicializa a classe com uma sessão Spark.
        
        Args:
            spark (SparkSession): Sessão Spark configurada
        """
        self.spark = spark
        self.catalog = "iceberg"
    
    def create_table(self, df: DataFrame, table_name: str, database: str = "default", 
                    partition_by: Optional[List[str]] = None) -> None:
        """
        Cria uma tabela Iceberg.
        
        Args:
            df (DataFrame): DataFrame com os dados
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            partition_by (List[str], optional): Colunas para particionamento
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        
        # Criar a tabela
        if partition_by:
            partition_spec = ", ".join(partition_by)
            self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {full_table_name}
            USING iceberg
            PARTITIONED BY ({partition_spec})
            AS SELECT * FROM temp_view
            """)
        else:
            df.writeTo(full_table_name).using("iceberg").createOrReplace()
        
        print(f"Tabela Iceberg criada: {full_table_name}")
    
    def read_table(self, table_name: str, database: str = "default") -> DataFrame:
        """
        Lê uma tabela Iceberg.
        
        Args:
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            
        Returns:
            DataFrame: DataFrame com os dados da tabela
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        return self.spark.table(full_table_name)
    
    def insert_data(self, df: DataFrame, table_name: str, database: str = "default") -> None:
        """
        Insere dados em uma tabela Iceberg.
        
        Args:
            df (DataFrame): DataFrame com os novos dados
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        df.writeTo(full_table_name).append()
        
        print(f"Dados inseridos na tabela Iceberg: {full_table_name}")
    
    def update_data(self, table_name: str, database: str, condition: str, update_expr: Dict[str, str]) -> None:
        """
        Atualiza dados em uma tabela Iceberg.
        
        Args:
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            condition (str): Condição para atualização
            update_expr (Dict[str, str]): Expressões de atualização
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        
        # Criar a expressão de atualização
        set_expr = ", ".join([f"{col} = {expr}" for col, expr in update_expr.items()])
        
        # Executar a atualização
        self.spark.sql(f"""
        UPDATE {full_table_name}
        SET {set_expr}
        WHERE {condition}
        """)
        
        print(f"Dados atualizados na tabela Iceberg: {full_table_name}")
    
    def delete_data(self, table_name: str, database: str, condition: str) -> None:
        """
        Exclui dados de uma tabela Iceberg.
        
        Args:
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            condition (str): Condição para exclusão
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        
        self.spark.sql(f"""
        DELETE FROM {full_table_name}
        WHERE {condition}
        """)
        
        print(f"Dados excluídos da tabela Iceberg: {full_table_name}")
    
    def merge_data(self, source_df: DataFrame, table_name: str, database: str, 
                  merge_condition: str, matched_update: Optional[Dict[str, str]] = None, 
                  not_matched_insert: bool = True) -> None:
        """
        Realiza operação MERGE em uma tabela Iceberg.
        
        Args:
            source_df (DataFrame): DataFrame com os dados de origem
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            merge_condition (str): Condição para o MERGE
            matched_update (Dict[str, str], optional): Expressões de atualização para registros correspondentes
            not_matched_insert (bool): Se deve inserir registros não correspondentes
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        
        # Registrar o DataFrame de origem como uma view temporária
        source_view = "source_data_temp"
        source_df.createOrReplaceTempView(source_view)
        
        # Construir a consulta MERGE
        merge_query = f"""
        MERGE INTO {full_table_name} target
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
        
        print(f"Operação MERGE concluída na tabela Iceberg: {full_table_name}")
    
    def time_travel(self, table_name: str, database: str = "default", 
                   snapshot_id: Optional[str] = None, as_of_timestamp: Optional[str] = None) -> DataFrame:
        """
        Realiza time travel em uma tabela Iceberg.
        
        Args:
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            snapshot_id (str, optional): ID do snapshot específico
            as_of_timestamp (str, optional): Timestamp específico (formato: 'yyyy-MM-dd HH:mm:ss')
            
        Returns:
            DataFrame: DataFrame com os dados da versão especificada
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        
        if snapshot_id is not None:
            return self.spark.read.option("snapshot-id", snapshot_id).table(full_table_name)
        elif as_of_timestamp is not None:
            return self.spark.read.option("as-of-timestamp", as_of_timestamp).table(full_table_name)
        else:
            return self.spark.table(full_table_name)
    
    def get_snapshots(self, table_name: str, database: str = "default") -> DataFrame:
        """
        Obtém os snapshots de uma tabela Iceberg.
        
        Args:
            table_name (str): Nome da tabela
            database (str): Nome do banco de dados
            
        Returns:
            DataFrame: DataFrame com os snapshots da tabela
        """
        full_table_name = f"{self.catalog}.{database}.{table_name}"
        return self.spark.sql(f"SELECT * FROM {full_table_name}.snapshots")
