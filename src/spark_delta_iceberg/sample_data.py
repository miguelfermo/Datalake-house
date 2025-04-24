# -*- coding: utf-8 -*-
"""
Módulo para carregar dados de exemplo para os notebooks.
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from typing import Tuple

def create_sample_dataframe(spark: SparkSession) -> DataFrame:
    """
    Cria um DataFrame de exemplo com dados de vendas.
    
    Args:
        spark (SparkSession): Sessão Spark configurada
        
    Returns:
        DataFrame: DataFrame com dados de vendas de exemplo
    """
    # Criar dados de exemplo
    data = [
        (1, "Produto A", "Eletrônicos", 1500.00, "2023-01-15", "SP"),
        (2, "Produto B", "Vestuário", 89.90, "2023-01-16", "RJ"),
        (3, "Produto C", "Alimentos", 25.50, "2023-01-16", "MG"),
        (4, "Produto D", "Eletrônicos", 3200.00, "2023-01-17", "SP"),
        (5, "Produto E", "Vestuário", 129.90, "2023-01-18", "SC"),
        (6, "Produto F", "Alimentos", 18.75, "2023-01-19", "RJ"),
        (7, "Produto G", "Eletrônicos", 899.00, "2023-01-20", "SP"),
        (8, "Produto H", "Vestuário", 59.90, "2023-01-21", "MG"),
        (9, "Produto I", "Alimentos", 32.80, "2023-01-22", "SC"),
        (10, "Produto J", "Eletrônicos", 2100.00, "2023-01-23", "SP")
    ]
    
    # Definir o esquema
    schema = "id INT, nome STRING, categoria STRING, preco DOUBLE, data_venda STRING, estado STRING"
    
    # Criar o DataFrame
    df = spark.createDataFrame(data, schema)
    
    return df

def create_sample_update_dataframe(spark: SparkSession) -> DataFrame:
    """
    Cria um DataFrame de exemplo para atualização.
    
    Args:
        spark (SparkSession): Sessão Spark configurada
        
    Returns:
        DataFrame: DataFrame com dados para atualização
    """
    # Criar dados de exemplo para atualização
    data = [
        (2, "Produto B", "Vestuário", 99.90, "2023-01-16", "RJ"),  # Preço atualizado
        (5, "Produto E Premium", "Vestuário", 149.90, "2023-01-18", "SC"),  # Nome e preço atualizados
        (11, "Produto K", "Eletrônicos", 1800.00, "2023-01-24", "RS"),  # Novo produto
        (12, "Produto L", "Alimentos", 45.60, "2023-01-25", "PR")   # Novo produto
    ]
    
    # Definir o esquema
    schema = "id INT, nome STRING, categoria STRING, preco DOUBLE, data_venda STRING, estado STRING"
    
    # Criar o DataFrame
    df = spark.createDataFrame(data, schema)
    
    return df

def load_public_dataset(spark: SparkSession) -> Tuple[DataFrame, str]:
    """
    Carrega um conjunto de dados público.
    
    Args:
        spark (SparkSession): Sessão Spark configurada
        
    Returns:
        Tuple[DataFrame, str]: DataFrame com os dados e descrição do dataset
    """
    # URL do dataset público (CSV de vendas de supermercado)
    url = "https://raw.githubusercontent.com/IBM/developer-dataset/master/supermarket-sales.csv"
    
    # Descrição do dataset
    description = """
    Dataset de Vendas de Supermercado
    
    Este conjunto de dados contém dados históricos de vendas de uma rede de supermercados.
    Os dados incluem informações sobre produtos, preços, quantidades, métodos de pagamento e outros detalhes de vendas.
    
    Fonte: IBM Developer Dataset (https://github.com/IBM/developer-dataset)
    """
    
    try:
        # Baixar o CSV usando pandas
        pdf = pd.read_csv(url)
        
        # Converter para DataFrame do Spark
        df = spark.createDataFrame(pdf)
        
        # Retornar o DataFrame e a descrição
        return df, description
    except Exception as e:
        print(f"Erro ao carregar o dataset público: {e}")
        
        # Em caso de erro, retornar o DataFrame de exemplo
        return create_sample_dataframe(spark), "Dataset de exemplo (fallback devido a erro no carregamento do dataset público)"
