# -*- coding: utf-8 -*-
"""
Módulo para configuração da sessão Spark com suporte a Delta Lake e Apache Iceberg.
"""

from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

def create_spark_session(app_name="SparkDeltaIceberg"):
    """
    Cria e configura uma sessão Spark com suporte a Delta Lake e Apache Iceberg.
    
    Args:
        app_name (str): Nome da aplicação Spark
        
    Returns:
        SparkSession: Sessão Spark configurada
    """
    # Carregar variáveis de ambiente se existirem
    load_dotenv()
    
    # Configurar o builder da sessão Spark
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        # Configurações para o Delta Lake
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Configurações para o Apache Iceberg
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "iceberg-warehouse")
    )
    
    # Adicionar pacotes necessários
    jars = [
        "io.delta:delta-core_2.12:2.4.0",
        "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1"
    ]
    
    # Adicionar os JARs à configuração
    builder = builder.config("spark.jars.packages", ",".join(jars))
    
    # Criar a sessão Spark
    spark = builder.getOrCreate()
    
    # Configurar o nível de log
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
