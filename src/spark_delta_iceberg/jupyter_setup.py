# -*- coding: utf-8 -*-
"""
Script para iniciar o Jupyter Lab com o ambiente PySpark configurado.
"""

import os
import sys
import subprocess
from pathlib import Path

def setup_jupyter():
    """
    Configura e inicia o Jupyter Lab com o ambiente PySpark.
    """
    # Obter o diretório do projeto
    project_dir = Path(__file__).resolve().parent.parent.parent
    
    # Configurar variáveis de ambiente para o PySpark
    os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
    os.environ["PYSPARK_DRIVER_PYTHON_OPTS"] = "lab"
    
    # Criar diretório para os notebooks se não existir
    notebooks_dir = project_dir / "notebooks"
    if not notebooks_dir.exists():
        notebooks_dir.mkdir(exist_ok=True)
        print(f"Diretório de notebooks criado: {notebooks_dir}")
    
    # Criar diretório para os dados se não existir
    data_dir = project_dir / "data"
    if not data_dir.exists():
        data_dir.mkdir(exist_ok=True)
        print(f"Diretório de dados criado: {data_dir}")
    
    # Criar diretórios para os warehouses do Delta Lake e Iceberg
    delta_warehouse = project_dir / "spark-warehouse"
    iceberg_warehouse = project_dir / "iceberg-warehouse"
    
    if not delta_warehouse.exists():
        delta_warehouse.mkdir(exist_ok=True)
        print(f"Diretório do warehouse Delta Lake criado: {delta_warehouse}")
    
    if not iceberg_warehouse.exists():
        iceberg_warehouse.mkdir(exist_ok=True)
        print(f"Diretório do warehouse Iceberg criado: {iceberg_warehouse}")
    
    # Adicionar o diretório src ao PYTHONPATH
    src_dir = project_dir / "src"
    sys.path.append(str(src_dir))
    
    print("Ambiente configurado com sucesso!")
    print(f"Diretório do projeto: {project_dir}")
    print(f"Diretório de notebooks: {notebooks_dir}")
    print(f"Diretório de dados: {data_dir}")
    print(f"Diretório do warehouse Delta Lake: {delta_warehouse}")
    print(f"Diretório do warehouse Iceberg: {iceberg_warehouse}")
    
    # Iniciar o Jupyter Lab
    print("\nIniciando Jupyter Lab...")
    subprocess.run(["jupyter", "lab", "--notebook-dir", str(notebooks_dir)])

if __name__ == "__main__":
    setup_jupyter()
