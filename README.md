# Apache Spark com Delta Lake e Apache Iceberg

Este projeto implementa um ambiente PySpark com Jupyter Labs, integrando Delta Lake e Apache Iceberg para demonstrar operações de INSERT, UPDATE e DELETE em tabelas de dados.

## Estrutura do Projeto

```
spark-delta-iceberg/
├── data/                    # Diretório para armazenar dados
├── docs/                    # Documentação do projeto
│   └── pesquisa.md          # Pesquisa sobre Apache Spark, Delta Lake e Apache Iceberg
├── notebooks/               # Notebooks Jupyter
│   ├── delta_lake_operations.ipynb       # Exemplos de operações com Delta Lake
│   ├── iceberg_operations.ipynb          # Exemplos de operações com Apache Iceberg
│   └── delta_iceberg_comparison.ipynb    # Comparação entre Delta Lake e Apache Iceberg
├── src/                     # Código-fonte do projeto
│   └── spark_delta_iceberg/ # Pacote principal
│       ├── __init__.py      # Inicializador do pacote
│       ├── spark_session.py # Configuração da sessão Spark
│       ├── delta_operations.py # Operações com Delta Lake
│       ├── iceberg_operations.py # Operações com Apache Iceberg
│       ├── sample_data.py   # Geração de dados de exemplo
│       └── jupyter_setup.py # Configuração do ambiente Jupyter
├── tests/                   # Testes unitários
├── .gitignore               # Arquivos a serem ignorados pelo Git
├── pyproject.toml           # Configuração do projeto Python
└── README.md                # Este arquivo
```

## Requisitos

- Python 3.8 ou superior
- UV (gerenciador de pacotes Python)
- Java 8 ou superior (necessário para o Apache Spark)

## Configuração do Ambiente

### 1. Instalação do UV

O UV é um gerenciador de pacotes Python rápido e confiável. Para instalá-lo, execute:

```bash
pip install uv
```

### 2. Clonagem do Repositório

Clone este repositório e navegue até o diretório do projeto:

```bash
git clone https://github.com/miguelfermo/Datalake-house.git
cd spark-delta-iceberg
```

### 3. Criação do Ambiente Virtual

Crie um ambiente virtual usando o UV:

```bash
uv venv
```

### 4. Ativação do Ambiente Virtual

Ative o ambiente virtual:

```bash
# No Linux/macOS
source .venv/bin/activate

# No Windows
.venv\Scripts\activate
```

### 5. Instalação das Dependências

Instale as dependências do projeto usando o UV:

```bash
uv pip install pyspark delta-spark jupyter jupyterlab pandas matplotlib pyarrow python-dotenv
```

## Execução do Ambiente Jupyter

Para iniciar o Jupyter Lab com o ambiente PySpark configurado, execute:

```bash
python -m src.spark_delta_iceberg.jupyter_setup
```

Isso iniciará o Jupyter Lab no diretório de notebooks, com todas as configurações necessárias para o PySpark, Delta Lake e Apache Iceberg.

## Detalhes de Implementação

### Configuração da Sessão Spark

A sessão Spark é configurada com suporte a Delta Lake e Apache Iceberg através do módulo `spark_session.py`. As principais configurações incluem:

```python
# Configurações para o Delta Lake
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
.config("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Configurações para o Apache Iceberg
.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.iceberg.type", "hadoop")
.config("spark.sql.catalog.iceberg.warehouse", "iceberg-warehouse")
```


## Versões das Bibliotecas

- Python: 3.10.12
- PySpark: 3.5.5
- Delta Lake: 3.3.0
- Jupyter: 1.1.1
- JupyterLab: 4.4.1
- Pandas: 2.2.3
- Matplotlib: 3.10.1
- PyArrow: 19.0.1
- Python-dotenv: 1.1.0

## Referências

- [Apache Spark](https://spark.apache.org/)
- [Delta Lake](https://delta.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Databricks](https://databricks.com/)
- [Canal DataWay BR no YouTube](https://www.youtube.com/c/DataWayBR)
- [Projeto spark-delta no GitHub](https://github.com/jlsilva01/spark-delta)
- [Projeto spark-iceberg no GitHub](https://github.com/jlsilva01/spark-iceberg)

## Autores

- Alexandre Destro Zanoni
- Miguel Rossi Fermo

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.
