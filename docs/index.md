# Apache Spark com Delta Lake e Apache Iceberg

Bem-vindo à documentação do projeto de Apache Spark com Delta Lake e Apache Iceberg.

## Visão Geral

Este projeto implementa um ambiente PySpark com Jupyter Labs, integrando Delta Lake e Apache Iceberg para demonstrar operações de INSERT, UPDATE e DELETE em tabelas de dados.

## Estrutura do Projeto

```
Datalake-house/
├── data/                    # Diretório para armazenar dados
├── docs/                    # Documentação do projeto (MkDocs)
├── notebooks/               # Notebooks Jupyter
│   ├── delta_lake_operations.ipynb       # Exemplos de operações com Delta Lake
│   ├── iceberg_operations.ipynb          # Exemplos de operações com Apache Iceberg
│   └── delta_iceberg_comparison.ipynb    # Comparação entre Delta Lake e Apache Iceberg
├── src/                     # Código-fonte do projeto
│   └── spark_delta_iceberg/ # Pacote principal
├── tests/                   # Testes unitários
├── .gitignore               # Arquivos a serem ignorados pelo Git
├── mkdocs.yml               # Configuração do MkDocs
├── pyproject.toml           # Configuração do projeto Python
└── README.md                # Documentação principal
```

## Tecnologias Utilizadas

- **Apache Spark**: Framework de processamento distribuído para big data
- **Delta Lake**: Camada de armazenamento para data lakes que traz confiabilidade ao Apache Spark
- **Apache Iceberg**: Formato de tabela de alto desempenho para conjuntos de dados analíticos enormes
- **Jupyter Labs**: Ambiente interativo para desenvolvimento e análise de dados
- **UV**: Gerenciador de pacotes Python utilizado no projeto

## Requisitos

- Python 3.8 ou superior
- UV (gerenciador de pacotes Python)
- Java 8 ou superior (necessário para o Apache Spark)

## Navegação

Utilize o menu de navegação para acessar as páginas específicas sobre:

- [Delta Lake](delta-lake.md): Detalhes sobre Delta Lake e exemplos de operações
- [Apache Iceberg](apache-iceberg.md): Detalhes sobre Apache Iceberg e exemplos de operações

## Autores

Alexandre Destro Zanoni
Miguel Rossi Fermo