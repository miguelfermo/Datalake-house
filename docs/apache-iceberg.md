# Apache Iceberg

O Apache Iceberg é um formato de tabela de alto desempenho para conjuntos de dados analíticos enormes. Nesta página, demonstramos as operações básicas de INSERT, UPDATE e DELETE usando Apache Iceberg com Apache Spark.

## O que é Apache Iceberg?

Apache Iceberg é um formato de tabela aberta para conjuntos de dados analíticos, projetado para abordar os desafios da gestão e consulta de grandes conjuntos de dados. Iceberg traz a confiabilidade e simplicidade das tabelas SQL para big data, enquanto torna possível que motores como Spark, Trino, Flink, Presto, Hive e Impala trabalhem com segurança com as mesmas tabelas, ao mesmo tempo.

### Principais Características

- **Formato de Tabela Aberta**: Iceberg é um formato de tabela aberto que permite que diferentes motores de processamento acessem os mesmos dados.
- **Transações ACID**: Garantem a consistência dos dados mesmo em caso de falhas.
- **Time Travel**: Permite acessar versões anteriores dos dados para auditoria ou rollback.
- **Evolução de Esquema**: Permite alterar o esquema dos dados sem afetar os consumidores.
- **Operações SQL Expressivas**: Suporte a operações como UPDATE, DELETE e MERGE, que não são nativas em formatos tradicionais de data lake.
- **Particionamento Oculto**: O particionamento é gerenciado pelo Iceberg, não pelo usuário, o que simplifica o uso.

## Configuração do Ambiente

Para trabalhar com Apache Iceberg, você precisa configurar uma sessão Spark com suporte ao Iceberg:

```python
from pyspark.sql import SparkSession

# Criar a sessão Spark com suporte ao Apache Iceberg
spark = SparkSession.builder \
    .appName("IcebergDemo") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "iceberg-warehouse") \
    .getOrCreate()
```

## Operações com Apache Iceberg

### Operação INSERT

O Apache Iceberg suporta operações de inserção de dados de forma simples e eficiente. Veja como criar uma tabela Iceberg e inserir dados:

```python
# Registrar o DataFrame como uma view temporária
df_vendas.createOrReplaceTempView("temp_view")

# Criar uma tabela Iceberg
spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.default.vendas_iceberg
USING iceberg
PARTITIONED BY (estado)
AS SELECT * FROM temp_view
""")

# Inserir novos dados
df_novos.writeTo("iceberg.default.vendas_iceberg").append()
```

### Operação UPDATE

O Apache Iceberg permite atualizações de dados usando SQL:

```python
# Atualizar o preço do Produto B
spark.sql("""
UPDATE iceberg.default.vendas_iceberg
SET preco = 99.90
WHERE nome = 'Produto B'
""")

# Atualizar o nome e o preço do Produto E
spark.sql("""
UPDATE iceberg.default.vendas_iceberg
SET nome = 'Produto E Premium', preco = 149.90
WHERE nome = 'Produto E'
""")
```

### Operação DELETE

O Apache Iceberg suporta exclusão de dados de forma eficiente:

```python
# Excluir produtos da categoria Alimentos com preço menor que 30
spark.sql("""
DELETE FROM iceberg.default.vendas_iceberg
WHERE categoria = 'Alimentos' AND preco < 30
""")
```

### Operação MERGE

O MERGE é uma operação poderosa que permite atualizar e inserir dados em uma única operação:

```python
# Registrar o DataFrame de origem como uma view temporária
df_update.createOrReplaceTempView("source_data_temp")

# Realizar a operação MERGE
spark.sql("""
MERGE INTO iceberg.default.vendas_iceberg target
USING source_data_temp source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
  target.nome = source.nome,
  target.preco = source.preco
WHEN NOT MATCHED THEN INSERT *
""")
```

### Time Travel

O Apache Iceberg permite acessar versões anteriores dos dados:

```python
# Obter os snapshots da tabela
snapshots_df = spark.sql("SELECT * FROM iceberg.default.vendas_iceberg.snapshots")

# Acessar um snapshot específico
first_snapshot_id = snapshots_df.select("snapshot_id").first()[0]
df_snapshot = spark.read.option("snapshot-id", first_snapshot_id).table("iceberg.default.vendas_iceberg")

# Acessar a tabela em um timestamp específico
df_timestamp = spark.read.option("as-of-timestamp", "2023-01-01 00:00:00").table("iceberg.default.vendas_iceberg")
```

### Evolução de Esquema

O Apache Iceberg suporta evolução de esquema de forma transparente:

```python
# Adicionar uma nova coluna à tabela
spark.sql("ALTER TABLE iceberg.default.vendas_iceberg ADD COLUMN desconto DOUBLE")

# Atualizar alguns registros com valores para a nova coluna
spark.sql("""
UPDATE iceberg.default.vendas_iceberg
SET desconto = preco * 0.1
WHERE categoria = 'Eletrônicos'
""")
```

## Vantagens do Apache Iceberg

1. **Formato de Tabela Aberta**: Iceberg é um formato de tabela aberto que permite que diferentes motores de processamento acessem os mesmos dados.
2. **Transações ACID**: Garantem a consistência dos dados mesmo em caso de falhas.
3. **Time Travel**: Permite acessar versões anteriores dos dados para auditoria ou rollback.
4. **Evolução de Esquema**: Permite alterar o esquema dos dados sem afetar os consumidores.
5. **Operações SQL Expressivas**: Suporte a operações como UPDATE, DELETE e MERGE, que não são nativas em formatos tradicionais de data lake.
6. **Particionamento Oculto**: O particionamento é gerenciado pelo Iceberg, não pelo usuário, o que simplifica o uso.

## Exemplo Completo

Para um exemplo completo de operações com Apache Iceberg, consulte o notebook [iceberg_operations.ipynb](../notebooks/iceberg_operations.ipynb) incluído neste projeto.

## Referências

- [Site oficial do Apache Iceberg](https://iceberg.apache.org/)
- [Documentação do Apache Iceberg](https://iceberg.apache.org/docs/latest/)
- [GitHub do Apache Iceberg](https://github.com/apache/iceberg)
