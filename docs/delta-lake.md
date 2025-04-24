# Delta Lake

O Delta Lake é uma camada de armazenamento de código aberto que adiciona confiabilidade ao seu data lake. Nesta página, demonstramos as operações básicas de INSERT, UPDATE e DELETE usando Delta Lake com Apache Spark.

## O que é Delta Lake?

Delta Lake é um framework de armazenamento de código aberto que permite a construção de uma arquitetura lakehouse agnóstica de formato. É uma camada de armazenamento otimizada que proporciona a base para as tabelas em uma instância de data lake.

### Principais Características

- **Transações ACID**: Protege seus dados com serializabilidade, o nível mais forte de isolamento
- **Metadados Escaláveis**: Lida com tabelas de escala petabyte com bilhões de partições e arquivos com facilidade
- **Time Travel**: Acessa/reverte para versões anteriores de dados para auditorias, rollbacks ou reprodução
- **Código Aberto**: Comunidade dirigida, padrões abertos, protocolo aberto, discussões abertas
- **Batch/Streaming Unificado**: Semântica de ingestão exatamente uma vez para backfill e consultas interativas
- **Evolução/Aplicação de Esquema**: Previne que dados ruins causem corrupção de dados
- **Histórico de Auditoria**: Delta Lake registra todos os detalhes de alterações, fornecendo um histórico completo de auditoria
- **Operações DML**: APIs SQL, Scala/Java e Python para mesclar, atualizar e excluir conjuntos de dados

## Configuração do Ambiente

Para trabalhar com Delta Lake, você precisa configurar uma sessão Spark com suporte ao Delta Lake:

```python
from pyspark.sql import SparkSession

# Criar a sessão Spark com suporte ao Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeDemo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
    .getOrCreate()
```

## Operações com Delta Lake

### Operação INSERT

O Delta Lake suporta operações de inserção de dados de forma simples e eficiente. Veja como criar uma tabela Delta e inserir dados:

```python
# Criar uma tabela Delta
df_vendas.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("estado") \
    .save("/caminho/para/tabela/delta")

# Inserir novos dados
df_novos.write \
    .format("delta") \
    .mode("append") \
    .save("/caminho/para/tabela/delta")
```

### Operação UPDATE

O Delta Lake permite atualizações de dados usando SQL ou a API do Delta:

```python
# Usando SQL
spark.sql("""
UPDATE delta.`/caminho/para/tabela/delta`
SET preco = 99.90
WHERE nome = 'Produto B'
""")

# Usando a API do Delta
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/caminho/para/tabela/delta")
deltaTable.update(
    condition="nome = 'Produto E'",
    set={"nome": "'Produto E Premium'", "preco": "149.90"}
)
```

### Operação DELETE

O Delta Lake suporta exclusão de dados de forma eficiente:

```python
# Usando SQL
spark.sql("""
DELETE FROM delta.`/caminho/para/tabela/delta`
WHERE categoria = 'Alimentos' AND preco < 30
""")

# Usando a API do Delta
deltaTable = DeltaTable.forPath(spark, "/caminho/para/tabela/delta")
deltaTable.delete("categoria = 'Alimentos' AND preco < 30")
```

### Operação MERGE

O MERGE é uma operação poderosa que permite atualizar e inserir dados em uma única operação:

```python
# Registrar o DataFrame de origem como uma view temporária
df_update.createOrReplaceTempView("source_data_temp")

# Realizar a operação MERGE
spark.sql("""
MERGE INTO delta.`/caminho/para/tabela/delta` target
USING source_data_temp source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
  target.nome = source.nome,
  target.preco = source.preco
WHEN NOT MATCHED THEN INSERT *
""")
```

### Time Travel

O Delta Lake permite acessar versões anteriores dos dados:

```python
# Obter o histórico da tabela
history_df = spark.sql("DESCRIBE HISTORY delta.`/caminho/para/tabela/delta`")

# Acessar a versão 0 (inicial) da tabela
df_versao_0 = spark.read.format("delta").option("versionAsOf", 0).load("/caminho/para/tabela/delta")

# Acessar a tabela em um timestamp específico
df_timestamp = spark.read.format("delta").option("timestampAsOf", "2023-01-01 00:00:00").load("/caminho/para/tabela/delta")
```

## Vantagens do Delta Lake

1. **Transações ACID**: Garantem a consistência dos dados mesmo em caso de falhas.
2. **Time Travel**: Permite acessar versões anteriores dos dados para auditoria ou rollback.
3. **Operações DML**: Suporte a operações como UPDATE, DELETE e MERGE, que não são nativas em formatos tradicionais de data lake.
4. **Evolução de Esquema**: Permite alterar o esquema dos dados sem afetar os consumidores.
5. **Unificação de Batch e Streaming**: Permite processar dados em batch e streaming de forma unificada.

## Exemplo Completo

Para um exemplo completo de operações com Delta Lake, consulte o notebook [delta_lake_operations.ipynb](../notebooks/delta_lake_operations.ipynb) incluído neste projeto.

## Referências

- [Site oficial do Delta Lake](https://delta.io/)
- [Documentação do Delta Lake](https://docs.delta.io/)
- [GitHub do Delta Lake](https://github.com/delta-io/delta)
