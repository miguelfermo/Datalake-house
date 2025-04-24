# Pesquisa sobre Apache Spark, Delta Lake e Apache Iceberg

## Apache Spark

### Definição e Características
O Apache Spark é um sistema de processamento distribuído de código aberto usado para workloads de big data. O sistema utiliza armazenamento em cache na memória e execução otimizada de consultas para processar dados de qualquer tamanho de forma eficiente.

### História
O Apache Spark começou em 2009 como um projeto de pesquisa no AMPLab da UC Berkeley, uma colaboração envolvendo estudantes, pesquisadores e professores, focada em domínios de aplicações com uso intensivo de dados. O primeiro artigo intitulado "Spark: Cluster Computing with Working Sets" foi publicado em junho de 2010, e o Spark era de código aberto sob uma licença BSD. Em junho de 2013, o Spark passou para o status de incubação na Apache Software Foundation (ASF) e se estabeleceu como um projeto de alto nível da Apache em fevereiro de 2014.

### Funcionamento
O Hadoop MapReduce é um modelo de programação para processar conjuntos de big data com um algoritmo distribuído paralelo, mas tem limitações devido ao processo sequencial de várias etapas necessário para executar um trabalho. O Spark foi criado para resolver essas limitações, processando na memória, reduzindo o número de etapas em uma tarefa e reutilizando dados em várias operações paralelas.

Com o Spark, é necessária apenas uma etapa em que os dados são lidos na memória, as operações são executadas e os resultados são gravados de volta, resultando em uma execução muito mais rápida. O Spark também reutiliza dados usando um cache na memória para acelerar consideravelmente os algoritmos de machine learning que chamam repetidamente uma função no mesmo conjunto de dados.

A reutilização de dados é realizada por meio da criação de DataFrames, uma abstração sobre o Conjunto de dados resiliente distribuído (RDD), que é uma coleção de objetos armazenados em cache na memória e reutilizados em várias operações do Spark. Isso reduz drasticamente a latência, fazendo com que o Spark seja várias vezes mais rápido que o MapReduce, especialmente ao realizar machine learning e análises interativas.

### Benefícios
- **Rápido**: Por meio do armazenamento em cache na memória e execução otimizada de consultas, o Spark pode oferecer consultas analíticas rápidas de dados de qualquer tamanho.
- **Para desenvolvedores**: O Apache Spark suporta de modo nativo Java, Scala, R e Python, oferecendo várias linguagens para a criação de aplicativos. Essas APIs facilitam as coisas para seus desenvolvedores, pois ocultam a complexidade do processamento distribuído por trás de operadores simples e de alto nível que reduzem drasticamente a quantidade de código necessária.
- **Várias workloads**: O Apache Spark vem com a capacidade de executar várias workloads, incluindo consultas interativas, análises em tempo real, machine learning e processamento de gráficos. Uma aplicação pode combinar várias workloads facilmente.

### Diferenças entre Spark e Hadoop
Além das diferenças no design do Spark e do Hadoop MapReduce, muitas organizações descobriram que essas estruturas de big data são complementares, usando-as juntas para resolver um desafio comercial mais amplo.

O Hadoop é uma estrutura de código aberto que tem o Sistema de Arquivos Distribuído do Hadoop (HDFS) como armazenamento, o YARN como gerenciamento de recursos e a programação MapReduce como mecanismo de execução. Em uma implementação típica do Hadoop, diferentes mecanismos de execução também são implantados, como Spark, Tez e Presto.

O Spark é uma estrutura de código aberto focada em consultas interativas, machine learning e workloads em tempo real. Não tem seu próprio sistema de armazenamento, mas executa análises em outros sistemas de armazenamento, como o HDFS, ou em outras lojas populares, como Amazon Redshift, Amazon S3, Couchbase, Cassandra e outras.

## Delta Lake

### Definição e Características
Delta Lake é um framework de armazenamento de código aberto que permite a construção de uma arquitetura lakehouse agnóstica de formato. É uma camada de armazenamento otimizada que proporciona a base para as tabelas em uma instância de data lake.

### Funcionalidades Principais
- **Transações ACID**: Protege seus dados com serializabilidade, o nível mais forte de isolamento
- **Metadados Escaláveis**: Lida com tabelas de escala petabyte com bilhões de partições e arquivos com facilidade
- **Viagem no Tempo (Time Travel)**: Acessa/reverte para versões anteriores de dados para auditorias, rollbacks ou reprodução
- **Código Aberto**: Orientado pela comunidade, padrões abertos, protocolo aberto, discussões abertas
- **Batch/Streaming Unificado**: Semântica de ingestão exatamente uma vez para backfill e consultas interativas
- **Evolução/Aplicação de Esquema**: Previne que dados ruins causem corrupção de dados
- **Histórico de Auditoria**: Delta Lake registra todos os detalhes de alterações, fornecendo um histórico completo de auditoria
- **Operações DML**: APIs SQL, Scala/Java e Python para mesclar, atualizar e excluir conjuntos de dados

### Integração com Ecossistemas
Delta Lake funciona com motores de computação incluindo Spark, PrestoDB, Flink, Trino, Hive, Snowflake, Google BigQuery, Athena, Redshift, Databricks, Azure Fabric e APIs para Scala, Java, Rust e Python. Com o Delta Universal Format (UniForm), você pode ler tabelas Delta com clientes Iceberg e Hudi.

## Apache Iceberg

### Definição e Características
Apache Iceberg é um formato de tabela de alto desempenho para conjuntos de dados analíticos enormes. Iceberg traz a confiabilidade e simplicidade das tabelas SQL para big data, enquanto torna possível que motores como Spark, Trino, Flink, Presto, Hive e Impala trabalhem com segurança com as mesmas tabelas, ao mesmo tempo.

### Funcionalidades Principais
- **SQL Expressivo**: Iceberg suporta comandos SQL flexíveis para mesclar novos dados, atualizar linhas existentes e realizar exclusões direcionadas. Iceberg pode reescrever avidamente arquivos de dados para desempenho de leitura, ou pode usar deltas de exclusão para atualizações mais rápidas.
- **Evolução Completa de Esquema**: A evolução de esquema funciona perfeitamente. Adicionar uma coluna não trará de volta dados antigos. Adicionar uma coluna com um valor padrão não reescreve os dados existentes. Os tipos de coluna podem ser promovidos sem reescrever dados.
- **Formato de Tabela Aberta**: Iceberg é um formato de tabela aberta para conjuntos de dados analíticos, projetado para abordar os desafios da gestão e consulta de grandes conjuntos de dados.

### Benefícios
- **Código Aberto**: Apache Iceberg é um projeto de código aberto, o que significa que é gratuito para usar e modificar.
- **Alto Desempenho**: Oferece uma forma rápida e eficiente de processar grandes conjuntos de dados em escala.
- **Compatibilidade**: Funciona com vários motores de processamento de dados, incluindo Spark, Trino, Flink, Presto, Hive e Impala.
- **Confiabilidade**: Traz a confiabilidade e simplicidade das tabelas SQL para big data.

### Casos de Uso
Apache Iceberg é particularmente útil para:
- Processamento de grandes conjuntos de dados analíticos
- Ambientes onde múltiplos motores de processamento precisam acessar os mesmos dados
- Cenários que exigem evolução de esquema sem interrupção
- Operações que necessitam de transações ACID em dados de big data

E se você leu até aqui, parabéns!