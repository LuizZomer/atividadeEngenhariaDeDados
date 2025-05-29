# Documentação do Código Delta Lake com PySpark

Este documento explica o código fornecido para manipulação de dados usando PySpark e Delta Lake.

---

## Inicialização do SparkSession com Delta Lake

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .master("local[*]")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
```

### Explicação

- Cria uma sessão Spark configurada para usar o Delta Lake.
- Configura os pacotes e extensões necessários para Delta Lake.

## Definição do Schema para o CSV

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("match_url", StringType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team_A", StringType(), True),
    StructField("team_B", StringType(), True),
    StructField("score_tA", IntegerType(), True),
    StructField("score_tB", IntegerType(), True),
    StructField("competition", StringType(), True),
    StructField("type_of_match", StringType(), True)
])
```

### Explicação

- Define a estrutura dos dados esperados no CSV.
- Garante que os dados sejam lidos com os tipos corretos.

## Leitura do CSV com Schema Aplicado

```python
csv_path = '../data/HLTVCsGoResults.csv'

df = spark.read.option("header", True).option("sep", ";").schema(schema).csv(csv_path)

```

### Explicação

- Lê o arquivo CSV usando o esquema definido.
- Especifica que o separador é ; e que o arquivo tem cabeçalho.

## Escrita dos Dados no Delta Lake

```python
delta_path = './spark-warehouse/csgo_matches'

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(delta_path)
```

### Explique

- Salva o DataFrame no formato Delta no caminho especificado.
- Usa modo overwrite para substituir dados antigos.
- Permite mesclar esquemas com mergeSchema.

## Criação da Tabela Delta no Spark SQL

```python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS csgo_matches
USING DELTA
LOCATION '{delta_path}'
""")
```

### Explicação

- Cria uma tabela SQL que aponta para os arquivos Delta.

## Atualização de Dados na Tabela Delta

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_path)

delta_table.update(
    condition="match_id = 2321666",
    set={"score_tB": "2"}
)
```

### Explicação

- Atualiza o campo score_tB onde match_id é 2321666.

## Deleção de Dados na Tabela Delta

```python
delta_table.delete("match_id = 2321666")
```

### Explicação

- Remove as linhas que têm match_id igual a 2321666.

## Inserção de Novos Dados

```python
from pyspark.sql import Row
from pyspark.sql.functions import col

new_row = [Row(
    match_url="https://www.hltv.org/matches/9999999/test-vs-test",
    match_id=9999999,
    team_A="TeamTestA",
    team_B="TeamTestB",
    score_tA=16,
    score_tB=14,
    competition="Test Cup",
    type_of_match="bo3"
)]

new_data = spark.createDataFrame(new_row)

new_data = new_data.select(
    col("match_url").cast("string"),
    col("match_id").cast("int"),
    col("team_A").cast("string"),
    col("team_B").cast("string"),
    col("score_tA").cast("int"),
    col("score_tB").cast("int"),
    col("competition").cast("string"),
    col("type_of_match").cast("string")
)

new_data.write.format("delta").mode("append").save(delta_path)
```

### Explicação

- Cria um novo registro e transforma em DataFrame.
- Ajusta os tipos das colunas para combinar com o schema da tabela.
- Insere o novo dado na tabela Delta no modo append.

## Conclusão

Neste exemplo, vimos como utilizar o Delta Lake integrado ao PySpark para gerenciar dados de forma eficiente, combinando funcionalidades de transações ACID, versionamento e performance. O Delta Lake simplifica operações complexas como atualização, deleção e inserção de dados em grandes volumes, mantendo a consistência e integridade das informações no data lake. Essa abordagem é muito útil para pipelines de dados confiáveis e escaláveis.
