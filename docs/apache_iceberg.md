# Documentação: Uso do Apache Iceberg com PySpark para Tabela de Jogos de Futebol

## Introdução

Este projeto demonstra como configurar um ambiente local usando Apache Iceberg junto com PySpark para manipular uma tabela que armazena dados de partidas de futebol. A tabela é criada, dados são inseridos, atualizados e deletados usando comandos SQL no Spark.

---

## Configuração do SparkSession com Iceberg

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("IcebergLocalDevelopment") \
  .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1') \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
  .getOrCreate()
```

### Explicação

É A conexão do pyspark com o delta lake

## Criação da Tabela Iceberg

```python
CREATE TABLE IF NOT EXISTS local.db.football_games (
  Round STRING,
  Date STRING,
  Time STRING,
  Team STRING,
  Team_Score STRING,
  Opponent_Score STRING,
  Opponent STRING,
  Home_Score_AET STRING,
  Away_Score_AET STRING,
  Home_Penalties STRING,
  Away_Penalties STRING,
  Team_Points STRING,
  Opponent_Points STRING,
  season STRING,
  Location STRING,
  Country STRING,
  Competition STRING
)
USING iceberg
LOCATION 'spark-warehouse/iceberg/db/football_games'
```

### Explicação

- Cria a tabela football_games dentro do catálogo local, banco db.
- Define as colunas e seus tipos (todos STRING no exemplo, pode ser ajustado conforme necessidade).
- Especifica USING iceberg para usar o formato Iceberg.
- Usa a localização física no disco para armazenar os dados.

## Inserindo Dados na Tabela

```python
spark.sql("""
    INSERT INTO local.db.football_games VALUES (
        'ROUND 2',          -- Round
        '15/09/2002',       -- Date
        '20:00',            -- Time
        'NEW TEAM',         -- Team
        3.0,                -- Team_Score
        1.0,                -- Opponent_Score
        'OLD TEAM',         -- Opponent
        NULL,               -- Home_Score_AET
        NULL,               -- Away_Score_AET
        NULL,               -- Home_Penalties
        NULL,               -- Away_Penalties
        3.0,                -- Team_Points
        0.0,                -- Opponent_Points
        2002,               -- season
        'Home',             -- Location
        'spain',            -- Country
        'primera-division'  -- Competition
    )
""")
```

### Explicação

- Insere uma nova linha na tabela.
- Os valores devem estar na ordem e no tipo correspondente das colunas.

## Atualizando Dados

```python
spark.sql("""
    UPDATE local.db.football_games
    SET Team_Score = '1.0'
    WHERE Team = 'RACING SANTANDER' AND Date = '31/08/2002'
""")
```

### Explicação

-Atualiza o campo Team_Score para '1.0' para partidas específicas.

## Deletando Dados

```python
spark.sql("""
    DELETE FROM local.db.football_games
    WHERE Team = 'RAYO VALLECANO' AND Date = '01/09/2002'
""")
```

### Explicação

- Remove linhas da tabela que atendem à condição especificada.

## Considerações Finais

- Apache Iceberg oferece suporte ACID, versionamento e consultas eficientes em grandes volumes de dados.
- Usar o SparkSQL facilita manipulação declarativa das tabelas.
- A configuração local é ideal para desenvolvimento e testes, para produção use um catálogo adequado (Hive, Glue, etc.).
