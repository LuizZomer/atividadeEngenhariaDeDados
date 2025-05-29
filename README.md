# Implementação do Apache Iceberg e Delta Lake com Spark

## Tecnologias usadas:
- UV (Gerenciador de pacotes python)
- Apache iceberg
- Delta Lake
- Apache spark
- Pyspark
- Jupiter notebook
- Docker

## Ambiente que o projeto foi testado:

- Distributor ID: Ubuntu
- Description:    Ubuntu 24.04.1 LTS
- Release:        24.04
- Codename:       noble

## Passos para rodar o projeto

### Passo 1 - Clonar projeto 

Para rodar o projeto roda este comando e siga as instruções a baixo:
```
git clone https://github.com/LuizZomer/atividadeEngenhariaDeDados.git
```

### Passo 2

Rodar Apache Iceberg:
- uv venv
- source .venv/bin/activate
- uv add pyspark==3.5.3 jupyterlab ipykernel

Rodar Delta lake:
- uv venv
- source .venv/bin/activate
- uv add pyspark==3.5.3 delta-spark==3.2.0 jupyterlab ipykernel


## Referencias

-  Repositório Delta Lake [Click aqui!](https://github.com/jlsilva01/spark-delta)
-  Repositório Iceberg [Click aqui!](https://github.com/jlsilva01/spark-iceberg)
