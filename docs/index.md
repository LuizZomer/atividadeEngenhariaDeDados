# Introdução ao Gerenciamento de Dados com Apache Iceberg e Delta Lake no PySpark

Este documento apresenta uma introdução prática ao uso das tecnologias Apache Iceberg e Delta Lake com PySpark para gerenciamento avançado de dados. Ambas as soluções são projetadas para aprimorar a manipulação de grandes volumes de dados em ambientes distribuídos, oferecendo suporte a transações ACID, versionamento e operações como inserção, atualização e exclusão de registros.

---

## Apache Iceberg com PySpark

Apache Iceberg é um formato de tabela aberto para grandes datasets analíticos. Ele permite que você gerencie e consulte dados de forma eficiente e segura, garantindo a integridade dos dados mesmo em operações concorrentes.

No exemplo apresentado, configuramos uma sessão Spark para trabalhar localmente com Iceberg, definindo um catálogo Hadoop e criando uma tabela chamada `football_games` com várias colunas relacionadas a partidas de futebol. Além disso, mostramos como inserir novos dados, atualizar campos específicos e deletar registros usando comandos SQL executados via Spark.

### Principais pontos:

- Configuração do SparkSession para suportar Iceberg.
- Criação de tabela Iceberg com esquema definido.
- Inserção, atualização e deleção de dados utilizando Spark SQL.

---

## Delta Lake com PySpark

Delta Lake é uma camada de armazenamento open-source que traz funcionalidades de banco de dados transacional para data lakes. Ele garante confiabilidade, atomicidade e performance nas operações sobre datasets grandes.

Na demonstração, iniciamos uma sessão Spark configurada para Delta Lake, lemos dados CSV com esquema explícito, salvamos no formato Delta e criamos uma tabela Delta. Também realizamos operações típicas como atualização e exclusão de registros, além de inserir novos dados programaticamente.

### Principais pontos:

- Configuração do SparkSession para Delta Lake.
- Definição de schema e leitura de arquivos CSV.
- Criação de tabela Delta e operações CRUD (Create, Read, Update, Delete).
- Uso da API Delta para manipulação eficiente dos dados.

---

## Considerações Finais

Tanto Apache Iceberg quanto Delta Lake são soluções poderosas para gerenciar dados analíticos em larga escala. Elas oferecem uma base sólida para construir pipelines de dados confiáveis, com suporte a transações e controle de versões. A escolha entre elas depende do ambiente, necessidades específicas e ecossistema adotado.

No geral, aprender e aplicar essas tecnologias com PySpark facilita o desenvolvimento de sistemas de dados robustos, escaláveis e consistentes para análise e processamento.

---
