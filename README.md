# SkyData Analytics - Engenharia de Dados

![Python](https://img.shields.io/badge/Python-3.13-blue) ![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange) ![Databricks](https://img.shields.io/badge/Databricks-Runtime-red) ![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-yellow)

Repositório oficial do Trabalho Final de Engenharia de Dados. Este projeto implementa um pipeline de dados completo (E2E) utilizando arquitetura Lakehouse no Databricks, com foco em ingestão incremental, qualidade de dados e modelagem dimensional (SCD Tipo 2).

Criadores:
Vinicius Lummertz
Matheus
Kauan
João Acordi
Victor
Lucas Guidi

## 🏗 Arquitetura do Projeto

O projeto segue a **Medallion Architecture** (Bronze, Silver, Gold) orchestrada via Databricks Workflows/Notebooks.

```mermaid
graph LR
    A[PostgreSQL<br/>(Transactional)] -->|JDBC Incremental| B(Landing Zone<br/>CSV Files)
    B -->|Spark Autoloader| C[(Bronze Layer<br/>Delta Lake)]
    C -->|Transformation| D[(Silver Layer<br/>Delta Lake)]
    D -->|SCD Type 2| E[(Gold Layer<br/>Star Schema)]
    E -->|API/Connector| F[Dashboard Analytics]