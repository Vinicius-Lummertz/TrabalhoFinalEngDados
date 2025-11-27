# SkyData - End-to-End Data Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue) ![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange) ![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)

Implementação completa de um pipeline de Engenharia de Dados para o setor de aviação. Este repositório contém a infraestrutura, geradores de dados (Faker), e os pipelines ETL para as camadas Landing, Bronze e Silver (SCD2).

**Criadores:**
Vinicius Lummertz, Matheus, Kauan, João Acordi, Victor, Lucas Guidi.

## 🏗 Arquitetura Implementada

O projeto utiliza a arquitetura Lakehouse com Delta Lake para garantir performance e versionamento de dados.

### Fluxo de Dados

```mermaid
graph LR
    A[PostgreSQL<br/>(Source)] -->|JDBC Incremental<br/>Watermark| B(Landing Zone<br/>CSV / Volumes)
    B -->|Spark Batch<br/>Upsert| C[(Bronze Layer<br/>Delta Lake)]
    C -->|Spark Hash Diff<br/>SCD Type 2| D[(Silver Layer<br/>Dimensions)]
    D -->|Ready for| E[Gold / BI / Analytics]
    
    style C fill:#cd7f32,stroke:#333,stroke-width:2px
    style D fill:#c0c0c0,stroke:#333,stroke-width:2px