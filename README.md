# SkyData - Ingestion Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue) ![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange) ![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red)

Implementação do pipeline de ingestão de dados (End-to-End Ingestion) para o projeto de aviação. Este repositório contém a infraestrutura de dados, geradores de massa de teste e os pipelines ETL para as camadas Landing e Bronze.

Criadores:
Vinicius Lummertz
Matheus
Kauan
João Acordi
Victor
Lucas Guidi

## 🏗 Arquitetura Implementada

O projeto foca na captura eficiente de dados transacionais e sua persistência em Delta Lake.

```mermaid
graph LR
    A[PostgreSQL<br/>(Source)] -->|JDBC Incremental<br/>Watermark| B(Landing Zone<br/>CSV / Volumes)
    B -->|Spark Batch Processing| C[(Bronze Layer<br/>Delta Lake)]
    C -->|Ready for| D[Silver/Gold Teams]