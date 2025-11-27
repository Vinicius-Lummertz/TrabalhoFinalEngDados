# Arquitetura de Ingestão

Nossa arquitetura foi desenhada para robustez. O fluxo segue o padrão de ingestão em dois estágios (Landing Zone e Bronze Layer).

## 1. Fonte de Dados (Simulador)
Utilizamos um banco **PostgreSQL** populado via script Python (`Faker`).
* **Diferencial:** As tabelas possuem colunas de auditoria (`AUD_DH_CRIACAO`, `AUD_DH_ALTERACAO`).
* **Importância:** Isso nos permite saber exatamente *quando* um dado foi criado ou modificado.

## 2. Pipeline de Landing (Ingestão)
O notebook de Landing conecta no banco via JDBC e aplica uma estratégia de **Watermark**.
* **Como funciona:** O sistema memoriza a data da última execução.
* **Ação:** Na próxima execução, ele pede ao banco apenas: *"Me dê tudo que mudou depois dessa data"*.
* **Saída:** Arquivos CSV salvos na camada de Landing (Volumes do Databricks), organizados por `batch_id`.

## 3. Pipeline Bronze (Consolidação)
O notebook Bronze processa os arquivos CSV e os transforma em tabelas **Delta Lake**.
* **Estratégia:** Utilizamos `MERGE INTO` (Upsert).
* **Lógica:** Se o registro já existe na Bronze, ele é atualizado. Se é novo, é inserido.
* **Rastreabilidade:** Adicionamos colunas de metadados (`bronze_batch_id`, `bronze_load_ts`) para saber a origem exata de cada linha.

## Tecnologias
* **Python & SQL:** Linguagens principais.
* **Apache Spark:** Processamento distribuído.
* **Delta Lake:** Formato de armazenamento confiável.