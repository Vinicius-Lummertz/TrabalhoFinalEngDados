# Arquitetura de Dados SkyData

Nossa arquitetura segue o padrão **Medallion Architecture** (Bronze, Silver, Gold), desenhada para robustez, rastreabilidade e performance.

## 1. Fonte de Dados (Simulador)
Utilizamos um banco **PostgreSQL** populado via script Python (`Faker`).
* **Diferencial:** As tabelas possuem colunas de auditoria (`AUD_DH_CRIACAO`, `AUD_DH_ALTERACAO`).
* **Importância:** Permite identificação precisa de janelas de alteração para cargas incrementais.

## 2. Pipeline de Landing (Ingestão)
O notebook de Landing conecta no banco via JDBC e aplica uma estratégia de **Watermark**.
* **Mecanismo:** O sistema persiste o timestamp da última execução bem-sucedida (High Watermark).
* **Execução:** A cada rodada, solicita ao banco apenas os registros com data de modificação superior ao último watermark.
* **Saída:** Arquivos CSV salvos em Volumes do Databricks, segregados por `batch_id`.

## 3. Pipeline Bronze (Consolidação)
Processa os arquivos da Landing Zone e os consolida em tabelas **Delta Lake**.
* **Estratégia:** `Upsert` (Merge).
* **Rastreabilidade:** Enriquecimento com colunas de metadados (`bronze_batch_id`, `bronze_load_ts`) para linhagem de dados.

## 4. Pipeline Silver (Refinamento e SCD2)
Nesta camada, transformamos dados brutos em **Dimensões** confiáveis.
* **Modelagem:** Implementação de **SCD Tipo 2 (Slowly Changing Dimension)**. Isso garante que não apenas atualizamos o dado, mas mantemos o histórico de todas as suas versões ao longo do tempo.
* **Detecção de Mudanças (Hashing):**
    * Criamos um `attr_hash` (SHA-256) concatenando colunas de negócio relevantes.
    * Comparamos o hash da Bronze com o hash vigente na Silver. Se for diferente, uma nova versão do registro é aberta e a anterior é fechada.
* **Colunas de Controle:**
    * `is_current`: Indica o registro vigente.
    * `vigencia_inicio` e `vigencia_fim`: Definem a janela temporal de validade daquela versão do dado.

## Tecnologias
* **Python & SQL:** Linguagens principais de transformação.
* **Apache Spark:** Processamento distribuído massivo.
* **Delta Lake:** Camada de armazenamento transacional ACID.
* **Databricks Volumes:** Gerenciamento de arquivos brutos.