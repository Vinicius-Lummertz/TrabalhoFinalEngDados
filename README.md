# Projeto Aviação no Databricks  
## Planejamento por Times + Contratos (com colunas e tipos)

Este documento descreve **como os dados do PostgreSQL (esquema `aviacao`) serão levados até os KPIs finais** usando Databricks e arquitetura em camadas (Landing, Bronze, Silver, Gold), seguindo as premissas do trabalho final (pipeline completo, Data Lake em arquitetura medalhão, Delta Lake, modelo dimensional e dashboard com 4 KPIs e 2 métricas):contentReference[oaicite:0]{index=0}.

Divisão de squads:

1. **Equipe 1 – Ingestão & Landing/Bronze**
2. **Equipe 2 – Transformação & Modelo Dimensional (Silver/Gold)**
3. **Equipe 3 – KPIs & Dashboard**

---

## 0. Convenções de Databricks e Nomes

- **Catálogo**: `aviacao_cat`
- **Schema por camada**:
  - `aviacao_landing`
  - `aviacao_bronze`
  - `aviacao_silver`
  - `aviacao_gold`
- **Formato**:
  - Landing: arquivos brutos (`.csv`) vindo do PostgreSQL.
  - Bronze/Silver/Gold: **Delta Lake** (tabelas managed do Databricks):contentReference[oaicite:1]{index=1}.
- **Colunas extras padrão (em Bronze/Silver/Gold)**:
  - `ingestion_ts TIMESTAMP` – quando chegou no Lake
  - `origem_arquivo STRING` – nome do arquivo/execução
  - para dimensões SCD2: `dt_inicio_vigencia DATE`, `dt_fim_vigencia DATE`, `flag_ativo BOOLEAN`

---

## 1. Equipe 1 – Ingestão (Origem → Landing → Bronze)

### 1.1. Objetivo

Trazer os dados do **PostgreSQL (schema `aviacao`)** para o Data Lake no Databricks, criando:

- **Camada Landing**: arquivos CSV brutos
- **Camada Bronze**: tabelas Delta espelhando o modelo operacional

### 1.2. Fluxo de alto nível

1. Ler tabelas do PostgreSQL via JDBC.
2. Gravar na **Landing** em CSV, 1 arquivo por tabela (ou particionado).
3. Carregar da Landing para **Bronze** já no formato Delta, mantendo os tipos originais.

### 1.3. Contrato de ENTRADA da Equipe 1

Fonte: **PostgreSQL – esquema `aviacao`**

A equipe 1 **recebe** as tabelas com estas colunas mínimas (tipos SQL originais, resumidos):

> _Obs.: só listei as colunas relevantes para os KPIs e modelo dimensional; as demais podem continuar existindo normalmente._

#### 1.3.1. `companhias_aereas`

| Coluna           | Tipo               | Observação                        |
|------------------|--------------------|-----------------------------------|
| id               | BIGINT (PK)        | chave técnica                     |
| nome             | VARCHAR(255)       | nome companhia                    |
| codigo_iata      | CHAR(2)           | código 2 letras                   |
| codigo_icao      | CHAR(3)           | código 3 letras                   |
| pais             | VARCHAR(120)       |                                   |
| alianca          | VARCHAR(80)        |                                   |
| ativo            | BOOLEAN            |                                   |
| AUD_DH_CRIACAO   | TIMESTAMPTZ        | auditoria                         |
| AUD_DH_ALTERACAO | TIMESTAMPTZ        | auditoria                         |

#### 1.3.2. `aeroportos`

| Coluna           | Tipo           |
|------------------|----------------|
| id               | BIGINT (PK)    |
| codigo_iata      | CHAR(3)        |
| codigo_icao      | CHAR(4)        |
| nome             | VARCHAR(255)   |
| cidade           | VARCHAR(120)   |
| pais             | VARCHAR(120)   |
| latitude         | DOUBLE         |
| longitude        | DOUBLE         |
| AUD_DH_CRIACAO   | TIMESTAMPTZ    |
| AUD_DH_ALTERACAO | TIMESTAMPTZ    |

#### 1.3.3. `aeronaves`

| Coluna           | Tipo           |
|------------------|----------------|
| id               | BIGINT (PK)    |
| matricula        | VARCHAR(16)    |
| ano_fabricacao   | INTEGER        |
| status           | VARCHAR(30)    |
| companhia_id     | BIGINT (FK)    |
| modelo_id        | BIGINT (FK)    |
| AUD_DH_CRIACAO   | TIMESTAMPTZ    |
| AUD_DH_ALTERACAO | TIMESTAMPTZ    |

#### 1.3.4. `clientes`

| Coluna           | Tipo           |
|------------------|----------------|
| id               | BIGINT (PK)    |
| nome             | VARCHAR(255)   |
| documento        | VARCHAR(32)    |
| data_nascimento  | DATE           |
| email            | VARCHAR(255)   |
| telefone         | VARCHAR(40)    |
| pais             | VARCHAR(120)   |
| AUD_DH_CRIACAO   | TIMESTAMPTZ    |
| AUD_DH_ALTERACAO | TIMESTAMPTZ    |

#### 1.3.5. `voos`

| Coluna                 | Tipo           |
|------------------------|----------------|
| id                     | BIGINT (PK)    |
| numero_voo             | VARCHAR(10)    |
| companhia_id           | BIGINT (FK)    |
| aeronave_id            | BIGINT (FK)    |
| aeroporto_origem_id    | BIGINT (FK)    |
| aeroporto_destino_id   | BIGINT (FK)    |
| data_partida           | TIMESTAMPTZ    |
| data_chegada_prevista  | TIMESTAMPTZ    |
| status                 | status_voo     |
| AUD_DH_CRIACAO         | TIMESTAMPTZ    |
| AUD_DH_ALTERACAO       | TIMESTAMPTZ    |

#### 1.3.6. `reservas`

| Coluna           | Tipo              |
|------------------|-------------------|
| id               | BIGINT (PK)       |
| codigo_localizador | VARCHAR(16)    |
| cliente_id       | BIGINT (FK)       |
| voo_id           | BIGINT (FK)       |
| status           | status_reserva    |
| data_reserva     | TIMESTAMPTZ       |
| AUD_DH_CRIACAO   | TIMESTAMPTZ       |
| AUD_DH_ALTERACAO | TIMESTAMPTZ       |

#### 1.3.7. `bilhetes`

| Coluna           | Tipo              |
|------------------|-------------------|
| id               | BIGINT (PK)       |
| reserva_id       | BIGINT (FK)       |
| numero_bilhete   | VARCHAR(32)       |
| assento          | VARCHAR(8)        |
| classe_cabine    | classe_cabine     |
| status_pagamento | status_pagamento  |
| valor_tarifa     | NUMERIC(12,2)     |
| AUD_DH_CRIACAO   | TIMESTAMPTZ       |
| AUD_DH_ALTERACAO | TIMESTAMPTZ       |

#### 1.3.8. `bagagens`

| Coluna           | Tipo              |
|------------------|-------------------|
| id               | BIGINT (PK)       |
| bilhete_id       | BIGINT (FK)       |
| peso_kg          | NUMERIC(5,1)      |
| tipo             | VARCHAR(30)       |
| volume           | INTEGER           |
| AUD_DH_CRIACAO   | TIMESTAMPTZ       |
| AUD_DH_ALTERACAO | TIMESTAMPTZ       |

#### 1.3.9. `manutencoes`

| Coluna           | Tipo              |
|------------------|-------------------|
| id               | BIGINT (PK)       |
| aeronave_id      | BIGINT (FK)       |
| tipo_manutencao  | VARCHAR(40)       |
| data_inicio      | TIMESTAMPTZ       |
| data_fim         | TIMESTAMPTZ       |
| custo_total      | NUMERIC(14,2)     |
| descricao        | TEXT              |
| AUD_DH_CRIACAO   | TIMESTAMPTZ       |
| AUD_DH_ALTERACAO | TIMESTAMPTZ       |

(As demais tabelas – `modelos_avioes`, `funcionarios`, `tripulacao_voo` – seguem o DDL original e podem ser trazidas igualmente.)

---

### 1.4. Contrato de SAÍDA da Equipe 1 (para Equipe 2)

A Equipe 1 **entrega**:

#### 1.4.1. Tabelas Landing

Local: **`aviacao_cat.aviacao_landing`** (ou apenas diretórios em cloud storage, se preferir)

- Arquivos CSV, 1 diretório por tabela:
  - `/mnt/aviacao/landing/companhias_aereas/AAAA-MM-DD/arquivo.csv`
  - `/mnt/aviacao/landing/voos/AAAA-MM-DD/arquivo.csv`
  - etc.

Contrato:
- Arquivos com **o mesmo conjunto de colunas** descrito na entrada (nome idêntico ao banco).
- Separador `;` ou `,` (documentado).
- Codificação UTF-8.

#### 1.4.2. Tabelas Bronze (Delta)

Local: **`aviacao_cat.aviacao_bronze`**

Exemplos de esquemas **(contrato mínimo)**:

##### `aviacao_bronze.voos`

| Coluna                 | Tipo Databricks | Origem              |
|------------------------|-----------------|---------------------|
| id                     | BIGINT          | voos.id             |
| numero_voo             | STRING          | voos.numero_voo     |
| companhia_id           | BIGINT          | voos.companhia_id   |
| aeronave_id            | BIGINT          | voos.aeronave_id    |
| aeroporto_origem_id    | BIGINT          | voos.aeroporto_origem_id |
| aeroporto_destino_id   | BIGINT          | voos.aeroporto_destino_id |
| data_partida           | TIMESTAMP       | voos.data_partida   |
| data_chegada_prevista  | TIMESTAMP       | voos.data_chegada_prevista |
| status                 | STRING          | voos.status         |
| AUD_DH_CRIACAO         | TIMESTAMP       |                     |
| AUD_DH_ALTERACAO       | TIMESTAMP       |                     |
| ingestion_ts           | TIMESTAMP       | gerado na ingestão  |
| origem_arquivo         | STRING          | gerado na ingestão  |

##### `aviacao_bronze.bilhetes`

| Coluna           | Tipo Databricks |
|------------------|-----------------|
| id               | BIGINT          |
| reserva_id       | BIGINT          |
| numero_bilhete   | STRING          |
| assento          | STRING          |
| classe_cabine    | STRING          |
| status_pagamento | STRING          |
| valor_tarifa     | DECIMAL(12,2)   |
| AUD_DH_CRIACAO   | TIMESTAMP       |
| AUD_DH_ALTERACAO | TIMESTAMP       |
| ingestion_ts     | TIMESTAMP       |
| origem_arquivo   | STRING          |

Mesma ideia para `reservas`, `companhias_aereas`, `aeronaves`, `aeroportos`, `clientes`, `bagagens`, `manutencoes`.

#### 1.5. Detalhamento de etapas (Equipe 1)

1. **Ingestão Full** (histórico inicial – últimos 3 anos, 20k+ linhas/table):contentReference[oaicite:2]{index=2}
2. **Ingestão Incremental**:
   - Filtrar por `AUD_DH_ALTERACAO > max(alteracao) já carregada` ou `AUD_DH_CRIACAO` para novos.
   - Usar checkpoint (tabela técnica `aviacao_bronze.checkpoints_ingestao`).
3. **Gravação Landing**:
   - Exportar em CSV.
4. **Gravação Bronze (Delta)**:
   - Ler CSV da landing, criar/atualizar tabelas Delta.
   - Particionar tabelas grandes por ano/mês (ex.: `PARTITIONED BY (year(data_partida), month(data_partida))`).

---

## 2. Equipe 2 – Transformação & Modelo Dimensional (Bronze → Silver → Gold)

### 2.1. Objetivo

Criar **camada Silver (dados limpos)** e **camada Gold (modelo dimensional)** com **dimensões SCD Tipo 2** e **tabelas fato com carga incremental**, conforme premissa do trabalho:contentReference[oaicite:3]{index=3}.

### 2.2. Contrato de ENTRADA da Equipe 2

A Equipe 2 **recebe** as tabelas Delta em:

- `aviacao_cat.aviacao_bronze.*`  
  Com schemas descritos na saída da Equipe 1 (colunas originais + `ingestion_ts`, `origem_arquivo`).

### 2.3. Camada Silver – Contrato de SAÍDA (para ela mesma / Gold)

Silver = dados **padronizados, normalizados e enriquecidos**, ainda em modelo “quase operacional”.

Exemplos de tabelas Silver:

#### 2.3.1. `aviacao_silver.voos`

| Coluna                | Tipo     | Regra                                   |
|-----------------------|----------|-----------------------------------------|
| voo_id                | BIGINT   | rename de `id`                          |
| numero_voo            | STRING   |                                         |
| companhia_id          | BIGINT   |                                         |
| aeronave_id           | BIGINT   |                                         |
| aeroporto_origem_id   | BIGINT   |                                         |
| aeroporto_destino_id  | BIGINT   |                                         |
| dt_partida            | DATE     | `date(data_partida)`                    |
| hora_partida          | STRING   | `date_format(data_partida, 'HH:mm')`    |
| dt_chegada_prevista   | DATE     |                                         |
| status_voo            | STRING   | upper, valores padronizados             |
| ingestion_ts          | TIMESTAMP|                                          |

#### 2.3.2. `aviacao_silver.reservas`

| Coluna            | Tipo     |
|-------------------|----------|
| reserva_id        | BIGINT   |
| codigo_localizador| STRING   |
| cliente_id        | BIGINT   |
| voo_id            | BIGINT   |
| status_reserva    | STRING   |
| dt_reserva        | DATE     |
| ingestion_ts      | TIMESTAMP|

#### 2.3.3. `aviacao_silver.bilhetes`

| Coluna           | Tipo           |
|------------------|----------------|
| bilhete_id       | BIGINT         |
| reserva_id       | BIGINT         |
| numero_bilhete   | STRING         |
| classe_cabine    | STRING         |
| status_pagamento | STRING         |
| valor_tarifa     | DECIMAL(12,2)  |
| ingestion_ts     | TIMESTAMP      |

#### 2.3.4. `aviacao_silver.manutencoes`

| Coluna           | Tipo           |
|------------------|----------------|
| manutencao_id    | BIGINT         |
| aeronave_id      | BIGINT         |
| tipo_manutencao  | STRING         |
| dt_inicio        | DATE           |
| dt_fim           | DATE           |
| duracao_horas    | INT            |
| custo_total      | DECIMAL(14,2)  |
| ingestion_ts     | TIMESTAMP      |

(Demais tabelas Silver seguem padrão: ids renomeados, datas separadas em `dt` e `hora`, enums para string, etc.)

### 2.4. Dimensões SCD Tipo 2 (Camada Gold)

Equipes 2 cria dimensões em `aviacao_cat.aviacao_gold`.

#### 2.4.1. `aviacao_gold.dim_tempo`

| Coluna             | Tipo   |
|--------------------|--------|
| sk_tempo           | INT (PK) |
| data               | DATE   |
| ano                | INT    |
| mes                | INT    |
| dia                | INT    |
| trimestre          | INT    |
| nome_mes           | STRING |
| dia_semana         | STRING |

#### 2.4.2. `aviacao_gold.dim_companhia` (SCD2)

| Coluna             | Tipo      | Comentário                              |
|--------------------|-----------|-----------------------------------------|
| sk_companhia       | BIGINT PK | surrogate key                           |
| companhia_id       | BIGINT    | chave natural (do operacional)         |
| nome               | STRING    |
| codigo_iata        | STRING    |
| codigo_icao        | STRING    |
| pais               | STRING    |
| alianca            | STRING    |
| ativo_operacional  | BOOLEAN   |
| dt_inicio_vigencia | DATE      |
| dt_fim_vigencia    | DATE      |
| flag_ativo         | BOOLEAN   |

#### 2.4.3. `aviacao_gold.dim_aeroporto` (SCD2)

| Coluna             | Tipo      |
|--------------------|-----------|
| sk_aeroporto       | BIGINT PK |
| aeroporto_id       | BIGINT    |
| codigo_iata        | STRING    |
| codigo_icao        | STRING    |
| nome               | STRING    |
| cidade             | STRING    |
| pais               | STRING    |
| dt_inicio_vigencia | DATE      |
| dt_fim_vigencia    | DATE      |
| flag_ativo         | BOOLEAN   |

#### 2.4.4. `aviacao_gold.dim_aeronave` (SCD2)

| Coluna             | Tipo      |
|--------------------|-----------|
| sk_aeronave        | BIGINT PK |
| aeronave_id        | BIGINT    |
| matricula          | STRING    |
| ano_fabricacao     | INT       |
| status_operacional | STRING    |
| companhia_id       | BIGINT    |
| dt_inicio_vigencia | DATE      |
| dt_fim_vigencia    | DATE      |
| flag_ativo         | BOOLEAN   |

#### 2.4.5. `aviacao_gold.dim_cliente` (SCD2 simplificado)

| Coluna             | Tipo      |
|--------------------|-----------|
| sk_cliente         | BIGINT PK |
| cliente_id         | BIGINT    |
| nome               | STRING    |
| pais               | STRING    |
| faixa_etaria       | STRING    |
| dt_inicio_vigencia | DATE      |
| dt_fim_vigencia    | DATE      |
| flag_ativo         | BOOLEAN   |

### 2.5. Fatos (Camada Gold) – Contrato de SAÍDA (para Equipe 3)

#### 2.5.1. `aviacao_gold.fato_voo`

| Coluna                | Tipo      | Descrição                          |
|-----------------------|-----------|------------------------------------|
| voo_id                | BIGINT    | chave natural                      |
| sk_tempo_partida      | INT       | FK → dim_tempo                     |
| sk_tempo_chegada_prev | INT       | FK → dim_tempo                     |
| sk_companhia          | BIGINT    | FK → dim_companhia                 |
| sk_aeronave           | BIGINT    | FK → dim_aeronave                  |
| sk_aeroporto_origem   | BIGINT    | FK → dim_aeroporto                 |
| sk_aeroporto_destino  | BIGINT    | FK → dim_aeroporto                 |
| status_voo            | STRING    |                                    |
| atraso_minutos        | INT       | max(0, dif em minutos)             |
| cancelado_flag        | BOOLEAN   | status in ('CANCELADO','DESVIADO') |
| qtd_passageiros       | INT       | contagem de reservas confirmadas   |
| receita_total_voo     | DECIMAL(14,2) | soma valor_tarifa bilhetes     |

#### 2.5.2. `aviacao_gold.fato_reserva`

| Coluna            | Tipo      |
|-------------------|-----------|
| reserva_id        | BIGINT    |
| sk_tempo_reserva  | INT       |
| sk_cliente        | BIGINT    |
| sk_companhia      | BIGINT    |
| sk_voo            | BIGINT    |
| status_reserva    | STRING    |
| qtd_bilhetes      | INT       |

#### 2.5.3. `aviacao_gold.fato_pagamento`

| Coluna            | Tipo        |
|-------------------|-------------|
| bilhete_id        | BIGINT      |
| sk_tempo_pagamento| INT         |
| sk_cliente        | BIGINT      |
| sk_companhia      | BIGINT      |
| valor_tarifa      | DECIMAL(12,2) |
| status_pagamento  | STRING      |

#### 2.5.4. `aviacao_gold.fato_manutencao`

| Coluna            | Tipo          |
|-------------------|---------------|
| manutencao_id     | BIGINT        |
| sk_aeronave       | BIGINT        |
| sk_tempo_inicio   | INT           |
| sk_tempo_fim      | INT           |
| tipo_manutencao   | STRING        |
| custo_total       | DECIMAL(14,2) |
| duracao_horas     | INT           |

#### 2.5.5. `aviacao_gold.fato_bagagem`

| Coluna            | Tipo          |
|-------------------|---------------|
| bagagem_id        | BIGINT        |
| bilhete_id        | BIGINT        |
| sk_tempo_voo      | INT           |
| sk_companhia      | BIGINT        |
| peso_kg           | DECIMAL(5,1)  |
| volume            | INT           |

### 2.6. Detalhamento das etapas (Equipe 2)

1. **Silver – limpeza**:
   - Converter enums para `STRING`.
   - Padronizar datas.
   - Remover linhas claramente inválidas.
2. **Construção de Dimensões**:
   - Dimensões derivadas de Silver.
   - Implementar SCD2 via `MERGE INTO` comparando atributos relevantes.
3. **Construção de Fatos**:
   - `fato_voo`: join de `voos`, `reservas`, `bilhetes` e dimensões para calcular `qtd_passageiros`, `receita_total_voo`, `atraso_minutos`.
   - `fato_pagamento`: join de bilhetes com clientes/companhias.
   - `fato_manutencao`: cálculo de `duracao_horas = hours_between(data_inicio, data_fim)`.
   - `fato_bagagem`: join bagagens → bilhetes → voos.
4. **Incrementalidade**:
   - Usar tabelas de checkpoint (`aviacao_silver.checkpoint_fato_*`) guardando max data de origem.
   - Carregar apenas novas linhas da Bronze/Silver.

---

## 3. Equipe 3 – KPIs & Dashboard (Gold → Visualização)

### 3.1. Contrato de ENTRADA da Equipe 3

A Equipe 3 **recebe** todas as tabelas da camada Gold:

- `aviacao_gold.dim_*`
- `aviacao_gold.fato_voo`
- `aviacao_gold.fato_reserva`
- `aviacao_gold.fato_pagamento`
- `aviacao_gold.fato_manutencao`
- `aviacao_gold.fato_bagagem`

Acesso:
- via **SQL Warehouse** do Databricks ou conexão direta Delta (conforme ferramenta de BI escolhida):contentReference[oaicite:4]{index=4}.

### 3.2. KPIs e Métricas (fórmulas explícitas)

Conforme premissa do trabalho, criar pelo menos **4 KPIs e 2 métricas**:contentReference[oaicite:5]{index=5}.

#### KPI 1 – Taxa de Pontualidade de Voos

- **Fórmula**  
  \[
  \text{Taxa Pontualidade} = \frac{\text{Voos com atraso\_minutos ≤ 15 e não cancelados}}{\text{Total de voos válidos}} \times 100
  \]
- Colunas usadas:
  - `fato_voo.atraso_minutos`
  - `fato_voo.cancelado_flag`
  - `fato_voo.status_voo`

#### KPI 2 – Receita Total por Companhia

- **Fórmula**  
  \[
  \text{Receita por Companhia} = \sum(\text{fato\_pagamento.valor\_tarifa})
  \]
- Agrupar por:
  - `dim_companhia.nome`
- Colunas usadas:
  - `fato_pagamento.valor_tarifa`
  - `fato_pagamento.sk_companhia` → `dim_companhia.nome`

#### KPI 3 – Ocupação Média por Voo

- **Fórmula**  
  \[
  \text{Ocupação Média} = \frac{\text{fato\_voo.qtd\_passageiros}}{\text{capacidade\_total\_assentos\_aeronave}}
  \]
- Colunas usadas:
  - `fato_voo.qtd_passageiros`
  - `dim_aeronave.capacidade_total` *(pode ser derivada de modelo de avião ou somatório das classes)*

#### KPI 4 – Custo de Manutenção sobre Receita

- **Fórmula**  
  \[
  \text{Custo/Receita} = \frac{\sum(\text{fato\_manutencao.custo\_total})}{\sum(\text{fato\_pagamento.valor\_tarifa})}
  \]

---

#### Métrica 1 – Média de Bagagem por Passageiro

\[
\text{Peso médio bagagem (kg)} =
\frac{\sum(\text{fato\_bagagem.peso\_kg})}{\text{Número de passageiros}}
\]

#### Métrica 2 – Receita Média por Passageiro

\[
\text{Receita média/passageiro} =
\frac{\sum(\text{fato\_pagamento.valor\_tarifa})}{\text{Número de passageiros únicos}}
\]

### 3.3. Detalhamento das etapas (Equipe 3)

1. **Conectar ferramenta de BI** ao SQL Warehouse (ou direto Delta).
2. **Criar views** auxiliares no Databricks (opcional), ex:
   - `aviacao_gold.vw_kpi_pontualidade`
   - `aviacao_gold.vw_kpi_receita_companhia`
3. **Modelar o dashboard**:
   - Layout *One Page View* com cards para cada KPI e gráficos de apoio.
4. **Agendamento**:
   - Garantir atualização diária, atrelando refresh do dashboard à execução dos Jobs da Equipe 2.
5. **Documentação**:
   - Documentar fórmulas, fontes de dados e periodicidade.
   - Criar mkdocs e diagramas do fluxo de dados.
   - Criar apresentação final do projeto (ppw).

---

## 4. Resumo dos Contratos entre Equipes

### Equipe 1 → Equipe 2

- **Entrega**: tabelas `aviacao_bronze.*` em Delta.
- **Colunas mínimas**: mesmos campos do PostgreSQL + `ingestion_ts`, `origem_arquivo`.
- **Garantia**: dados consistentes, sem alteração semântica.

### Equipe 2 → Equipe 3

- **Entrega**: 
  - Dimensões SCD2: `dim_tempo`, `dim_companhia`, `dim_aeroporto`, `dim_aeronave`, `dim_cliente`.
  - Fatos: `fato_voo`, `fato_reserva`, `fato_pagamento`, `fato_manutencao`, `fato_bagagem`.
- **Colunas**: todas explicitadas nas tabelas acima.
- **Garantia**: chaves `sk_*` resolvidas, métricas básicas já calculadas (`qtd_passageiros`, `receita_total_voo`, etc.).

---