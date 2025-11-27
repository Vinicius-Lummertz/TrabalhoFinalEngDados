# Indicadores de Sucesso (KPIs) & Métricas

O objetivo final da engenharia de dados é suportar a decisão. Abaixo, os indicadores e métricas calculados na camada Gold e apresentados no Dashboard Databricks.

Estes indicadores foram construídos a partir das tabelas fato (`fato_voos_diario`, `fato_manutencoes`) e views enriquecidas (`vw_bilhetes_enriquecido`).

## 📊 KPIs Estratégicos (Key Performance Indicators)

### 1. Ticket Médio (Média de Valor por Ticket)
- **O que é:** O valor monetário médio obtido por cada bilhete aéreo vendido, independentemente da classe ou destino.
- **Regra de Negócio:** Média aritmética simples da coluna `valor_tarifa` na view de bilhetes.
- **Impacto:** Indicador vital de *Revenue Management*. Monitora a saúde financeira e a eficácia das estratégias de precificação (*pricing*) frente à demanda.

### 2. Volume Total de Voos (Operações Realizadas)
- **O que é:** O número absoluto de voos únicos realizados (pousados ou não) no período analisado.
- **Regra de Negócio:** Contagem distinta (`COUNT DISTINCT`) dos identificadores de voo (`voo_id`) na tabela `fato_voos_diario`.
- **Impacto:** Define a escala da operação. É o termômetro macroscópico da capacidade produtiva da companhia aérea e seu *market share* em volume.

### 3. Custo Médio por Tipo de Manutenção
- **O que é:** Análise comparativa do custo médio gasto em cada categoria de revisão técnica (A-Check, B-Check, C-Check).
- **Regra de Negócio:** Média da coluna `custo_total` agrupada por `tipo_manutencao` na tabela `fato_manutencoes`.
- **Impacto:** Permite o controle de custos operacionais (*OPEX*), identificando se revisões mais complexas estão consumindo orçamento desproporcionalmente.

---

## 📈 Métricas de Apoio & Análise Temporal

### 4. Diversidade de Aeroportos por País (Market Reach)
- **O que é:** A contagem de aeroportos distintos operados, segmentada pelo país de origem do cliente.
- **Regra de Negócio:** Contagem única de aeroportos cruzada com a dimensão de clientes (via `vw_bilhetes_enriquecido`).
- **Uso:** Mede a capilaridade da malha aérea (alcance de mercado) e ajuda a entender de onde vêm os clientes que utilizam as rotas mais diversas.

### 5. Sazonalidade de Voos (Evolução Temporal)
- **O que é:** A distribuição do volume de voos ao longo da linha do tempo (eixo temporal).
- **Regra de Negócio:** Soma de voos agrupada por `data_voo` (Mensal/Trimestral).
- **Uso:** Diferente do volume total, esta visualização identifica picos de demanda (*peak seasons*) e janelas de ociosidade, essenciais para o planejamento de escala de tripulação e aeronaves.

### 6. Sazonalidade de Carga de Bagagem
- **O que é:** A variação do peso médio de bagagem despachada por voo ao longo do tempo.
- **Regra de Negócio:** Média da coluna `peso_total_bagagens_kg` agrupada por `data_voo` na tabela `fato_voos_diario`.
- **Uso:** Revela padrões de comportamento do passageiro (ex: viagens de férias no verão tendem a ter mais carga que viagens corporativas), impactando diretamente o cálculo de combustível e logística de *ground handling*.

---

> **Nota Técnica:** Todos os indicadores utilizam agregações nativas do Databricks SQL (`AVG`, `SUM`, `COUNT DISTINCT`) sobre os *Data Marts* da camada Gold, garantindo performance otimizada no carregamento dos painéis visualizados pelos executivos.