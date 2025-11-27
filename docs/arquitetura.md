# Como Funciona (Visão Geral)

Para garantir que os dados cheguem com qualidade ao dashboard, utilizamos uma arquitetura de "Medalhão" (Medallion Architecture). Imagine uma linha de produção industrial:

## 1. A Fonte (Origem)
Nossos dados nascem em um banco de dados transacional (PostgreSQL) que simula a operação dia-a-dia: vendas de passagens, decolagens e registros de manutenção.
* **Desafio:** O banco operacional não pode parar.
* **Solução:** Nossa engenharia lê apenas o que mudou (Carga Incremental) sem travar o sistema.

## 2. Camada Bronze (Matéria-Prima)
É onde os dados chegam "brutos".
* Recebemos os dados em formato CSV e convertemos para um formato otimizado e seguro (Delta Lake).
* Garantimos que nenhum dado se perca no caminho.

## 3. Camada Silver (Refinaria)
Aqui ocorre a limpeza.
* Removemos duplicatas.
* Corrigimos formatos de datas e tipagem.
* Padronizamos nomes (Ex: transformar "SP" e "S. Paulo" em "São Paulo").

## 4. Camada Gold (Produto Final)
É a camada de Negócio.
* Aqui aplicamos as regras complexas, como o **SCD Tipo 2** (rastrear histórico de mudanças).
* Os dados são modelados em formato de "Estrela" (Fatos e Dimensões) para alimentar o Dashboard de forma rápida.

## Tecnologias Utilizadas
* **Apache Spark:** O motor que processa grandes volumes de dados.
* **Databricks:** A plataforma unificada onde tudo acontece.
* **Delta Lake:** A tecnologia que traz confiabilidade para o Data Lake.