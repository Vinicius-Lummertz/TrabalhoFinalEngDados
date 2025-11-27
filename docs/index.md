# ✈️ SkyData Ingestion Module
> O alicerce de dados para operações aéreas eficientes.

## O Escopo Atual
Este projeto compreende o **Módulo de Ingestão e Camada Bronze** da plataforma de dados SkyData.
Nosso objetivo é garantir que os dados operacionais (voos, passageiros, manutenção) saiam dos sistemas transacionais e cheguem ao Data Lake de forma segura, rastreável e incremental.

## O Que Foi Entregue?
Atualmente, o pipeline é capaz de:
1.  **Simular Operação Real:** Gerar massa de dados complexa e relacional em PostgreSQL.
2.  **Captura Incremental:** Detectar apenas o que mudou na origem (CDC lógico) sem sobrecarregar o banco.
3.  **Data Lake Bronze:** Armazenar o histórico bruto em formato Delta Lake, pronto para ser refinado.

---
*Projeto desenvolvido como parte do Trabalho Final de Engenharia de Dados - Foco em Ingestão.*