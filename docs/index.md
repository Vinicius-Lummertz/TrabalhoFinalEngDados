# ✈️ SkyData Platform
> A inteligência de dados para operações aéreas eficientes.

## O Escopo do Projeto
Este projeto compreende a construção da plataforma de dados moderna **SkyData**.
Nosso objetivo é garantir que os dados operacionais (voos, passageiros, manutenção) saiam dos sistemas transacionais e sejam transformados em ativos de informação confiáveis, históricos e prontos para análise.

## O Que Foi Entregue?
O pipeline de dados está maduro e cobre desde a ingestão até o refinamento:

1.  **Simulação de Operação Real:** Gerador de massa de dados complexa e relacional (PostgreSQL) para testes de carga.
2.  **Captura Inteligente (Landing):** Sistema que detecta apenas o que mudou na origem, evitando sobrecarga nos bancos de dados operacionais.
3.  **Histórico Bruto (Bronze):** Armazenamento seguro de todos os dados crus em formato Delta Lake.
4.  **Refinamento e Histórico (Silver):** Transformação dos dados em entidades de negócio. Aqui, aplicamos regras de qualidade e criamos uma "máquina do tempo", permitindo saber exatamente qual era o estado de um dado (ex: status de um voo ou aliança de uma companhia) em qualquer momento do passado.

---
*Projeto desenvolvido como parte do Trabalho Final de Engenharia de Dados - Foco em Ingestão e Modelagem.*