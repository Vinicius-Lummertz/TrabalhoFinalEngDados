from scripts.gerador_dados import gerar_dados

quantidades = {
    "companhias_aereas": 20,
    "modelos_avioes": 12,
    "aeroportos": 100,
    "aeronaves": 300,
    "funcionarios": 500,
    "clientes": 5000,
    "voos": 2000,
    "reservas": 4000,
    "bilhetes": 4000,
    "bagagens": 4000,
    "manutencoes": 200,
    "tripulacao_voo": 150
}

pg_url = (
    "postgresql+psycopg2://postgres:0mZhbgsBlOtYIUW3KNXq"
    "@aviao-metrics-databricks.c5kyywe4w1hd.us-east-1.rds.amazonaws.com:5432/postgres"
)

gerar_dados(quantidades, pg_url)