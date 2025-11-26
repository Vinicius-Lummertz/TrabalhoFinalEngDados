import random
from faker import Faker
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

fake = Faker("pt_BR")

# ===========================================================
#  VARIÁVEIS GLOBAIS PARA GARANTIR UNICIDADE
# ===========================================================
usados = {
    "iata": set(),
    "icao": set(),
    "matriculas": set(),
    "documentos": set(),
    "localizadores": set(),
    "bilhetes": set(),
}

# ===========================================================
#  FUNÇÕES AUXILIARES
# ===========================================================

def random_date_3y():
    end = datetime.now()
    start = end - timedelta(days=365*3)
    return fake.date_time_between(start_date=start, end_date=end)

def carregar_valores_existentes(engine):
    consultas = {
        "iata": [
            "SELECT codigo_iata FROM aviacao.companhias_aereas",
            "SELECT codigo_iata FROM aviacao.aeroportos"
        ],
        "icao": [
            "SELECT codigo_icao FROM aviacao.companhias_aereas",
            "SELECT codigo_icao FROM aviacao.aeroportos"
        ],
        "matriculas": ["SELECT matricula FROM aviacao.aeronaves"],
        "documentos": ["SELECT documento FROM aviacao.clientes"],
        "localizadores": ["SELECT codigo_localizador FROM aviacao.reservas"],
        "bilhetes": ["SELECT numero_bilhete FROM aviacao.bilhetes"]
    }

    for chave, sql_list in consultas.items():
        for sql in sql_list:
            try:
                df = pd.read_sql(sql, engine)
                for val in df.iloc[:, 0].dropna().tolist():
                    usados[chave].add(val)
            except:
                pass

# ===========================================================
#  FUNÇÕES GERADORAS — SEM CAMPOS DE AUDITORIA
# ===========================================================

def gerar_companhias_aereas(qtd):
    iatas = usados["iata"]
    icaos = usados["icao"]

    novos_iata, novos_icao = [], []

    while len(novos_iata) < qtd:
        iata = fake.lexify("??").upper()
        if iata not in iatas:
            novos_iata.append(iata)
            iatas.add(iata)

    while len(novos_icao) < qtd:
        icao = fake.lexify("???").upper()
        if icao not in icaos:
            novos_icao.append(icao)
            icaos.add(icao)

    return pd.DataFrame({
        "nome": [fake.company() for _ in range(qtd)],
        "codigo_iata": novos_iata,
        "codigo_icao": novos_icao,
        "pais": [fake.country() for _ in range(qtd)],
        "alianca": [random.choice(["Star Alliance", "SkyTeam", "OneWorld", None]) for _ in range(qtd)],
        "ativo": True
    })


def gen_modelos_avioes(ids_companhias):
    fabricante = random.choice(["Airbus", "Boeing", "Embraer", "ATR"])
    return {
        "fabricante": fabricante,
        "modelo": f"{fabricante} {random.randint(100, 999)}",
        "capacidade_economica": random.randint(80, 300),
        "capacidade_executiva": random.randint(0, 40),
        "alcance_km": random.randint(2000, 15000),
        "peso_max_decolagem_kg": random.randint(40000, 300000),
        "companhia_padrao_id": random.choice(ids_companhias)
    }


def gen_aeroportos():
    iata = fake.lexify("???").upper()
    icao = fake.lexify("????").upper()

    while iata in usados["iata"]:
        iata = fake.lexify("???").upper()
    usados["iata"].add(iata)

    while icao in usados["icao"]:
        icao = fake.lexify("????").upper()
    usados["icao"].add(icao)

    return {
        "codigo_iata": iata,
        "codigo_icao": icao,
        "nome": fake.city() + " International Airport",
        "cidade": fake.city(),
        "pais": fake.country(),
        "latitude": fake.latitude(),
        "longitude": fake.longitude()
    }


def gen_aeronaves(ids_companhias, ids_modelos):
    matricula = "PT-" + fake.lexify("???").upper() + str(random.randint(100, 999))

    while matricula in usados["matriculas"]:
        matricula = "PT-" + fake.lexify("???").upper() + str(random.randint(100, 999))
    usados["matriculas"].add(matricula)

    return {
        "matricula": matricula,
        "ano_fabricacao": random.randint(1990, 2024),
        "status": random.choice(["ATIVA", "EM_MANUTENCAO", "ESTOCADA"]),
        "companhia_id": random.choice(ids_companhias),
        "modelo_id": random.choice(ids_modelos)
    }


def gen_funcionarios(ids_companhias):
    return {
        "nome": fake.name(),
        "cargo": random.choice(["PILOTO", "COPILOTO", "COMISSARIO", "MECANICO"]),
        "tipo_funcionario": random.choice(["TRIPULACAO", "SOLO"]),
        "data_admissao": fake.date_between(start_date="-10y", end_date="today"),
        "salario": round(random.uniform(3000, 40000), 2),
        "companhia_id": random.choice(ids_companhias)
    }


def gen_clientes():
    doc = fake.bothify("###.###.###-##")
    while doc in usados["documentos"]:
        doc = fake.bothify("###.###.###-##")
    usados["documentos"].add(doc)

    return {
        "nome": fake.name(),
        "documento": doc,
        "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=80),
        "email": fake.email(),
        "telefone": fake.phone_number(),
        "pais": fake.country()
    }


def gen_voos(ids_companhias, ids_aeronaves, ids_aeroportos):
    origem, destino = random.sample(ids_aeroportos, 2)
    partida = random_date_3y()
    return {
        "numero_voo": "VOO" + str(random.randint(1000, 9999)),
        "companhia_id": random.choice(ids_companhias),
        "aeronave_id": random.choice(ids_aeronaves),
        "aeroporto_origem_id": origem,
        "aeroporto_destino_id": destino,
        "data_partida": partida,
        "data_chegada_prevista": partida + timedelta(hours=random.randint(1, 12)),
        "status": random.choice(["PROGRAMADO", "DECOLADO", "POUSADO", "CANCELADO"])
    }


def gen_reservas(ids_clientes, ids_voos):
    loc = fake.lexify("??????").upper()
    while loc in usados["localizadores"]:
        loc = fake.lexify("??????").upper()
    usados["localizadores"].add(loc)

    return {
        "codigo_localizador": loc,
        "cliente_id": random.choice(ids_clientes),
        "voo_id": random.choice(ids_voos),
        "status": random.choice(["PENDENTE", "CONFIRMADA", "CANCELADA"]),
        "data_reserva": random_date_3y()
    }


def gen_bilhetes(ids_reservas):
    nb = fake.bothify("##########")
    while nb in usados["bilhetes"]:
        nb = fake.bothify("##########")
    usados["bilhetes"].add(nb)

    return {
        "reserva_id": random.choice(ids_reservas),
        "numero_bilhete": nb,
        "assento": fake.bothify("??##"),
        "classe_cabine": random.choice(["ECONOMICA", "PREMIUM_ECONOMICA", "EXECUTIVA", "PRIMEIRA"]),
        "status_pagamento": random.choice(["APROVADO", "PENDENTE", "RECUSADO"]),
        "valor_tarifa": round(random.uniform(150, 20000), 2)
    }


def gen_bagagens(ids_bilhetes):
    return {
        "bilhete_id": random.choice(ids_bilhetes),
        "peso_kg": round(random.uniform(5.0, 45.0), 1),
        "tipo": random.choice(["DESPACHADA", "MAO", "ESPECIAL"]),
        "volume": random.randint(10, 120)
    }


def gen_manutencoes(ids_aeronaves):
    inicio = random_date_3y()
    return {
        "aeronave_id": random.choice(ids_aeronaves),
        "tipo_manutencao": random.choice(["A_CHECK", "B_CHECK", "C_CHECK"]),
        "data_inicio": inicio,
        "data_fim": inicio + timedelta(days=random.randint(1, 10)),
        "custo_total": round(random.uniform(5000, 500000), 2),
        "descricao": fake.sentence(12)
    }


def gen_tripulacao_voo(ids_voos, ids_funcionarios):
    inicio = random_date_3y()
    return {
        "voo_id": random.choice(ids_voos),
        "funcionario_id": random.choice(ids_funcionarios),
        "funcao": random.choice(["COMANDANTE", "COPILOTO", "COMISSARIO"]),
        "inicio_escala": inicio,
        "fim_escala": inicio + timedelta(hours=random.randint(2, 14))
    }

# ===========================================================
#  INSERÇÃO
# ===========================================================

def inserir_dataframe(engine, tabela, df):
    df.to_sql(tabela, engine, schema="aviacao", if_exists="append", index=False)

# ===========================================================
#  PIPELINE GERADOR PRINCIPAL
# ===========================================================

def gerar_dados(quantidades, pg_url):

    engine = create_engine(pg_url)
    print("\n🔗 Conectado ao PostgreSQL")

    carregar_valores_existentes(engine)

    tabelas = [
        ("companhias_aereas", lambda: gerar_companhias_aereas(quantidades["companhias_aereas"])),
        ("modelos_avioes", lambda: pd.DataFrame([gen_modelos_avioes(ids_companhias) for _ in range(quantidades["modelos_avioes"])])),
        ("aeroportos", lambda: pd.DataFrame([gen_aeroportos() for _ in range(quantidades["aeroportos"])])),
        ("aeronaves", lambda: pd.DataFrame([gen_aeronaves(ids_companhias, ids_modelos) for _ in range(quantidades["aeronaves"])])),
        ("funcionarios", lambda: pd.DataFrame([gen_funcionarios(ids_companhias) for _ in range(quantidades["funcionarios"])])),
        ("clientes", lambda: pd.DataFrame([gen_clientes() for _ in range(quantidades["clientes"])])),
        ("voos", lambda: pd.DataFrame([gen_voos(ids_companhias, ids_aeronaves, ids_aeroportos) for _ in range(quantidades["voos"])])),
        ("reservas", lambda: pd.DataFrame([gen_reservas(ids_clientes, ids_voos) for _ in range(quantidades["reservas"])])),
        ("bilhetes", lambda: pd.DataFrame([gen_bilhetes(ids_reservas) for _ in range(quantidades["bilhetes"])])),
        ("bagagens", lambda: pd.DataFrame([gen_bagagens(ids_bilhetes) for _ in range(quantidades["bagagens"])])),
        ("manutencoes", lambda: pd.DataFrame([gen_manutencoes(ids_aeronaves) for _ in range(quantidades["manutencoes"])])),
        ("tripulacao_voo", lambda: pd.DataFrame([gen_tripulacao_voo(ids_voos, ids_funcionarios) for _ in range(quantidades["tripulacao_voo"])])),
    ]

    for nome_tabela, fn in tabelas:
        print(f"➡ Inserindo {nome_tabela}...")
        df = fn()
        inserir_dataframe(engine, nome_tabela, df)

        if nome_tabela == "companhias_aereas":
            ids_companhias = pd.read_sql("SELECT id FROM aviacao.companhias_aereas", engine)["id"].tolist()
        if nome_tabela == "modelos_avioes":
            ids_modelos = pd.read_sql("SELECT id FROM aviacao.modelos_avioes", engine)["id"].tolist()
        if nome_tabela == "aeroportos":
            ids_aeroportos = pd.read_sql("SELECT id FROM aviacao.aeroportos", engine)["id"].tolist()
        if nome_tabela == "aeronaves":
            ids_aeronaves = pd.read_sql("SELECT id FROM aviacao.aeronaves", engine)["id"].tolist()
        if nome_tabela == "funcionarios":
            ids_funcionarios = pd.read_sql("SELECT id FROM aviacao.funcionarios", engine)["id"].tolist()
        if nome_tabela == "clientes":
            ids_clientes = pd.read_sql("SELECT id FROM aviacao.clientes", engine)["id"].tolist()
        if nome_tabela == "voos":
            ids_voos = pd.read_sql("SELECT id FROM aviacao.voos", engine)["id"].tolist()
        if nome_tabela == "reservas":
            ids_reservas = pd.read_sql("SELECT id FROM aviacao.reservas", engine)["id"].tolist()
        if nome_tabela == "bilhetes":
            ids_bilhetes = pd.read_sql("SELECT id FROM aviacao.bilhetes", engine)["id"].tolist()

    print("\n✔ Dados gerados com sucesso!")

# ===========================================================
#  EXECUÇÃO
# ===========================================================

if __name__ == "__main__":

    quantidades = {
        "companhias_aereas": 30,
        "modelos_avioes": 120,
        "aeroportos": 1000,
        "aeronaves": 300,
        "funcionarios": 5000,
        "clientes": 50000,
        "voos": 20000,
        "reservas": 40000,
        "bilhetes": 40000,
        "bagagens": 40000,
        "manutencoes": 2000,
        "tripulacao_voo": 1500
    }

    pg_url = (
        "postgresql+psycopg2://postgres:0mZhbgsBlOtYIUW3KNXq"
        "@aviao-metrics-databricks.c5kyywe4w1hd.us-east-1.rds.amazonaws.com:5432/postgres"
    )

    gerar_dados(quantidades, pg_url)
