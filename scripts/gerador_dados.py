import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)  
random.seed(42)

os.makedirs("data", exist_ok=True)


def random_date_3y():
    """Gera uma data aleatória nos últimos 3 anos a partir de hoje."""
    end = datetime.now()
    start = end - timedelta(days=3*365)
    return fake.date_time_between(start_date=start, end_date=end)

def gen_modelos_avioes(n=20000):
    print(f"Gerando {n} modelos de aviões...")
    modelos = []
    fabricantes = ["Airbus", "Boeing", "Embraer", "Bombardier"]
    for i in range(n):
        modelos.append({
            "id_modelo": i+1,
            "modelo": f"Modelo-{fake.random_int(100,999)}",
            "fabricante": random.choice(fabricantes),
            "capacidade": random.randint(50, 300),
            "peso_max_decolagem": random.randint(50000, 300000),
            "data_registro": random_date_3y()
        })
    pd.DataFrame(modelos).to_csv("data/modelos_avioes.csv", index=False)

def gen_destinos(n=20000):
    print(f"Gerando {n} destinos...")
    destinos = []
    for i in range(n):
        destinos.append({
            "id_destino": i+1,
            "cidade": fake.city(),
            "pais": fake.country(),
            "iata": fake.lexify(text="???").upper(),
            "duracao_media_voo_min": random.randint(40, 900),
            "data_registro": random_date_3y()
        })
    pd.DataFrame(destinos).to_csv("data/destinos.csv", index=False)

def gen_aeroportos(n=20000):
    print(f"Gerando {n} aeroportos...")
    aeroportos = []
    for i in range(n):
        aeroportos.append({
            "id_aeroporto": i+1,
            "nome": f"Aeroporto {fake.last_name()}",
            "cidade": fake.city(),
            "pais": fake.country(),
            "codigo_iata": fake.lexify(text="???").upper(),
            "capacidade_anual": random.randint(1000000, 100000000),
            "data_registro": random_date_3y()
        })
    pd.DataFrame(aeroportos).to_csv("data/aeroportos.csv", index=False)

def gen_funcionarios(n=20000):
    print(f"Gerando {n} funcionários...")
    cargos = ["Piloto", "Copiloto", "Comissário", "Mecânico", "Operador"]
    funcionarios = []
    for i in range(n):
        funcionarios.append({
            "id_funcionario": i+1,
            "nome": fake.name(),
            "idade": random.randint(21, 65),
            "cargo": random.choice(cargos),
            "data_contratacao": random_date_3y(),
            "salario": random.randint(3000, 35000)
        })
    pd.DataFrame(funcionarios).to_csv("data/funcionarios.csv", index=False)

def gen_clientes(n=20000):
    print(f"Gerando {n} clientes...")
    clientes = []
    for i in range(n):
        clientes.append({
            "id_cliente": i+1,
            "nome": fake.name(),
            "email": fake.email(),
            "telefone": fake.phone_number(),
            "nacionalidade": fake.country(),
            "data_cadastro": random_date_3y()
        })
    pd.DataFrame(clientes).to_csv("data/clientes.csv", index=False)

def gen_planos_viagem(n=20000):
    print(f"Gerando {n} planos de viagem...")
    planos = []
    for i in range(n):
        planos.append({
            "id_plano": i+1,
            "id_cliente": random.randint(1, n),
            "id_destino": random.randint(1, n),
            "id_modelo": random.randint(1, n),
            "data_viagem": random_date_3y(),
            "preco": round(random.uniform(300, 10000), 2),
            "status": random.choice(["Confirmado", "Cancelado", "Concluído"])
        })
    pd.DataFrame(planos).to_csv("data/planos_viagem.csv", index=False)

def gen_voos(n=20000):
    """
    CORRIGIDO: A data de chegada agora é sempre posterior à data de partida.
    """
    print(f"Gerando {n} voos (Lógica corrigida)...")
    voos = []
    for i in range(n):
        dt_partida = random_date_3y()
        # Voo dura entre 40 min e 15 horas (900 min)
        duracao_minutos = random.randint(40, 900)
        dt_chegada = dt_partida + timedelta(minutes=duracao_minutos)
        
        voos.append({
            "id_voo": i+1,
            "numero_voo": f"AV{fake.random_int(1000,9999)}",
            "id_aeroporto_origem": random.randint(1, n),
            "id_aeroporto_destino": random.randint(1, n),
            "data_partida": dt_partida,
            "data_chegada": dt_chegada,
            "status": random.choice(["Programado", "Atrasado", "Cancelado", "Concluído"])
        })
    pd.DataFrame(voos).to_csv("data/voos.csv", index=False)

def gen_manutencoes(n=20000):
    print(f"Gerando {n} registros de manutenção...")
    manut = []
    tipos = ["Preventiva", "Corretiva"]
    for i in range(n):
        manut.append({
            "id_manutencao": i+1,
            "id_modelo": random.randint(1, n),
            "tipo": random.choice(tipos),
            "data_manutencao": random_date_3y(),
            "duracao_horas": random.randint(1, 120)
        })
    pd.DataFrame(manut).to_csv("data/manutencoes.csv", index=False)

def gen_passagens(n=20000):
    print(f"Gerando {n} passagens...")
    passagens = []
    for i in range(n):
        passagens.append({
            "id_passagem": i+1,
            "id_cliente": random.randint(1, n),
            "id_voo": random.randint(1, n),
            "assento": fake.bothify(text="??-##"),
            "preco_pago": random.randint(200, 15000),
            "data_compra": random_date_3y()
        })
    pd.DataFrame(passagens).to_csv("data/passagens.csv", index=False)

def gen_bagagens(n=20000):
    print(f"Gerando {n} bagagens...")
    bags = []
    for i in range(n):
        bags.append({
            "id_bagagem": i+1,
            "id_cliente": random.randint(1, n),
            "peso_kg": round(random.uniform(5, 45), 2),
            "tipo": random.choice(["Mão", "Despachada"]),
            "data_registro": random_date_3y()
        })
    pd.DataFrame(bags).to_csv("data/bagagens.csv", index=False)

def gerar_massa_completa():
    gen_modelos_avioes()
    gen_destinos()
    gen_aeroportos()
    gen_funcionarios()
    gen_clientes()
    gen_planos_viagem()
    gen_voos()
    gen_manutencoes()
    gen_passagens()
    gen_bagagens()
    print("\n--- Processo Finalizado ---")
    print("Todos os arquivos CSV foram gerados na pasta 'data/'.")

if __name__ == "__main__":
    gerar_massa_completa()