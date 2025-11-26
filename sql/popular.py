import os
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine, text, insert, Table, MetaData
from sqlalchemy.orm import sessionmaker
from pymongo import MongoClient
from tqdm import tqdm  # Barra de progresso

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE CONEXÃO ---
# Ajuste conforme seu docker-compose
PG_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
MONGO_URI = os.getenv("MONGO_URL", "mongodb://localhost:27017/")

# --- CONFIGURAÇÕES DE GERAÇÃO (Qtd solicitada) ---
COUNTS = {
    "companhias": 8,
    "modelos": 24,
    "aeroportos": 80,
    "aeronaves": 60,
    "funcionarios": 800,
    "clientes": 35000,
    "voos": 18000,
    "reservas": 22000,
    "bilhetes": 27000,
    "bagagens": 32400,
    "manutencoes": 2628,
    "tripulacao": 60000
}

fake = Faker('pt_BR')
Faker.seed(42)
random.seed(42)


class DataGenerator:
    """Classe responsável por orquestrar a geração e inserção de dados."""

    def __init__(self):
        self.pg_engine = create_engine(PG_URL, echo=False)
        self.metadata = MetaData(schema="aviacao")
        self.metadata.reflect(bind=self.pg_engine)
        self.mongo_client = MongoClient(MONGO_URI)
        self.mongo_db = self.mongo_client["aviacao_feedback"]
        
        # Cache de IDs para FKs
        self.ids: Dict[str, List[int]] = {
            "companhias": [], "modelos": [], "aeroportos": [],
            "aeronaves": [], "clientes": [], "funcionarios": [],
            "voos": [], "reservas": [], "bilhetes": []
        }
        
        # Cache específico para funcionários por cargo
        self.funcionarios_pilotos: List[int] = []
        self.funcionarios_comissarios: List[int] = []

    def _batch_insert(self, table_name: str, data: List[Dict[str, Any]], batch_size: int = 1000):
        """Insere dados em lote no PostgreSQL usando SQLAlchemy Core."""
        if not data:
            return

        table = self.metadata.tables[f"aviacao.{table_name}"]
        
        with self.pg_engine.begin() as conn:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                conn.execute(insert(table), batch)
        
        logger.info(f"Tabela '{table_name}': {len(data)} registros inseridos.")

    def get_ids(self, table_name: str, id_column: str) -> List[int]:
        """Recupera IDs gerados para usar como Foreign Keys."""
        with self.pg_engine.connect() as conn:
            query = text(f"SELECT {id_column} FROM aviacao.{table_name}")
            result = conn.execute(query).fetchall()
            return [row[0] for row in result]

    # --- GERADORES NÍVEL 1 (SEM DEPENDÊNCIA) ---

    def generate_companhias(self):
        """Gera Companhias Aéreas."""
        data = []
        cias_reais = [
            ("LA", "Latam Airlines"), ("AD", "Azul Linhas Aéreas"), ("G3", "Gol Linhas Aéreas"),
            ("TP", "TAP Air Portugal"), ("AA", "American Airlines"), ("AF", "Air France"),
            ("LH", "Lufthansa"), ("EK", "Emirates")
        ]
        
        for i in range(COUNTS["companhias"]):
            iata, nome = cias_reais[i]
            data.append({
                "codigo_iata": iata,
                "codigo_icao": f"{iata}X", # Simulado
                "nome_fantasia": nome,
                "razao_social": f"{nome} S.A.",
                "pais_sede": "Brasil" if i < 3 else "Exterior",
                "criado_em": datetime.now()
            })
        self._batch_insert("companhias_aereas", data)
        self.ids["companhias"] = self.get_ids("companhias_aereas", "id_companhia")

    def generate_modelos(self):
        """Gera Modelos de Avião."""
        data = []
        fabricantes = ["Boeing", "Airbus", "Embraer"]
        for i in range(COUNTS["modelos"]):
            fab = random.choice(fabricantes)
            data.append({
                "fabricante": fab,
                "codigo_modelo": f"{fab}-{fake.bothify(text='###')}",
                "nome_comercial": f"{fab} Series {fake.random_digit()}",
                "capacidade_assentos": random.choice([150, 180, 220, 300]),
                "alcance_max_km": random.randint(2000, 12000),
                "criado_em": datetime.now()
            })
        self._batch_insert("modelos_avioes", data)
        self.ids["modelos"] = self.get_ids("modelos_avioes", "id_modelo")

    def generate_aeroportos(self):
        """Gera Aeroportos."""
        data = []
        # Lista parcial de IATA codes reais para dar realismo
        iatas = list(set([fake.lexify(text='???').upper() for _ in range(COUNTS["aeroportos"])]))
        
        for iata in iatas[:COUNTS["aeroportos"]]:
            data.append({
                "codigo_iata": iata,
                "codigo_icao": f"S{iata}",
                "nome": f"Aeroporto Internacional de {fake.city()}",
                "cidade": fake.city(),
                "pais": "Brasil",
                "latitude": float(fake.latitude()),
                "longitude": float(fake.longitude()),
                "hub_principal": random.choice([True, False]),
                "criado_em": datetime.now()
            })
        self._batch_insert("aeroportos", data)
        self.ids["aeroportos"] = self.get_ids("aeroportos", "id_aeroporto")

    def generate_clientes(self):
        """Gera Clientes (Massa grande)."""
        logger.info("Gerando clientes... isso pode demorar um pouco.")
        data = []
        for _ in tqdm(range(COUNTS["clientes"]), desc="Clientes"):
            dt_cadastro = fake.date_time_between(start_date="-2y", end_date="now")
            data.append({
                "nome_completo": fake.name(),
                "email": f"{fake.user_name()}_{random.randint(1,9999)}@{fake.free_email_domain()}",
                "telefone": fake.phone_number(),
                "documento": fake.cpf(),
                "pais_nacionalidade": "Brasil",
                "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=90),
                "data_cadastro": dt_cadastro,
                "atualizado_em": dt_cadastro, # Inicialmente igual
                "status_fidelidade": random.choice(['ATIVO', 'INATIVO']),
                "milhas_acumuladas": random.randint(0, 100000)
            })
        self._batch_insert("clientes", data)
        self.ids["clientes"] = self.get_ids("clientes", "id_cliente")

    # --- GERADORES NÍVEL 2 (DEPENDEM DE NÍVEL 1) ---

    def generate_aeronaves(self):
        data = []
        for _ in range(COUNTS["aeronaves"]):
            dt_entrada = fake.date_between(start_date="-5y", end_date="today")
            data.append({
                "id_modelo": random.choice(self.ids["modelos"]),
                "id_companhia": random.choice(self.ids["companhias"]),
                "id_aeroporto_base": random.choice(self.ids["aeroportos"]),
                "matricula": f"PT-{fake.bothify(text='###')}",
                "ano_fabricacao": random.randint(2010, 2024),
                "em_operacao": random.choices([True, False], weights=[0.9, 0.1])[0],
                "data_entrada_frota": dt_entrada,
                "criado_em": datetime.now()
            })
        self._batch_insert("aeronaves", data)
        self.ids["aeronaves"] = self.get_ids("aeronaves", "id_aeronave")

    def generate_funcionarios(self):
        data = []
        cargos = ["Piloto", "Copiloto", "Comissário", "Mecânico", "Agente de Solo"]
        
        for _ in range(COUNTS["funcionarios"]):
            cargo = random.choices(cargos, weights=[0.1, 0.1, 0.4, 0.2, 0.2])[0]
            data.append({
                "id_companhia": random.choice(self.ids["companhias"]),
                "id_aeroporto_base": random.choice(self.ids["aeroportos"]),
                "nome_completo": fake.name(),
                "cargo": cargo,
                "data_nascimento": fake.date_of_birth(minimum_age=20, maximum_age=60),
                "data_admissao": fake.date_between(start_date="-10y", end_date="today"),
                "salario": random.uniform(3000, 25000),
                "criado_em": datetime.now()
            })
        
        self._batch_insert("funcionarios", data)
        
        # Recupera IDs segregados para uso na tripulação
        with self.pg_engine.connect() as conn:
            self.funcionarios_pilotos = [r[0] for r in conn.execute(text("SELECT id_funcionario FROM aviacao.funcionarios WHERE cargo IN ('Piloto', 'Copiloto')")).fetchall()]
            self.funcionarios_comissarios = [r[0] for r in conn.execute(text("SELECT id_funcionario FROM aviacao.funcionarios WHERE cargo = 'Comissário'")).fetchall()]
            # Fallback se a geração aleatória não criou o suficiente
            all_funcs = self.get_ids("funcionarios", "id_funcionario")
            if not self.funcionarios_pilotos: self.funcionarios_pilotos = all_funcs
            if not self.funcionarios_comissarios: self.funcionarios_comissarios = all_funcs
            self.ids["funcionarios"] = all_funcs

    # --- GERADORES NÍVEL 3 (VOOS) ---

    def generate_voos(self):
        logger.info("Gerando voos...")
        data = []
        for _ in tqdm(range(COUNTS["voos"]), desc="Voos"):
            origem = random.choice(self.ids["aeroportos"])
            destino = random.choice([a for a in self.ids["aeroportos"] if a != origem])
            dt_partida = fake.date_time_between(start_date="-1y", end_date="+2d")
            duracao = timedelta(hours=random.randint(1, 5))
            
            data.append({
                "id_companhia": random.choice(self.ids["companhias"]),
                "id_aeronave": random.choice(self.ids["aeronaves"]),
                "id_aeroporto_origem": origem,
                "id_aeroporto_destino": destino,
                "numero_voo": f"{fake.bothify(text='??####')}",
                "partida_prevista": dt_partida,
                "chegada_prevista": dt_partida + duracao,
                "partida_real": dt_partida + timedelta(minutes=random.randint(-10, 60)), # Simula atraso
                "status": random.choice(['POUSADO', 'PROGRAMADO', 'CANCELADO']),
                "tarifa_media_bruta": random.uniform(200, 1500),
                "criado_em": datetime.now()
            })
        self._batch_insert("voos", data)
        self.ids["voos"] = self.get_ids("voos", "id_voo")

    # --- GERADORES NÍVEL 4 E 5 (RESERVAS E BILHETES) ---

    def generate_reservas_bilhetes(self):
        """Gera Reservas e Bilhetes juntos para consistência."""
        logger.info("Gerando reservas e bilhetes...")
        
        reservas_data = []
        bilhetes_data = []
        
        # Gera Reservas
        for _ in tqdm(range(COUNTS["reservas"]), desc="Reservas"):
            id_cliente = random.choice(self.ids["clientes"])
            dt_reserva = fake.date_time_between(start_date="-1y", end_date="now")
            
            # Precisamos do ID da reserva, mas como estamos fazendo batch insert,
            # vamos gerar o objeto reserva, inserir, e depois fazer os bilhetes?
            # Para performance, vamos gerar tudo em memória e assumir IDs sequenciais 
            # OU (melhor) inserir reservas, recuperar IDs e depois gerar bilhetes.
            
            reservas_data.append({
                "codigo_localizador": fake.bothify(text='??????').upper(),
                "id_cliente_titular": id_cliente,
                "data_reserva": dt_reserva,
                "valor_total_reserva": random.uniform(500, 5000),
                "status_reserva": "CONFIRMADA",
                "status_pagamento": "APROVADO",
                "criado_em": dt_reserva
            })
        
        self._batch_insert("reservas", reservas_data)
        self.ids["reservas"] = self.get_ids("reservas", "id_reserva")

        # Gera Bilhetes (Baseado nas reservas criadas)
        logger.info("Gerando bilhetes baseados nas reservas...")
        count_bilhetes = 0
        for id_reserva in self.ids["reservas"]:
            if count_bilhetes >= COUNTS["bilhetes"]: break
            
            # Cada reserva tem 1 ou 2 bilhetes
            qtd_bilhetes_reserva = random.choices([1, 2], weights=[0.8, 0.2])[0]
            
            for _ in range(qtd_bilhetes_reserva):
                bilhetes_data.append({
                    "numero_bilhete": fake.bothify(text='176##########'),
                    "id_reserva": id_reserva,
                    "id_cliente_passageiro": random.choice(self.ids["clientes"]), # Pode ser diferente do titular
                    "id_voo": random.choice(self.ids["voos"]),
                    "classe_cabine": random.choice(['ECONOMICA', 'EXECUTIVA']),
                    "tarifa_base": random.uniform(200, 1000),
                    "impostos_taxas": random.uniform(50, 200),
                    "data_emissao": fake.date_time_between(start_date="-1y", end_date="now"),
                    "criado_em": datetime.now()
                })
                count_bilhetes += 1

        self._batch_insert("bilhetes", bilhetes_data)
        self.ids["bilhetes"] = self.get_ids("bilhetes", "id_bilhete")

    # --- GERADORES FINAIS (BAGAGEM, TRIPULAÇÃO, MANUTENÇÃO) ---

    def generate_operacional(self):
        """Gera dados operacionais."""
        
        # Bagagens
        bagagens = []
        for _ in tqdm(range(COUNTS["bagagens"]), desc="Bagagens"):
            bagagens.append({
                "id_bilhete": random.choice(self.ids["bilhetes"]),
                "tag_bagagem": fake.bothify(text='##-#####'),
                "tipo": random.choice(['DESPACHADA', 'MAO']),
                "peso_kg": random.uniform(5.0, 23.0),
                "criado_em": datetime.now()
            })
        self._batch_insert("bagagens", bagagens)

        # Manutenções
        manutencoes = []
        for _ in range(COUNTS["manutencoes"]):
            dt_inicio = fake.date_time_between(start_date="-1y", end_date="now")
            manutencoes.append({
                "id_aeronave": random.choice(self.ids["aeronaves"]),
                "tipo": random.choice(['PREVENTIVA', 'CORRETIVA']),
                "status": random.choice(['CONCLUIDA', 'EM_ANDAMENTO']),
                "data_abertura": dt_inicio - timedelta(days=2),
                "data_inicio": dt_inicio,
                "custo_total": random.uniform(1000, 50000),
                "criado_em": datetime.now()
            })
        self._batch_insert("manutencoes", manutencoes)

        # Tripulação (Associação Voo <-> Funcionario)
        tripulacao = []
        logger.info("Alocando tripulação...")
        
        # Garante a meta de 60k registros iterando sobre voos
        registros_criados = 0
        voos_embaralhados = list(self.ids["voos"])
        random.shuffle(voos_embaralhados)

        # Loop infinito controlado para preencher a meta
        while registros_criados < COUNTS["tripulacao"]:
            for id_voo in voos_embaralhados:
                if registros_criados >= COUNTS["tripulacao"]: break
                
                # Tenta adicionar tripulantes ao voo
                try:
                    # 1 Piloto
                    tripulacao.append({"id_voo": id_voo, "id_funcionario": random.choice(self.funcionarios_pilotos), "funcao_no_voo": "Comandante"})
                    registros_criados += 1
                    
                    if registros_criados < COUNTS["tripulacao"]:
                        tripulacao.append({"id_voo": id_voo, "id_funcionario": random.choice(self.funcionarios_comissarios), "funcao_no_voo": "Comissário Chefe"})
                        registros_criados += 1
                        
                except IndexError:
                    pass # Caso falte funcionário
                    
        # Remove duplicatas (Chave composta id_voo + id_funcionario)
        # Pandas é rápido para dedulicar lista de dicts
        df_trip = pd.DataFrame(tripulacao).drop_duplicates(subset=["id_voo", "id_funcionario"])
        
        # Insere usando o método batch do pandas para variar
        df_trip.to_sql("tripulacao_voo", self.pg_engine, schema="aviacao", if_exists="append", index=False, chunksize=1000)
        logger.info(f"Tabela 'tripulacao_voo': {len(df_trip)} registros inseridos.")

    def generate_mongodb_feedback(self):
        """Gera documentos no MongoDB vinculados aos voos/clientes SQL."""
        logger.info("Gerando feedbacks no MongoDB...")
        
        # Seleciona amostra de voos e clientes
        amostra_voos = random.choices(self.ids["voos"], k=5000)
        amostra_clientes = random.choices(self.ids["clientes"], k=5000)
        
        docs = []
        for i in tqdm(range(5000), desc="Mongo Docs"):
            docs.append({
                "id_voo": int(amostra_voos[i]), # Cast para int Python nativo (Mongo não gosta de numpy int)
                "id_cliente": int(amostra_clientes[i]),
                "data_resposta": fake.date_time_between(start_date="-1y", end_date="now"),
                "canal": random.choice(["APP", "EMAIL"]),
                "nota_geral": random.randint(1, 10),
                "comentarios": {
                    "texto": fake.text(),
                    "sentimento": random.choice(["POSITIVE", "NEUTRAL", "NEGATIVE"])
                },
                "criado_em": datetime.now()
            })
        
        self.mongo_db.insert_many(docs)
        logger.info("MongoDB populado com sucesso.")

    def run(self):
        logger.info("--- INICIANDO POPULAÇÃO ---")
        self.generate_companhias()
        self.generate_modelos()
        self.generate_aeroportos()
        self.generate_clientes()
        self.generate_aeronaves()
        self.generate_funcionarios()
        self.generate_voos()
        self.generate_reservas_bilhetes()
        self.generate_operacional()
        self.generate_mongodb_feedback()
        logger.info("--- PROCESSO CONCLUÍDO ---")

if __name__ == "__main__":
    generator = DataGenerator()
    generator.run()