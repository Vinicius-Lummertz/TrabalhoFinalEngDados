"""
Script de Geração de Massa de Dados para o Projeto SkyData.
Disciplina: Engenharia de Dados
Modo: LOCAL (Gera CSVs e JSONs simulando uma ingestão de API)
Stack: Python 3.10+, Pandas, Faker.

Este script gera arquivos na pasta 'data_bronze/' que servem como
Landing Zone para o pipeline Spark.
"""

import os
import random
import logging
import json
from datetime import datetime, timedelta
from typing import List, Tuple, Any, Dict

import pandas as pd
from faker import Faker
from tqdm import tqdm

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES DE SAÍDA ---
OUTPUT_DIR = "data_bronze"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- METAS DE GERAÇÃO (~200k linhas) ---
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
    """Orquestrador de geração e persistência local."""

    def __init__(self):
        # Cache de IDs para garantir Integridade Referencial (FKs)
        self.ids: Dict[str, List[int]] = {k: [] for k in COUNTS.keys() if k != "tripulacao"}
        self.ids["tripulacao"] = [] 
        
        self.funcionarios_pilotos: List[int] = []
        self.funcionarios_comissarios: List[int] = []

        # Limpa arquivos antigos para não duplicar em execuções seguidas
        self._clean_output_dir()

    def _clean_output_dir(self):
        """Remove arquivos CSV/JSON antigos da pasta de saída."""
        for f in os.listdir(OUTPUT_DIR):
            if f.endswith(".csv") or f.endswith(".json"):
                os.remove(os.path.join(OUTPUT_DIR, f))

    def api_insert(self, table_name: str, columns: Tuple[str, ...], values: List[Tuple[Any, ...]]):
        """
        SIMULAÇÃO DE API - PERSISTÊNCIA EM ARQUIVO LOCAL.
        
        Recebe os dados como se fosse um INSERT de banco, mas escreve em CSV/JSON.
        Mantém o modo 'append' para suportar escritas em lote.
        """
        if not values:
            return

        filename = f"{table_name}.csv"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # Converte para DataFrame para facilitar a escrita CSV
        df = pd.DataFrame(values, columns=columns)
        
        # Garante formatação ISO 8601 para datas (compatível com Spark)
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

        # Escreve no disco (Append se já existir, Write se for novo)
        header = not os.path.exists(filepath)
        df.to_csv(filepath, mode='a', header=header, index=False, encoding='utf-8')
        
        logger.debug(f"Persistido {len(values)} registros em {filename}")

    def api_insert_nosql(self, collection_name: str, documents: List[Dict[str, Any]]):
        """Simula inserção em banco NoSQL (Salva JSON Lines)."""
        if not documents:
            return

        filename = f"{collection_name}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)

        def json_serial(obj):
            """Função auxiliar para serializar datas em JSON"""
            if isinstance(obj, (datetime, timedelta)):
                return obj.isoformat()
            raise TypeError (f"Type {type(obj)} not serializable")

        with open(filepath, 'a', encoding='utf-8') as f:
            for doc in documents:
                f.write(json.dumps(doc, default=json_serial, ensure_ascii=False) + '\n')
        
        logger.debug(f"Persistido {len(documents)} docs em {filename}")

    # --- HELPER DE DATAS PRECISAS ---
    def get_precise_timestamp(self, days_ago: int = 0) -> datetime:
        dt = datetime.now() - timedelta(days=days_ago)
        return dt

    # --- GERADORES ---

    def generate_companhias(self):
        cols = ("id_companhia", "codigo_iata", "codigo_icao", "nome_fantasia", "razao_social", "pais_sede", "criado_em", "atualizado_em")
        rows = []
        cias_reais = [
            ("LA", "Latam Airlines"), ("AD", "Azul Linhas Aéreas"), ("G3", "Gol Linhas Aéreas"),
            ("TP", "TAP Air Portugal"), ("AA", "American Airlines"), ("AF", "Air France"),
            ("LH", "Lufthansa"), ("EK", "Emirates")
        ]
        
        for i in range(COUNTS["companhias"]):
            id_atual = i + 1
            if i < len(cias_reais):
                iata, nome = cias_reais[i]
            else:
                iata = fake.lexify(text='??').upper()
                nome = f"Cia Aérea {fake.company()}"

            dt_base = self.get_precise_timestamp(days_ago=random.randint(100, 365))
            
            rows.append((
                id_atual,
                iata,
                f"{iata}X",
                nome,
                f"{nome} S.A.",
                "Brasil" if i < 3 else "Exterior",
                dt_base,
                dt_base
            ))
            self.ids["companhias"].append(id_atual)

        self.api_insert("companhias_aereas", cols, rows)
        logger.info(f"Gerado: companhias_aereas ({len(rows)})")

    def generate_modelos(self):
        cols = ("id_modelo", "fabricante", "codigo_modelo", "nome_comercial", "capacidade_assentos", "alcance_max_km", "criado_em", "atualizado_em")
        rows = []
        fabricantes = ["Boeing", "Airbus", "Embraer"]
        
        for i in range(COUNTS["modelos"]):
            id_atual = i + 1
            fab = random.choice(fabricantes)
            dt_base = self.get_precise_timestamp(days_ago=random.randint(200, 500))
            rows.append((
                id_atual,
                fab,
                f"{fab}-{fake.bothify(text='###')}",
                f"{fab} Series {fake.random_digit()}",
                random.choice([150, 180, 220, 300]),
                random.randint(2000, 12000),
                dt_base,
                dt_base
            ))
            self.ids["modelos"].append(id_atual)
            
        self.api_insert("modelos_avioes", cols, rows)
        logger.info(f"Gerado: modelos_avioes ({len(rows)})")

    def generate_aeroportos(self):
        cols = ("id_aeroporto", "codigo_iata", "codigo_icao", "nome", "cidade", "pais", "latitude", "longitude", "hub_principal", "criado_em", "atualizado_em")
        rows = []
        iatas_gerados = set()
        
        for i in range(COUNTS["aeroportos"]):
            id_atual = i + 1
            while True:
                iata = fake.lexify(text='???').upper()
                if iata not in iatas_gerados:
                    iatas_gerados.add(iata)
                    break
            
            dt_base = self.get_precise_timestamp(days_ago=random.randint(300, 1000))
            dt_upd = dt_base if random.random() > 0.2 else datetime.now()
            
            rows.append((
                id_atual,
                iata,
                f"S{iata}",
                f"Aeroporto Internacional de {fake.city()}",
                fake.city(),
                "Brasil",
                float(fake.latitude()),
                float(fake.longitude()),
                random.choice([True, False]),
                dt_base,
                dt_upd
            ))
            self.ids["aeroportos"].append(id_atual)
            
        self.api_insert("aeroportos", cols, rows)
        logger.info(f"Gerado: aeroportos ({len(rows)})")

    def generate_clientes(self):
        cols = ("id_cliente", "nome_completo", "email", "telefone", "documento", "pais_nacionalidade", "data_nascimento", "data_cadastro", "atualizado_em", "status_fidelidade", "milhas_acumuladas")
        
        batch_size = 5000
        id_counter = 1
        for _ in tqdm(range(0, COUNTS["clientes"], batch_size), desc="Clientes"):
            rows = []
            for _ in range(batch_size):
                if id_counter > COUNTS["clientes"]: break
                
                dt_cad = fake.date_time_between(start_date="-2y", end_date="now")
                dt_upd = dt_cad if random.random() > 0.3 else datetime.now()
                
                rows.append((
                    id_counter,
                    fake.name(),
                    f"{fake.user_name()}_{random.randint(1,99999)}@{fake.free_email_domain()}",
                    fake.phone_number(),
                    fake.cpf(),
                    "Brasil",
                    fake.date_of_birth(minimum_age=18, maximum_age=90),
                    dt_cad,
                    dt_upd,
                    random.choice(['ATIVO', 'INATIVO']),
                    random.randint(0, 100000)
                ))
                self.ids["clientes"].append(id_counter)
                id_counter += 1
            self.api_insert("clientes", cols, rows)

    def generate_aeronaves(self):
        cols = ("id_aeronave", "id_modelo", "id_companhia", "id_aeroporto_base", "matricula", "ano_fabricacao", "em_operacao", "data_entrada_frota", "criado_em", "atualizado_em")
        rows = []
        
        for i in range(COUNTS["aeronaves"]):
            id_atual = i + 1
            dt_ent = fake.date_between(start_date="-5y", end_date="today")
            dt_base = datetime.combine(dt_ent, datetime.min.time())
            dt_upd = dt_base + timedelta(days=random.randint(0, 100))
            
            rows.append((
                id_atual,
                random.choice(self.ids["modelos"]),
                random.choice(self.ids["companhias"]),
                random.choice(self.ids["aeroportos"]),
                f"PT-{fake.bothify(text='###')}",
                random.randint(2010, 2024),
                random.choices([True, False], weights=[0.9, 0.1])[0],
                dt_ent,
                dt_base,
                dt_upd
            ))
            self.ids["aeronaves"].append(id_atual)
        self.api_insert("aeronaves", cols, rows)
        logger.info(f"Gerado: aeronaves ({len(rows)})")

    def generate_funcionarios(self):
        cols = ("id_funcionario", "id_companhia", "id_aeroporto_base", "nome_completo", "cargo", "data_nascimento", "data_admissao", "salario", "criado_em", "atualizado_em")
        rows = []
        cargos = ["Piloto", "Copiloto", "Comissário", "Mecânico", "Agente de Solo"]
        
        for i in range(COUNTS["funcionarios"]):
            id_atual = i + 1
            cargo = random.choices(cargos, weights=[0.1, 0.1, 0.4, 0.2, 0.2])[0]
            dt_adm = fake.date_between(start_date="-10y", end_date="today")
            dt_base = datetime.combine(dt_adm, datetime.min.time())
            dt_upd = dt_base + timedelta(days=random.randint(0, 300))
            if dt_upd > datetime.now(): dt_upd = datetime.now()

            rows.append((
                id_atual,
                random.choice(self.ids["companhias"]),
                random.choice(self.ids["aeroportos"]),
                fake.name(),
                cargo,
                fake.date_of_birth(minimum_age=20, maximum_age=60),
                dt_adm,
                random.uniform(3000, 25000),
                dt_base,
                dt_upd
            ))
            self.ids["funcionarios"].append(id_atual)
            
            if cargo in ["Piloto", "Copiloto"]:
                self.funcionarios_pilotos.append(id_atual)
            elif cargo == "Comissário":
                self.funcionarios_comissarios.append(id_atual)

        # Fallback de segurança
        if not self.funcionarios_pilotos: self.funcionarios_pilotos = self.ids["funcionarios"][:5]
        if not self.funcionarios_comissarios: self.funcionarios_comissarios = self.ids["funcionarios"][5:]

        self.api_insert("funcionarios", cols, rows)
        logger.info(f"Gerado: funcionarios ({len(rows)})")

    def generate_voos(self):
        cols = ("id_voo", "id_companhia", "id_aeronave", "id_aeroporto_origem", "id_aeroporto_destino", "numero_voo", "partida_prevista", "chegada_prevista", "partida_real", "status", "tarifa_media_bruta", "criado_em", "atualizado_em")
        
        batch_size = 2000
        id_counter = 1
        for _ in tqdm(range(0, COUNTS["voos"], batch_size), desc="Voos"):
            rows = []
            for _ in range(batch_size):
                if id_counter > COUNTS["voos"]: break

                origem = random.choice(self.ids["aeroportos"])
                destino = random.choice([a for a in self.ids["aeroportos"] if a != origem])
                dt_partida = fake.date_time_between(start_date="-1y", end_date="+2d")
                duracao = timedelta(hours=random.randint(1, 5))
                dt_criacao = dt_partida - timedelta(days=random.randint(10, 60))
                dt_upd = dt_partida + duracao + timedelta(minutes=30)

                rows.append((
                    id_counter,
                    random.choice(self.ids["companhias"]),
                    random.choice(self.ids["aeronaves"]),
                    origem,
                    destino,
                    f"{fake.bothify(text='??####')}",
                    dt_partida,
                    dt_partida + duracao,
                    dt_partida + timedelta(minutes=random.randint(-10, 60)),
                    random.choice(['POUSADO', 'PROGRAMADO', 'CANCELADO']),
                    random.uniform(200, 1500),
                    dt_criacao,
                    dt_upd
                ))
                self.ids["voos"].append(id_counter)
                id_counter += 1
            self.api_insert("voos", cols, rows)

    def generate_reservas_bilhetes(self):
        logger.info("Gerando Fluxo de Reservas e Bilhetes...")
        
        # 1. Reservas
        cols_res = ("id_reserva", "codigo_localizador", "id_cliente_titular", "data_reserva", "valor_total_reserva", "status_reserva", "status_pagamento", "criado_em", "atualizado_em")
        
        batch_size = 2000
        id_res_counter = 1
        for _ in tqdm(range(0, COUNTS["reservas"], batch_size), desc="Reservas"):
            rows = []
            for _ in range(batch_size):
                if id_res_counter > COUNTS["reservas"]: break

                dt_res = fake.date_time_between(start_date="-1y", end_date="now")
                rows.append((
                    id_res_counter,
                    fake.bothify(text='??????').upper(),
                    random.choice(self.ids["clientes"]),
                    dt_res,
                    random.uniform(500, 5000),
                    "CONFIRMADA",
                    "APROVADO",
                    dt_res,
                    dt_res
                ))
                self.ids["reservas"].append(id_res_counter)
                id_res_counter += 1
            self.api_insert("reservas", cols_res, rows)

        # 2. Bilhetes
        cols_bil = ("id_bilhete", "numero_bilhete", "id_reserva", "id_cliente_passageiro", "id_voo", "classe_cabine", "tarifa_base", "impostos_taxas", "data_emissao", "criado_em", "atualizado_em")
        
        rows_bil = []
        current_reserva_idx = 0
        total_bilhetes_target = COUNTS["bilhetes"]
        id_bil_counter = 1
        
        pbar = tqdm(total=total_bilhetes_target, desc="Bilhetes")
        
        while id_bil_counter <= total_bilhetes_target:
            if current_reserva_idx >= len(self.ids["reservas"]):
                current_reserva_idx = 0
            
            id_res = self.ids["reservas"][current_reserva_idx]
            current_reserva_idx += 1
            
            dt_emissao = fake.date_time_between(start_date="-1y", end_date="now")
            
            rows_bil.append((
                id_bil_counter,
                fake.bothify(text='176##########'),
                id_res,
                random.choice(self.ids["clientes"]),
                random.choice(self.ids["voos"]),
                random.choice(['ECONOMICA', 'EXECUTIVA']),
                random.uniform(200, 1000),
                random.uniform(50, 200),
                dt_emissao,
                dt_emissao,
                dt_emissao
            ))
            
            self.ids["bilhetes"].append(id_bil_counter)
            id_bil_counter += 1

            if len(rows_bil) >= 2000:
                self.api_insert("bilhetes", cols_bil, rows_bil)
                pbar.update(len(rows_bil))
                rows_bil = []

        if rows_bil:
            self.api_insert("bilhetes", cols_bil, rows_bil)
            pbar.update(len(rows_bil))

    def generate_operacional(self):
        logger.info("Gerando Operacional (Bagagens, Manutenção, Tripulação)...")
        
        # Bagagens
        cols_bag = ("id_bagagem", "id_bilhete", "tag_bagagem", "tipo", "peso_kg", "criado_em", "atualizado_em")
        rows_bag = []
        for i in range(COUNTS["bagagens"]):
            dt_now = datetime.now()
            rows_bag.append((
                i + 1,
                random.choice(self.ids["bilhetes"]),
                fake.bothify(text='##-#####'),
                random.choice(['DESPACHADA', 'MAO']),
                random.uniform(5.0, 23.0),
                dt_now,
                dt_now
            ))
            if len(rows_bag) >= 3000:
                self.api_insert("bagagens", cols_bag, rows_bag)
                rows_bag = []
        if rows_bag: self.api_insert("bagagens", cols_bag, rows_bag)

        # Manutenções
        cols_man = ("id_manutencao", "id_aeronave", "tipo", "status", "data_abertura", "data_inicio", "custo_total", "criado_em", "atualizado_em")
        rows_man = []
        for i in range(COUNTS["manutencoes"]):
            dt_inicio = fake.date_time_between(start_date="-1y", end_date="now")
            dt_fim = dt_inicio + timedelta(hours=random.randint(2, 48))
            rows_man.append((
                i + 1,
                random.choice(self.ids["aeronaves"]),
                random.choice(['PREVENTIVA', 'CORRETIVA']),
                random.choice(['CONCLUIDA', 'EM_ANDAMENTO']),
                dt_inicio - timedelta(days=2),
                dt_inicio,
                random.uniform(1000, 50000),
                dt_inicio,
                dt_fim
            ))
        self.api_insert("manutencoes", cols_man, rows_man)

        # Tripulação
        cols_trip = ("id_voo", "id_funcionario", "funcao_no_voo")
        rows_trip = []
        registros_criados = 0
        voos_embaralhados = list(self.ids["voos"])
        random.shuffle(voos_embaralhados)

        while registros_criados < COUNTS["tripulacao"]:
            for id_voo in voos_embaralhados:
                if registros_criados >= COUNTS["tripulacao"]: break
                try:
                    rows_trip.append((id_voo, random.choice(self.funcionarios_pilotos), "Comandante"))
                    registros_criados += 1
                    if registros_criados < COUNTS["tripulacao"]:
                        rows_trip.append((id_voo, random.choice(self.funcionarios_comissarios), "Comissário Chefe"))
                        registros_criados += 1
                except IndexError: pass
                
                if len(rows_trip) >= 5000:
                    df = pd.DataFrame(rows_trip, columns=cols_trip).drop_duplicates(subset=["id_voo", "id_funcionario"])
                    self.api_insert("tripulacao_voo", cols_trip, [tuple(x) for x in df.values])
                    rows_trip = []
        
        if rows_trip:
            df = pd.DataFrame(rows_trip, columns=cols_trip).drop_duplicates(subset=["id_voo", "id_funcionario"])
            self.api_insert("tripulacao_voo", cols_trip, [tuple(x) for x in df.values])

    def generate_mongodb_feedback(self):
        logger.info("Gerando Feedbacks (Simulação NoSQL)...")
        
        amostra_voos = random.choices(self.ids["voos"], k=5000)
        amostra_clientes = random.choices(self.ids["clientes"], k=5000)
        
        docs = []
        for i in tqdm(range(5000), desc="JSON Docs"):
            dt_resp = fake.date_time_between(start_date="-1y", end_date="now")
            
            docs.append({
                "id_feedback": str(fake.uuid4()),
                "id_voo": amostra_voos[i],
                "id_cliente": amostra_clientes[i],
                "data_resposta": dt_resp,
                "canal": random.choice(["APP", "EMAIL"]),
                "nota_geral": random.randint(1, 10),
                "comentarios": {
                    "texto": fake.text(),
                    "sentimento": random.choice(["POSITIVE", "NEUTRAL", "NEGATIVE"])
                },
                "criado_em": dt_resp,
                "atualizado_em": dt_resp
            })
        
        self.api_insert_nosql("feedbacks", docs)

    def run(self):
        logger.info("--- 🚀 INICIANDO GERAÇÃO LOCAL (ARQUIVOS) ---")
        try:
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
            logger.info(f"--- ✅ SUCESSO! Arquivos gerados em: {OUTPUT_DIR}/ ---")
        except Exception as e:
            logger.error(f"FALHA FATAL: {e}")
            raise

if __name__ == "__main__":
    generator = DataGenerator()
    generator.run()