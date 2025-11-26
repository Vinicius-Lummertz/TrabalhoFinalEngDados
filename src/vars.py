from typing import Dict

CATALOG = ""
LANDING_BASE_PATH = "/mnt/aviacao/landing"
BRONZE_SCHEMA = "aviacao_bronze"
META_SCHEMA = "aviacao_meta"
ORIGEM_SISTEMA = "postgres-aviacao"

JDBC_URL = "jdbc:postgresql://host:port/database"
JDBC_USER = "user"
JDBC_PASSWORD = "password"
JDBC_DRIVER = "org.postgresql.Driver"

TABLE_CONFIGS: Dict[str, Dict] = {
    "companhias_aereas": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "modelos_avioes": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "aeroportos": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "aeronaves": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "funcionarios": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "clientes": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "voos": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "reservas": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "bilhetes": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "bagagens": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "manutencoes": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
    "tripulacao_voo": {
        "schema": "aviacao",
        "business_key": ["id"],
    },
}
