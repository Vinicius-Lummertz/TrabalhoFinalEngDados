CREATE SCHEMA IF NOT EXISTS aviacao;
SET search_path TO aviacao, public;


CREATE TYPE classe_cabine AS ENUM (
    'ECONOMICA',
    'PREMIUM_ECONOMICA',
    'EXECUTIVA',
    'PRIMEIRA'
);

CREATE TYPE status_voo AS ENUM (
    'PROGRAMADO',
    'EMBARQUE',
    'DECOLADO',
    'POUSADO',
    'ATRASADO',
    'CANCELADO',
    'DESVIADO'
);

CREATE TYPE status_reserva AS ENUM (
    'PENDENTE',
    'CONFIRMADA',
    'CANCELADA',
    'FINALIZADA',
    'NO_SHOW'
);

CREATE TYPE status_pagamento AS ENUM (
    'PENDENTE',
    'APROVADO',
    'RECUSADO',
    'ESTORNADO'
);

CREATE TABLE IF NOT EXISTS companhias_aereas (
    id                  BIGSERIAL PRIMARY KEY,
    nome                VARCHAR(255) NOT NULL,
    codigo_iata         CHAR(2) NOT NULL,
    codigo_icao         CHAR(3) NOT NULL,
    pais                VARCHAR(120) NOT NULL,
    alianca             VARCHAR(80),
    ativo               BOOLEAN NOT NULL DEFAULT TRUE,

    AUD_DH_CRIACAO      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO    TIMESTAMPTZ NULL,

    CONSTRAINT uq_companhias_codigo_iata UNIQUE (codigo_iata),
    CONSTRAINT uq_companhias_codigo_icao UNIQUE (codigo_icao)
);


CREATE TABLE IF NOT EXISTS modelos_avioes (
    id                          BIGSERIAL PRIMARY KEY,
    fabricante                  VARCHAR(120) NOT NULL,
    modelo                      VARCHAR(120) NOT NULL,
    capacidade_economica        INTEGER NOT NULL,
    capacidade_executiva        INTEGER NOT NULL,
    alcance_km                  INTEGER NOT NULL,
    peso_max_decolagem_kg       INTEGER NOT NULL,
    companhia_padrao_id         BIGINT REFERENCES companhias_aereas(id),

    AUD_DH_CRIACAO              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO            TIMESTAMPTZ NULL
);


CREATE TABLE IF NOT EXISTS aeroportos (
    id              BIGSERIAL PRIMARY KEY,
    codigo_iata     CHAR(3) NOT NULL,
    codigo_icao     CHAR(4) NOT NULL,
    nome            VARCHAR(255) NOT NULL,
    cidade          VARCHAR(120) NOT NULL,
    pais            VARCHAR(120) NOT NULL,
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,

    AUD_DH_CRIACAO  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO TIMESTAMPTZ NULL,

    CONSTRAINT uq_aeroportos_iata UNIQUE (codigo_iata),
    CONSTRAINT uq_aeroportos_icao UNIQUE (codigo_icao)
);

CREATE TABLE IF NOT EXISTS aeronaves (
    id                  BIGSERIAL PRIMARY KEY,
    matricula           VARCHAR(16) NOT NULL,
    ano_fabricacao      INTEGER NOT NULL,
    status              VARCHAR(30) NOT NULL, -- ATIVA, EM_MANUTENCAO, ESTOCADA
    companhia_id        BIGINT NOT NULL REFERENCES companhias_aereas(id),
    modelo_id           BIGINT NOT NULL REFERENCES modelos_avioes(id),

    AUD_DH_CRIACAO      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO    TIMESTAMPTZ NULL,

    CONSTRAINT uq_aeronaves_matricula UNIQUE (matricula)
);

CREATE TABLE IF NOT EXISTS funcionarios (
    id                  BIGSERIAL PRIMARY KEY,
    nome                VARCHAR(255) NOT NULL,
    cargo               VARCHAR(80) NOT NULL,
    tipo_funcionario    VARCHAR(30) NOT NULL,
    data_admissao       DATE NOT NULL,
    salario             NUMERIC(12,2) NOT NULL,
    companhia_id        BIGINT NOT NULL REFERENCES companhias_aereas(id),

    AUD_DH_CRIACAO      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO    TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS clientes (
    id                  BIGSERIAL PRIMARY KEY,
    nome                VARCHAR(255) NOT NULL,
    documento           VARCHAR(32) NOT NULL,
    data_nascimento     DATE NOT NULL,
    email               VARCHAR(255),
    telefone            VARCHAR(40),
    pais                VARCHAR(120) NOT NULL,

    AUD_DH_CRIACAO      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO    TIMESTAMPTZ NULL
);


CREATE TABLE IF NOT EXISTS voos (
    id                      BIGSERIAL PRIMARY KEY,
    numero_voo              VARCHAR(10) NOT NULL,
    companhia_id            BIGINT NOT NULL REFERENCES companhias_aereas(id),
    aeronave_id             BIGINT NOT NULL REFERENCES aeronaves(id),
    aeroporto_origem_id     BIGINT NOT NULL REFERENCES aeroportos(id),
    aeroporto_destino_id    BIGINT NOT NULL REFERENCES aeroportos(id),
    data_partida            TIMESTAMPTZ NOT NULL,
    data_chegada_prevista   TIMESTAMPTZ NOT NULL,
    status                  status_voo NOT NULL,

    AUD_DH_CRIACAO          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO        TIMESTAMPTZ NULL,

    CONSTRAINT uq_voos_numero_data UNIQUE (numero_voo, data_partida)
);

CREATE TABLE IF NOT EXISTS reservas (
    id                  BIGSERIAL PRIMARY KEY,
    codigo_localizador  VARCHAR(16) NOT NULL,
    cliente_id          BIGINT NOT NULL REFERENCES clientes(id),
    voo_id              BIGINT NOT NULL REFERENCES voos(id),
    status              status_reserva NOT NULL,
    data_reserva        TIMESTAMPTZ NOT NULL,

    AUD_DH_CRIACAO      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO    TIMESTAMPTZ NULL,

    CONSTRAINT uq_reservas_localizador UNIQUE (codigo_localizador)
);

CREATE TABLE IF NOT EXISTS bilhetes (
    id                  BIGSERIAL PRIMARY KEY,
    reserva_id          BIGINT NOT NULL REFERENCES reservas(id),
    numero_bilhete      VARCHAR(32) NOT NULL,
    assento             VARCHAR(8) NOT NULL,
    classe_cabine       classe_cabine NOT NULL,
    status_pagamento    status_pagamento NOT NULL,
    valor_tarifa        NUMERIC(12,2) NOT NULL,

    AUD_DH_CRIACAO      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO    TIMESTAMPTZ NULL,

    CONSTRAINT uq_bilhetes_numero UNIQUE (numero_bilhete)
);

CREATE TABLE IF NOT EXISTS bagagens (
    id              BIGSERIAL PRIMARY KEY,
    bilhete_id      BIGINT NOT NULL REFERENCES bilhetes(id),
    peso_kg         NUMERIC(5,1) NOT NULL,
    tipo            VARCHAR(30) NOT NULL,
    volume          INTEGER NOT NULL,

    AUD_DH_CRIACAO  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS manutencoes (
    id              BIGSERIAL PRIMARY KEY,
    aeronave_id     BIGINT NOT NULL REFERENCES aeronaves(id),
    tipo_manutencao VARCHAR(40) NOT NULL,
    data_inicio     TIMESTAMPTZ NOT NULL,
    data_fim        TIMESTAMPTZ NOT NULL,
    custo_total     NUMERIC(14,2) NOT NULL,
    descricao       TEXT,

    AUD_DH_CRIACAO  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO TIMESTAMPTZ NULL
);


CREATE TABLE IF NOT EXISTS tripulacao_voo (
    id              BIGSERIAL PRIMARY KEY,
    voo_id          BIGINT NOT NULL REFERENCES voos(id),
    funcionario_id  BIGINT NOT NULL REFERENCES funcionarios(id),
    funcao          VARCHAR(40) NOT NULL,
    inicio_escala   TIMESTAMPTZ NOT NULL,
    fim_escala      TIMESTAMPTZ NOT NULL,

    AUD_DH_CRIACAO  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    AUD_DH_ALTERACAO TIMESTAMPTZ NULL,

    CONSTRAINT uq_tripulacao_voo UNIQUE (voo_id, funcionario_id, inicio_escala)
);

CREATE INDEX IF NOT EXISTS idx_modelos_companhia_padrao
    ON modelos_avioes (companhia_padrao_id);

CREATE INDEX IF NOT EXISTS idx_aeronaves_companhia
    ON aeronaves (companhia_id);

CREATE INDEX IF NOT EXISTS idx_aeronaves_modelo
    ON aeronaves (modelo_id);

CREATE INDEX IF NOT EXISTS idx_funcionarios_companhia
    ON funcionarios (companhia_id);

CREATE INDEX IF NOT EXISTS idx_voos_companhia
    ON voos (companhia_id);

CREATE INDEX IF NOT EXISTS idx_voos_aeronave
    ON voos (aeronave_id);

CREATE INDEX IF NOT EXISTS idx_voos_origem
    ON voos (aeroporto_origem_id);

CREATE INDEX IF NOT EXISTS idx_voos_destino
    ON voos (aeroporto_destino_id);

CREATE INDEX IF NOT EXISTS idx_reservas_cliente
    ON reservas (cliente_id);

CREATE INDEX IF NOT EXISTS idx_reservas_voo
    ON reservas (voo_id);

CREATE INDEX IF NOT EXISTS idx_bilhetes_reserva
    ON bilhetes (reserva_id);

CREATE INDEX IF NOT EXISTS idx_bagagens_bilhete
    ON bagagens (bilhete_id);

CREATE INDEX IF NOT EXISTS idx_manutencoes_aeronave
    ON manutencoes (aeronave_id);

CREATE INDEX IF NOT EXISTS idx_tripulacao_voo_voo
    ON tripulacao_voo (voo_id);

CREATE INDEX IF NOT EXISTS idx_tripulacao_voo_funcionario
    ON tripulacao_voo (funcionario_id);


CREATE INDEX IF NOT EXISTS idx_companhias_aereas_aud_ultima_data
    ON companhias_aereas (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_modelos_avioes_aud_ultima_data
    ON modelos_avioes (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_aeroportos_aud_ultima_data
    ON aeroportos (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_aeronaves_aud_ultima_data
    ON aeronaves (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_funcionarios_aud_ultima_data
    ON funcionarios (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_clientes_aud_ultima_data
    ON clientes (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_voos_aud_ultima_data
    ON voos (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_reservas_aud_ultima_data
    ON reservas (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_bilhetes_aud_ultima_data
    ON bilhetes (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_bagagens_aud_ultima_data
    ON bagagens (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_manutencoes_aud_ultima_data
    ON manutencoes (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));

CREATE INDEX IF NOT EXISTS idx_tripulacao_voo_aud_ultima_data
    ON tripulacao_voo (COALESCE(AUD_DH_ALTERACAO, AUD_DH_CRIACAO));
