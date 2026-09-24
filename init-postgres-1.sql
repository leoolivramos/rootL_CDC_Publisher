-- Setup cdc_user role
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

-- 1. Database financeiro (default DB)
\c financeiro

CREATE TABLE IF NOT EXISTS contratos (
    id SERIAL PRIMARY KEY,
    numero VARCHAR(50) NOT NULL,
    valor DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE contratos REPLICA IDENTITY FULL;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
CREATE PUBLICATION cdc_publication FOR TABLE contratos;
SELECT pg_create_logical_replication_slot('cdc_slot_financeiro', 'pgoutput');

-- 2. Database logistica
CREATE DATABASE logistica;
\c logistica

CREATE ROLE cdc_user_dummy; -- to avoid error if exists, or grant
GRANT USAGE ON SCHEMA public TO cdc_user;
CREATE TABLE IF NOT EXISTS entregas (
    id SERIAL PRIMARY KEY,
    codigo_rastreio VARCHAR(50) NOT NULL,
    destinatario VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE entregas REPLICA IDENTITY FULL;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
CREATE PUBLICATION cdc_publication FOR TABLE entregas;
SELECT pg_create_logical_replication_slot('cdc_slot_logistica', 'pgoutput');

-- 3. Database vendas
CREATE DATABASE vendas;
\c vendas

CREATE TABLE IF NOT EXISTS pedidos (
    id SERIAL PRIMARY KEY,
    cliente VARCHAR(100) NOT NULL,
    total DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE pedidos REPLICA IDENTITY FULL;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
CREATE PUBLICATION cdc_publication FOR TABLE pedidos;
SELECT pg_create_logical_replication_slot('cdc_slot_vendas', 'pgoutput');
