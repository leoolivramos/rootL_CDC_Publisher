CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';

CREATE TABLE IF NOT EXISTS faturas (
    id SERIAL PRIMARY KEY,
    codigo_fatura VARCHAR(50) NOT NULL,
    valor DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE faturas REPLICA IDENTITY FULL;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

CREATE PUBLICATION cdc_publication FOR TABLE faturas;
SELECT pg_create_logical_replication_slot('cdc_slot_pagamentos', 'pgoutput');
