-- 1. Database financeiro
CREATE DATABASE IF NOT EXISTS financeiro;
USE financeiro;

CREATE TABLE IF NOT EXISTS contratos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    numero VARCHAR(50) NOT NULL,
    valor DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Database faturamento
CREATE DATABASE IF NOT EXISTS faturamento;
USE faturamento;

CREATE TABLE IF NOT EXISTS notas_fiscais (
    id INT AUTO_INCREMENT PRIMARY KEY,
    chave_acesso VARCHAR(50) NOT NULL,
    valor_total DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Database estoque
CREATE DATABASE IF NOT EXISTS estoque;
USE estoque;

CREATE TABLE IF NOT EXISTS produtos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(50) NOT NULL,
    quantidade INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create CDC User with full replication privileges across all databases
CREATE USER IF NOT EXISTS 'cdc_user'@'%' IDENTIFIED WITH mysql_native_password BY 'cdc_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
