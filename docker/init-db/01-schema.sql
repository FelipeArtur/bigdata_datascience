-- ==============================================================================
-- DDL DO BANCO DE DADOS RELACIONAL: LOJA LEAGUE OF LEGENDS (POSTGRESQL)
-- 7 TABELAS NORMALIZADAS EM 3ª FORMA NORMAL (3FN)
-- ==============================================================================

-- Limpeza de tabelas prévias, se existirem
DROP TABLE IF EXISTS compra CASCADE;
DROP TABLE IF EXISTS item_categoria CASCADE;
DROP TABLE IF EXISTS partida CASCADE;
DROP TABLE IF EXISTS jogador CASCADE;
DROP TABLE IF EXISTS item CASCADE;
DROP TABLE IF EXISTS categoria CASCADE;
DROP TABLE IF EXISTS elo CASCADE;

-- 1. Domínio de Ranques (Elos)
CREATE TABLE elo (
    id_elo INT PRIMARY KEY,
    elo VARCHAR(50) NOT NULL
);

-- 2. Domínio de Categorias Funcionais de Itens
CREATE TABLE categoria (
    id_categoria INT PRIMARY KEY,
    nome_categoria VARCHAR(100) NOT NULL
);

-- 3. Catálogo de Itens da Loja
CREATE TABLE item (
    item_id INT PRIMARY KEY,
    nome VARCHAR(100) NOT NULL,
    preco_unitario INT NOT NULL,
    descricao TEXT
);

-- 4. Tabela Associativa N:N (Itens e Categorias)
CREATE TABLE item_categoria (
    item_id INT NOT NULL REFERENCES item(item_id) ON DELETE CASCADE,
    id_categoria INT NOT NULL REFERENCES categoria(id_categoria) ON DELETE CASCADE,
    PRIMARY KEY (item_id, id_categoria)
);

-- 5. Cadastro de Jogadores
CREATE TABLE jogador (
    id_jogador INT PRIMARY KEY,
    nick VARCHAR(50) NOT NULL,
    id_elo INT NOT NULL REFERENCES elo(id_elo),
    regiao VARCHAR(20) NOT NULL
);

-- 6. Histórico de Partidas
CREATE TABLE partida (
    id_partida INT PRIMARY KEY,
    data_partida DATE NOT NULL,
    hora_partida TIME NOT NULL,
    duracao_minutos INT NOT NULL,
    resultado VARCHAR(20) NOT NULL
);

-- 7. Fato Transacional de Compras
CREATE TABLE compra (
    id_compra INT PRIMARY KEY,
    id_jogador INT NOT NULL REFERENCES jogador(id_jogador),
    id_item INT NOT NULL REFERENCES item(item_id),
    id_partida INT NOT NULL REFERENCES partida(id_partida),
    data_compra DATE NOT NULL,
    minuto_compra INT NOT NULL,
    quantidade INT NOT NULL DEFAULT 1,
    preco_unitario INT NOT NULL,
    total_compra INT NOT NULL
);

-- Índices B-Tree para aceleração de consultas frequentes
CREATE INDEX idx_jogador_regiao ON jogador(regiao);
CREATE INDEX idx_compra_jogador ON compra(id_jogador);
CREATE INDEX idx_compra_data ON compra(data_compra);
CREATE INDEX idx_compra_partida ON compra(id_partida);
