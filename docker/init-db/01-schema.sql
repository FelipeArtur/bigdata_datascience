-- ==============================================================================
-- DDL DO BANCO DE DADOS RELACIONAL: LOJA LEAGUE OF LEGENDS (POSTGRESQL)
-- 7 TABELAS RELACIONAIS; TOTAL_COMPRA É REDUNDÂNCIA VALIDADA
-- ==============================================================================

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
    preco_unitario INT NOT NULL CHECK (preco_unitario >= 0),
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
    duracao_minutos INT NOT NULL CHECK (duracao_minutos > 0),
    resultado VARCHAR(20) NOT NULL
);

-- 7. Fato Transacional de Compras
CREATE TABLE compra (
    id_compra INT PRIMARY KEY,
    id_jogador INT NOT NULL REFERENCES jogador(id_jogador),
    id_item INT NOT NULL REFERENCES item(item_id),
    id_partida INT NOT NULL REFERENCES partida(id_partida),
    data_compra DATE NOT NULL,
    minuto_compra INT NOT NULL CHECK (minuto_compra > 0),
    quantidade INT NOT NULL DEFAULT 1 CHECK (quantidade > 0),
    preco_unitario INT NOT NULL CHECK (preco_unitario >= 0),
    total_compra INT NOT NULL CHECK (total_compra = quantidade * preco_unitario)
);

-- Índices B-Tree para aceleração de consultas frequentes
CREATE INDEX idx_jogador_regiao ON jogador(regiao);
CREATE INDEX idx_compra_jogador ON compra(id_jogador);
CREATE INDEX idx_compra_data ON compra(data_compra);
CREATE INDEX idx_compra_partida ON compra(id_partida);
