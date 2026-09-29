#!/bin/bash
set -e

echo "==> Iniciando carga automática dos dados estruturados em data/processed/ para o PostgreSQL..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \copy elo(id_elo, elo) FROM '/data_processed/elo.csv' WITH (FORMAT csv, HEADER true);
    \copy categoria(id_categoria, nome_categoria) FROM '/data_processed/categoria.csv' WITH (FORMAT csv, HEADER true);
    \copy item(item_id, nome, preco_unitario, descricao) FROM '/data_processed/item.csv' WITH (FORMAT csv, HEADER true);
    \copy item_categoria(item_id, id_categoria) FROM '/data_processed/item_categoria.csv' WITH (FORMAT csv, HEADER true);
    \copy jogador(id_jogador, nick, id_elo, regiao) FROM '/data_processed/jogador.csv' WITH (FORMAT csv, HEADER true);
    \copy partida(id_partida, data_partida, hora_partida, duracao_minutos, resultado) FROM '/data_processed/partida.csv' WITH (FORMAT csv, HEADER true);
    \copy compra(id_compra, id_jogador, id_item, id_partida, data_compra, minuto_compra, quantidade, preco_unitario, total_compra) FROM '/data_processed/compra.csv' WITH (FORMAT csv, HEADER true);
EOSQL

echo "==> Sucesso! Todas as 7 tabelas foram populadas automaticamente no PostgreSQL."
