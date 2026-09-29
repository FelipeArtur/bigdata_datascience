"""Migração validada de PostgreSQL (ou CSV offline) para MongoDB."""
import argparse
import csv
from contextlib import closing
from datetime import date, datetime, time, timezone
import json
from hashlib import sha256
import os
from pathlib import Path
import sys
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
TABLES = {'elo': 'id_elo', 'categoria': 'id_categoria', 'item': 'item_id',
          'item_categoria': ('item_id', 'id_categoria'), 'jogador': 'id_jogador',
          'partida': 'id_partida', 'compra': 'id_compra'}


def input_hashes():
    return {str(p.relative_to(ROOT)): sha256(p.read_bytes()).hexdigest()
            for p in sorted((ROOT / 'data/processed').glob('*.csv'))}


def load_tables(source='csv'):
    if source == 'csv':
        tables = {}
        for table in TABLES:
            with (ROOT / 'data/processed' / f'{table}.csv').open() as f:
                tables[table] = list(csv.DictReader(f))
        return tables
    import psycopg2
    from psycopg2.extras import RealDictCursor
    # Nomes das tabelas vêm da constante acima, não da entrada do usuário.
    with closing(psycopg2.connect('')) as conn:
        conn.set_session(readonly=True, isolation_level='REPEATABLE READ')
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            tables = {}
            for table in TABLES:
                key = TABLES[table]
                order = ', '.join(key) if isinstance(key, tuple) else key
                cur.execute(f'SELECT * FROM {table} ORDER BY {order}')
                tables[table] = [dict(r) for r in cur]
            return tables


def build_documents(tables):
    """Valida todo o snapshot antes de produzir qualquer saída."""
    numbers = {'id_elo', 'id_categoria', 'item_id', 'id_item', 'id_jogador',
               'id_partida', 'id_compra', 'preco_unitario', 'quantidade',
               'total_compra', 'minuto_compra', 'duracao_minutos'}
    t = {name: [{k: int(v) if k in numbers else
                v.isoformat() if isinstance(v, (date, time)) else v
                for k, v in r.items()} for r in rows] for name, rows in tables.items()}
    # COPY CSV representa campo vazio não-quotado como NULL no PostgreSQL.
    for item in t['item']:
        if item['descricao'] == '':
            item['descricao'] = None
    for name, key in TABLES.items():
        keys = key if isinstance(key, tuple) else (key,)
        t[name].sort(key=lambda r: tuple(r[k] for k in keys))
        ids = [tuple(r[k] for k in keys) for r in t[name]]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError(f'Tabela vazia ou chave duplicada: {name}')
    indexed = {name: {r[key]: r for r in t[name]} for name, key in TABLES.items()
               if isinstance(key, str)}
    links = [('jogador', 'id_elo', 'elo'), ('item_categoria', 'item_id', 'item'),
             ('item_categoria', 'id_categoria', 'categoria'),
             ('compra', 'id_jogador', 'jogador'), ('compra', 'id_item', 'item'),
             ('compra', 'id_partida', 'partida')]
    for name, field, target in links:
        if any(r[field] not in indexed[target] for r in t[name]):
            raise ValueError(f'Referência inválida: {name}.{field}')
    categories = {i: [] for i in indexed['item']}
    for link in t['item_categoria']:
        categories[link['item_id']].append(indexed['categoria'][link['id_categoria']])
    items = [{'_id': r['item_id'], **r, 'categorias': categories[r['item_id']]}
             for r in t['item']]
    matches = [{'_id': r['id_partida'], **r} for r in t['partida']]
    purchases = {i: [] for i in indexed['jogador']}
    for r in t['compra']:
        match = indexed['partida'][r['id_partida']]
        if (r['quantidade'] <= 0 or r['preco_unitario'] < 0 or
                r['total_compra'] != r['quantidade'] * r['preco_unitario'] or
                r['data_compra'] != match['data_partida'] or
                not 1 <= r['minuto_compra'] <= match['duracao_minutos']):
            raise ValueError(f'Compra inconsistente: {r["id_compra"]}')
        purchase = {k: v for k, v in r.items() if k not in ('id_jogador', 'total_compra')}
        purchase.update(item=indexed['item'][r['id_item']]['nome'], total_ouro=r['total_compra'])
        purchases[r['id_jogador']].append(purchase)
    players = []
    for r in t['jogador']:
        buys = purchases[r['id_jogador']]
        players.append({'_id': r['id_jogador'], **r, 'elo': indexed['elo'][r['id_elo']]['elo'],
                        'compras': buys, 'total_transacoes': len(buys),
                        'total_itens_adquiridos': sum(c['quantidade'] for c in buys),
                        'total_gasto_ouro': sum(c['total_ouro'] for c in buys)})
    # Domínios completos preservados, inclusive elos sem jogadores.
    domain = [{'_id': 'dominios', 'elos': t['elo'], 'categorias': t['categoria']}]
    return {'itens': items, 'partidas': matches, 'jogadores': players, 'dominios': domain}


def migrate(source='postgres', export_only=False):
    tables = load_tables(source)
    docs = build_documents(tables)
    summary = {'executed_at': datetime.now(timezone.utc).isoformat(), 'source': source,
               'source_counts': {k: len(v) for k, v in tables.items()},
               'input_sha256': input_hashes(),
               'collections': {k: len(v) for k, v in docs.items()},
               'purchases': sum(d['total_transacoes'] for d in docs['jogadores']),
               'gold': sum(d['total_gasto_ouro'] for d in docs['jogadores'])}
    if export_only:
        path = ROOT / 'data/processed/mongo_export'
        path.mkdir(exist_ok=True)
        for name, rows in docs.items():
            (path / f'{name}.json').write_text(json.dumps(rows, ensure_ascii=False, indent=2))
        return {**summary, 'target': 'json_offline'}

    from bson import BSON
    from pymongo import MongoClient
    uri = os.environ['MONGODB_URI']
    # A URI define TLS; mongodb:// local não recebe parâmetros TLS forçados.
    with MongoClient(uri, serverSelectionTimeoutMS=5000) as client:
        client.admin.command('ping')
        db = client[os.environ.get('MONGODB_DATABASE', 'loja_lol')]
        stages = {name: f'_stage_{name}_{uuid4().hex}' for name in docs}
        try:
            for name, rows in docs.items():
                collection = db[stages[name]]
                collection.insert_many(rows)
                if collection.count_documents({}) != len(rows):
                    raise ValueError(f'Contagem divergente: {name}')
                # Compara os documentos completos, não apenas as contagens.
                loaded = sorted(collection.find(), key=lambda d: str(d['_id']))
                if loaded != sorted(rows, key=lambda d: str(d['_id'])):
                    raise ValueError(f'Conteúdo divergente: {name}')
            db[stages['jogadores']].create_index('regiao')
            # Troca atômica por coleção; não é uma transação entre coleções.
            for name, stage in stages.items():
                db[stage].rename(name, dropTarget=True)
        finally:
            for stage in stages.values():
                db.drop_collection(stage)
        summary.update(target='mongodb', mongo_version=client.server_info()['version'],
                       database=db.name, validation='full_document_equality',
                       max_player_bson_bytes=max(len(BSON.encode(d)) for d in docs['jogadores']),
                       indexes=list(db.jogadores.index_information()),
                       sample=db.jogadores.find_one({'_id': 1}, {'compras': {'$slice': 1}}))
    path = ROOT / 'reports/evidence'
    path.mkdir(parents=True, exist_ok=True)
    (path / 'migration.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=['csv', 'postgres'], default='postgres')
    parser.add_argument('--export-only', action='store_true', help='Gera JSON offline sem carregar MongoDB')
    args = parser.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / '.env')
    except ImportError:
        pass
    try:
        result = migrate(args.source, args.export_only)
        print(json.dumps({k: v for k, v in result.items() if k != 'sample'}, ensure_ascii=False, indent=2))
    except Exception as exc:
        # Não vazar URI/credenciais em logs de falha.
        print(f'Falha na migração ({type(exc).__name__}); confira dados, serviços e configuração.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
