#!/usr/bin/env python3
"""
scripts/migrate_to_mongodb.py
Realiza a transformação dos dados relacionais (PostgreSQL/CSVs) para documentos NoSQL (MongoDB Atlas).
Aplica as estratégias de Modelagem:
1. Embedding: compras e elo embutidos dentro de cada jogador.
2. Referencing: catálogo de itens e histórico de partidas mantidos como coleções referenciadas.
3. Denormalização controlada: categorias embutidas como lista dentro de itens, eliminando a tabela associativa N:N.
"""

import os
import csv
import json
import sys

def carregar_csv(caminho):
    with open(caminho, 'r', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data', 'processed')
    output_dir = os.path.join(data_dir, 'mongo_export')
    os.makedirs(output_dir, exist_ok=True)

    print("==> 1. Carregando dados normalizados de data/processed/ ...")
    elo_rows = carregar_csv(os.path.join(data_dir, 'elo.csv'))
    cat_rows = carregar_csv(os.path.join(data_dir, 'categoria.csv'))
    item_rows = carregar_csv(os.path.join(data_dir, 'item.csv'))
    item_cat_rows = carregar_csv(os.path.join(data_dir, 'item_categoria.csv'))
    jogador_rows = carregar_csv(os.path.join(data_dir, 'jogador.csv'))
    partida_rows = carregar_csv(os.path.join(data_dir, 'partida.csv'))
    compra_rows = carregar_csv(os.path.join(data_dir, 'compra.csv'))

    # Mapeamento de auxílio
    elo_map = {r['id_elo']: r['elo'] for r in elo_rows}
    cat_map = {r['id_categoria']: r['nome_categoria'] for r in cat_rows}

    # Mapeamento N:N de categorias por item
    item_categorias_map = {}
    for r in item_cat_rows:
        it_id = r['item_id']
        cat_nome = cat_map.get(r['id_categoria'], 'Diversos')
        item_categorias_map.setdefault(it_id, []).append(cat_nome)

    # 1. Coleção ITENS (Referencing - Catálogo de Produtos)
    print("==> 2. Construindo coleção 'itens' (Referencing + categorias embutidas)...")
    itens_docs = []
    itens_info_map = {}
    for r in item_rows:
        it_id = r['item_id']
        cats = item_categorias_map.get(it_id, ['Diversos'])
        doc = {
            '_id': int(it_id),
            'item_id': int(it_id),
            'nome': r['nome'],
            'preco_unitario': int(r['preco_unitario']),
            'descricao': r['descricao'],
            'categorias': cats
        }
        itens_docs.append(doc)
        itens_info_map[it_id] = doc

    # 2. Coleção PARTIDAS (Referencing - Histórico de Partidas)
    print("==> 3. Construindo coleção 'partidas' (Referencing)...")
    partidas_docs = []
    for r in partida_rows:
        partidas_docs.append({
            '_id': int(r['id_partida']),
            'id_partida': int(r['id_partida']),
            'data_partida': r['data_partida'],
            'hora_partida': r['hora_partida'],
            'duracao_minutos': int(r['duracao_minutos']),
            'resultado': r['resultado']
        })

    # Indexar compras por jogador
    compras_por_jogador = {}
    for c in compra_rows:
        compras_por_jogador.setdefault(c['id_jogador'], []).append(c)

    # 3. Coleção JOGADORES (Embedding - Elo + Lista de Compras com Snapshot de Item)
    print("==> 4. Construindo coleção 'jogadores' (Embedding)...")
    jogadores_docs = []
    for j in jogador_rows:
        j_id = j['id_jogador']
        compras_do_j = compras_por_jogador.get(j_id, [])
        
        compras_embutidas = []
        total_gasto = 0
        total_itens = 0

        for c in compras_do_j:
            it_info = itens_info_map.get(c['id_item'], {
                'nome': 'Item Desconhecido',
                'preco_unitario': int(c['preco_unitario']),
                'categorias': ['Diversos']
            })
            qtd = int(c['quantidade'])
            p_unit = int(c['preco_unitario'])
            total_item = int(c['total_compra'])

            total_gasto += total_item
            total_itens += qtd

            compras_embutidas.append({
                'id_compra': int(c['id_compra']),
                'id_item': int(c['id_item']),
                'item': it_info['nome'],
                'categorias': it_info['categorias'],
                'preco_unitario': p_unit,
                'quantidade': qtd,
                'total_ouro': total_item,
                'data_compra': c['data_compra'],
                'minuto_compra': int(c['minuto_compra']),
                'id_partida': int(c['id_partida'])
            })

        jogadores_docs.append({
            '_id': int(j_id),
            'id_jogador': int(j_id),
            'nick': j['nick'],
            'regiao': j['regiao'],
            'elo': elo_map.get(j['id_elo'], 'Desconhecido'),
            'total_gasto_ouro': total_gasto,
            'total_itens_adquiridos': total_itens,
            'total_transacoes': len(compras_embutidas),
            'compras': compras_embutidas
        })

    # Exportar para arquivos JSON locais
    print("==> 5. Exportando documentos JSON para data/processed/mongo_export/ ...")
    arquivos_export = [
        ('jogadores.json', jogadores_docs),
        ('itens.json', itens_docs),
        ('partidas.json', partidas_docs)
    ]
    for nome, docs in arquivos_export:
        caminho = os.path.join(output_dir, nome)
        with open(caminho, 'w', encoding='utf-8') as f:
            json.dump(docs, f, ensure_ascii=False, indent=2)
        print(f"    [OK] {nome:<16} ({len(docs):>4} documentos gerados)")

    print("\n--- Exemplo de Documento de Jogador (Embedding) ---")
    print(json.dumps(jogadores_docs[0], ensure_ascii=False, indent=2)[:800] + "\n  ... [compras truncadas no preview]\n}")

    # Conexão opcional com MongoDB (Local em Docker ou Atlas em Nuvem)
    mongo_uri = os.environ.get('MONGODB_URI')
    if mongo_uri:
        print(f"\n==> Conectando ao MongoDB configurado em MONGODB_URI...")
        try:
            from pymongo import MongoClient
            kwargs = {}
            if 'mongodb+srv' in mongo_uri:
                try:
                    import certifi
                    kwargs['tlsCAFile'] = certifi.where()
                except ImportError:
                    pass
            
            client = MongoClient(mongo_uri, **kwargs)
            try:
                db = client.get_default_database()
            except Exception:
                db = client['loja_lol']
            
            if db is None or db.name == 'admin' or db.name == 'test':
                db = client['loja_lol']
                
            for nome_col, docs in [('jogadores', jogadores_docs), ('itens', itens_docs), ('partidas', partidas_docs)]:
                col = db[nome_col]
                col.delete_many({})
                col.insert_many(docs)
                print(f"    [OK] Coleção '{nome_col}': {col.count_documents({})} documentos carregados no banco '{db.name}'.")
            print("[OK] Carga no MongoDB concluída com sucesso!")
        except Exception as e:
            print(f"[AVISO] Erro ao conectar ou carregar no MongoDB: {e}")
    else:
        print("\n[INFO] Dica: Para carregar diretamente em um container ou no MongoDB Atlas:")
        print("   Local (Docker):  export MONGODB_URI='mongodb://localhost:27017/loja_lol'")
        print("   Nuvem (Atlas):   export MONGODB_URI='mongodb+srv://<user>:<pwd>@cluster0.abcd.mongodb.net/loja_lol'")
        print("   Em seguida execute: python3 scripts/migrate_to_mongodb.py")

    print("\n[OK] Processo de estruturação NoSQL finalizado com êxito!")

if __name__ == '__main__':
    main()
