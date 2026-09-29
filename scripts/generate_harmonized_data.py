#!/usr/bin/env python3
"""
scripts/generate_harmonized_data.py
Gera os dados normalizados do cenário da Loja de League of Legends para o projeto de Banco de Dados e Big Data.
Cria datasets consistentes para:
- UA 01 e UA 02: Modelagem relacional e migração NoSQL (MongoDB Atlas)
- UA 03 e UA 04: Processamento distribuído no Databricks / Apache Spark e Delta Lake
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta

def main():
    random.seed(42)

    raw_path = 'data/raw/itens_raw.csv'
    processed_dir = 'data/processed'
    os.makedirs(processed_dir, exist_ok=True)

    print("==> 1. Processando itens reais e categorias...")
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Arquivo {raw_path} não encontrado!")

    with open(raw_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        raw_items = list(reader)

    # Extrair itens e categorias
    categorias_set = set()
    itens_dict = {}
    item_categoria_links = []

    for row in raw_items:
        item_id = row['item_id']
        nome = row['nome']
        descricao = row['descricao_texto']
        try:
            preco = int(row['preco_total_ouro'])
        except (ValueError, TypeError):
            preco = 300

        # Tratamento para itens com preço 0
        if preco <= 0:
            preco = 150

        itens_dict[item_id] = {
            'item_id': item_id,
            'nome': nome,
            'preco_unitario': preco,
            'descricao': descricao
        }

        # Categorias JSON
        cat_json_str = row.get('categorias_json', '[]')
        try:
            cats = json.loads(cat_json_str) if cat_json_str else []
        except Exception:
            cats = []

        if not cats:
            cats = ['Diversos']

        for cat in cats:
            categorias_set.add(cat)
            item_categoria_links.append((item_id, cat))

    # Tabela de categorias
    categorias_sorted = sorted(list(categorias_set))
    categoria_to_id = {cat: idx + 1 for idx, cat in enumerate(categorias_sorted)}
    categoria_rows = [{'id_categoria': idx, 'nome_categoria': cat} for cat, idx in categoria_to_id.items()]

    # Tabela item_categoria (N:N relacional)
    item_categoria_rows = []
    seen_links = set()
    for item_id, cat in item_categoria_links:
        link = (item_id, categoria_to_id[cat])
        if link not in seen_links:
            seen_links.add(link)
            item_categoria_rows.append({'item_id': item_id, 'id_categoria': categoria_to_id[cat]})

    # Tabela de itens
    item_rows = list(itens_dict.values())

    print(f"    - Categorias extraídas: {len(categoria_rows)}")
    print(f"    - Itens extraídos: {len(item_rows)}")
    print(f"    - Relacionamentos Item-Categoria (N:N): {len(item_categoria_rows)}")

    print("==> 2. Gerando Elos...")
    elos = [
        'Ferro', 'Bronze', 'Prata', 'Ouro', 'Platina',
        'Esmeralda', 'Diamante', 'Mestre', 'Grão-Mestre', 'Desafiante'
    ]
    elo_rows = [{'id_elo': i + 1, 'elo': e} for i, e in enumerate(elos)]

    print("==> 3. Gerando Jogadores e Demografia Complementar (para DBFS / Spark)...")
    players_data = [
        (1, 'Faker', 10, 'KR', 'Coreia do Sul', 'Seul', 28, 'M', 'VIP', 'Desktop Gamer'),
        (2, 'ShowMaker', 9, 'KR', 'Coreia do Sul', 'Seul', 24, 'M', 'VIP', 'Desktop Gamer'),
        (3, 'Chovy', 10, 'KR', 'Coreia do Sul', 'Seul', 23, 'M', 'VIP', 'Desktop Gamer'),
        (4, 'Caps', 9, 'EUW', 'Dinamarca', 'Copenhague', 25, 'M', 'VIP', 'Desktop Gamer'),
        (5, 'Rekkles', 8, 'EUW', 'Suécia', 'Estocolmo', 27, 'M', 'VIP', 'Desktop Gamer'),
        (6, 'BrTT', 8, 'BR', 'Brasil', 'Rio de Janeiro', 33, 'M', 'VIP', 'Desktop Gamer'),
        (7, 'Kami', 7, 'BR', 'Brasil', 'Pelotas', 28, 'M', 'VIP', 'Desktop Gamer'),
        (8, 'Tinowns', 9, 'BR', 'Brasil', 'Campinas', 27, 'M', 'VIP', 'Desktop Gamer'),
        (9, 'Robo', 8, 'BR', 'Brasil', 'São Paulo', 26, 'M', 'VIP', 'Desktop Gamer'),
        (10, 'Cariok', 7, 'BR', 'Brasil', 'Rio de Janeiro', 25, 'M', 'Pro', 'Desktop Gamer'),
        (11, 'Titan', 8, 'BR', 'Brasil', 'Manaus', 24, 'M', 'VIP', 'Notebook Gamer'),
        (12, 'Aegis', 6, 'BR', 'Brasil', 'Curitiba', 23, 'M', 'Gratuito', 'Desktop Gamer'),
        (13, 'Ceos', 7, 'BR', 'Brasil', 'Belo Horizonte', 24, 'M', 'Gratuito', 'Desktop Gamer'),
        (14, 'Route', 8, 'KR', 'Coreia do Sul', 'Busan', 26, 'M', 'VIP', 'Desktop Gamer'),
        (15, 'Dynquedo', 6, 'BR', 'Brasil', 'Florianópolis', 27, 'M', 'Gratuito', 'Desktop Gamer'),
        (16, 'Jojo', 5, 'BR', 'Brasil', 'Porto Alegre', 25, 'M', 'Gratuito', 'Notebook Gamer'),
        (17, 'TitaN_Prime', 7, 'BR', 'Brasil', 'Manaus', 24, 'M', 'Pro', 'Desktop Gamer'),
        (18, 'Gvoy', 4, 'BR', 'Brasil', 'Salvador', 22, 'M', 'Gratuito', 'Desktop Gamer'),
        (19, 'Damage', 5, 'BR', 'Brasil', 'Fortaleza', 25, 'M', 'Gratuito', 'Notebook Gamer'),
        (20, 'Wizer', 8, 'KR', 'Coreia do Sul', 'Incheon', 26, 'M', 'VIP', 'Desktop Gamer'),
        (21, 'Crocodilo', 4, 'BR', 'Brasil', 'Recife', 22, 'M', 'Gratuito', 'Desktop Gamer'),
        (22, 'Smeb', 9, 'KR', 'Coreia do Sul', 'Seul', 29, 'M', 'Pro', 'Desktop Gamer'),
        (23, 'BeryL', 9, 'KR', 'Coreia do Sul', 'Daegu', 27, 'M', 'VIP', 'Desktop Gamer'),
        (24, 'Deft', 10, 'KR', 'Coreia do Sul', 'Seul', 28, 'M', 'VIP', 'Desktop Gamer'),
        (25, 'Keria', 10, 'KR', 'Coreia do Sul', 'Seul', 22, 'M', 'VIP', 'Desktop Gamer')
    ]

    jogador_rows = [
        {'id_jogador': p[0], 'nick': p[1], 'id_elo': p[2], 'regiao': p[3]}
        for p in players_data
    ]

    demografia_rows = [
        {
            'id_jogador': p[0],
            'nick': p[1],
            'regiao': p[3],
            'pais': p[4],
            'cidade': p[5],
            'idade': p[6],
            'genero': p[7],
            'tier_assinatura': p[8],
            'plataforma': p[9]
        }
        for p in players_data
    ]

    print(f"    - Jogadores: {len(jogador_rows)}")
    print(f"    - Registros demográficos complementares (DBFS): {len(demografia_rows)}")

    print("==> 4. Gerando Partidas...")
    base_date = datetime(2025, 8, 15)
    partidas_rows = []
    resultados = ['Vitoria', 'Derrota']
    
    # 30 partidas distribuídas ao longo de 5 semanas (para cobrir todos os dias da semana)
    for i in range(1, 31):
        dt = base_date + timedelta(days=(i * 2) % 36, hours=random.randint(10, 23), minutes=random.randint(0, 59))
        duracao = random.randint(18, 45)
        partidas_rows.append({
            'id_partida': i,
            'data_partida': dt.strftime('%Y-%m-%d'),
            'hora_partida': dt.strftime('%H:%M:%S'),
            'duracao_minutos': duracao,
            'resultado': random.choice(resultados)
        })

    print(f"    - Partidas geradas: {len(partidas_rows)}")

    print("==> 5. Gerando Compras...")
    item_ids_list = [it['item_id'] for it in item_rows]
    item_price_map = {it['item_id']: it['preco_unitario'] for it in item_rows}
    jogador_ids_list = [j['id_jogador'] for j in jogador_rows]

    compras_rows = []
    id_compra = 1

    for p in partidas_rows:
        p_id = p['id_partida']
        p_date = p['data_partida']
        p_duracao = p['duracao_minutos']

        # 10 jogadores disputam cada partida (5v5)
        participantes = random.sample(jogador_ids_list, 10)

        for jog_id in participantes:
            # Cada jogador realiza entre 2 e 5 compras de itens ao longo do jogo
            qtd_compras = random.randint(2, 5)
            for _ in range(qtd_compras):
                item_id = random.choice(item_ids_list)
                preco = item_price_map[item_id]
                qtd = random.choices([1, 2], weights=[0.85, 0.15])[0]
                minuto = random.randint(1, p_duracao)

                compras_rows.append({
                    'id_compra': id_compra,
                    'id_jogador': jog_id,
                    'id_item': item_id,
                    'id_partida': p_id,
                    'data_compra': p_date,
                    'minuto_compra': minuto,
                    'quantidade': qtd,
                    'preco_unitario': preco,
                    'total_compra': preco * qtd
                })
                id_compra += 1

    print(f"    - Compras geradas: {len(compras_rows)}")

    # Salvar todos os CSVs
    def salvar_csv(caminho, rows, colunas):
        with open(caminho, 'w', encoding='utf-8', newline='') as fp:
            writer = csv.DictWriter(fp, fieldnames=colunas)
            writer.writeheader()
            writer.writerows(rows)
        print(f"    [OK] Salvo: {caminho} ({len(rows)} linhas)")

    print("\n==> 6. Exportando arquivos para data/processed/ ...")
    salvar_csv(os.path.join(processed_dir, 'elo.csv'), elo_rows, ['id_elo', 'elo'])
    salvar_csv(os.path.join(processed_dir, 'categoria.csv'), categoria_rows, ['id_categoria', 'nome_categoria'])
    salvar_csv(os.path.join(processed_dir, 'item.csv'), item_rows, ['item_id', 'nome', 'preco_unitario', 'descricao'])
    salvar_csv(os.path.join(processed_dir, 'item_categoria.csv'), item_categoria_rows, ['item_id', 'id_categoria'])
    salvar_csv(os.path.join(processed_dir, 'jogador.csv'), jogador_rows, ['id_jogador', 'nick', 'id_elo', 'regiao'])
    salvar_csv(os.path.join(processed_dir, 'jogadores_demografia.csv'), demografia_rows, 
               ['id_jogador', 'nick', 'regiao', 'pais', 'cidade', 'idade', 'genero', 'tier_assinatura', 'plataforma'])
    salvar_csv(os.path.join(processed_dir, 'partida.csv'), partidas_rows, ['id_partida', 'data_partida', 'hora_partida', 'duracao_minutos', 'resultado'])
    salvar_csv(os.path.join(processed_dir, 'compra.csv'), compras_rows, 
               ['id_compra', 'id_jogador', 'id_item', 'id_partida', 'data_compra', 'minuto_compra', 'quantidade', 'preco_unitario', 'total_compra'])

    print("\n[OK] Concluído! Todos os dados relacionais e complementares foram gerados com sucesso.")

if __name__ == '__main__':
    main()
