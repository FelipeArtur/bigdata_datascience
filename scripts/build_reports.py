"""Atualiza resultados dos Markdown canônicos e renderiza os dois PDFs."""
import json
from pathlib import Path
import re

from markdown_it import MarkdownIt
from weasyprint import HTML

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / 'reports'
CSS = '''
@page { size: A4; margin: 21mm 19mm 19mm 23mm;
  @bottom-center { content: counter(page); font-size: 9pt; } }
body { font-family: "Liberation Serif", serif; font-size: 11pt; line-height: 1.35; color: #172635; }
h1, h2, h3, h4 { font-family: "DejaVu Sans", sans-serif; color: #163650; break-after: avoid; }
h1 { font-size: 21pt; line-height: 1.2; margin: 0 0 5mm; }
h2 { font-size: 13pt; margin: 0 0 8mm; font-weight: normal; }
h3 { font-size: 13pt; margin: 5mm 0 3mm; }
h4 { font-size: 11pt; margin: 4mm 0 2mm; }
p { text-align: justify; margin: 0 0 3.2mm; orphans: 3; widows: 3; }
a { color: #245b7b; overflow-wrap: anywhere; }
table { width: 100%; border-collapse: collapse; margin: 2mm 0 3mm; font-size: 9pt; line-height: 1.25; }
th, td { border-bottom: 0.4pt solid #c5d0d8; padding: 0.85mm; text-align: left; }
th { background: #edf3f7; font-family: "DejaVu Sans", sans-serif; font-size: 8pt; }
tr { break-inside: avoid; } thead { display: table-header-group; }
.metrics-tables table { font-size: 8.5pt; line-height: 1.15; }
.metrics-tables th, .metrics-tables td { padding: 0.4mm; }
pre { background: #f2f5f7; padding: 3mm; font-size: 8pt; line-height: 1.25;
      white-space: pre-wrap; overflow-wrap: anywhere; break-inside: avoid; }
code { font-family: "DejaVu Sans Mono", monospace; font-size: 0.86em; }
pre code { font-size: inherit; }
ul, ol { padding-left: 6mm; } li { margin: 1.5mm 0; }
.page-break { break-before: page; }
'''


def generate_html(markdown):
    body = MarkdownIt('commonmark', {'html': True}).enable('table').render(markdown)
    return f'<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><title>Relatório técnico</title><style>{CSS}</style></head><body>{body}</body></html>'


def table(headers, rows):
    def row(values):
        return '| ' + ' | '.join(str(v).replace('|', '\\|').replace('\n', ' ') for v in values) + ' |'
    return '\n'.join([row(headers), row(['---'] * len(headers)), *[row(r) for r in rows]])


def section(text, name, content):
    pattern = rf'<!-- {name}:start -->.*?<!-- {name}:end -->'
    result, count = re.subn(pattern, lambda _: f'<!-- {name}:start -->\n{content}\n<!-- {name}:end -->', text, flags=re.S)
    if count != 1:
        raise ValueError(f'Seção ausente ou duplicada: {name}')
    return result


def main():
    from scripts.migrate_to_mongodb import build_documents, load_tables, input_hashes
    migration = json.loads((REPORTS / 'evidence/migration.json').read_text())
    pipeline = json.loads((REPORTS / 'evidence/pipeline.json').read_text())
    if any(e.get('input_sha256') != input_hashes() for e in (migration, pipeline)):
        raise ValueError('Dados alterados desde a medição; execute make run')
    docs = build_documents(load_tables('csv'))
    metrics, bench = pipeline['metrics'], pipeline['benchmark']
    gold = sum(j['total_gasto_ouro'] for j in docs['jogadores'])
    purchases = sum(j['total_transacoes'] for j in docs['jogadores'])
    if not (gold == metrics['gold'] == migration['gold'] and
            purchases == metrics['purchases'] == migration['purchases'] == pipeline['delta_rows']):
        raise ValueError('Evidências não correspondem à base; execute make run')

    player = docs['jogadores'][0]
    purchase = player['compras'][0]
    item = next(i for i in docs['itens'] if i['_id'] == purchase['id_item'])
    match = next(p for p in docs['partidas'] if p['_id'] == purchase['id_partida'])
    # JSON válido, abreviado por campos; uma compra exibida, totais do documento inteiro.
    examples = [('jogadores', {k: player[k] for k in ('_id', 'nick', 'elo', 'total_transacoes')} | {'compras': [purchase]}),
                ('itens', {k: item[k] for k in ('_id', 'nome', 'preco_unitario')} | {'categorias': item['categorias'][:1]}),
                ('partidas', match), ('dominios', {'_id': 'dominios', 'elos': docs['dominios'][0]['elos'][:1]})]
    # Um objeto compacto por linha; nomes e valores vêm dos documentos reais.
    samples = '\n\n'.join(f'**{name} (recorte):**\n\n```json\n{json.dumps(doc, ensure_ascii=False)}\n```' for name, doc in examples)
    evidence_table = table(['Verificação', 'Resultado'], [
        ['Origem efetiva', migration['source']], ['Destino', 'MongoDB local'],
        ['Data da execução (UTC)', migration['executed_at']],
        ['Coleções e documentos', ', '.join(f'{k}: {v}' for k, v in migration['collections'].items())],
        ['Compras / ouro', f'{purchases} / {gold}'],
        ['Comparação após carga', 'Igualdade dos documentos completos'],
        ['Maior documento de jogador (BSON)', f'{migration["max_player_bson_bytes"]} bytes'],
        ['Índices em jogadores', ', '.join(migration['indexes'])],
    ])
    first = REPORTS / 'Entrega_1_UA1_UA2_Relatorio_Tecnico.md'
    text = section(first.read_text(), 'documents', samples)
    text = section(text, 'migration', evidence_table)
    first.write_text(text)

    region = table(['Região', 'Elo', 'Ouro', 'Transações', 'Ticket'],
                   [[r[k] for k in ('regiao', 'elo', 'total_ouro', 'transacoes', 'ticket_medio_ouro')] for r in metrics['region_elo']])
    top = table(['Item (top 10 por unidades)', 'Unidades', 'Ouro'],
                [[r['item'], r['unidades'], r['total_ouro']] for r in metrics['top_volume'][:10]])
    results = (f'O pipeline processou {purchases} compras, com {metrics["units"]} unidades e {gold} ouro. '
               f'Sábados e domingos concentraram {metrics["weekend_gold"]} ouro ({metrics["weekend_percent"]:.2f}%).\n\n'
               + '<div class="metrics-tables">\n\n' + region + '\n\n' + top + '\n\n</div>')
    premium = sum(r['total_ouro'] for r in metrics['subscriptions'] if r['tier_assinatura'] in ('VIP', 'Pro'))
    period = sum(r['total_ouro'] for r in metrics['subscriptions'])
    segment = table(['Tier', 'Ouro no período', 'Transações', 'Ticket'],
                    [[r[k] for k in ('tier_assinatura', 'total_ouro', 'transacoes', 'ticket_medio_ouro')] for r in metrics['subscriptions']])
    days = {}
    for r in metrics['weekday_platform']:
        gold_day, count_day = days.get(r['dia_semana'], (0, 0))
        days[r['dia_semana']] = (gold_day + r['total_ouro'], count_day + r['transacoes'])
    names = ['Domingo', 'Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado']
    segment += '\n\n' + table(['Dia', 'Ouro', 'Transações'], [[names[d-1], *v] for d, v in sorted(days.items())])
    segment += f'\n\nVIP e Pro somam **{premium} de {period} ouro ({100 * premium / period:.2f}%)** no período filtrado.'
    segment += '\n\n' + table(['Item (top 10 por ouro)', 'Ouro', 'Unidades'],
                              [[r['item'], r['total_ouro'], r['unidades']] for r in metrics['top_gold'][:10]])
    segment = '<div class="metrics-tables">\n\n' + segment + '\n\n</div>'
    medians = bench['median_seconds']
    benchmark_text = table(['Variante', 'Mediana (s)'], [[k, f'{v:.6f}'] for k, v in medians.items()])
    benchmark_text += '\n\n' + table(['Repetição', 'Sort-merge', 'Broadcast', 'Sem cache', 'Com cache'],
        [[i+1] + [f'{bench["samples_seconds"][k][i]:.6f}' for k in ('sort_merge', 'broadcast', 'uncached', 'cached')]
         for i in range(bench['repetitions'])])
    benchmark_text += (f'\n\nMaterialização do cache: **{bench["cache_materialization_seconds"]:.6f} s**. '
                       f'Razão sem/com cache: **{bench["cache_speedup"]:.2f}x**; '
                       f'razão sort-merge/broadcast: **{bench["join_speedup"]:.2f}x**. '
                       'As razões não incluem o custo inicial de materializar o cache.')
    for label, reference, ratio in [('cache', 'consulta sem cache', bench['cache_speedup']),
                                    ('broadcast', 'consulta com sort-merge', bench['join_speedup'])]:
        benchmark_text += f'\n\nA mediana com {label} foi {"menor" if ratio > 1 else "maior ou igual"} à da {reference} nesta execução.'
    benchmark_text += ('\n\nOs tempos se referem a esta consulta e a este ambiente; não permitem concluir que o ganho '
                       'se repita em grande escala. O cache precisa ser reutilizado para compensar sua materialização.')
    persistence = table(['Evidência', 'Resultado'], [
        ['Data da execução (UTC)', pipeline['executed_at']], ['Origem Spark', pipeline['source']],
        ['PostgreSQL × Spark', 'Todos os campos de compra iguais'],
        ['Spark × Delta', 'Diferenças vazias nas duas direções'], ['Linhas relidas', pipeline['delta_rows']],
        ['Versões', f'Python {pipeline["environment"]["python"]}; Spark {pipeline["environment"]["spark"]}; Delta {pipeline["environment"]["delta"]}; Java {pipeline["environment"]["java"]}'],
        ['Execução', pipeline['environment']['master']],
    ])
    second = REPORTS / 'Entrega_2_UA3_UA4_Relatorio_Tecnico.md'
    text = second.read_text()
    for name, content in [('documents', samples), ('metrics', results), ('segmentation', segment),
                          ('benchmark', benchmark_text), ('persistence', persistence)]:
        text = section(text, name, content)
    second.write_text(text)
    for source in (first, second):
        target = source.with_suffix('.pdf')
        temporary = target.with_suffix('.tmp.pdf')
        HTML(string=generate_html(source.read_text()), base_url=str(REPORTS)).write_pdf(temporary)
        temporary.replace(target)
        print(f'[OK] {target.name}')


if __name__ == '__main__':
    main()
