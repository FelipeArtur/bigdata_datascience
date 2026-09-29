"""MongoDB + CSV demográfico -> Spark -> Delta, com métricas e benchmark reais."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
from statistics import median
from time import perf_counter

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

ROOT = Path(__file__).resolve().parents[1]
CONNECTOR = 'org.mongodb.spark:mongo-spark-connector_2.12:10.4.0'


def create_spark():
    builder = (SparkSession.builder.master('local[2]').appName('LojaLoL')
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .config('spark.sql.shuffle.partitions', '4')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.jars.repositories', 'https://repo.maven.apache.org/maven2')
               .config('spark.redaction.regex', '(?i)secret|password|token|uri|url')
               .config('spark.sql.redaction.options.regex', '(?i)uri|url|password'))
    return configure_spark_with_delta_pip(builder, extra_packages=[CONNECTOR]).getOrCreate()


def read_sources(spark, data_dir=None):
    uri = os.environ['MONGODB_URI']
    raw = (spark.read.format('mongodb').option('connection.uri', uri)
           .option('database', os.environ.get('MONGODB_DATABASE', 'loja_lol'))
           .option('collection', 'jogadores')
           .option('partitioner', 'com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner')
           .load())
    # Schema explícito evita inferência dependente da amostra.
    names = ['id_jogador', 'nick', 'regiao', 'pais', 'cidade', 'idade', 'genero', 'tier_assinatura', 'plataforma']
    schema = StructType([StructField(n, IntegerType() if n in ('id_jogador', 'idade') else StringType())
                         for n in names])
    demo = (spark.read.schema(schema).option('header', True).option('mode', 'FAILFAST')
            .csv(str(Path(data_dir or ROOT / 'data/processed') / 'jogadores_demografia.csv')))
    return raw, demo


def explode_purchases(raw):
    return raw.select('id_jogador', 'nick', 'regiao', 'elo', F.explode('compras').alias('c')).select(
        'id_jogador', 'nick', 'regiao', 'elo',
        *[F.col(f'c.{n}').alias(n) for n in ('id_compra', 'id_item', 'item', 'preco_unitario',
                                           'quantidade', 'total_ouro', 'minuto_compra', 'id_partida')],
        F.to_date('c.data_compra').alias('data_compra'))


def transform(raw, demo):
    if demo.filter(F.col('id_jogador').isNull()).limit(1).count():
        raise ValueError('Demografia com ID nulo')
    if demo.groupBy('id_jogador').count().filter('count > 1').limit(1).count():
        raise ValueError('Demografia duplicada')
    fact = explode_purchases(raw)
    if fact.join(demo, 'id_jogador', 'left_anti').limit(1).count():
        raise ValueError('Compra sem demografia')
    if fact.filter(F.col('data_compra').isNull()).limit(1).count():
        raise ValueError('Data de compra inválida')
    if fact.groupBy('id_compra').count().filter('count > 1').limit(1).count():
        raise ValueError('ID de compra duplicado')
    dimension = demo.drop('nick', 'regiao')
    return fact.join(F.broadcast(dimension), 'id_jogador', 'inner')


def region_metric(frame):
    return frame.groupBy('regiao', 'elo').agg(
        F.sum('total_ouro').alias('total_ouro'), F.sum('quantidade').alias('unidades'),
        F.count('*').alias('transacoes'), F.round(F.avg('total_ouro'), 2).alias('ticket_medio_ouro')
    ).orderBy(F.desc('total_ouro'), 'regiao', 'elo')


def metrics(frame):
    totals = frame.agg(F.count('*').alias('purchases'), F.sum('total_ouro').alias('gold'),
                       F.sum('quantidade').alias('units')).first().asDict()
    if not totals['purchases'] or not totals['gold']:
        raise ValueError('Base analítica vazia ou sem movimentação')
    weekend = frame.filter(F.dayofweek('data_compra').isin(1, 7)).agg(F.sum('total_ouro')).first()[0] or 0
    by_item = frame.groupBy('id_item', 'item').agg(F.sum('quantidade').alias('unidades'),
                F.sum('total_ouro').alias('total_ouro'), F.count('*').alias('transacoes'))
    seasonal = frame.withColumn('dia_semana', F.dayofweek('data_compra')).groupBy('dia_semana', 'plataforma').agg(
        F.sum('total_ouro').alias('total_ouro'), F.count('*').alias('transacoes')).orderBy('dia_semana', 'plataforma')
    subscriptions = frame.filter(F.col('data_compra').between('2025-08-20', '2025-09-20')).groupBy('tier_assinatura').agg(
        F.sum('total_ouro').alias('total_ouro'), F.count('*').alias('transacoes'),
        F.round(F.avg('total_ouro'), 2).alias('ticket_medio_ouro')).orderBy(F.desc('total_ouro'), 'tier_assinatura')
    rows = lambda df: [r.asDict() for r in df.collect()]
    return {**totals, 'weekend_gold': weekend, 'weekend_percent': round(100 * weekend / totals['gold'], 4),
            'region_elo': rows(region_metric(frame)),
            'top_volume': rows(by_item.orderBy(F.desc('unidades'), 'id_item').limit(10)),
            'top_gold': rows(by_item.orderBy(F.desc('total_ouro'), 'id_item').limit(10)),
            'weekday_platform': rows(seasonal), 'subscriptions': rows(subscriptions)}


def benchmark(raw, demo, repetitions=5):
    """Mesmo resultado e mesma consulta; aquecimento e medições alternadas por join."""
    if repetitions < 3:
        raise ValueError('Use ao menos três repetições')
    spark = raw.sparkSession
    settings = ['spark.sql.autoBroadcastJoinThreshold', 'spark.sql.adaptive.enabled']
    previous = {k: spark.conf.get(k) for k in settings}
    cached = None
    try:
        spark.catalog.clearCache()
        spark.conf.set(settings[0], -1)
        spark.conf.set(settings[1], 'false')
        fact, dimension = explode_purchases(raw), demo.drop('nick', 'regiao')
        broadcast = fact.join(F.broadcast(dimension), 'id_jogador')
        # Reconstrói cada consulta: reutilizar o mesmo QueryExecution pode reaproveitar
        # resultados de shuffle e subestimar o custo de reexecutar o pipeline.
        queries = {
            'sort_merge': lambda: region_metric(fact.join(dimension, 'id_jogador')),
            'broadcast': lambda: region_metric(fact.join(F.broadcast(dimension), 'id_jogador')),
        }
        reference = queries['sort_merge']().collect()
        if reference != queries['broadcast']().collect():
            raise ValueError('Joins produziram resultados diferentes')
        plans = {k: q()._jdf.queryExecution().executedPlan().toString() for k, q in queries.items()}
        if 'SortMergeJoin' not in plans['sort_merge'] or 'BroadcastHashJoin' not in plans['broadcast']:
            raise ValueError('Planos diferentes dos esperados pelo experimento')
        samples = {k: [] for k in queries}

        def measure(query):
            start = perf_counter()
            result = query.collect()
            elapsed = perf_counter() - start
            if result != reference:
                raise ValueError('Resultado mudou durante benchmark')
            return elapsed

        for i in range(repetitions):
            for name in (list(queries) if i % 2 == 0 else list(reversed(queries))):
                samples[name].append(measure(queries[name]()))
        # Sem cache: mesmo plano broadcast e agregação que a variante com cache.
        uncached = [measure(region_metric(broadcast)) for _ in range(repetitions)]
        cached = broadcast.cache()
        start = perf_counter()
        cached.count()
        materialization = perf_counter() - start
        measure(region_metric(cached))  # aquecimento, não incluído na mediana
        cache_samples = [measure(region_metric(cached)) for _ in range(repetitions)]
        plans['cached'] = region_metric(cached)._jdf.queryExecution().executedPlan().toString()
        samples.update(uncached=uncached, cached=cache_samples)
        medians = {k: median(v) for k, v in samples.items()}
        return {'repetitions': repetitions, 'warmup': 'uma execução por variante',
                'query': 'ouro, unidades, contagem e média por regiao/elo, ordenados',
                'automatic_broadcast': False, 'aqe': False, 'rows_equal': True,
                'samples_seconds': samples, 'median_seconds': medians,
                'cache_materialization_seconds': materialization,
                'cache_speedup': medians['uncached'] / medians['cached'],
                'join_speedup': medians['sort_merge'] / medians['broadcast'], 'plans': plans}
    finally:
        if cached is not None:
            cached.unpersist(blocking=True)
        for key, value in previous.items():
            spark.conf.set(key, value)


def persist_delta(frame, path):
    # Base pequena: particionar por região geraria arquivos pequenos sem benefício medido.
    frame.write.format('delta').mode('overwrite').save(str(path))
    loaded = frame.sparkSession.read.format('delta').load(str(path))
    if frame.exceptAll(loaded).limit(1).count() or loaded.exceptAll(frame).limit(1).count():
        raise ValueError('Delta diverge do DataFrame de origem')
    return loaded


def run(spark, repetitions=5):
    from scripts.migrate_to_mongodb import load_tables, input_hashes
    raw, demo = read_sources(spark)
    enriched = transform(raw, demo)
    # Reconciliação independente com a origem relacional em todos os campos transacionais.
    fields = ['id_compra', 'id_jogador', 'id_item', 'id_partida', 'data_compra',
              'minuto_compra', 'quantidade', 'preco_unitario', 'total_compra']
    original = load_tables('postgres')['compra']
    expected = sorted(tuple(str(r[f]) for f in fields) for r in original)
    actual = sorted(tuple(str(v) for v in r) for r in enriched.withColumnRenamed('total_ouro', 'total_compra').select(*fields).collect())
    if actual != expected:
        raise ValueError('Spark diverge das compras no PostgreSQL')
    stats = metrics(enriched)
    bench = benchmark(raw, demo, repetitions)
    delta_path = ROOT / 'data/delta/analise_compras_jogadores'
    loaded = persist_delta(enriched, delta_path)
    evidence = ROOT / 'reports/evidence'
    evidence.mkdir(parents=True, exist_ok=True)
    plans = bench.pop('plans')
    for name, plan in plans.items():
        (evidence / f'plan_{name}.txt').write_text(plan)
    from importlib.metadata import version
    result = {'executed_at': datetime.now(timezone.utc).isoformat(),
              'environment': {'mode': 'Docker local; não executado em Atlas/Databricks',
                              'python': platform.python_version(), 'spark': spark.version,
                              'delta': version('delta-spark'), 'mongo_connector': CONNECTOR,
                              'master': spark.sparkContext.master,
                              'java': spark._jvm.java.lang.System.getProperty('java.version'),
                              'shuffle_partitions': spark.conf.get('spark.sql.shuffle.partitions')},
              'input_sha256': input_hashes(),
              'source': 'mongodb_connector', 'relational_reconciliation': 'all_purchase_fields_equal',
              'delta_rows': loaded.count(), 'delta_reconciliation': 'exceptAll_both_directions_empty',
              'metrics': stats, 'benchmark': bench}
    (evidence / 'pipeline.json').write_text(json.dumps(result, ensure_ascii=False, indent=2))
    return result


def main():
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repetitions', type=int, default=5)
    args = parser.parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        result = run(spark, args.repetitions)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
