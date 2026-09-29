"""Integração Spark: métricas, cobertura do join e persistência Delta."""
import tempfile
import unittest


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from scripts.spark_pipeline import create_spark
        cls.spark = create_spark()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self):
        # 2025-08-23 é sábado: 60 de 100 ouro devem pertencer ao fim de semana.
        raw = self.spark.read.json(self.spark.sparkContext.parallelize([
            '{"id_jogador":1,"nick":"A","regiao":"BR","elo":"Ouro","compras":['
            '{"id_compra":1,"id_item":10,"item":"X","quantidade":2,"preco_unitario":30,"total_ouro":60,"data_compra":"2025-08-23","minuto_compra":1,"id_partida":1},'
            '{"id_compra":2,"id_item":11,"item":"Y","quantidade":1,"preco_unitario":40,"total_ouro":40,"data_compra":"2025-08-25","minuto_compra":2,"id_partida":2}]}',
        ]))
        demo = self.spark.createDataFrame([(1, 'Brasil', 'Salvador', 25, 'M', 'VIP', 'Desktop')],
                                         'id_jogador long, pais string, cidade string, idade int, genero string, tier_assinatura string, plataforma string')
        return raw, demo

    def test_metrics_and_delta_preserve_purchases(self):
        from scripts.spark_pipeline import transform, metrics, persist_delta
        raw, demo = self.fixture()
        frame = transform(raw, demo)
        result = metrics(frame)
        self.assertEqual(result['gold'], 100)
        self.assertEqual(result['purchases'], 2)
        self.assertEqual(result['weekend_percent'], 60)
        self.assertEqual(result['region_elo'][0]['ticket_medio_ouro'], 50)
        self.assertEqual(result['top_volume'][0]['id_item'], 10)
        with tempfile.TemporaryDirectory() as tmp:
            loaded = persist_delta(frame, tmp + '/delta')
            self.assertEqual(loaded.count(), 2)
            self.assertEqual(loaded.exceptAll(frame).count(), 0)

    def test_missing_demography_is_not_silently_dropped(self):
        from scripts.spark_pipeline import transform
        raw, demo = self.fixture()
        with self.assertRaises(ValueError):
            transform(raw, demo.filter('id_jogador = 99'))

    def test_duplicate_demography_is_not_multiplied(self):
        from scripts.spark_pipeline import transform
        raw, demo = self.fixture()
        with self.assertRaises(ValueError):
            transform(raw, demo.union(demo))


if __name__ == '__main__':
    unittest.main()
