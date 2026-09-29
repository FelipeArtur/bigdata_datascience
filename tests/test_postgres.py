"""Verifica restrições reais no banco local, sempre com rollback."""
import os
from contextlib import closing
import unittest


@unittest.skipUnless(os.environ.get('PGHOST'), 'Requer PostgreSQL do Docker Compose')
class PostgresTests(unittest.TestCase):
    def test_database_rejects_orphan_reference(self):
        import psycopg2
        with closing(psycopg2.connect('')) as conn:
            with conn.cursor() as cur:
                with self.assertRaises(psycopg2.errors.ForeignKeyViolation):
                    cur.execute('UPDATE compra SET id_item = -1 WHERE id_compra = 1')
            conn.rollback()

    def test_database_rejects_incorrect_purchase_total(self):
        import psycopg2
        with closing(psycopg2.connect('')) as conn:
            with conn.cursor() as cur:
                with self.assertRaises(psycopg2.errors.CheckViolation):
                    cur.execute('UPDATE compra SET total_compra = -1 WHERE id_compra = 1')
            conn.rollback()


if __name__ == '__main__':
    unittest.main()
