"""Regressões de integridade e falha de carga, sem alterar os dados do projeto."""
import csv
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        shutil.copytree(ROOT / 'data/processed', self.root / 'data/processed')
        (self.root / 'scripts').mkdir()
        shutil.copy(ROOT / 'scripts/migrate_to_mongodb.py', self.root / 'scripts')

    def run_script(self, *args, uri=None):
        env = dict(os.environ)
        env.pop('MONGODB_URI', None)
        if uri:
            env['MONGODB_URI'] = uri
        return subprocess.run(
            [sys.executable, str(self.root / 'scripts/migrate_to_mongodb.py'), *args],
            env=env, capture_output=True, text=True, timeout=20,
        )

    def test_orphan_purchase_is_rejected_before_export(self):
        path = self.root / 'data/processed/compra.csv'
        with path.open() as f:
            rows = list(csv.DictReader(f))
        rows[0]['id_item'] = '99999999'
        with path.open('w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=rows[0])
            w.writeheader()
            w.writerows(rows)
        export = self.root / 'data/processed/mongo_export/jogadores.json'
        export.parent.mkdir(exist_ok=True)
        export.write_text('snapshot anterior')
        original = export.read_bytes()
        result = self.run_script('--source', 'csv', '--export-only')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(export.read_bytes(), original)

    def test_empty_description_matches_postgres_null(self):
        from scripts.migrate_to_mongodb import load_tables, build_documents
        tables = load_tables('csv')
        tables['item'][0]['descricao'] = ''
        item_id = int(tables['item'][0]['item_id'])
        docs = build_documents(tables)
        item = next(d for d in docs['itens'] if d['_id'] == item_id)
        self.assertIsNone(item['descricao'])

    def test_requested_load_failure_returns_nonzero(self):
        result = self.run_script('--source', 'csv', uri='mongodb://127.0.0.1:1/loja_lol')
        self.assertNotEqual(result.returncode, 0, result.stdout)


if __name__ == '__main__':
    unittest.main()
