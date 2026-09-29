"""Executa os três notebooks em ordem, preservando as saídas verificáveis."""
from pathlib import Path

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]


def main():
    for path in sorted((ROOT / 'notebooks').glob('*.ipynb')):
        notebook = nbformat.read(path, as_version=4)
        NotebookClient(notebook, timeout=900, kernel_name='python3',
                       resources={'metadata': {'path': str(ROOT)}}).execute()
        nbformat.write(notebook, path)
        print(f'[OK] {path.name}')


if __name__ == '__main__':
    main()
