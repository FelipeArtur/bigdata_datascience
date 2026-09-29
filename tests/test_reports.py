"""Regressão do conversor: listas paralelas e formatação inline."""
from html.parser import HTMLParser
import unittest

from scripts.build_reports import generate_html


class ListParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.depth = self.maximum = 0

    def handle_starttag(self, tag, attrs):
        if tag == 'ul':
            self.depth += 1
            self.maximum = max(self.depth, self.maximum)

    def handle_endtag(self, tag):
        if tag == 'ul':
            self.depth -= 1


class ReportTests(unittest.TestCase):
    def test_parallel_bullets_do_not_nest_and_inline_markdown_renders(self):
        html = generate_html('# Relatório\n\n- **Um**\n- `Dois`\n- Três\n')
        parser = ListParser()
        parser.feed(html)
        self.assertEqual(parser.maximum, 1)
        self.assertEqual(parser.depth, 0)
        self.assertIn('<strong>Um</strong>', html)
        self.assertIn('<code>Dois</code>', html)


if __name__ == '__main__':
    unittest.main()
