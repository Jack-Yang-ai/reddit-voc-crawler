"""Offline checks with synthetic fixtures; never call Reddit or a model."""
import csv
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import reddit
import openclaw_reddit as crawler
import research_batch as batch
import annotate
import brand_scope
import build_report
import settings
import pandas as pd


class PipelineTests(unittest.TestCase):
    def test_pagination_uses_after_and_dedicated_session(self):
        class Response:
            status_code = 200
            def __init__(self, data): self.data = data
            def json(self): return {'data': self.data}
        class Session:
            calls = []
            def get(self, url, params, timeout):
                self.calls.append(params.copy())
                if len(self.calls) == 1:
                    return Response({'children': [{'data': {'id': 'synthetic-1', 'title': 'Example'}}], 'after': 'next'})
                return Response({'children': [{'data': {'id': 'synthetic-2'}}], 'after': None})
        session = Session()
        with patch.object(reddit, '_throttle'):
            rows = reddit._fetch_posts(session, 'https://example.invalid/new.json', {})
        self.assertEqual([r['id'] for r in rows], ['synthetic-1', 'synthetic-2'])
        self.assertNotIn('after', session.calls[0])
        self.assertEqual(session.calls[1]['after'], 'next')

    def test_cookie_not_embedded_and_environment_configured(self):
        with patch.object(reddit, 'BROWSER_COOKIE', ''):
            self.assertNotIn('Cookie', reddit._build_session().headers)
        with patch.object(reddit, 'BROWSER_COOKIE', 'synthetic_cookie=example'):
            self.assertEqual(reddit._build_session().headers['Cookie'], 'synthetic_cookie=example')

    def test_blend_preserves_unique_score_discussion_and_recent(self):
        data = pd.DataFrame([
            {'id': str(i), 'score': 20-i, 'num_comments': i,
             'created_time': pd.Timestamp('2020-01-01') + pd.Timedelta(days=i)}
            for i in range(20)])
        out = crawler._blend_select(data, 10)
        self.assertEqual(len(out), 10)
        self.assertEqual(out['id'].nunique(), 10)
        self.assertTrue({'0','1','2','3','4','17','18','19'}.issubset(set(out['id'])))

    def test_flatten_export_preserves_comment_parent(self):
        fixture = {'topic':'synthetic', 'brand':'Example', 'posts': [
            {'post_id':'example-id', 'title':'Synthetic title', 'selftext':'Synthetic text',
             'permalink':'https://example.invalid/post',
             'top_comments':[{'body':'Synthetic comment','score':3}]}]}
        rows = crawler._flatten_to_rows(fixture)
        self.assertEqual([r['row_type'] for r in rows], ['post', 'comment'])
        self.assertEqual(rows[1]['parent_post_id'], 'example-id')
        with tempfile.TemporaryDirectory() as directory:
            path = crawler.export_raw(fixture, directory)
            with open(path, encoding='utf-8-sig') as f:
                self.assertEqual(len(list(csv.DictReader(f))), 2)
            lines = (Path(directory)/'corpus.jsonl').read_text().splitlines()
            self.assertEqual(json.loads(lines[1])['text'], 'Synthetic comment')

    def test_brand_scope_uses_row_devices_over_bucket(self):
        self.assertEqual(brand_scope.scope_of({'brand':'RingConn','设备':'["Oura"]'}), '仅他牌')
        self.assertFalse(brand_scope.brand_ok({'brand':'RingConn','设备':'["Oura"]'}))
        self.assertEqual(brand_scope.scope_of({'brand':'RingConn','设备':'["RingConn", "Oura"]'}), '本牌+他牌')
        self.assertEqual(brand_scope.scope_of({'brand':'RingConn','text':'Oura is mentioned'}), '仅他牌')
        self.assertEqual(brand_scope.scope_of({'brand':'Apple Watch','设备':'["AppleWatch"]'}), '纯本牌')

    def test_filter_preserves_original_rid(self):
        df = pd.DataFrame([
            {'rid':4, 'text':'[deleted]', 'row_type':'post','score':1},
            {'rid':9, 'text':'A useful synthetic comment about battery', 'row_type':'comment','score':3}])
        self.assertEqual(annotate.filter_rows(df)['rid'].tolist(), [9])

    def test_merge_appends_without_reordering_existing_prefix(self):
        def row(text):
            item = dict.fromkeys(crawler.RAW_COLUMNS, '')
            item.update(sku='example', row_type='comment', permalink='https://example.invalid/post', text=text)
            return item
        with tempfile.TemporaryDirectory() as directory, patch.object(batch, 'BASE', directory):
            Path(directory, 'bucket').mkdir()
            pd.DataFrame([row('existing')]).to_csv(Path(directory, 'corpus_all.csv'), index=False)
            pd.DataFrame([row('new'), row('existing')]).to_csv(Path(directory, 'bucket/corpus.csv'), index=False)
            dest = batch.merge_corpus()
            self.assertEqual(pd.read_csv(dest)['text'].tolist(), ['existing','new'])
            batch.merge_corpus()
            self.assertEqual(pd.read_csv(dest)['text'].tolist(), ['existing','new'])

    def test_report_escapes_labels_and_omits_raw_content(self):
        rendered = build_report.render([{'brand':'<script>alert(1)</script>', 'text':'PRIVATE SYNTHETIC BODY','author':'EXAMPLE AUTHOR'}])
        self.assertNotIn('<script>', rendered)
        self.assertIn('&lt;script&gt;', rendered)
        self.assertNotIn('PRIVATE SYNTHETIC BODY', rendered)
        self.assertNotIn('EXAMPLE AUTHOR', rendered)

    def test_environment_takes_precedence(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'EXAMPLE_SETTING':'shell-value'}):
            fake = Path(directory, 'scripts')
            fake.mkdir()
            (fake.parent/'.env').write_text('EXAMPLE_SETTING=file-value\n')
            with patch.object(settings, '__file__', str(fake/'settings.py')):
                settings.load_local_env()
            self.assertEqual(os.environ['EXAMPLE_SETTING'], 'shell-value')


if __name__ == '__main__':
    unittest.main()
