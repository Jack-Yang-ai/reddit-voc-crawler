"""Build a local, self-contained HTML summary from labeled CSV. No raw quotes."""
import argparse
import csv
from collections import Counter
from html import escape
from pathlib import Path


def render(rows):
    cards = []
    for key in ('品牌归属', '情绪', '场景', '品牌', 'brand'):
        counts = Counter(row.get(key, '') for row in rows if row.get(key, ''))
        if not counts:
            continue
        cells = ''.join(f'<tr><td>{escape(str(k))}</td><td>{v}</td></tr>'
                        for k, v in counts.most_common())
        cards.append(f'<section><h2>{escape(key)}</h2><table><tr><th>分类</th><th>行数</th></tr>{cells}</table></section>')
    return ('<!doctype html><html lang="zh-CN"><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width,initial-scale=1">'
            '<title>Reddit VOC Summary</title><style>body{font:16px system-ui;max-width:900px;margin:40px auto;padding:16px;background:#f5f6fa;color:#202430}section{background:white;padding:20px;margin:16px 0;border-radius:12px}table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:8px;border-bottom:1px solid #ddd}</style>'
            f'<h1>Reddit VOC Summary</h1><p>输入 {len(rows)} 行；仅按输入标签计数，未自动排除噪声或仅他牌。作者名和正文不展示。</p>'
            + ''.join(cards) + '</html>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    with open(args.input, encoding='utf-8-sig', newline='') as f:
        rows = list(csv.DictReader(f))
    dest = Path(args.output)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(render(rows), encoding='utf-8')
    print(f'Saved {len(rows)}-row summary to {dest}')


if __name__ == '__main__':
    main()
