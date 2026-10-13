# -*- coding: utf-8 -*-
"""给 master 回写品牌归属两列, 让所有下游分析都能直接用, 不必各自重算。

    品牌归属  纯本牌 / 本牌+他牌 / 仅他牌 / 无法判定   (口径见 brand_scope.py)
    品牌可用  True / False                          (默认口径: 仅他牌=False)

原地追加两列, 行序不动 —— master 的 rid 是行号, 绝不能重排(需保留原始 rid)。
先写临时文件再原子替换, 避免中途失败留下半截 CSV(保留已有数据)。

用法:
    python3 annotate_brand_scope.py --dry     只统计不写
    python3 annotate_brand_scope.py           写回 master
"""
import collections
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from brand_scope import ALIAS, brand_col, brand_ok, scope_of  # noqa: E402

csv.field_size_limit(10 ** 9)
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, 'openclaw_output/health-wearable/labeled_corpus.csv')
# 可选位置参数覆盖目标文件（如 v4 增量版），默认仍写 v3（向后兼容）
if len(sys.argv) > 1 and not sys.argv[1].startswith('--'):
    SRC = sys.argv[1] if os.path.isabs(sys.argv[1]) else os.path.join(HERE, sys.argv[1])
NEW_COLS = ['品牌归属', '品牌可用']
DRY = '--dry' in sys.argv


def main():
    with open(SRC, encoding='utf-8-sig') as f:   # utf-8-sig 剥 BOM, 否则首列变 '﻿品牌'
        rd = csv.DictReader(f)
        cols = list(rd.fieldnames)
        rows = list(rd)

    already = [c for c in NEW_COLS if c in cols]
    if already:
        print('⚠ master 已有 %s 列 —— 本次为重算覆盖(幂等, 安全)' % '/'.join(already))
    out_cols = cols + [c for c in NEW_COLS if c not in cols]

    stat = collections.defaultdict(collections.Counter)
    for x in rows:
        b = brand_col(x)
        sc = scope_of(x, b)
        x['品牌归属'] = sc
        x['品牌可用'] = 'True' if brand_ok(x, b) else 'False'
        stat[b][sc] += 1

    print('\n%-14s %7s %8s %9s %8s %9s %8s' % ('品牌', '行数', '纯本牌', '本牌+他牌', '仅他牌', '无法判定', '可用率'))
    drop = 0
    for b, c in sorted(stat.items(), key=lambda kv: -sum(kv[1].values())):
        n = sum(c.values())
        drop += c['仅他牌']
        tag = '' if b in ALIAS else '  (非品牌桶, 不判)'
        print('%-14s %7d %8d %9d %8d %9d %7.1f%%%s'
              % (b, n, c['纯本牌'], c['本牌+他牌'], c['仅他牌'], c['无法判定'],
                 (n - c['仅他牌']) / n * 100, tag))
    print('\n合计 %d 行, 标为「仅他牌」(默认口径剔除) %d 行 = %.1f%%'
          % (len(rows), drop, drop / len(rows) * 100 if rows else 0))

    if DRY:
        print('\n--dry: 未写盘')
        return

    tmp = SRC + '.tmp'
    with open(tmp, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=out_cols)
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, SRC)                       # 原子替换
    print('\n✅ 已写回 %s (+%d 列, %d 行, 行序未动)'
          % (os.path.basename(SRC), len(out_cols) - len(cols), len(rows)))


if __name__ == '__main__':
    main()
