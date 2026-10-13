# -*- coding: utf-8 -*-
"""行级品牌归属过滤：避免把爬取桶的品牌当作每句文本的品牌。
设备列优先，文本提及兜底。默认排除仅他牌，无法判定仍保留。
严格统计可只选纯本牌；规则匹配存在误判，需要人工抽查。
"""
import json
import re

# 设备列的 16 个枚举值(与 master 一致); 品牌列取值多一个 '' 和两个非品牌桶
DEVICES = ['Amazfit', 'AppleWatch', 'Bevel', 'Coros', 'Fitbit', 'Garmin', 'Oura', 'Polar',
           'RingConn', 'Samsung', 'Suunto', 'Ultrahuman', 'Whoop', '其他', '华为', '小米']

# 正文兜底用的品牌别名。设备列为空时才用得上。
ALIAS = {
    'Amazfit':    [r'amazfit', r'zepp', r'\bgtr\b', r'\bgts\b'],
    'AppleWatch': [r'apple watch', r'\bawu\b', r'\bapple ?watch'],
    'Bevel':      [r'\bbevel\b'],
    'Coros':      [r'\bcoros\b'],
    'Fitbit':     [r'\bfitbit\b', r'\bcharge \d', r'\bversa\b', r'\bsense\b', r'fitbit air'],
    'Garmin':     [r'\bgarmin\b', r'\bfenix\b', r'\bforerunner\b', r'\bvenu\b', r'\bepix\b', r'\binstinct\b'],
    'Oura':       [r'\boura\b'],
    'Polar':      [r'\bpolar\b', r'\bvantage\b', r'\bgrit x\b'],
    'RingConn':   [r'ringconn', r'ring conn', r'\bringcon\b', r'\brc2\b', r'\brc 2\b'],
    'Samsung':    [r'galaxy ring', r'galaxy watch', r'\bsamsung\b'],
    'Suunto':     [r'\bsuunto\b'],
    'Ultrahuman': [r'ultrahuman', r'ultra human', r'\buh ring\b', r'\bring air\b', r'\buh\b'],
    'Whoop':      [r'\bwhoop\b'],
    '华为':        [r'\bhuawei\b', r'\bwatch gt\b', r'\bhonor\b'],
    '小米':        [r'\bxiaomi\b', r'\bmi ?band\b', r'\bredmi\b', r'\bamazfit\b'],
}
_SELF = {b: re.compile('|'.join(p), re.I) for b, p in ALIAS.items()}
_RIVAL = {b: re.compile('|'.join(p for o, ps in ALIAS.items() if o != b for p in ps), re.I)
          for b in ALIAS}

SCOPE = ('纯本牌', '本牌+他牌', '仅他牌', '无法判定')

# 品牌别名归一。
BRAND_ALIAS = {
    '华米 Amazfit': 'Amazfit',
    'Apple Watch': 'AppleWatch',
    'SamsungRing': 'Samsung',
    'Galaxy Ring': 'Samsung',
    'Huawei': '华为',
    'Xiaomi': '小米',
}


def norm_brand(b):
    """把桶级品牌列的取值归一到 ALIAS 的键。认不出的原样返回。"""
    b = _s(b).strip()
    return BRAND_ALIAS.get(b, b)


def split_devices(v):
    """设备列可能是 JSON list / | 分隔 / 单值; 也可能是 pandas 的 NaN。"""
    v = _s(v).strip()
    if not v or v in ('无', '[]'):
        return []
    try:
        p = json.loads(v)
        if isinstance(p, list):
            return [str(x).strip() for x in p if str(x).strip()]
    except Exception:
        pass
    if '|' in v:
        return [x.strip() for x in v.split('|') if x.strip()]
    return [v]


def _s(v):
    """字段可能来自 csv.DictReader(str/None) 或 pandas(NaN=float)。统一成 str。
    pandas 的 NaN 不是 None 也不是 str, `x.get(k) or ''` 拿到的是 float, join 会炸。"""
    if v is None:
        return ''
    if isinstance(v, str):
        return v
    if v != v:                          # NaN
        return ''
    return str(v)


def _text(x):
    return ' '.join(_s(x.get(k)) for k in ('key_quote', '正文', '标题', 'text', 'title'))


def brand_col(x):
    """桶级品牌列在两份语料里名字不同:
         master_海外v3.csv  -> `品牌` (中文)
         labeled_corpus.csv -> `brand` (英文, 与 sku/subreddit/topic 同组)
    两种都认, 否则"唯一真源"在半个 codebase 上用不了。"""
    for k in ('品牌', 'brand'):
        v = _s(x.get(k)).strip()
        if v:
            return norm_brand(v)
    return ''


def scope_of(x, brand=None):
    """返回这行相对 brand 的归属档位, 取值见 SCOPE。brand 省略时自动取桶级品牌列。"""
    brand = norm_brand(brand) or brand_col(x)
    if brand not in ALIAS:              # 非品牌桶(探针/人群桶)或空品牌, 不判
        return '无法判定'
    dev = split_devices(x.get('设备'))
    if dev:
        if brand not in dev:
            return '仅他牌'
        return '纯本牌' if len(dev) == 1 else '本牌+他牌'
    txt = _text(x)
    has_self = bool(_SELF[brand].search(txt))
    has_rival = bool(_RIVAL[brand].search(txt))
    if has_self and has_rival:
        return '本牌+他牌'
    if has_self:
        return '纯本牌'
    if has_rival:
        return '仅他牌'
    return '无法判定'


def brand_ok(x, brand=None):
    """默认口径: 这行能不能算进 brand 的统计。'仅他牌' 弃, 其余收。"""
    return scope_of(x, brand) != '仅他牌'
