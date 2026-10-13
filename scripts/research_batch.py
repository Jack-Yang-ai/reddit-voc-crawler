"""
运动穿戴 VOC 研究批量编排

从 SKU×Query Excel（第 6 列 "Reddit query / subreddit"）读取每个 SKU 的
subreddit + 关键词，逐 SKU 以「研究参数」（近 6 个月 Top100 帖 + Top20 高赞评论）
调用 openclaw_reddit 的抓取流水线，产出：
  - 每 SKU: openclaw_output/health-wearable/<sku-slug>/{today.json, corpus.csv, corpus.jsonl}
  - 合并:   openclaw_output/health-wearable/corpus_all.csv（供后续 AI 标注）
另含「未满足需求探针」（咖啡机方法论）：在运动 sub 跑 wish/alternative/holy grail 等
句式 + 心智词，单独落 probe/，供发现未满足需求。

用法:
    # 冒烟（前台，小样本验证管线）
    python3 research_batch.py --group smoke

    # P0 全量（建议后台）
    python3 research_batch.py --group p0

    # 指定 SKU（slug 子串匹配）
    python3 research_batch.py --skus whoop-5.0 ouraring-gen-4

    # 仅跑未满足需求探针
    python3 research_batch.py --probe-only

参数 slug 由「第一个 (subreddit, 关键词) 对」生成，如 whoop-5.0 / ouraring-gen-4 /
amazfit-helio-strap / smartrings-ringconn-gen-3。用 `--list` 查看全部可选 slug。
"""

from __future__ import annotations

import argparse
import os
import re
import time

import pandas as pd

from reddit import _build_session, SLEEP_INTERVAL
from openclaw_reddit import (
    scrape_all, extract_today, enrich_and_save, export_raw,
    RAW_COLUMNS, USE_PROXY, PROXY_URL,
)

# ============ 默认值 ============

DEFAULT_EXCEL = os.environ.get("REDDIT_SKU_EXCEL", "")
SHEET = "SKU与Query清单"
RESEARCH_TOPIC = "health-wearable"
BASE = os.path.join(os.environ.get("REDDIT_OUTPUT_DIR", "openclaw_output"), RESEARCH_TOPIC)

# 研究参数（Brief：近 6 个月 Top100 帖 + 高赞评论）
# 窗口放宽到一年：几个最关键的退订/升级心智帖在 6 个月外，365 天能捞回
RESEARCH_WINDOW = "year"
RESEARCH_RECENT_DAYS = 365
RESEARCH_POST_LIMIT = 100
RESEARCH_COMMENTS = 20
RESEARCH_SAMPLING = "blend"   # 混合采样，摆脱纯 upvote 戏剧偏差

# 冒烟参数（小样本，快速验证管线形态）
SMOKE_POST_LIMIT = 12
SMOKE_COMMENTS = 10

# 品牌名 → slug 前缀（用于兜底，slug 主要由首个 sub 决定，这里只做记录）
BRAND_CANON = {
    "Apple Watch": "Apple", "华为": "华为", "小米": "小米",
    "华米 Amazfit": "Amazfit", "Garmin": "Garmin", "Polar": "Polar",
    "Whoop": "Whoop", "Oura": "Oura", "RingConn": "RingConn",
}

# ============ Excel 解析 ============

def _slugify(text: str) -> str:
    """ascii 化 slug：保留字母数字和点，空格转连字符，小写。"""
    text = text.strip().lower()
    text = re.sub(r"[^\w.\- ]+", "", text)  # 去掉引号/括号等
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text or "sku"


def parse_reddit_cell(cell: str) -> list[tuple[str, str]]:
    """
    把 "r/AppleWatch + \"Ultra 2\" / r/Garmin \"Apple Watch Ultra\"" 这类单元格
    解析为 [(subreddit, keyword), ...]。
    规则：
      - 按 ' / ' 切 chunk；
      - 每 chunk 取 r/<Sub>（无则沿用上一个 sub）+ 所有 "引号关键词"；
      - 只有 sub 没关键词的 chunk（如 / r/triathlon）→ 用「首个关键词」绑定
        （= 在该相关 sub 内搜同一型号，比整 sub feed 更聚焦）；
      - 只有关键词没 sub 的 chunk → 绑定上一个 sub。
    """
    pairs: list[tuple[str, str | None]] = []
    primary_kw: str | None = None
    last_sub: str | None = None
    for chunk in [c.strip() for c in re.split(r"\s+/\s+", str(cell)) if c.strip()]:
        m = re.search(r"r/(\w+)", chunk)
        if m:
            last_sub = m.group(1)
        sub = last_sub
        kws = re.findall(r'"([^"]+)"', chunk)
        if primary_kw is None and kws:
            primary_kw = kws[0]
        if kws:
            for kw in kws:
                pairs.append((sub, kw))
        else:
            pairs.append((sub, None))  # sub-only，待用 primary_kw 兜底

    out: list[tuple[str, str]] = []
    seen = set()
    for sub, kw in pairs:
        k = kw if kw else primary_kw
        if sub and k and (sub, k) not in seen:
            seen.add((sub, k))
            out.append((sub, k))
    return out


def load_skus(excel_path: str) -> list[dict]:
    """读 Excel → [{slug, brand, model, category, pairs:[(sub,kw)]}]，slug 去重。"""
    raw = pd.read_excel(excel_path, sheet_name=SHEET, header=None)
    rows = raw.iloc[4:]  # 前 4 行是标题/说明/表头
    skus = []
    used = {}
    for _, r in rows.iterrows():
        brand, tier, model, category, reddit_cell = r[0], r[1], r[2], r[3], r[8]
        if pd.isna(model) or pd.isna(reddit_cell):
            continue
        pairs = parse_reddit_cell(reddit_cell)
        if not pairs:
            continue
        sub0, kw0 = pairs[0]
        slug = _slugify(f"{sub0}-{kw0}")
        if slug in used:  # 罕见碰撞 → 加序号
            used[slug] += 1
            slug = f"{slug}-{used[slug]}"
        else:
            used[slug] = 1
        skus.append({
            "slug": slug,
            "brand": str(brand) if not pd.isna(brand) else "",
            "model": str(model),
            "category": str(category) if not pd.isna(category) else "",
            "tier": str(tier) if not pd.isna(tier) else "",
            "pairs": pairs,
        })
    return skus


# ============ P0 / 冒烟 选择 ============

# P0：直接对位无屏手环/戒指 + 最 sharp 运动人群（HYROX/跑步铁三）重叠表
P0_SLUGS = [
    "whoop-5.0", "whoop-mg", "whoop-4.0",
    "amazfit-helio-strap", "amazfit-helio-ring", "polar-loop",
    "ouraring-gen-4", "ouraring-gen-3",
    "smartrings-ringconn-gen-3", "smartrings-ringconn-gen-2",
    "applewatch-ultra-3", "garmin-fr965", "garmin-fr970",
    "garmin-instinct", "amazfit-t-rex-3", "amazfit-cheetah-2-pro",
]
SMOKE_SLUGS = ["whoop-5.0", "ouraring-gen-4"]

# ============ 未满足需求探针（咖啡机方法论）============

# 探针只投高流量、求购/转投帖密集的 device/sport sub（hyrox/crossfit 太小、召回弱已去掉），
# 关键词收敛到最核心的 8 个，控制总请求量保护账号（4×8=32 对）。
PROBE_SUBS = ["whoop", "ouraring", "Garmin", "running"]
PROBE_KEYWORDS = [
    # 未满足需求/求购句式（最前置的产品机会信号）
    "wish there was a band", "alternative to whoop", "switching from",
    "holy grail tracker", "no screen band", "without subscription",
    # 心智词（Sheet2：验证主词从原话长出）
    "recovery", "readiness",
]


# ============ 单 SKU 抓取 ============

def crawl_one(session, sku: dict, post_limit: int, comments: int,
              window: str, recent_days: int, sampling: str = RESEARCH_SAMPLING) -> str | None:
    """抓单个 SKU，落 today.json + corpus.csv，返回 corpus.csv 路径。"""
    out_dir = os.path.join(BASE, sku["slug"])
    pairs = sku["pairs"]
    print("\n" + "=" * 60)
    print(f"  [SKU] {sku['slug']}  ({sku['brand']} {sku['model']})")
    for s, k in pairs:
        print(f"        r/{s} → \"{k}\"")
    print("=" * 60)

    df = scrape_all(session, pairs, [], window)
    if df.empty:
        print(f"[跳过] {sku['slug']} 无数据")
        return None
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, "all_posts.csv"), index=False, encoding="utf-8-sig")
    today_df = extract_today(df, limit=post_limit, recent_days=recent_days, sampling=sampling)
    subreddit_scope = sorted({s for s, _ in pairs})
    output = enrich_and_save(session, today_df, sku["slug"], subreddit_scope, out_dir,
                             comments_per_post=comments,
                             sku=sku["slug"], brand=sku["brand"])
    return export_raw(output, out_dir)


def crawl_probe(session, subs: list[str], post_limit: int, comments: int,
                window: str, recent_days: int) -> str | None:
    """探针：每个 sub × 每个 probe 关键词，单独落 probe/。"""
    out_dir = os.path.join(BASE, "probe")
    pairs = [(sub, kw) for sub in subs for kw in PROBE_KEYWORDS]
    print("\n" + "=" * 60)
    print(f"  [PROBE] 未满足需求探针 — {len(subs)} sub × {len(PROBE_KEYWORDS)} 词 = {len(pairs)} 组")
    print("=" * 60)
    df = scrape_all(session, pairs, [], window)
    if df.empty:
        print("[探针] 无数据")
        return None
    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(os.path.join(out_dir, "all_posts.csv"), index=False, encoding="utf-8-sig")
    today_df = extract_today(df, limit=post_limit, recent_days=recent_days, sampling=RESEARCH_SAMPLING)
    output = enrich_and_save(session, today_df, "probe", sorted(set(subs)), out_dir,
                             comments_per_post=comments, sku="probe", brand="")
    return export_raw(output, out_dir)


# ============ 合并 ============

def merge_corpus(csv_paths: list[str] | None = None) -> str:
    """
    合并 corpus_all.csv。
    扫描 BASE 下所有 <子目录>/corpus.csv（含 probe），而非只合本次运行的，
    这样多次分批跑会「累积」而不是相互覆盖。按 (sku,row_type,permalink,text) 去重。
    """
    import glob
    paths = sorted(glob.glob(os.path.join(BASE, "*", "corpus.csv")))
    frames = [pd.read_csv(p) for p in paths if os.path.exists(p)]
    if not frames:
        print("[合并] 无 corpus 可合并")
        return ""
    out = os.path.join(BASE, "corpus_all.csv")
    if os.path.exists(out):
        frames.insert(0, pd.read_csv(out))
    alldf = pd.concat(frames, ignore_index=True)
    before = len(alldf)
    alldf = alldf.drop_duplicates(subset=["sku", "row_type", "permalink", "text"]).reset_index(drop=True)
    if before != len(alldf):
        print(f"[合并] 去重 {before} → {len(alldf)} 行")
    os.makedirs(BASE, exist_ok=True)
    out = os.path.join(BASE, "corpus_all.csv")
    alldf.to_csv(out, index=False, encoding="utf-8-sig", columns=RAW_COLUMNS)
    n_post = (alldf["row_type"] == "post").sum()
    n_cmt = (alldf["row_type"] == "comment").sum()
    print(f"\n[合并] {len(frames)} 个 SKU → {out}（{n_post} 帖 + {n_cmt} 评论，共 {len(alldf)} 行）")
    return out


# ============ main ============

def main():
    ap = argparse.ArgumentParser(
        description="运动穿戴 VOC 研究批量编排",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--excel", default=DEFAULT_EXCEL, help="SKU×Query Excel 路径")
    ap.add_argument("--group", choices=["smoke", "p0", "all"], default=None,
                    help="预设组：smoke=2 SKU 冒烟 / p0=优先级样本 / all=全部 SKU")
    ap.add_argument("--skus", nargs="+", default=[],
                    help="显式指定 SKU slug（子串匹配），覆盖 --group")
    ap.add_argument("--list", action="store_true", help="只列出全部 SKU slug 后退出")
    ap.add_argument("--probe", action="store_true", help="同时跑未满足需求探针")
    ap.add_argument("--probe-only", action="store_true", help="只跑探针，不抓 SKU")
    ap.add_argument("--probe-subs", nargs="+", default=PROBE_SUBS,
                    help="探针覆盖的 sub（冒烟可只给一个，如 hyrox）")
    ap.add_argument("--post-limit", type=int, default=None,
                    help=f"每 SKU 取 TopN 帖（默认研究 {RESEARCH_POST_LIMIT} / smoke {SMOKE_POST_LIMIT}）")
    ap.add_argument("--comments-per-post", type=int, default=None,
                    help=f"每帖 TopN 评论（默认研究 {RESEARCH_COMMENTS} / smoke {SMOKE_COMMENTS}）")
    ap.add_argument("--recent-days", type=int, default=RESEARCH_RECENT_DAYS)
    ap.add_argument("--time-window", default=RESEARCH_WINDOW)
    args = ap.parse_args()

    if not args.probe_only and not args.excel:
        ap.error("请提供 --excel SKU 查询清单")
    skus = [] if args.probe_only else load_skus(args.excel)

    if args.list:
        print(f"共 {len(skus)} 个 SKU：")
        for s in skus:
            print(f"  {s['slug']:32s} {s['brand']} {s['model']}")
        return

    is_smoke = args.group == "smoke"
    post_limit = args.post_limit or (SMOKE_POST_LIMIT if is_smoke else RESEARCH_POST_LIMIT)
    comments = args.comments_per_post or (SMOKE_COMMENTS if is_smoke else RESEARCH_COMMENTS)

    # 选 SKU
    selected = []
    if args.probe_only:
        selected = []
    elif args.skus:
        selected = [s for s in skus if any(tok in s["slug"] for tok in args.skus)]
    elif args.group == "smoke":
        selected = [s for s in skus if s["slug"] in SMOKE_SLUGS]
    elif args.group == "p0":
        selected = [s for s in skus if s["slug"] in P0_SLUGS]
    elif args.group == "all":
        selected = skus
    else:
        ap.error("需指定 --group / --skus / --probe-only / --list 之一")

    print(f"[计划] {len(selected)} 个 SKU | post_limit={post_limit} comments={comments} "
          f"window={args.time_window} recent_days={args.recent_days}")
    for s in selected:
        print(f"        - {s['slug']}")

    session = _build_session()
    if USE_PROXY:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
        print("[爬取] 使用已配置的代理")

    corpus_paths = []
    for sku in selected:
        path = crawl_one(session, sku, post_limit, comments,
                         args.time_window, args.recent_days)
        if path:
            corpus_paths.append(path)
        time.sleep(SLEEP_INTERVAL)

    if args.probe or args.probe_only:
        probe_path = crawl_probe(session, args.probe_subs, post_limit, comments,
                                 args.time_window, args.recent_days)
        if probe_path:
            corpus_paths.append(probe_path)

    merge_corpus(corpus_paths)
    print("\n[完成] 输出根目录:", BASE)


if __name__ == "__main__":
    main()
