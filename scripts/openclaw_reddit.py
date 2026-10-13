"""
Reddit 今日热帖 + 评论区抓取脚本
按主题（--topic）抓取指定 subreddit 的最近热帖，并为每篇抓取 Top 评论，
供模型总结"每个帖子在讨论什么、用户的具体观点是什么"。

使用方式:
    # 全量 feed（无关键词过滤），每个 subreddit 抓 new/hot/top-week/top-month
    python3 openclaw_reddit.py --topic whoop --subreddits whoop

    # subreddit:关键词 对（关键词始终加引号强制精确匹配，支持单词或短语）
    python3 openclaw_reddit.py --topic kimiclaw \\
        --subreddit-keywords "kimi:Kimi Claw" "AI_agents:KimiClaw"

    # 两种可混用：feed 抓全量 + 指定 subreddit 内精确搜关键词
    python3 openclaw_reddit.py --topic maxclaw \\
        --subreddits openclaw \\
        --subreddit-keywords "aicuriosity:MaxClaw" "LLM:MaxClaw"
"""

from __future__ import annotations

import argparse
import os
import json
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from reddit import _build_session, _fetch_posts, _fetch_top_comments, SLEEP_INTERVAL, _throttle

# ============ 全局默认值 ============

PER_STRATEGY = 30          # 每个爬取策略最多贡献的新帖数
MAX_PAGES_PER_FEED = 3     # 每个策略最多翻几页（每页100条）
TODAY_POST_LIMIT = 10      # "今日"取最近48h内 score 最高的几篇
COMMENTS_PER_POST = 15     # 每篇帖子抓多少条 Top 评论
RECENT_HOURS = 48          # "今日"窗口：最近多少小时（北京时间早上爬取时 UTC 当天数据不足，故取 48h）
BASE_OUTPUT_DIR = os.environ.get("REDDIT_OUTPUT_DIR", "openclaw_output")

# 代理配置（部署环境设为 True，本地测试设为 False）
USE_PROXY = os.environ.get("USE_PROXY", "false").lower() == "true"
PROXY_URL = os.environ.get("PROXY_URL", "http://127.0.0.1:7890")


# ============ 爬取策略构建 ============

def build_searches(
    sr_keyword_pairs: list[tuple[str, str]],
    feed_subreddits: list[str] | None = None,
    time_window: str = "today",
) -> list[tuple[str, dict]]:
    """
    生成爬取策略：
    - sr_keyword_pairs: [(subreddit, keyword), ...] 每对生成 sort=top + sort=new 两条搜索
    - feed_subreddits: 这些 subreddit 使用全量 feed（无关键词过滤）
    - time_window: today/week/month/year/all。决定搜索 t= 与 feed top 的窗口；
      "today" 与 "week" 等价于 t=week（再叠加 48h 过滤），更宽的窗口会扩到 year/all。
    """
    # 把抽象窗口映射成 Reddit 支持的 t= 值
    # search 走「主+副」两条；feed 走 hot + new + 两个 top 窗口
    window_map = {
        "today": ("week", "month", ["week", "month"]),
        "week":  ("week", "month", ["week", "month"]),
        "month": ("month", "year", ["month", "year"]),
        "year":  ("year", "all",  ["year", "all"]),
        "all":   ("all",  "all",  ["year", "all"]),
    }
    search_t_primary, search_t_secondary, feed_top_windows = window_map[time_window]

    searches = []

    for sr, kw in sr_keyword_pairs:
        base = f"https://old.reddit.com/r/{sr}"
        # 始终加引号，强制 Reddit 做精确匹配，避免对 CamelCase 关键词做分词（如 KimiClaw→Kimi+Claw）
        q = f'"{kw}"'
        searches.append((f"{base}/search.json", {
            "q": q, "sort": "top", "t": search_t_primary, "restrict_sr": "on"
        }))
        searches.append((f"{base}/search.json", {
            "q": q, "sort": "top", "t": search_t_secondary, "restrict_sr": "on"
        }))
        if time_window in ("today", "week"):
            # 短窗口：再补一条 new 抓最新讨论
            searches.append((f"{base}/search.json", {
                "q": q, "sort": "new", "t": "week", "restrict_sr": "on"
            }))
        else:
            # 宽窗口（研究）：除高赞 top 外，补 sort=comments（高讨论帖）+ sort=new（最新帖），
            # 让候选池不只偏高赞戏剧帖，给后续 blend 采样留材料
            searches.append((f"{base}/search.json", {
                "q": q, "sort": "comments", "t": search_t_primary, "restrict_sr": "on"
            }))
            searches.append((f"{base}/search.json", {
                "q": q, "sort": "new", "t": search_t_primary, "restrict_sr": "on"
            }))

    for sr in (feed_subreddits or []):
        base = f"https://old.reddit.com/r/{sr}"
        # 宽窗口下不再抓 new/hot（噪音大），只取目标窗口的 top
        if time_window in ("today", "week"):
            searches.append((f"{base}/new.json", {}))
            searches.append((f"{base}/hot.json", {}))
        for t in feed_top_windows:
            searches.append((f"{base}/top.json", {"t": t}))

    return searches


def parse_subreddit_keywords(raw: list[str]) -> list[tuple[str, str]]:
    """
    解析 --subreddit-keywords 参数，格式为 "subreddit:keyword"。
    keyword 中可含空格，冒号只分割第一个。
    示例: ["kimi:claw", "AI_agents:kimi claw"] -> [("kimi","claw"), ("AI_agents","kimi claw")]
    """
    pairs = []
    for item in raw:
        if ":" not in item:
            raise ValueError(f"--subreddit-keywords 格式错误（缺少冒号）：{item!r}，应为 'subreddit:keyword'")
        sr, kw = item.split(":", 1)
        pairs.append((sr.strip(), kw.strip()))
    return pairs


def _strategy_label(url: str, params: dict) -> str:
    """生成策略的可读标签，用于日志输出"""
    sr = url.split("/r/")[1].split("/")[0] if "/r/" in url else "?"
    feed = url.split("/")[-1].replace(".json", "")
    label = f"r/{sr}/{feed}"
    if params.get("t"):
        label += f"({params['t']})"
    if params.get("q"):
        label += f"?q={params['q']}"
    return label


# ============ 爬取逻辑 ============

def scrape_all(session, sr_keyword_pairs: list[tuple[str, str]],
               feed_subreddits: list[str] | None = None,
               time_window: str = "today") -> pd.DataFrame:
    """
    爬取指定主题的帖子。
    sr_keyword_pairs: [(subreddit, keyword), ...] 走关键词搜索
    feed_subreddits: 走全量 feed（无关键词过滤）
    time_window: today/week/month/year/all，决定搜索与 feed 的 t= 窗口
    """
    print("[爬取] 初始化连接...")
    try:
        session.get("https://old.reddit.com/", timeout=15)
        time.sleep(2)
        print("[爬取] 连接成功")
    except Exception as e:
        print(f"[爬取] 警告: 首页访问失败 ({e}), 继续尝试...")

    searches = build_searches(sr_keyword_pairs, feed_subreddits, time_window)
    print(f"[爬取] {len(searches)} 个策略, 每策略上限 {PER_STRATEGY} 条")

    all_posts = []
    seen_ids = set()

    for i, (url, extra_params) in enumerate(searches):
        label = _strategy_label(url, extra_params)
        print(f"[{i+1}/{len(searches)}] {label}")

        results = _fetch_posts(session, url, extra_params, max_pages=MAX_PAGES_PER_FEED)
        strategy_new = 0
        for p in results:
            if p["id"] not in seen_ids and strategy_new < PER_STRATEGY:
                seen_ids.add(p["id"])
                all_posts.append(p)
                strategy_new += 1
        print(f"  获取 {len(results)}, 新增 {strategy_new}, 累计 {len(all_posts)}")
        _throttle()

    if not all_posts:
        print("[爬取] 未获取到帖子")
        return pd.DataFrame()

    df = pd.DataFrame(all_posts)
    df["created_time"] = pd.to_datetime(df["created_utc"], unit="s")
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    print(f"[爬取] 完成, 共 {len(df)} 条")
    return df


# ============ 热帖提取（按时间窗）============

def _blend_select(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """
    混合采样，摆脱纯 upvote 的「戏剧偏差」：
      - 50% 名额给 score Top（高赞共识帖）
      - 30% 名额给 num_comments Top（高讨论但未必高赞，常含分歧/求助）
      - 20% 名额给最新帖（created_time 最新，抓正在发生的讨论）
    三路按上述配额取、去重合并；不足部分用剩余里 score 最高的补齐。
    """
    if len(df) <= limit:
        return df.sort_values("score", ascending=False).reset_index(drop=True)
    n_score = max(1, int(limit * 0.5))
    n_cmt = max(1, int(limit * 0.3))
    n_new = max(1, limit - n_score - n_cmt)
    picked_ids: set = set()
    parts = []
    for sortcol, n, asc in [("score", n_score, False),
                            ("num_comments", n_cmt, False),
                            ("created_time", n_new, False)]:
        pool = df[~df["id"].isin(picked_ids)]
        if pool.empty:
            continue
        chunk = pool.sort_values(sortcol, ascending=asc).head(n)
        picked_ids.update(chunk["id"].tolist())
        parts.append(chunk)
    out = pd.concat(parts, ignore_index=True) if parts else df.head(0)
    if len(out) < limit:  # 配额没凑满 → 用剩余里 score 最高的补
        fill = df[~df["id"].isin(picked_ids)].nlargest(limit - len(out), "score")
        out = pd.concat([out, fill], ignore_index=True)
    return out.sort_values("score", ascending=False).reset_index(drop=True)


def extract_today(df: pd.DataFrame, limit: int = TODAY_POST_LIMIT,
                  recent_days: int | None = None, sampling: str = "score") -> pd.DataFrame:
    """
    按时间窗取 limit 篇热帖。
    - recent_days 为 None：沿用「今日」语义，取最近 RECENT_HOURS 小时（48h）内。
    - recent_days 给定（研究模式，如 365≈一年）：按天过滤。
    - sampling="score"：纯按 upvote 取 Top（日报默认）；
      sampling="blend"：score/评论数/最新 混合采样（研究默认，摆脱戏剧偏差）。
    过滤后不足 limit 条则保留实际数量（log 告知，不静默截断）。
    """
    if df.empty:
        return df
    now_utc = datetime.now(timezone.utc)
    if recent_days is not None:
        cutoff = (now_utc - timedelta(days=recent_days)).replace(tzinfo=None)
        recent = df[df["created_time"] >= cutoff].copy()
        if recent.empty:
            print(f"[窗口] 最近 {recent_days} 天内无帖子，退回全量")
            recent = df
        elif len(recent) < limit:
            print(f"[窗口] 最近 {recent_days} 天内仅 {len(recent)} 帖（不足 {limit}），全部保留")
    else:
        cutoff = (now_utc - timedelta(hours=RECENT_HOURS)).replace(tzinfo=None)
        recent = df[df["created_time"] >= cutoff].copy()
        if recent.empty:
            print(f"[今日] 最近 {RECENT_HOURS}h 内无帖子，退回全量")
            recent = df

    if sampling == "blend":
        return _blend_select(recent, limit)
    return recent.nlargest(limit, "score").reset_index(drop=True)


# ============ 抓评论并保存供 LLM 读取的 JSON ============

def enrich_and_save(session, today_df: pd.DataFrame, topic: str,
                    subreddit_scope: list[str], output_dir: str,
                    comments_per_post: int = COMMENTS_PER_POST,
                    sku: str = "", brand: str = "") -> dict:
    """
    为每篇热帖抓取 Top 评论，连同帖子一起存为 today.json。
    permalink 在保存时由 old.reddit.com 改写为 reddit.com（便于点击）。
    研究模式下额外保留 created_utc / author / upvote_ratio / url / flair 供 raw 导出。
    """
    os.makedirs(output_dir, exist_ok=True)
    now_utc = datetime.now(timezone.utc)

    posts = []
    total = len(today_df)
    print(f"[评论] 为 {total} 篇热帖抓取 Top{comments_per_post} 评论...")
    for rank, (_, row) in enumerate(today_df.iterrows(), start=1):
        old_permalink = str(row.get("permalink", ""))
        print(f"  [{rank}/{total}] 🔥{int(row['score'])} 💬{int(row.get('num_comments', 0))} | {row['title'][:50]}")
        comments = _fetch_top_comments(session, old_permalink, limit=comments_per_post)
        _throttle()
        created_utc = float(row.get("created_utc", 0) or 0)
        posts.append({
            "rank": rank,
            "post_id": row.get("id", ""),
            "title": row["title"],
            "selftext": str(row.get("selftext", ""))[:1500],
            "subreddit": row.get("subreddit", ""),
            "author": row.get("author", ""),
            "score": int(row["score"]),
            "upvote_ratio": float(row.get("upvote_ratio", 0) or 0),
            "num_comments": int(row.get("num_comments", 0)),
            "created_utc": created_utc,
            "created_date": datetime.utcfromtimestamp(created_utc).strftime("%Y-%m-%d") if created_utc else "",
            "url": row.get("url", ""),
            "flair": row.get("link_flair_text", ""),
            "permalink": old_permalink.replace("https://old.reddit.com", "https://reddit.com"),
            "top_comments": comments,
        })

    output = {
        "generated_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "topic": topic,
        "sku": sku,
        "brand": brand,
        "date": now_utc.strftime("%Y-%m-%d"),
        "recent_hours": RECENT_HOURS,
        "subreddit_scope": subreddit_scope,
        "post_count": len(posts),
        "posts": posts,
    }

    json_path = os.path.join(output_dir, "today.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"[保存] {len(posts)} 篇热帖 + 评论 → {json_path}")
    return output


# ============ raw 语料导出（供 AI 标注 + Quote Bank）============

RAW_COLUMNS = [
    "topic", "sku", "brand", "subreddit", "row_type", "post_id", "parent_post_id",
    "title", "text", "score", "upvote_ratio", "num_comments",
    "author", "permalink", "url", "created_utc", "created_date", "flair",
]


def _flatten_to_rows(output: dict) -> list[dict]:
    """把 today.json 结构 flatten 成 raw 行：每帖一行 post + 每条评论一行 comment。"""
    topic = output.get("topic", "")
    sku = output.get("sku", "")
    brand = output.get("brand", "")
    rows = []
    for p in output.get("posts", []):
        rows.append({
            "topic": topic, "sku": sku, "brand": brand,
            "subreddit": p.get("subreddit", ""), "row_type": "post",
            "post_id": p.get("post_id", ""), "parent_post_id": "",
            "title": p.get("title", ""), "text": p.get("selftext", ""),
            "score": p.get("score", 0), "upvote_ratio": p.get("upvote_ratio", 0),
            "num_comments": p.get("num_comments", 0), "author": p.get("author", ""),
            "permalink": p.get("permalink", ""), "url": p.get("url", ""),
            "created_utc": p.get("created_utc", 0), "created_date": p.get("created_date", ""),
            "flair": p.get("flair", ""),
        })
        for c in p.get("top_comments", []):
            rows.append({
                "topic": topic, "sku": sku, "brand": brand,
                "subreddit": p.get("subreddit", ""), "row_type": "comment",
                "post_id": "", "parent_post_id": p.get("post_id", ""),
                "title": "", "text": c.get("body", ""),
                "score": c.get("score", 0), "upvote_ratio": "",
                "num_comments": "", "author": c.get("author", ""),
                "permalink": p.get("permalink", ""), "url": "",
                "created_utc": "", "created_date": "", "flair": "",
            })
    return rows


def export_raw(output: dict, output_dir: str) -> str:
    """把单主题 today.json 导出为 corpus.csv + corpus.jsonl（flatten 的 post/comment 行）。"""
    os.makedirs(output_dir, exist_ok=True)
    rows = _flatten_to_rows(output)
    csv_path = os.path.join(output_dir, "corpus.csv")
    pd.DataFrame(rows, columns=RAW_COLUMNS).to_csv(csv_path, index=False, encoding="utf-8-sig")
    jsonl_path = os.path.join(output_dir, "corpus.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    n_posts = sum(1 for r in rows if r["row_type"] == "post")
    n_comments = sum(1 for r in rows if r["row_type"] == "comment")
    print(f"[raw] {n_posts} 帖 + {n_comments} 评论 → {csv_path}")
    return csv_path


def main():
    parser = argparse.ArgumentParser(
        description="Reddit 今日热帖 + 评论区抓取",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--topic", required=True,
                        help="主题名称（用于输出目录命名，如 whoop / bevel）")
    parser.add_argument("--subreddit-keywords", nargs="+", default=[],
                        metavar="SR:KEYWORD",
                        help="subreddit:关键词 对，如 \"kimi:claw\" \"AI_agents:kimi claw\"，每对生成 top+new 两条搜索")
    parser.add_argument("--subreddits", nargs="*", default=[],
                        help="使用全量 feed 的 subreddit（无关键词过滤），与 --subreddit-keywords 可混用")
    # —— 研究模式参数（默认值保持旧「今日速览」行为不变）——
    parser.add_argument("--time-window", choices=["today", "week", "month", "year", "all"],
                        default="today", help="搜索/feed 时间窗（研究近6个月用 year）")
    parser.add_argument("--recent-days", type=int, default=None,
                        help="按天过滤热帖（研究≈6个月传 183）；不传则沿用 48h「今日」逻辑")
    parser.add_argument("--post-limit", type=int, default=TODAY_POST_LIMIT,
                        help=f"取 score 最高的几篇（默认 {TODAY_POST_LIMIT}，研究用 100）")
    parser.add_argument("--comments-per-post", type=int, default=COMMENTS_PER_POST,
                        help=f"每篇抓多少条 Top 评论（默认 {COMMENTS_PER_POST}，研究用 20）")
    parser.add_argument("--sampling", choices=["score", "blend"], default="score",
                        help="score=纯按 upvote 取 Top（日报默认）；blend=score/评论数/最新 混合采样（研究用，摆脱戏剧偏差）")
    parser.add_argument("--export", choices=["digest", "raw", "both"], default="digest",
                        help="digest=仅 today.json（供讨论速览）；raw=额外出 corpus.csv/jsonl；both=两者")
    parser.add_argument("--sku", default="", help="研究模式：SKU 标识，写入 raw 行")
    parser.add_argument("--brand", default="", help="研究模式：品牌标识，写入 raw 行")
    args = parser.parse_args()

    if not args.subreddit_keywords and not args.subreddits:
        parser.error("至少需要提供 --subreddit-keywords 或 --subreddits 之一")

    output_dir = os.path.join(BASE_OUTPUT_DIR, args.topic)
    raw_csv = os.path.join(output_dir, "all_posts.csv")

    sr_keyword_pairs = parse_subreddit_keywords(args.subreddit_keywords)
    subreddit_scope = sorted(set(args.subreddits) | {sr for sr, _ in sr_keyword_pairs})

    print("=" * 60)
    print(f"  Reddit 今日热帖 + 评论抓取 — 主题: {args.topic}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if sr_keyword_pairs:
        for sr, kw in sr_keyword_pairs:
            print(f"  r/{sr} → \"{kw}\"")
    if args.subreddits:
        print(f"  feed subreddits: {args.subreddits}")
    if args.recent_days is not None:
        print(f"  时间窗: t={args.time_window}, 近 {args.recent_days} 天, 取 Top{args.post_limit} 篇, 每篇 Top{args.comments_per_post} 评论 | export={args.export}")
    else:
        print(f"  今日窗口: 最近 {RECENT_HOURS}h, 取 Top{args.post_limit} 篇, 每篇 Top{args.comments_per_post} 评论 | export={args.export}")
    print("=" * 60)

    session = _build_session()
    if USE_PROXY:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
        print("[爬取] 使用已配置的代理")

    # 1. 爬取帖子
    df = scrape_all(session, sr_keyword_pairs, args.subreddits, args.time_window)
    if df.empty:
        print("[完成] 无数据")
        return

    # 2. 保存原始数据
    os.makedirs(output_dir, exist_ok=True)
    df.to_csv(raw_csv, index=False, encoding="utf-8-sig")
    print(f"[保存] 全量数据 → {raw_csv} ({len(df)} 条)")

    # 3. 提取热帖（按时间窗 + 采样策略）
    today_df = extract_today(df, limit=args.post_limit, recent_days=args.recent_days,
                             sampling=args.sampling)
    print(f"[热帖] 取 Top{len(today_df)} 篇")

    # 4. 抓评论并保存 today.json
    output = enrich_and_save(session, today_df, args.topic, subreddit_scope, output_dir,
                             comments_per_post=args.comments_per_post,
                             sku=args.sku, brand=args.brand)

    # 5. 按 --export 决定是否额外导出 raw 语料
    if args.export in ("raw", "both"):
        export_raw(output, output_dir)

    print(f"\n{'='*60}")
    print(f"[完成] 结果在 {output_dir}/ 目录:")
    print(f"  all_posts.csv  - 全量原始帖子数据")
    print(f"  today.json     - 热帖 + 每篇 Top 评论（供生成讨论速览）")
    if args.export in ("raw", "both"):
        print(f"  corpus.csv/jsonl - flatten 的 post/comment 行（供 AI 标注 + Quote Bank）")
    print("=" * 60)


if __name__ == "__main__":
    main()
