"""
Reddit 数据爬取脚本（步骤 1：爬取）
使用 .json 公开接口，无需 API Key

支持通过 run() 函数传入主题配置，供 pipeline 调用。
"""

from __future__ import annotations

from settings import load_local_env
load_local_env()

import requests
import time
import os
import html
import random
import pandas as pd
from datetime import datetime

# ============ 配置区 ============

TARGET_COUNT = 10000  # 目标数量
# 请求间隔（秒），避免被限速。可用环境变量 REDDIT_SLEEP 覆盖（大批量研究建议设 7~8）。
SLEEP_INTERVAL = float(os.environ.get("REDDIT_SLEEP", "3"))
# 429 退避：连续限速时指数退避（базовый秒 → ×2 递增，封顶），连续 N 次仍 429 则放弃该请求。
RATELIMIT_BASE_WAIT = float(os.environ.get("REDDIT_RL_WAIT", "60"))
RATELIMIT_MAX_WAIT = 600
RATELIMIT_MAX_RETRIES = int(os.environ.get("REDDIT_RL_RETRIES", "4"))


def _throttle(base: float | None = None) -> None:
    """请求间隔 + 随机抖动（去机械化，降低被风控标记的概率）。"""
    b = SLEEP_INTERVAL if base is None else base
    time.sleep(b + random.uniform(0, max(1.0, b * 0.4)))

# 可选 Cookie 只从本机环境读取，禁止写入源码。
BROWSER_COOKIE = os.environ.get("REDDIT_COOKIE", "")

# 通用扩展搜索词（可按主题在 pipeline 配置中覆盖）
DEFAULT_EXTRA_KEYWORDS = [
    "battery", "health", "band", "upgrade", "review", "worth buy",
    "app", "sleep", "charging", "notification", "repair screen",
    "workout fitness", "water swim", "scratch crack", "case protector",
    "GPS", "issue problem", "recommend suggestion",
]

# 更完整的 headers，模拟真实浏览器
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}


# ============ 核心逻辑 ============

def _build_session() -> requests.Session:
    """创建带 Cookie 和 Headers 的 session"""
    session = requests.Session()
    h = dict(DEFAULT_HEADERS)
    if BROWSER_COOKIE:
        h["Cookie"] = BROWSER_COOKIE
    session.headers.update(h)
    return session


def _build_search_configs(keyword: str, subreddit: str,
                          extra_subreddits: list[str],
                          extra_keywords: list[str]) -> list[dict]:
    """根据主题参数自动生成搜索配置"""
    keyword_no_space = keyword.replace(" ", "")
    base_url = f"https://old.reddit.com/r/{subreddit}"
    search_url = f"{base_url}/search.json"
    search_base = {"restrict_sr": "on"}

    configs = []

    # 1. 主 subreddit 直接浏览
    for feed in ["hot", "best", "new"]:
        configs.append({"url": f"{base_url}/{feed}.json", "q": None, "sort": None, "t": None})
    for t in ["all", "year", "month"]:
        configs.append({"url": f"{base_url}/top.json", "q": None, "sort": None, "t": t})

    # 2. 主 subreddit 内搜索 - 核心关键词
    for q in [keyword, keyword_no_space]:
        for sort in ["new", "top", "relevance"]:
            configs.append({"url": search_url, "q": q, "sort": sort, "t": "all", **search_base})

    # 3. 主 subreddit 内搜索 - 扩展关键词
    for q in extra_keywords:
        for sort in ["new", "top"]:
            configs.append({"url": search_url, "q": q, "sort": sort, "t": "all", **search_base})

    # 4. 额外 subreddit 搜索核心关键词
    for sub in extra_subreddits:
        extra_search_url = f"https://old.reddit.com/r/{sub}/search.json"
        for sort in ["new", "top", "relevance"]:
            configs.append({"url": extra_search_url, "q": keyword, "sort": sort, "t": "all", "restrict_sr": "on"})

    return configs


def _fetch_posts(session: requests.Session, url: str, params: dict, max_pages: int = 100) -> list[dict]:
    """从一个搜索配置中获取帖子，翻页直到没有更多结果"""
    results = []
    after = None
    rl_hits = 0  # 连续 429 计数

    for page in range(max_pages):
        req_params = {k: v for k, v in params.items() if v is not None}
        req_params["limit"] = 100
        if after:
            req_params["after"] = after

        try:
            resp = session.get(url, params=req_params, timeout=15)
            if resp.status_code == 429:
                rl_hits += 1
                if rl_hits > RATELIMIT_MAX_RETRIES:
                    print(f"  [限速] 连续 {rl_hits} 次 429，放弃该请求（保护账号，不硬刚）")
                    break
                wait = min(RATELIMIT_BASE_WAIT * (2 ** (rl_hits - 1)), RATELIMIT_MAX_WAIT)
                print(f"  [限速] 第 {rl_hits} 次 429，退避 {wait:.0f}s...")
                time.sleep(wait)
                continue
            rl_hits = 0
            if resp.status_code != 200:
                print(f"  [错误] HTTP {resp.status_code}")
                break

            data = resp.json().get("data", {})
            children = data.get("children", [])

            if not children:
                break

            for post in children:
                p = post.get("data", {})
                results.append({
                    "id": p.get("id"),
                    "title": p.get("title", ""),
                    "selftext": p.get("selftext", ""),
                    "subreddit": p.get("subreddit", ""),
                    "author": p.get("author", ""),
                    "score": p.get("score", 0),
                    "upvote_ratio": p.get("upvote_ratio", 0),
                    "num_comments": p.get("num_comments", 0),
                    "created_utc": p.get("created_utc", 0),
                    "url": p.get("url", ""),
                    "permalink": f"https://old.reddit.com{p.get('permalink', '')}",
                    "link_flair_text": p.get("link_flair_text", ""),
                    "is_self": p.get("is_self", False),
                })

            after = data.get("after")
            if not after:
                break

            _throttle()

        except Exception as e:
            print(f"  [错误] {e}")
            break

    return results


def _fetch_top_comments(session: requests.Session, permalink: str,
                        limit: int = 15, sort: str = "top") -> list[dict]:
    """
    抓取某帖子的 Top 评论（顶层评论为主），用于总结"评论区在讨论什么"。
    permalink 形如 https://old.reddit.com/r/<sub>/comments/<id>/<slug>/
    返回 [{score, author, body}]，body 已做 HTML 反转义、去换行、截断到 600 字。
    """
    try:
        post_id = permalink.rstrip("/").split("/comments/")[1].split("/")[0]
        sub = permalink.split("/r/")[1].split("/")[0]
    except (IndexError, AttributeError):
        return []

    url = f"https://old.reddit.com/r/{sub}/comments/{post_id}.json"
    try:
        resp = session.get(url, params={"sort": sort, "limit": limit + 5}, timeout=15)
        rl_hits = 0
        while resp.status_code == 429 and rl_hits < RATELIMIT_MAX_RETRIES:
            rl_hits += 1
            wait = min(RATELIMIT_BASE_WAIT * (2 ** (rl_hits - 1)), RATELIMIT_MAX_WAIT)
            print(f"  [评论限速] 第 {rl_hits} 次 429，退避 {wait:.0f}s @ {post_id}")
            time.sleep(wait)
            resp = session.get(url, params={"sort": sort, "limit": limit + 5}, timeout=15)
        if resp.status_code != 200:
            print(f"  [评论] HTTP {resp.status_code} @ {post_id}")
            return []
        nodes = resp.json()[1]["data"]["children"]
    except Exception as e:
        print(f"  [评论错误] {e}")
        return []

    # 跳过机器人/自动评论：AutoModerator 等会在每帖置顶一条"请看 FAQ/反馈板"，是噪音不是用户观点
    BOT_AUTHORS = {"automoderator"}

    out = []
    for c in nodes:
        if c.get("kind") == "more":
            continue
        cd = c.get("data", {})
        if (cd.get("author") or "").lower() in BOT_AUTHORS:
            continue
        body = html.unescape((cd.get("body") or "").replace("\n", " ")).strip()
        if not body:
            continue
        out.append({
            "score": cd.get("score", 0),
            "author": cd.get("author", ""),
            "body": body[:600],
        })
        if len(out) >= limit:
            break
    return out


def run(keyword: str, subreddit: str, output_csv: str,
        extra_subreddits: list[str] = None,
        extra_keywords: list[str] = None,
        target_count: int = TARGET_COUNT):
    """
    执行爬取流程。供 pipeline 调用。

    参数:
        keyword: 核心搜索关键词（如 "Apple Watch", "bevelhealth"）
        subreddit: 主 subreddit 名称（不带 r/）
        output_csv: 输出 CSV 文件路径
        extra_subreddits: 额外搜索的 subreddit 列表
        extra_keywords: 扩展搜索词列表（None 则使用默认）
        target_count: 目标帖子数
    """
    if extra_subreddits is None:
        extra_subreddits = []
    if extra_keywords is None:
        extra_keywords = DEFAULT_EXTRA_KEYWORDS

    # 1. 建立 session
    session = _build_session()
    print("[爬取] 初始化连接...")
    try:
        session.get("https://old.reddit.com/", timeout=15)
        time.sleep(2)
        print("[爬取] 连接成功")
    except Exception as e:
        print(f"[爬取] 警告: 首页访问失败 ({e})，继续尝试...")

    # 2. 生成搜索配置
    search_configs = _build_search_configs(keyword, subreddit, extra_subreddits, extra_keywords)
    print(f"[爬取] 共 {len(search_configs)} 个搜索策略, 目标 {target_count} 条")

    # 3. 执行爬取
    all_posts = []
    seen_ids = set()

    for i, config in enumerate(search_configs):
        if len(seen_ids) >= target_count:
            break

        url = config["url"]
        params = {
            "q": config.get("q"),
            "sort": config.get("sort"),
            "t": config.get("t"),
            "restrict_sr": config.get("restrict_sr"),
        }

        label = config.get("q") or url.split("/")[-1].replace(".json", "")
        print(f"\n[{i + 1}/{len(search_configs)}] 搜索: {label}")

        results = _fetch_posts(session, url, params)

        new_count = 0
        for post in results:
            if post["id"] not in seen_ids:
                seen_ids.add(post["id"])
                all_posts.append(post)
                new_count += 1

        print(f"  获取 {len(results)} 条，新增 {new_count} 条，累计 {len(all_posts)} 条")

    # 4. 保存结果
    df = pd.DataFrame(all_posts)
    df["created_time"] = pd.to_datetime(df["created_utc"], unit="s")
    df = df.sort_values("created_time", ascending=False).reset_index(drop=True)

    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"\n[爬取] 完成! 共 {len(df)} 条帖子 → {output_csv}")
    if len(df) > 0:
        print(f"[爬取] Subreddit 分布:\n{df['subreddit'].value_counts().head(10).to_string()}")
        print(f"[爬取] 时间范围: {df['created_time'].min()} ~ {df['created_time'].max()}")


def main():
    # 独立运行时的默认配置（向下兼容）
    keyword = "bevelhealth"
    subreddit = "bevelhealth"
    run(
        keyword=keyword,
        subreddit=subreddit,
        output_csv=f"{keyword.lower().replace(' ', '_')}_reddit.csv",
        extra_subreddits=[subreddit],
    )


if __name__ == "__main__":
    main()
