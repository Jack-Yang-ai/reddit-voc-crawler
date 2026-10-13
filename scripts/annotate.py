"""
语料标注脚本—— 通过可配置的 messages API 给 corpus_all.csv 逐行打标。

设计要点：
- 心智词「两层」：动机词_原话（开放抽取，逐字保留用户原声）+ 命中假设轴（宽松软映射，仅验证 H1-H3）。
  词从用户嘴里来，桶在标完后由人/聚类再画 —— 不在打标阶段硬塞闭枚举。
- 过滤琐碎：丢 [deleted]/[removed] + 赞<2 且长度<25 的评论。
- 评论带「所属 post 标题」作上下文，否则无法判。
- 并发 worker（ThreadPoolExecutor）集中控并发 + 抖动 + 429 退避，保护 key。
- 增量落盘 + 断点续跑：按原始行号 rid 去重，中断可重跑补齐。

用法：
    # 10% 分层抽样试点（每个 SKU 抽 10%）
    python3 annotate.py --frac 0.1 --workers 6
    # 全量
    python3 annotate.py --workers 8
    # 续跑（自动跳过已标注的 rid）
    python3 annotate.py --workers 8   # 再跑一次即可
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pandas as pd
import requests

# ============ 配置 ============

HERE = os.path.dirname(os.path.abspath(__file__))
CORPUS = os.path.join(HERE, "openclaw_output", "health-wearable", "corpus_all.csv")
OUT = os.path.join(HERE, "openclaw_output", "health-wearable", "labeled_corpus.csv")

LABEL_FIELDS = ["人群", "设备", "动机词_原话", "情绪", "场景", "健康用例",
                "unmet_need", "is_noise", "key_quote"]

BATCH_SIZE = 15         # 每次 API 调用塞几行（实测甜点：20行思考超线性变慢，15最优）
MAX_TOKENS = 16000      # 够 medium思考(~7-8k)+15行JSON 输出
MAX_RETRIES = 5
TIMEOUT = 180


from settings import load_local_env
load_local_env()
BASE_URL = os.environ.get("LLM_BASE_URL", "").rstrip("/")
MODEL = os.environ.get("LLM_MODEL", "")
API_KEY = os.environ.get("LLM_API_KEY", "")

# ============ 提示词 ============

SYSTEM = """你是运动健康穿戴设备的用户研究标注员。给定若干条 Reddit 原始语料（帖子或评论），\
逐条打标。这是对公开社区反馈做的通用 VOC 分析。

对每条语料输出以下字段（严格按枚举，不要自创枚举值）：

- 人群: 该用户属于哪类运动人群，单选 [HYROX, CrossFit, 跑步铁三, 力量增肌, 户外, 综合健康, 不明]。判定顺序严格执行：\
(1) 文中有【可识别的具体运动项目/赛事/专项指标】（如 HYROX、CrossFit/WOD、marathon/跑步/配速/Zone 2、lifting/举铁/增肌、登山/越野/滑雪/骑行等）→ 归入对应专项人群；\
(2) 其余一切情况一律填"不明"——包括完全没有运动信号，以及泛泛提到 workout/gym/训练但认不出具体专项的（认不出专项就是不明，不要硬归）。**不要**因为提到睡眠/HRV/恢复/压力/减重/设备体验/健康监测就归类；\
(3) "综合健康"仅在用户【明确表示自己不做运动 / 只为日常健康监测而非训练】时才用（如 "I don't really work out, just track sleep/stress"）。除此之外禁止用"综合健康"兜底。\
判断不出就填"不明"，不要硬猜。
- 设备: 文中提及的设备品牌，可多选，数组，取值 [Whoop, Oura, Garmin, AppleWatch, 华为, 小米, Amazfit, Polar, RingConn, Ultrahuman, Bevel, Fitbit, Coros, Suunto, Samsung, 其他]；没提到设备就空数组 []。\
注意：Bevel 是「软件+AI 健康教练」App（自带 Apple Watch/Garmin/Oura 等硬件喂数据），不是硬件，但仍作为品牌计入；提到 Bevel 就加 "Bevel"，同时把它接的硬件也一并标上。\
Samsung 含 Galaxy Watch / Galaxy Ring / Galaxy Fit；Fitbit 含 Charge/Versa/Sense/Inspire/Air；枚举外的其它品牌（Honor/Withings/Wahoo/Mobvoi 等）才归 "其他"，不要自创新值；大小写严格按上面写法（Whoop 不是 whoop、华为 不是 Huawei）。
- 动机词_原话: 【最重要】用户表达的核心"动机/状态/价值/痛点"，提炼成一个**简短短语**（≤6个词、≤40字符），\
词来自原文（英文保留英文），但要精炼成"可聚类的标签级短语"，不要整句照抄。\
例如原句 "I just wish it could push me to go harder" → 动机词填 "push me harder"；\
"the lack of NFC payments really bugs me" → 填 "lack of NFC"；"stupidly expensive and losing their way" → 填 "too expensive"。\
没有明显动机/价值/痛点就填 ""。不要替用户概括成抽象词、不要套用品牌 marketing 词。\
【语言必须与原文一致】：原文是英文就用英文短语，原文是中文就用中文短语——绝不要把英文原话概括或翻译成中文。
- 情绪: 单选 [正向, 负向, 中性]。
- 场景: 该语料发生/讨论的【时间或活动情境】，单选 [训练前, 训练中, 训练后, 睡眠, 小睡, 工作日常, 比赛, 旅行, 其他, 无]。这是"什么时候/什么活动"，不要和健康用例混。
- 健康用例: 该语料涉及哪些【健康主题/用例】，可多选，数组，取值 \
[睡眠, 压力, 恢复, 体能训练, 经期, 孕期, 生病预警, 心血管, 代谢血糖, 冥想正念, 饮食咖啡因酒精, 日常活动久坐, 无]；\
没明确涉及就 ["无"]。与"场景"正交——场景=什么时间/活动，健康用例=什么健康主题。\
对照（四大穿戴在打的）：Whoop/Oura/RingConn/Ultrahuman 都重压力(daytime stress)与恢复；Whoop/Oura 打经期/小睡；Oura 打孕期/生病预警(Symptom Radar)/冥想；Ultrahuman 打代谢血糖/饮食咖啡因；华为 Watch D2 打心血管(血压)。
- unmet_need: 是否表达了未被满足的需求，单选 [求购, 想转投, 功能缺失, holy_grail, 无]。\
求购=在找某种还不存在/不知道哪有的产品；想转投=要从某设备换到另一个；功能缺失=抱怨缺某功能；holy_grail=明确说"理想中但市面没有的那款"；都不是填"无"。
- is_noise: 是否测评号通稿/纯广告/纯参数罗列（无人称、无具体场景、无真实体验），布尔 true/false。
- key_quote: 从原文摘一句最有代表性的【完整句子】（英文原文，≤200字符），用于 Quote Bank。\
应与"动机词_原话"不同——动机词是从中提炼的短语，key_quote 是完整原句。是通稿或无价值就填 ""。

输出规则：只输出一个 JSON 数组，每个元素对应输入的一条（按 idx 顺序），元素含且仅含字段：\
idx, 人群, 设备, 动机词_原话, 情绪, 场景, 健康用例, unmet_need, is_noise, key_quote。\
不要输出任何解释、前后缀或 markdown 代码围栏。"""


def build_user_msg(batch: list[dict]) -> str:
    items = []
    for b in batch:
        ctx = "（这条本身是帖子标题+正文）" if b["row_type"] == "post" else f"（这是评论，所属帖标题：{b['ctx']}）"
        items.append({
            "idx": b["idx"],
            "类型": b["row_type"],
            "搜索SKU": b["sku"],
            "上下文": ctx,
            "正文": b["text"][:600],
        })
    return "请给下面 " + str(len(items)) + " 条语料打标，按 idx 顺序返回 JSON 数组：\n" + \
           json.dumps(items, ensure_ascii=False)


# ============ API 调用 ============

def _extract_text(resp_json: dict) -> str:
    """从 StepFun messages 返回里取 type=text 的块。"""
    for blk in resp_json.get("content", []):
        if blk.get("type") == "text":
            return blk.get("text", "")
    return ""


def _parse_json_array(text: str) -> list:
    text = text.strip()
    text = re.sub(r"^```(json)?", "", text).strip()
    text = re.sub(r"```$", "", text).strip()
    # 容错：截取第一个 [ 到最后一个 ]
    i, j = text.find("["), text.rfind("]")
    if i != -1 and j != -1:
        text = text[i:j + 1]
    return json.loads(text)


def call_api(batch: list[dict], session: requests.Session, base_url: str = None) -> dict:
    """返回 {idx: label_dict}。失败抛异常。base_url 可覆盖（双接口并行用）。"""
    base_url = base_url or BASE_URL
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "user", "content": SYSTEM + "\n\n" + build_user_msg(batch)},
        ],
        "reasoning_effort": "medium",
        "max_tokens": MAX_TOKENS,
    }
    rl = 0
    while True:
        try:
            r = session.post(f"{base_url}/messages",
                             headers={"Authorization": f"Bearer {API_KEY}",
                                      "Content-Type": "application/json"},
                             json=payload, timeout=TIMEOUT)
            if r.status_code == 429:
                rl += 1
                if rl > MAX_RETRIES:
                    raise RuntimeError("429 超过重试上限")
                wait = min(10 * (2 ** (rl - 1)), 120)
                time.sleep(wait + random.uniform(0, 3))
                continue
            r.raise_for_status()
            arr = _parse_json_array(_extract_text(r.json()))
            return {int(o["idx"]): o for o in arr if "idx" in o}
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            rl += 1
            if rl > MAX_RETRIES:
                raise RuntimeError(f"调用失败 {type(e).__name__}: {e}")
            time.sleep(2 * rl + random.uniform(0, 2))


# ============ 数据 ============

def filter_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["text"] = df["text"].fillna("").astype(str)
    n0 = len(df)
    # 丢已删除
    df = df[~df["text"].str.strip().str.lower().isin(["[deleted]", "[removed]", ""])]
    # 丢琐碎评论：评论 且 赞<2 且 长度<25
    is_cmt = df["row_type"] == "comment"
    sc = pd.to_numeric(df["score"], errors="coerce").fillna(0)
    trivial = is_cmt & (sc < 2) & (df["text"].str.len() < 25)
    df = df[~trivial]
    print(f"[过滤] {n0} → {len(df)} 行（丢删除帖 + 琐碎评论 {n0 - len(df)} 条）")
    return df.reset_index(drop=True)


def build_context_map(full: pd.DataFrame) -> dict:
    """post_id -> 帖标题，给评论补上下文。"""
    posts = full[full["row_type"] == "post"]
    return dict(zip(posts["post_id"].astype(str), posts["title"].astype(str)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=CORPUS, help="输入语料 CSV")
    ap.add_argument("--frac", type=float, default=None, help="分层抽样比例（如 0.1 试点）")
    ap.add_argument("--limit", type=int, default=None, help="只标前 N 行（调试）")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--base-url", default=None, help="覆盖接口地址（双接口并行用，如 https://api.stepfun.com/v1）")
    ap.add_argument("--shard", default=None, help="分片 'MOD:IDX'，只处理 rid%%MOD==IDX 的行（双任务分工）")
    ap.add_argument("--min-rid", type=int, default=None, help="只标 rid>=N 的行（增量追加：跳过旧语料，rid 是 corpus_all 行号）")
    ap.add_argument("--done-extra", default=None, help="逗号分隔的额外已完成文件，其 rid 视为已标注跳过")
    args = ap.parse_args()
    base_url = args.base_url or BASE_URL

    if not API_KEY or not base_url or not MODEL:
        ap.error("需配置 LLM_API_KEY、LLM_BASE_URL 和 LLM_MODEL")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    full = pd.read_csv(args.corpus)
    full["rid"] = range(len(full))  # 原始行号，作去重主键
    ctx_map = build_context_map(full)

    work = filter_rows(full)
    if args.min_rid is not None:
        work = work[work["rid"] >= args.min_rid]
        print(f"[增量] 只标 rid>={args.min_rid} → {len(work)} 行")
    if args.frac:
        work = work.groupby("sku", group_keys=False).apply(
            lambda g: g.sample(frac=args.frac, random_state=42)).reset_index(drop=True)
        print(f"[抽样] frac={args.frac} → {len(work)} 行（分层 by sku）")
    if args.limit:
        work = work.head(args.limit)

    # 分片：双任务分工，'MOD:IDX' 或 'MOD:IDX1,IDX2,...'（按接口速度不均分配）
    if args.shard:
        mod_s, idx_s = args.shard.split(":")
        mod = int(mod_s)
        idxs = {int(x) for x in idx_s.split(",")}
        work = work[(work["rid"] % mod).isin(idxs)]
        print(f"[分片] {args.shard} → 本片 {len(work)} 行")

    # 断点续跑：跳过已标注 rid（本片 out + 额外 done 文件如主表）
    done_rids = set()
    done_files = [args.out] + (args.done_extra.split(",") if args.done_extra else [])
    for fp in done_files:
        fp = fp.strip()
        if fp and os.path.exists(fp):
            done_rids |= set(pd.read_csv(fp)["rid"].tolist())
    if done_rids:
        print(f"[续跑] 已标注 {len(done_rids)} 行（含额外文件），跳过")
    work = work[~work["rid"].isin(done_rids)]
    if work.empty:
        print("[完成] 无新行可标")
        return

    # 组装任务
    rows = []
    for _, r in work.iterrows():
        rows.append({
            "rid": int(r["rid"]), "idx": int(r["rid"]),
            "row_type": r["row_type"], "sku": r["sku"],
            "text": str(r["text"]),
            "ctx": ctx_map.get(str(r["parent_post_id"]), "")[:120],
        })
    batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
    print(f"[计划] {len(rows)} 行 / {len(batches)} 批 / {args.workers} 并发 worker")

    write_lock = Lock()
    write_header = not os.path.exists(args.out)
    # 追加到已有文件时记录其表头列序，写前按它对齐，避免列序不一致导致错位
    existing_cols = None if write_header else list(pd.read_csv(args.out, nrows=0).columns)
    rid_to_row = {int(r["rid"]): r for _, r in work.iterrows()}
    done = [0]
    fail = [0]

    def process(batch):
        session = requests.Session()
        try:
            labels = call_api(batch, session, base_url)
        except Exception as e:
            return ("err", str(e), [b["rid"] for b in batch])
        out_rows = []
        for b in batch:
            lab = labels.get(b["rid"], {})
            base = rid_to_row[b["rid"]].to_dict()
            for f in LABEL_FIELDS:
                v = lab.get(f, "")
                base[f] = json.dumps(v, ensure_ascii=False) if isinstance(v, list) else v
            out_rows.append(base)
        return ("ok", out_rows, None)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(process, b) for b in batches]
        for fut in as_completed(futs):
            status, payload, rids = fut.result()
            if status == "err":
                fail[0] += len(rids)
                print(f"  [批失败] {payload[:80]} | rids={rids[:3]}...")
                continue
            with write_lock:
                df = pd.DataFrame(payload)
                if existing_cols is not None:
                    extra = [c for c in df.columns if c not in existing_cols]
                    if extra:
                        raise SystemExit(f"[列不一致] 新列 {extra} 不在既有表头，已停止以防错位；"
                                         f"请对齐 schema 或换一个新的 --out 文件")
                    df = df.reindex(columns=existing_cols).fillna("")
                df.to_csv(args.out, mode="a", index=False,
                          header=write_header, encoding="utf-8-sig")
                write_header = False
                done[0] += len(payload)
                if done[0] % 60 == 0 or done[0] < 60:
                    print(f"  进度 {done[0]}/{len(rows)} | 失败 {fail[0]}")

    print(f"\n[完成] 标注 {done[0]} 行，失败 {fail[0]} 行 → {args.out}")


if __name__ == "__main__":
    main()
