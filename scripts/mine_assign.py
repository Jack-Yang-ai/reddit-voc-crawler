"""
主词挖掘 · 第2步：把全部动机词短语归类到 taxonomy.json 的 可配置主题。
复用 annotate 的 API 接口/key；支持 --base-url 双接口、--shard 分片、断点续跑、增量落盘。
输出 assigned.csv：rid, theme_id, theme_name。
"""
from __future__ import annotations
import argparse, json, os, random, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import pandas as pd, requests
import annotate as A  # 复用 BASE_URL/MODEL/API_KEY/_extract_text/_parse_json_array

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.path.join(HERE, "openclaw_output", "health-wearable")
LABELED = os.path.join(BASE, "labeled_corpus.csv")
TAXO = os.path.join(HERE, "..", "examples", "taxonomy.json")
OUT = os.path.join(BASE, "assigned.csv")
BATCH = 25
MAXTOK = 8000


def build_prompt(themes, batch):
    lines = [f"{i}. {t['name']}：{t['definition'][:40]}" for i, t in enumerate(themes)]
    sys = ("你把穿戴设备用户的「动机词短语」归类到下列主题之一。只看语义，选最贴切的一个主题编号；"
           "若都不沾就给 -1。\n主题表：\n" + "\n".join(lines) +
           "\n\n输出只要 JSON 数组，每元素 {idx, t}（t=主题编号整数）。无解释无围栏。")
    items = [{"idx": b["idx"], "短语": b["mw"][:80]} for b in batch]
    return sys + "\n\n待归类：\n" + json.dumps(items, ensure_ascii=False)


def call(themes, batch, base_url):
    payload = {"model": A.MODEL, "messages": [{"role": "user", "content": build_prompt(themes, batch)}],
               "reasoning_effort": "medium", "max_tokens": MAXTOK}
    rl = 0
    while True:
        try:
            r = requests.post(f"{base_url}/messages",
                              headers={"Authorization": f"Bearer {A.API_KEY}", "Content-Type": "application/json"},
                              json=payload, timeout=180)
            if r.status_code == 429:
                rl += 1
                if rl > 5: raise RuntimeError("429")
                time.sleep(min(10 * 2 ** (rl - 1), 120) + random.uniform(0, 3)); continue
            r.raise_for_status()
            arr = A._parse_json_array(A._extract_text(r.json()))
            return {int(o["idx"]): int(o["t"]) for o in arr if "idx" in o and "t" in o}
        except Exception as e:
            rl += 1
            if rl > 5: raise RuntimeError(str(e))
            time.sleep(2 * rl + random.uniform(0, 2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--labeled", default=LABELED)
    ap.add_argument("--taxonomy", default=TAXO)
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--shard", default=None)
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--done-extra", default=None)
    a = ap.parse_args()
    base_url = a.base_url or A.BASE_URL
    if not A.API_KEY or not base_url or not A.MODEL:
        ap.error("需配置 LLM_API_KEY、LLM_BASE_URL 和 LLM_MODEL")
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    themes = json.load(open(a.taxonomy, encoding="utf-8"))

    d = pd.read_csv(a.labeled)
    if "rid" not in d:
        ap.error("输入必须保留标注阶段的 rid 列")
    d["mw"] = d["动机词_原话"].fillna("").astype(str).str.strip()
    work = d[d["mw"].str.len() > 2][["rid", "mw"]]

    if a.shard:
        mod, idx_s = a.shard.split(":"); mod = int(mod); idxs = {int(x) for x in idx_s.split(",")}
        work = work[(work["rid"] % mod).isin(idxs)]
    done = set()
    for fp in [a.out] + (a.done_extra.split(",") if a.done_extra else []):
        if fp and os.path.exists(fp.strip()):
            done |= set(pd.read_csv(fp.strip())["rid"].tolist())
    work = work[~work["rid"].isin(done)]
    if work.empty:
        print("[完成] 无新行"); return

    rows = [{"idx": int(r.rid), "rid": int(r.rid), "mw": r.mw} for r in work.itertuples()]
    batches = [rows[i:i + BATCH] for i in range(0, len(rows), BATCH)]
    print(f"[计划] {len(rows)} 短语 / {len(batches)} 批 / {a.workers}并发 @ {base_url}")
    tname = {i: t["name"] for i, t in enumerate(themes)}
    tname[-1] = "都不沾"
    lock = Lock(); header = not os.path.exists(a.out); done_n = [0]; fail = [0]

    def proc(batch):
        try:
            res = call(themes, batch, base_url)
        except Exception as e:
            return ("err", [b["rid"] for b in batch])
        return ("ok", [{"rid": b["rid"], "theme_id": res.get(b["idx"], -1),
                        "theme_name": tname.get(res.get(b["idx"], -1), "都不沾")} for b in batch])

    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        for fut in as_completed([ex.submit(proc, b) for b in batches]):
            st, pl = fut.result()
            if st == "err":
                fail[0] += len(pl); continue
            with lock:
                pd.DataFrame(pl).to_csv(a.out, mode="a", index=False, header=header, encoding="utf-8-sig")
                header = False; done_n[0] += len(pl)
                if done_n[0] % 400 < BATCH: print(f"  {done_n[0]}/{len(rows)} | 失败{fail[0]}")
    print(f"[完成] 归类 {done_n[0]}，失败 {fail[0]} → {a.out}")


if __name__ == "__main__":
    main()
