# Reddit VOC Crawler

按社区或关键词收集 Reddit 帖子与 Top 评论，导出研究语料，可选接入模型做结构化打标、主题归类和品牌归属过滤。

这是从实际研究项目中整理出的公开代码版。仓库不包含原项目的 Cookie、API Key、真实语料、内部文档、研究结论、日志或历史报告。`examples/taxonomy.json` 是通用示例分类法，不代表研究结果。

## 能力

- 通过 `requests` 读取 `old.reddit.com` JSON listing、搜索和评论接口。
- Feed + 关键词多路召回、帖子 ID 去重、分页、请求间隔与 429 退避。
- 日报默认最近 48 小时 Top10；研究模式支持时间窗和 50% 高赞 / 30% 高评论 / 20% 最新的混合采样。
- 导出 `today.json`、`all_posts.csv`、`corpus.csv` 和 `corpus.jsonl`。
- 可选 AI 打标：人群、设备、动机词原话、情绪、场景、健康用例、未满足需求、噪声、关键引语。
- 打标支持并发、分片、增量追加和断点续跑；品牌过滤区分本牌、跨牌、仅他牌和无法判定。
- 本地生成自包含 HTML 统计报告，不依赖 CDN。

## 安装

Python 3.10+（CI 使用 3.10 和 3.12）。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

编辑根目录 `.env`。环境变量优先于 `.env`；仅支持普通 `KEY=value`，不执行 shell 表达式。Cookie 可选，仓库不提供任何凭据。

## 抓取

在仓库根目录运行：

```bash
# 日报：抓某个社区的近期帖子和评论
python scripts/openclaw_reddit.py --topic whoop --subreddits whoop --export both

# 研究：精确关键词 + 近一年 + 混合采样
python scripts/openclaw_reddit.py --topic ring-study \
  --subreddit-keywords 'SmartRings:RingConn' \
  --time-window year --recent-days 365 --sampling blend \
  --post-limit 100 --comments-per-post 20 --export both --brand RingConn
```

输出默认在当前工作目录的 `openclaw_output/<topic>/`，可通过 `REDDIT_OUTPUT_DIR` 修改。

宽窗口 Feed 主要取 top；关键词搜索会补 new/comments。Reddit listing 有回溯限制，因此这是抽样研究工具，不能保证历史全量召回。若时间窗内没有帖子，现有实现会回退到候选池并记录提示，检查输出日期后再解读结果。

## 可选 AI 打标与主题归类

配置 `LLM_BASE_URL`、`LLM_MODEL`、`LLM_API_KEY` 后运行：

```bash
python scripts/annotate.py --corpus openclaw_output/whoop/corpus.csv \
  --out openclaw_output/whoop/labeled.csv --workers 4
python scripts/mine_assign.py --labeled openclaw_output/whoop/labeled.csv \
  --taxonomy examples/taxonomy.json --out openclaw_output/whoop/themes.csv
python scripts/annotate_brand_scope.py ../openclaw_output/whoop/labeled.csv --dry
python scripts/annotate_brand_scope.py ../openclaw_output/whoop/labeled.csv
python scripts/build_report.py --input openclaw_output/whoop/labeled.csv \
  --output openclaw_output/whoop/report.html
```

模型适配沿用原项目的 messages 协议：`POST <LLM_BASE_URL>/messages`，Bearer 认证，响应 `content: [{type: "text", text: "..."}]`；请求含 `reasoning_effort`。不是所有模型服务都兼容，需自行选择兼容端点或修改适配函数。仓库验证不调用付费模型。

品牌写回脚本的位置参数相对于 `scripts/` 解析，也可直接传绝对路径。`--dry` 只统计；去掉它会原地追加/更新两列。`无法判定` 默认保留，严格统计可仅保留 `纯本牌`。规则有误判可能，建议抽查。

`rid` 是输入语料行号，续跑期间不能重排或修改既有语料前缀。主题归类沿用标注的 `rid`，不重新编号。不同输入请使用不同输出文件，避免续跑混淆。模型生成结果需要抽查，不能直接当成已验证事实。

## Excel 批量研究

```bash
python scripts/research_batch.py --excel /path/to/queries.xlsx --list
python scripts/research_batch.py --excel /path/to/queries.xlsx --group all
# 无需 Excel 的未满足需求探针
python scripts/research_batch.py --probe-only
```

Excel 工作表名称为 `SKU与Query清单`，前 4 行为标题/说明/表头。从第 5 行开始读取：第 1 列品牌、第 2 列分组、第 3 列型号、第 4 列品类、第 9 列 Reddit 查询，例如 `r/ouraring + "Oura" / r/SmartRings "Oura"`。smoke/p0 是可编辑的示例品牌组；通用表建议 `--group all`。

批量输出在 `openclaw_output/health-wearable/`，合并会保留既有语料前缀，再按 `(sku, row_type, permalink, text)` 追加去重。已有标注不能随意重新构建输入库。

## 验证与数据边界

```bash
python -m unittest discover -s tests -v
python scripts/check_public.py
```

测试用人工构造数据和模拟 HTTP，覆盖分页、采样、导出、品牌过滤、增量合并和 HTML 转义。CI 不抓取 Reddit，也不访问模型。公开 JSON 接口可能返回 403/429 或改变行为，离线测试通过不代表实时服务可访问。

默认抓取结果包含作者名、链接和用户正文，仅写到本机且被 Git 忽略。AI 打标会把正文及上下文发送到配置的模型服务。公开报告或数据前请自行审核、脱敏，并遵守 Reddit 的适用条款和数据访问规则；不要提交 Cookie、凭据或原始健康讨论数据。不要通过强制添加绕过 `.gitignore`。

## 目录

```text
scripts/reddit.py                 HTTP 会话、分页、评论与退避
scripts/openclaw_reddit.py        单主题抓取、采样、语料导出
scripts/research_batch.py         Excel 编排、需求探针、追加合并
scripts/annotate.py               可配置模型打标
scripts/mine_assign.py            动机词主题归类
scripts/brand_scope.py            品牌归属规则
scripts/annotate_brand_scope.py   品牌归属写回 CSV
scripts/build_report.py           通用 HTML 统计报告
scripts/check_public.py           发布文件检查（不替代人工审核）
```
