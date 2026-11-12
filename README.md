# Delta Lake 版本可恢复性分析网页

对象存储误删文件后，湖仓值班员用本工具查清一张 Delta Lake 表还有哪些
版本可以完整回退。值班员在网页上填写版本区间，上传三类材料：

1. `_delta_log` 中留存的 **JSON 提交**（`NNN.json`，20 位补零版本号）；
2. 留存的**经典单文件 Parquet 检查点**（`NNN.checkpoint.parquet`，用 PyArrow 读取）；
3. **现存数据文件的表内相对路径清单**（每行一个路径，如 `data/part-00000-….parquet`）。

工具逐版从 **版本 0 的提交**或**不晚于该版的最新检查点**出发，连续重放
`add` / `remove` 动作到目标版本，再把重建出的快照引用文件与现存清单核对：

- 🟢 **可重建 · 文件齐全**：重放连续无断档，且快照引用的数据文件全部现存；
- 🔴 **缺失提交，重放中断**：明确给出缺失的提交号（版本号列表）；
- 🟠 **重放完整但缺数据文件**：明确给出每个缺失的数据文件表内相对路径。

**仅处理无删除向量（Deletion Vector）的表**：协议特性或任何 add/remove
动作中出现 `deletionVector` 时直接拒绝分析，并指出来源文件。

## 大日志与 256 MiB 内存约束

容器通过 Docker Compose 硬限 **256 MiB 内存**（`mem_limit` /
`memswap_limit`，含 swap 也不放宽）。备份找回的旧提交可能是上百 MiB 的
逐行 JSON，体积主要来自大量 add 动作内嵌的列统计（`stats` /
`statsParser`）。为在限额内完成核查：

- **接收不落内存**：上传体按 1 MiB 分块假脱机（spool）到临时文件，
  网页进程从不把整个请求读进内存；
- **逐行流式解析**：ndjson 提交按行读取，单行解析完即释放，列统计
  只在单行短暂存活；整体 JSON 数组提交仍按原方式支持（该格式体量小）；
- **重放不保留列统计**：`ParsedCommit` 只存归一化后的文件路径，
  `stats` / `statsParser` 等元数据在解析时即丢弃；
- **检查点按需投影**：Parquet 检查点只读取 `add` / `remove` /
  `protocol` 三列，且按文件实际 schema 选择，缺列也能读；
- **坏行 / 删除向量在末尾也拦截**：即使问题出现在提交最后一行，也会
  点名来源文件并返回 400，不渲染任何版本核查结果。

`scripts/generate_large_log.py` 生成可直接在网页上传的压力材料
（默认 `samples_large/orders_table/`）：约 192 MiB 的 v0 逐行提交
（数千个带大列统计的 add）+ 一个把快照收敛到 5 个文件的 v1 提交 +
现存文件清单。上传两份提交与清单、版本区间填 **1–1**，应在 256 MiB
限额内得到“可重建 · 文件齐全”。

```bash
python -I scripts/generate_large_log.py                 # 默认 192 MiB
python -I scripts/generate_large_log.py --target-mib 64 # 自定义体积
```

## 目录结构

```
app/
  analyzer.py            # 核心逻辑：流式日志解析、逐版重放、文件核对、DV 拦截
  web.py                 # Flask 路由：上传假脱机 + 分析、本地样例 API
  templates/index.html   # 单页界面（服务端渲染与 JS 渲染共用）
scripts/generate_samples.py      # 用 PyArrow 构造两套样例 Delta 表
scripts/generate_large_log.py    # 生成约 192 MiB 逐行提交压力材料
samples/                 # 本地样例（健康表 / 误删事故表）
tests/                   # pytest：核心逻辑与路由测试（含大日志内存用例）
Dockerfile / docker-compose.yml
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

容器内存硬限 256 MiB（`docker-compose.yml` 的 `mem_limit` /
`memswap_limit`），Gunicorn 采用单 worker + 4 线程，配合流式解析与
上传假脱机，在该限额内即可处理约 192 MiB 的逐行提交。

页面下半部有两个按钮可**直接加载本地样例**，无需手动选择文件：

- **健康表样例（v0–v4）**：含 v2 检查点，5 个版本全部可重建；
- **误删事故样例（v0–v5）**：v3 提交丢失、数据文件 `part-00004` /
  `part-00006` 被删，但 v2/v4 检查点留存。预期结论：

  | 版本 | 结论 | 说明 |
  |---|---|---|
  | v0–v2 | 🟢 可重建 | v2 起以 `checkpoint-2` 为基线 |
  | v3 | 🔴 缺提交 | 缺提交号 v3 |
  | v4 | 🟠 缺数据文件 | 缺 `data/part-00004-incident.parquet`（v4 检查点仍引用它） |
  | v5 | 🟠 缺数据文件 | 缺 `part-00004` 与 `part-00006` |

  目录中还放了一个分片检查点残片，工具会明确列为“已忽略：仅支持经典
  单文件检查点”。

## 手工上传分析

1. 填写“起始版本 / 结束版本”；
2. 多选上传 `_delta_log` 里留存的 `.json` 与 `.checkpoint.parquet`
   （文件名必须保持 20 位补零版本号，上传时带不带目录前缀都可以）；
3. 上传现存数据文件清单（支持 `#` 注释、空行、`./` 前缀、URI 编码路径，
   两侧路径会做同一归一化）；不上传清单则所有引用文件都判为缺失；
4. 提交后得到总览统计、输入材料盘点和逐版核对明细（可展开查看每个
   引用文件的 ✓ / ✗ 状态）。

也可以只上传检查点：对不晚于检查点版本的目标版本，直接以检查点中的
add 列为快照基线；目标版本更新时，则从检查点版本 +1 起连续重放提交，
中途缺任一提交即报缺失提交号。检查点里的 remove 是墓碑，不属于快照。

## 本地开发（无 Docker 时）

```bash
python -m venv --without-pip .venv   # 系统无 ensurepip 时
python .venv/get-pip.py              # 再引导 pip
pip install -r requirements.txt
python -I scripts/generate_samples.py
gunicorn --bind 0.0.0.0:8000 app.web:app
python -I -m pytest tests -q
```

## 设计取舍

- **流式优先、只留路径**：逐行提交按行解析、上传体假脱机到磁盘，
  重放状态里只保留文件路径，列统计（`stats` / `statsParser`）与其余
  add/remove 元数据一律不保留，确保 256 MiB 限额内可处理上百 MiB 提交；
- **基线选择**：优先使用不晚于目标版本的最新检查点（重放距离最短），
  无检查点时才要求版本 0 的提交；
- **逐版独立重放**：每个目标版本都从基线完整重放一次，缺提交不会污染
  其他版本的结论，报告中的“重放起点”对每版透明；
- **只认经典单文件检查点**：分片检查点（`N.checkpoint.K.M.parquet`）
  与 `_last_checkpoint` 指针不参与重放，只在页面登记忽略原因；
- **DV 硬拦截**：删除向量会让“文件存在 ≠ 行数据完整”，超出本工具
  基于文件清单的核对能力，因此在协议层和动作层双重检测并拒绝，
  即使问题动作位于提交末尾也会点名文件且不输出核查结果。
