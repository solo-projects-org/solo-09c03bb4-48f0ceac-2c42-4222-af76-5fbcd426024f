"""生成"超大逐行 JSON 提交"压力材料，供网页上传核查。

场景：备份找回的 Delta 日志里，版本 0 的提交是一个约 192 MiB 的逐行 JSON
（ndjson），体积主要来自大量 add 动作内嵌的列统计（``stats`` /
``statsParser``）。版本 1 的提交删除其中绝大多数文件，只保留少量新快照
文件。工具在 256 MiB 内存限额内必须：

1. 接收与解析逐行提交时**不整份读入内存**（流式逐行）；
2. 重放**不保留列统计**，只留路径；
3. 只查 v1（快照仅引用少量文件）时网页能正常给出核查结果。

本脚本自身也按行流式写出，生成过程内存占用恒定。产物默认写到
``samples_large/orders_table/``：

    00000000000000000000.json   ~192 MiB，N 个带大列统计的 add 动作
    00000000000000000001.json   小提交：删除绝大多数文件、补两个新文件
    existing_files.txt          现存数据文件清单（v1 快照引用的文件均在）

运行（输出目录与大小可用参数覆盖）：
    python -I scripts/generate_large_log.py
    python -I scripts/generate_large_log.py --target-mib 192 --out /tmp/orders
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT = ROOT / "samples_large" / "orders_table"

# v1 重放后仍保留的 v0 文件数；加上 v1 新增的 2 个，快照只引用少量文件
KEPT_FILES = 3
NEW_IN_V1 = 2
# 每个 add 动作里伪造的统计列数；stats 体积靠这些列的 min/max/nullCount 撑起
STAT_COLUMNS = 64
# 每条 add 行的目标字节预算（约 10 KiB），行数由目标体积 / 行预算推导
LINE_BUDGET_BYTES = 10 * 1024


def part_path(n: int) -> str:
    return f"data/part-{n:05d}-orders.parquet"


def build_stats(seed: int) -> dict:
    """构造一个体积可观但结构合法的列统计对象（含 min/max/nullCount）。"""
    columns = {}
    for col in range(STAT_COLUMNS):
        # 用较长的字符串值把单行 stats 撑到约 10 KiB，模拟真实宽表统计
        marker = f"c{col:02d}-s{seed}"
        columns[f"order_col_{col:02d}"] = {
            "min": f"{marker}-min-" + "x" * 96,
            "max": f"{marker}-max-" + "y" * 96,
            "nullCount": (seed + col) % 7,
        }
    return {
        "numRecords": 100_000 + seed,
        "tightBounds": True,
        "columnStats": columns,
    }


def add_action(path: str, seed: int) -> dict:
    stats = build_stats(seed)
    return {
        "add": {
            "path": path,
            "partitionValues": {},
            "size": 268_435_456,
            "modificationTime": 1700000000000 + seed,
            "dataChange": True,
            # Delta 常见的两种列统计承载方式，都属于重放必须丢弃的部分
            "stats": json.dumps(stats, separators=(",", ":")),
            "statsParser": {"parsedStatsStruct": stats},
        }
    }


def remove_action(path: str) -> dict:
    return {"remove": {"path": path, "deletionTimestamp": 1700000100000,
                       "dataChange": True}}


def write_commit_zero(log_dir: Path, target_bytes: int) -> int:
    """流式写出 v0：首行 protocol，之后不断追加带大列统计的 add，直到达标。"""
    log_dir.mkdir(parents=True, exist_ok=True)
    target = log_dir / f"{0:020d}.json"
    written = 0
    n_adds = 0
    with target.open("w", encoding="utf-8") as fh:
        protocol = {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}
        line = json.dumps(protocol, ensure_ascii=False)
        fh.write(line + "\n")
        written += len(line.encode("utf-8")) + 1
        # 每次 write 只持有一行在内存，写入量随文件增长而内存恒定
        while written < target_bytes:
            n_adds += 1
            line = json.dumps(add_action(part_path(n_adds), n_adds),
                              ensure_ascii=False, separators=(",", ":"))
            fh.write(line + "\n")
            written += len(line.encode("utf-8")) + 1
    return n_adds


def write_commit_one(log_dir: Path, n_adds: int) -> None:
    """v1：删除 v0 中除前 KEPT_FILES 个外的全部文件，再新增 NEW_IN_V1 个。"""
    actions = []
    for n in range(KEPT_FILES + 1, n_adds + 1):
        actions.append(remove_action(part_path(n)))
    for n in range(n_adds + 1, n_adds + 1 + NEW_IN_V1):
        actions.append(add_action(part_path(n), n))
    target = log_dir / f"{1:020d}.json"
    with target.open("w", encoding="utf-8") as fh:
        for action in actions:
            fh.write(json.dumps(action, ensure_ascii=False,
                                separators=(",", ":")) + "\n")


def write_manifest(table_dir: Path, n_adds: int) -> None:
    """现存清单：v1 快照引用的文件全部现存（前 KEPT_FILES 个 + v1 新增）。"""
    kept = [part_path(n) for n in range(1, KEPT_FILES + 1)]
    new = [part_path(n)
           for n in range(n_adds + 1, n_adds + 1 + NEW_IN_V1)]
    lines = [
        "# 现存数据文件表内相对路径清单（# 开头为注释，空行忽略）",
        "# v1 快照只引用下列少量文件；v0 旧文件已被 remove 且物理删除",
        "",
    ]
    for i, path in enumerate(kept + new):
        # 故意混入 ./ 前缀与 URI 编码，验证服务端归一化
        lines.append(("./" + path) if i % 2 == 0 else path)
    (table_dir / "existing_files.txt").write_text(
        "\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="输出表目录（默认 samples_large/orders_table）")
    parser.add_argument("--target-mib", type=int, default=192,
                        help="v0 提交的目标体积（MiB，默认 192）")
    args = parser.parse_args()

    table_dir = args.out
    log_dir = table_dir / "_delta_log"
    table_dir.mkdir(parents=True, exist_ok=True)

    target_bytes = args.target_mib * 1024 * 1024
    # 按实际字节逐行补齐到目标体积，生成过程中内存占用恒定
    actual = write_commit_zero(log_dir, target_bytes)
    write_commit_one(log_dir, actual)
    write_manifest(table_dir, actual)

    v0 = log_dir / f"{0:020d}.json"
    v1 = log_dir / f"{1:020d}.json"
    print(f"材料已生成于 {table_dir}")
    print(f"  {v0.name}: {v0.stat().st_size / 1024 / 1024:.1f} MiB，"
          f"{actual} 个带列统计的 add 动作（逐行 JSON）")
    print(f"  {v1.name}: {v1.stat().st_size} 字节，"
          f"删除 {actual - KEPT_FILES} 个、新增 {NEW_IN_V1} 个")
    print(f"  existing_files.txt: v1 快照引用 {KEPT_FILES + NEW_IN_V1} 个文件，均现存")
    print("网页上传时选这两份提交与清单，版本区间填 1–1，应得到可重建结论。")


if __name__ == "__main__":
    main()
