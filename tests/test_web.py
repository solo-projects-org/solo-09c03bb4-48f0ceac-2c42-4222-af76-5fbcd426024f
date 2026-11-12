"""Flask 路由层测试（含本地样例加载）。"""

from __future__ import annotations

import io
import json

from app.analyzer import InputFile
from app.web import app, _spool_upload


def _commit_bytes(actions):
    return ("\n".join(json.dumps(a) for a in actions)).encode("utf-8")


def test_index_renders():
    client = app.test_client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "Delta Lake 版本可恢复性分析".encode() in resp.data


def test_upload_analyze_flow():
    client = app.test_client()
    data = {
        "from_version": "0",
        "to_version": "1",
        "log_files": [
            (io.BytesIO(_commit_bytes([{"add": {"path": "data/a.parquet"}}])),
             "00000000000000000000.json"),
            (io.BytesIO(_commit_bytes([{"add": {"path": "data/b.parquet"}}])),
             "00000000000000000001.json"),
        ],
        "manifest_files": [
            (io.BytesIO(b"data/a.parquet\ndata/b.parquet\n"), "existing.txt"),
        ],
    }
    resp = client.post("/analyze", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert "可重建 · 文件齐全".encode() in resp.data


def test_upload_missing_file_reports_400():
    client = app.test_client()
    resp = client.post("/analyze", data={
        "from_version": "0", "to_version": "1",
    }, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "至少上传".encode() in resp.data


def test_sample_api_healthy_and_incident():
    client = app.test_client()
    for name, expected_recoverable in (("healthy_table", 5),
                                       ("incident_table", None)):
        resp = client.post(f"/api/analyze-sample/{name}")
        assert resp.status_code == 200, resp.data
        report = resp.get_json()
        assert report["range"] == {"from": 0, "to": (4 if name == "healthy_table" else 5)}
        if expected_recoverable:
            assert report["summary"]["recoverable"] == expected_recoverable


def test_unknown_sample_404():
    resp = app.test_client().post("/api/analyze-sample/nope")
    assert resp.status_code == 404


def test_sample_api_list():
    resp = app.test_client().get("/api/samples")
    assert resp.status_code == 200
    names = resp.get_json()
    assert "healthy_table" in names and "incident_table" in names


class _GuardedBytesIO(io.BytesIO):
    """整份 read()（无参数 / 负数）直接报错，强制走分块假脱机路径。"""

    def read(self, n=-1):
        if n is None or n < 0:
            raise AssertionError("上传接收不允许整份 read()，必须分块假脱机")
        return super().read(n)


class _FakeStorage:
    """模拟 werkzeug FileStorage：只有 stream，不提供整份 read 到内存的用法。"""

    def __init__(self, filename: str, payload: bytes):
        self.filename = filename
        self.stream = _GuardedBytesIO(payload)


def test_spool_upload_never_reads_whole_file_into_memory():
    payload = b"x" * (4 * 1024 * 1024)  # 4 MiB，超过 1 MiB 假脱机阈值
    storage = _FakeStorage("00000000000000000000.json", payload)
    item = _spool_upload(storage)
    assert isinstance(item, InputFile)
    assert item.stream is not None
    # 假脱机文件已落盘（TemporaryFile），内存里没有整份副本
    assert not isinstance(item.stream, io.BytesIO)
    item.stream.seek(0)
    assert item.stream.read() == payload


def test_upload_large_ndjson_returns_report_without_stats():
    # 约 4 MiB 的逐行提交，列统计占绝大部分体积
    actions = [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}]
    for i in range(1, 257):
        actions.append({"add": {
            "path": f"data/part-{i:05d}.parquet",
            "dataChange": True,
            "stats": json.dumps({"numRecords": i,
                                 "c": {"min": "m" * 16000}}),
        }})
    v0 = ("\n".join(json.dumps(a) for a in actions) + "\n").encode()
    assert len(v0) > 3 * 1024 * 1024

    client = app.test_client()
    resp = client.post("/analyze", data={
        "from_version": "0",
        "to_version": "0",
        "log_files": [(io.BytesIO(v0), "00000000000000000000.json")],
        "manifest_files": [(
            io.BytesIO("\n".join(f"data/part-{i:05d}.parquet"
                                 for i in range(1, 257)).encode()),
            "existing_files.txt")],
    }, content_type="multipart/form-data")
    assert resp.status_code == 200, resp.data[:400]
    assert "可重建 · 文件齐全".encode() in resp.data
    # 列统计绝不出现在渲染结果里
    assert b"numRecords" not in resp.data


def test_upload_bad_tail_line_returns_400_with_filename():
    payload = b'{"add": {"path": "data/a.parquet"}}\n' * 4 + b"{broken tail\n"
    client = app.test_client()
    resp = client.post("/analyze", data={
        "from_version": "0",
        "to_version": "0",
        "log_files": [(io.BytesIO(payload), "00000000000000000000.json")],
    }, content_type="multipart/form-data")
    assert resp.status_code == 400
    assert "00000000000000000000.json".encode() in resp.data
    # 出错时只渲染错误横幅；报告 JSON 仅在成功路径嵌入，出错时必须不存在
    assert b'class="error"' in resp.data
    assert b'id="report-data"' not in resp.data
    assert b'"versions":' not in resp.data
