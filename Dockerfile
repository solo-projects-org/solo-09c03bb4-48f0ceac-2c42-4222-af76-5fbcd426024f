FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts
COPY samples ./samples

# 容器启动前确保内置样例就绪（仓库已带样例，此步为幂等保险）
RUN python -I scripts/generate_samples.py

EXPOSE 8000

# 容器硬限 256 MiB：单 worker（多线程）避免两份 PyArrow 常驻内存；
# 大提交按行流式解析，上传体假脱机到磁盘，不依赖多进程扩容。
# 超时放宽到 10 分钟，给逐行解析大日志留足时间。
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "1", "--threads", "4", "--timeout", "600", "app.web:app"]
