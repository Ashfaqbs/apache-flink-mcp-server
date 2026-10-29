FROM python:3.12-slim-bookworm AS base

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mcp_server.py .

RUN useradd --create-home --shell /bin/bash appuser
USER appuser

EXPOSE 9090

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import httpx; httpx.get('http://127.0.0.1:9090/mcp', timeout=3)" || exit 1

CMD ["python", "mcp_server.py"]
