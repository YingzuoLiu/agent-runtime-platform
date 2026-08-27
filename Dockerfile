FROM python:3.12-slim AS builder

ENV VIRTUAL_ENV=/opt/venv
RUN python -m venv "$VIRTUAL_ENV"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /app
COPY requirements.txt requirements-postgres.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt -r requirements-postgres.txt

FROM python:3.12-slim

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RUNTIME_STORE_BACKEND=postgres \
    RUNTIME_EXTERNAL_ACTION_MODE=disabled \
    RUNTIME_WORKER_COUNT=1 \
    RUNTIME_HTTP_CONCURRENCY_LIMIT=32 \
    RUNTIME_MANAGER_SHUTDOWN_GRACE_SECONDS=5 \
    RUNTIME_SERVER_GRACEFUL_SHUTDOWN_SECONDS=15

RUN apt-get update \
    && apt-get install --yes --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 appuser

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=appuser:appuser . .
RUN mkdir -p /app/provider_data /app/runtime_data \
    && chown -R appuser:appuser /app/provider_data /app/runtime_data

USER appuser
EXPOSE 8000
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2)"

# tini runs as container PID 1, forwards signals, and reaps orphaned descendants
# left behind when a timed-out sandbox process group is terminated.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "runtime_service.serve"]
