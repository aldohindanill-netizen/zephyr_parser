# zephyr_parser — stdlib-only batch image (embeddings use host venv / optional image).
FROM python:3.12-slim

RUN groupadd --gid 1000 zephyr \
    && useradd --uid 1000 --gid zephyr --create-home --shell /usr/sbin/nologin zephyr

WORKDIR /app

COPY PIPELINE_VERSION zephyr_weekly_report.py zephyr_audit.py zephyr_security.py \
     zephyr_pipeline_health.py zephyr_weekly_analytics.py bug_duplicate_detection.py \
     repo_env.py run_zephyr.sh ./
COPY report_templates/ report_templates/

RUN chown -R zephyr:zephyr /app

ENV PYTHONUNBUFFERED=1 \
    ZEPHYR_LOG_DIR=/data/logs \
    ZEPHYR_DAILY_READABLE_DIR=/data/reports/daily_readable \
    ZEPHYR_AUDIT_LOG=/data/reports/audit/audit.jsonl \
    ZEPHYR_PIPELINE_HEALTH_HTML=/data/reports/pipeline_health.html \
    ZEPHYR_RUN_LOCK_FILE=/data/reports/.zephyr_weekly_report.lock

USER zephyr

# Expect secrets and overrides via mounted env file or -e at runtime.
ENTRYPOINT ["python", "-u", "zephyr_weekly_report.py"]
