# Slim runtime image for zephyr_parser (stdlib only, no pip deps).
FROM python:3.12-slim-bookworm

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin zephyr \
    && mkdir -p /data/reports /data/logs \
    && chown -R zephyr:zephyr /data

WORKDIR /app
COPY zephyr_weekly_report.py zephyr_security.py zephyr_audit.py run_zephyr.sh ./
COPY report_templates/ ./report_templates/

RUN chmod +x run_zephyr.sh \
    && chown -R zephyr:zephyr /app

USER zephyr
ENV PYTHONUNBUFFERED=1 \
    ZEPHYR_OUTPUT=/data/weekly_zephyr_report.csv \
    ZEPHYR_PER_FOLDER_DIR=/data/reports/by_folder \
    ZEPHYR_LOG_DIR=/data/logs \
    ZEPHYR_AUDIT_LOG=/data/reports/audit/audit.jsonl

CMD ["./run_zephyr.sh"]
