FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY zephyr_weekly_report.py ./
COPY redis_runner.py ./

# Output reports go to /data (Amvera persistent storage mount point)
ENV ZEPHYR_OUTPUT=/data/weekly_zephyr_report.csv
ENV ZEPHYR_PER_FOLDER_DIR=/data/reports/by_folder
ENV ZEPHYR_CYCLES_CASES_OUTPUT=/data/reports/cycles_and_cases.csv
ENV ZEPHYR_CASE_STEPS_OUTPUT=/data/reports/case_steps.csv
ENV ZEPHYR_DAILY_READABLE_DIR=/data/reports/daily_readable

CMD ["python", "redis_runner.py"]
