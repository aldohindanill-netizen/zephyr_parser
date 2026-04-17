FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY bot.py config.py zephyr.py handlers.py ./

# Copy the Zephyr report library from the parent project
COPY zephyr_weekly_report.py ./

CMD ["python", "bot.py"]
