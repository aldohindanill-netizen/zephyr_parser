# zephyr_parser

CLI utility to fetch Zephyr test executions and build a weekly summary table.

## What it does

- downloads paginated execution data from Zephyr API
- groups runs by week (week starts on Monday)
- calculates totals by status (passed/failed/blocked/not executed/other)
- writes CSV report and prints a console table

## Usage

```bash
python3 zephyr_weekly_report.py \
  --base-url "https://api.zephyrscale.smartbear.com" \
  --endpoint "/v2/testexecutions" \
  --token "$ZEPHYR_TOKEN" \
  --extra-param "projectKey=DEMO" \
  --extra-param "testCycleKey=DEMO-R1" \
  --from-date "2026-01-01" \
  --to-date "2026-12-31" \
  --output "weekly_zephyr_report.csv"
```

## Notes

- Default auth header is `Authorization: Bearer <token>`.
- If your Zephyr instance uses different fields for date/status, pass custom paths:
  - `--date-field "some.path.to.date"`
  - `--status-field "some.path.to.status"`
- You can pass multiple `--date-field` or `--status-field` values.
