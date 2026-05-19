# Паспорт ИС: Zephyr Report Generator (batch)

## Назначение

Автоматизированная выгрузка данных Zephyr/Jira, формирование CSV/HTML/wiki-отчётов и опциональная публикация в Confluence.

## Граница ответственности

| В scope | Вне scope (другие системы) |
|---------|---------------------------|
| `zephyr_weekly_report.py`, `run_zephyr.*` | [zephyr-bot](https://github.com/aldohindanill-netizen/zephyr-bot) (Telegram) |
| Read Zephyr/Jira, write `reports/` | Бывшие n8n/NocoDB/Sheets (удалены из репо) |
| Confluence REST publish | AD-логин пользователей в Jira/Confluence |

## Категория данных

Возможны **ПДн** в комментариях тест-кейсов и полях Jira. Массовые выгрузки в `reports/` — объект аудита.

## Компоненты

- **Хост:** Windows Task Scheduler или Linux systemd (`deploy/zephyr-weekly-report.service.example`)
- **Секреты:** `ZEPHYR_API_TOKEN`, `ZEPHYR_CONFLUENCE_API_TOKEN` в `.env` (ACL на файл)
- **Артефакты:** `reports/`, `logs/`, `reports/audit/audit.jsonl`

## Сеть (egress)

- `ZEPHYR_BASE_URL` (Jira/Zephyr)
- `ZEPHYR_CONFLUENCE_BASE_URL` (если publish)
- Опционально logviewer (`ZEPHYR_LOGVIEWER_URL_REGEX`)

## N/A по корпоративному PDF

- AD-интеграция CLI, сессии web, SQLi в СУБД приложения, Redis/n8n — не применимо к slim-стеку.

## Хранение и retention

| Путь | Переменная | По умолчанию |
|------|------------|--------------|
| `logs/zephyr_*.log` | `ZEPHYR_LOG_RETENTION_DAYS` | 7 |
| `reports/*` (by_folder, readable, build_log) | `ZEPHYR_REPORTS_RETENTION_DAYS` | 0 (не удалять) |
| `reports/audit/audit.jsonl` | `ZEPHYR_AUDIT_RETENTION_DAYS` | 186 (~6 мес.) |

## Ветка внедрения

`security/internal-services-compliance` → PR в `main`.

См. также: `docs/security-topology.md`, `docs/security-deploy.md`, `docs/security-ecosystem.md`.
