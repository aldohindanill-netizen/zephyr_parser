# Экосистема: компоненты вне zephyr_parser

Следующие системы **не входят** в репозиторий `zephyr_parser`, но могут обрабатывать те же данные Zephyr. Для них нужен отдельный паспорт ИС и чеклист по корпоративным «Требованиям к внутренним сервисам».

## zephyr-bot (Telegram)

- Ввод результатов тестов операторами
- Секреты: `TELEGRAM_BOT_TOKEN`, webhook secret
- Требования: ограничение `TELEGRAM_ALLOWED_CHAT_IDS`, HTTPS webhook, ротация токенов, audit writeback в Zephyr

## Бывший стек (удалён из zephyr_parser)

Если в production ещё используются:

- n8n / NocoDB / Postgres sync
- Google Sheets + Apps Script

— оформить отдельные паспорта: AD/сессии для UI, Redis/Postgres hardening, audit выгрузок ПДн.

## Разделение ответственности

| Действие | zephyr_parser | zephyr-bot / другие |
|----------|---------------|---------------------|
| Read Zephyr, отчёты | да | — |
| Publish Confluence | да (опционально) | — |
| Write test results (оператор) | — | да |
