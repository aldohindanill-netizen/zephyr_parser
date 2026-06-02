# Deploy hardening (slim stack)

## Windows Task Scheduler

- Запуск от **выделенной service account** (не интерактивного пользователя).
- `.env` и `.env.secrets` в корне репо: ACL **только** для service account и администраторов.
- Не передавать `ZEPHYR_API_TOKEN` в аргументах задачи — только через `.env.secrets`.
- Уже есть: lock file (`ZEPHYR_RUN_LOCK_FILE`), timeout (`ZEPHYR_RUN_TIMEOUT_MINUTES`).

## Linux systemd

1. Секреты в `/etc/zephyr-parser/env` (chmod `600`, owner root:zephyr).
2. Скопировать и отредактировать `deploy/zephyr-weekly-report.service.example`.
3. Рекомендуемые дополнения в `[Service]`:

```ini
User=zephyr
Group=zephyr
EnvironmentFile=-/etc/zephyr-parser/env
ProtectSystem=strict
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true
```

4. Логи: `journalctl -u zephyr-weekly-report -f`
5. Audit: forward `reports/audit/audit.jsonl` в SIEM (optional).

## Docker

- Образ: корневой `Dockerfile` (Python 3.12-slim, non-root user `zephyr`).
- Секреты: platform secrets → env, не в git.
- Persistence: mount `/data` для `reports/` и `logs/`.

## Backup

- `reports/` (включая audit) — по политике RPO/RTO владельца ИС.
- `.env.secrets` — отдельно, зашифрованное хранилище секретов.

## Host ACL

- `reports/` и `logs/` — только service account + группа аудиторов (read-only для аудиторов на audit).
