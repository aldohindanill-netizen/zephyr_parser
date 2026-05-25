# Сетевая топология (Zephyr Report Generator)

## Потоки

```
[QA host: Task Scheduler / systemd]
        |
        | HTTPS (TLS 1.2+)
        v
+------------------+     +-------------------+
| jira.navio.auto  |     | Confluence (opt.) |
| Zephyr REST API  |     | REST publish      |
+------------------+     +-------------------+
        |
        | (optional, build-log links)
        v
+------------------+
| logviewer.df.*   |
+------------------+
```

## Egress allowlist (рекомендуется)

| Назначение | Хост / шаблон |
|------------|----------------|
| Zephyr/Jira API | `ZEPHYR_BASE_URL` |
| Jira metadata | `ZEPHYR_JIRA_BASE_URL` или тот же хост |
| Confluence | `ZEPHYR_CONFLUENCE_BASE_URL` |
| Logviewer | домен из `ZEPHYR_LOGVIEWER_URL_REGEX` |

Исходящий интернет с runner-хоста — только по списку. Входящие соединения к batch-процессу не требуются.

## Хранение на хосте

| Путь | Содержимое | ACL |
|------|------------|-----|
| `.env` | токены API | только service account |
| `reports/` | CSV, HTML, wiki | service account + аудиторы |
| `reports/audit/` | audit.jsonl | append: service; read: аудиторы |
| `logs/` | операционные логи | service account |

## Вне scope этого репозитория

- [zephyr-bot](https://github.com/aldohindanill-netizen/zephyr-bot) — Telegram, отдельный egress и секреты.
