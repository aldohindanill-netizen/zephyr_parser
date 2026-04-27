# zephyr_parser

CLI-утилита и Redis-воркер для генерации отчётов по тест-экзекьюшенам Zephyr.

Связанный репозиторий: **[zephyr-bot](https://github.com/aldohindanill-netizen/zephyr-bot)** — Telegram-бот для ввода результатов.

---

## Что делает

- Скачивает пагинированные данные экзекьюшенов из Zephyr API
- Обнаруживает дерево папок (tree-режим) или выводит папки из экзекьюшенов
- Агрегирует по ISO-неделям (понедельник) и считает totals по статусам
- Экспортирует CSV-отчёты и HTML/wiki-страницы для Confluence
- Предоставляет Redis-воркер (`redis_runner.py`) для управления запусками через очередь

---

## Запуск локально

```bash
cp .env.example .env
# установить ZEPHYR_API_TOKEN и другие переменные

bash ./run_navio_folder_report.sh
```

Launcher запускает в tree-режиме:

- пробует кастомный источник (`ZEPHYR_TREE_SOURCE_*`) если настроен
- делает `POST` на `ZEPHYR_FOLDER_SEARCH_ENDPOINT`
- fallback: `GET` на `ZEPHYR_FOLDERTREE_ENDPOINT`
- выбирает совпадающие узлы (leaf + regex/path фильтры)
- скачивает экзекьюшены для каждой папки

---

## Выходные файлы

| Переменная | Путь по умолчанию | Содержимое |
|------------|------------------|-----------|
| `ZEPHYR_OUTPUT` | `weekly_zephyr_report.csv` | Сводный отчёт по неделям |
| `ZEPHYR_PER_FOLDER_DIR` | `reports/by_folder/` | Отчёт по каждой папке |
| `ZEPHYR_CYCLES_CASES_OUTPUT` | `reports/cycles_and_cases.csv` | Папка → цикл → кейс |
| `ZEPHYR_CASE_STEPS_OUTPUT` | `reports/case_steps.csv` | Кейс → шаги → статус |
| `ZEPHYR_DAILY_READABLE_DIR` | `reports/daily_readable/` | HTML и wiki для Confluence |

---

## Деплой на Amvera (Redis-воркер)

`redis_runner.py` — постоянно работающий воркер. Принимает задания из Redis-очереди и выполняет одно из трёх действий:

| `action` | Что делает |
|----------|-----------|
| `run_report` | Запускает полный пайплайн отчёта, пишет CSV в `/data` |
| `list_folders` | Возвращает дерево папок как JSON-массив |
| `upload_result` | POST/PUT результат тест-кейса обратно в Zephyr |

### Деплой

1. Создать **преднастроенный сервис Redis** в Amvera.  
   В разделе «Переменные» добавить секрет `REDIS_ARGS=--requirepass <пароль>`.

2. Создать **проект приложения** из этого репозитория.  
   В разделе «Переменные» добавить:

   | Переменная | Тип | Значение |
   |------------|-----|---------|
   | `ZEPHYR_API_TOKEN` | секрет | токен Zephyr API |
   | `ZEPHYR_BASE_URL` | переменная | `https://jira.example.com` |
   | `ZEPHYR_PROJECT_ID` | переменная | `10904` |
   | `ZEPHYR_ROOT_FOLDER_IDS` | переменная | `10545` |
   | `REDIS_HOST` | переменная | `amvera-<логин>-run-<имя-redis>` |
   | `REDIS_PASSWORD` | секрет | пароль Redis |

3. После сохранения переменных — **перезапустить** контейнер.

### Отправить задание из клиента

```python
import json, redis

r = redis.Redis(host="amvera-user-run-my-redis", port=6379,
                password="...", decode_responses=True)

# Запустить отчёт
r.lpush("zephyr:jobs", json.dumps({
    "action": "run_report",
    "job_id": "r01",
    "ZEPHYR_FROM_DATE": "2026-04-01",
    "ZEPHYR_TO_DATE":   "2026-04-30",
}))

# Получить результат
_, raw = r.blpop("zephyr:results")
print(json.loads(raw)["exit_code"])   # 0 = успех
```

### Redis env vars воркера

| Переменная | По умолчанию | Описание |
|------------|-------------|---------|
| `REDIS_HOST` | `localhost` | хост Redis |
| `REDIS_PORT` | `6379` | порт |
| `REDIS_PASSWORD` | — | пароль |
| `REDIS_DB` | `0` | номер БД |
| `REDIS_JOB_QUEUE` | `zephyr:jobs` | ключ очереди заданий |
| `REDIS_RESULT_KEY` | `zephyr:results` | ключ списка результатов |
| `REDIS_RESULT_CHANNEL` | `zephyr:done` | pub/sub канал |
| `REDIS_RESULT_TTL` | `3600` | TTL результатов (сек) |
| `REDIS_HEARTBEAT_KEY` | `zephyr:heartbeat` | ключ heartbeat |
| `REDIS_HEARTBEAT_INTERVAL` | `30` | интервал heartbeat (сек) |

---

## Хранение токена

- Используйте `.env` + переменная `ZEPHYR_API_TOKEN`, загружаемая лаунчером.
- Не передавайте токен через `--token` в обычном использовании.
- Не коммитьте `.env` в git.
- `.env.example` — безопасный шаблон, хранится в репозитории.

---

## Устранение неполадок

### TelegramConflictError: can't use getUpdates while webhook is active

Если Telegram-бот падает с ошибкой вида:

```
TelegramConflictError: Conflict: can't use getUpdates method while webhook is active;
use deleteWebhook to delete the webhook first
```

Это значит, что ранее для бота был зарегистрирован webhook, и теперь он мешает работе в режиме polling.

**Быстрое решение — удалить webhook один раз:**

```bash
TELEGRAM_BOT_TOKEN=<токен> python delete_webhook.py
```

Или через curl:

```bash
curl "https://api.telegram.org/bot<TOKEN>/deleteWebhook?drop_pending_updates=true"
```

**Постоянное решение** — добавить удаление webhook в стартап бота перед запуском polling.
Для aiogram 3.x это делается через `await bot.delete_webhook(drop_pending_updates=True)`
перед вызовом `await dp.start_polling(bot)`:

```python
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)
```

---

## Notes

- Заголовок авторизации по умолчанию: `Authorization: Bearer <token>`.
- Эндпоинты обнаружения папок настраиваются через `ZEPHYR_FOLDER_SEARCH_ENDPOINT` / `ZEPHYR_FOLDERTREE_ENDPOINT`.
- `ZEPHYR_TREE_AUTOPROBE=true` — только для диагностики.
- `ZEPHYR_QUERY_TEMPLATE` должен содержать плейсхолдер `{folder_id}`.
- Для кастомных полей даты/статуса: `--date-field` / `--status-field` (можно передавать несколько).
