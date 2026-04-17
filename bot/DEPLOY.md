# Деплой бота на Amvera

## Что нужно создать в Amvera

Два проекта в **одном аккаунте**:

| # | Тип | Название (пример) | Назначение |
|---|-----|-------------------|-----------|
| 1 | Преднастроенный сервис → Redis | `my-redis` | хранение сессий пользователей и FSM-состояния бота |
| 2 | Приложение (Docker) | `zephyr-bot` | сам Telegram-бот |

---

## Шаг 1 — Redis

1. Amvera Dashboard → **Преднастроенные сервисы** → **Создать**
2. Параметры сервиса: **Базы данных**
3. Тип сервиса: **Redis**
4. Название: `my-redis` (или любое), тариф **не ниже «Начальный»**
5. После создания перейти в раздел **«Переменные»** → **Создать секрет**:

   | Название | Значение |
   |----------|---------|
   | `REDIS_ARGS` | `--requirepass ВашСложныйПароль` |

6. Открыть страницу **«Инфо»** — там будет внутреннее DNS-имя вида:
   ```
   amvera-<ваш-логин>-run-my-redis
   ```
   Сохранить это имя — оно нужно в шаге 2.

---

## Шаг 2 — Telegram-бот

### 2.1 Получить токен бота

В Telegram обратиться к [@BotFather](https://t.me/BotFather) → `/newbot` → скопировать токен.

### 2.2 Создать проект в Amvera

1. Amvera Dashboard → **Создать проект** → **Приложение**
2. Подключить этот git-репозиторий
3. В настройках сборки указать папку: `bot/`  
   *(Amvera найдёт `bot/Dockerfile` и `bot/amvera.yaml` автоматически)*

### 2.3 Переменные окружения проекта `zephyr-bot`

Перейти в раздел **«Переменные»** и добавить:

#### Секреты (чувствительные данные — нажать «Создать секрет»)

| Название | Значение |
|----------|---------|
| `BOT_TOKEN` | токен от BotFather |
| `ZEPHYR_API_TOKEN` | токен Zephyr API |
| `REDIS_PASSWORD` | пароль из шага 1 |

#### Обычные переменные

| Название | Пример значения | Описание |
|----------|----------------|---------|
| `ZEPHYR_BASE_URL` | `https://jira.navio.auto` | базовый URL вашего Zephyr |
| `ZEPHYR_PROJECT_ID` | `10904` | ID проекта в Zephyr |
| `ZEPHYR_ROOT_FOLDER_IDS` | `10545` | корневая папка для фильтрации |
| `ZEPHYR_TREE_NAME_REGEX` | `^2026\.\d{2}\.\d{2}$` | фильтр имён папок |
| `ZEPHYR_ENDPOINT` | `rest/tests/1.0/testrun/search` | эндпоинт тест-ранов |
| `ZEPHYR_FOLDERTREE_ENDPOINT` | `rest/tests/1.0/project/10904/foldertree/testrun` | эндпоинт дерева папок |
| `ZEPHYR_FOLDER_SEARCH_ENDPOINT` | `rest/tests/1.0/folder/search` | поиск папок (fallback) |
| `ZEPHYR_TESTCASE_ENDPOINT_TEMPLATE` | `rest/tests/1.0/testrun/{cycle_id}/testcase/search` | тест-кейсы цикла |
| `ZEPHYR_QUERY_TEMPLATE` | `testRun.projectId IN (10904) AND testRun.folderTreeId IN ({folder_id}) ORDER BY testRun.name ASC` | шаблон запроса |
| `REDIS_HOST` | `amvera-<логин>-run-my-redis` | DNS-имя из шага 1 |
| `REDIS_PORT` | `6379` | порт Redis |
| `REDIS_DB` | `0` | номер базы |
| `REDIS_SESSION_TTL` | `1800` | TTL сессии в секундах (30 мин) |

### 2.4 Запустить деплой

После сохранения переменных нажать **«Задеплоить»** (или запушить в ветку — Amvera подхватит автоматически).

---

## Проверка

В логах контейнера `zephyr-bot` должна появиться строка:

```
Bot started, polling…
```

Открыть бота в Telegram, написать `/start` — должен появиться список папок из Zephyr.

---

## Сценарий работы бота

```
/start
  ↓ загружает папки из Zephyr (foldertree API)
  → пользователь выбирает папку
  ↓ загружает тест-раны папки
  → пользователь выбирает тест-ран
  ↓ загружает тест-кейсы тест-рана
  → пользователь выбирает тест-кейс
  ↓ загружает список статусов из Zephyr
  → пользователь выбирает статус
  → пользователь вводит комментарий (или /skip)
  → бот показывает итоговую карточку
  → пользователь нажимает «Отправить в Zephyr»
  ↓ POST /testresults → Zephyr API
  ✅ «Результат успешно загружен»
```

Состояние диалога хранится в Redis. Незавершённые сессии удаляются через `REDIS_SESSION_TTL` секунд.

---

## Структура файлов

```
bot/
├── amvera.yaml          ← конфиг деплоя Amvera
├── Dockerfile           ← образ на python:3.12-slim
├── requirements.txt     ← aiogram==3.7.0, redis>=5.0.0
├── .env.example         ← шаблон переменных для локальной разработки
├── DEPLOY.md            ← этот файл
├── config.py            ← читает env-переменные, валидирует обязательные
├── zephyr.py            ← async-обёртки над zephyr_weekly_report
├── handlers.py          ← FSM-диалог (aiogram 3)
└── bot.py               ← точка входа
```

---

## Локальная разработка

```bash
cd bot

# Скопировать конфиг и заполнить реальные значения
cp .env.example .env

# Запустить локальный Redis
docker run -d -p 6379:6379 redis

# Установить зависимости
pip install -r requirements.txt

# Запустить бота
set -a && source .env && set +a
python bot.py
```

> **Важно:** в `bot/Dockerfile` предполагается, что `zephyr_weekly_report.py`
> находится на уровень выше (`../zephyr_weekly_report.py`).
> При сборке на Amvera репозиторий монтируется целиком — файл будет доступен.
> При локальной сборке Docker из папки `bot/` нужно скопировать файл туда:
> ```bash
> cp ../zephyr_weekly_report.py .
> docker build -t zephyr-bot .
> ```
