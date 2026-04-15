# VPN Resume Checklist (Grist -> Postgres -> Zephyr)

## 0) Текущее baseline состояние (уже сделано)

- `grist_to_postgres_sync` и `postgres_to_grist_status_sync` идут на минутном тике.
- `document per folder` активен, маппинги лежат в `grist_folder_docs`.
- Offline путь подтвержден: `Grist -> test_results -> sync_queue`.
- Тестовый кейс: `test_result_id=112522`.

## 1) Перед подключением VPN

- Убедиться, что локальный стек поднят:
  - `docker compose --env-file infra/.env.nocodb-n8n -f infra/docker-compose.nocodb-n8n.yml ps`
- Быстрая проверка оффлайн-состояния:
  - `.\scripts\offline-sync-check.ps1 -TestResultId 112522`

## 2) После подключения VPN (финальный smoke)

- Запустить post-VPN проверку:
  - `.\scripts\post-vpn-smoke.ps1 -TestResultId 112522 -WaitSeconds 120`

## 3) Критерии успеха

- В `sync_queue` для `112522` статус уходит из `queued` в `done`.
- В `sync_audit` появляется запись с `success = true` и `response_status` 2xx.
- В Zephyr виден обновленный статус (145/146) и комментарий.

## 4) Если неуспех

- Если `sync_queue.status = dead_letter` или есть `last_error`:
  - посмотреть последние ошибки: `.\scripts\offline-sync-check.ps1 -TestResultId 112522`
  - проверить `sync_audit.response_body` и `response_status`
  - убедиться в корректности Zephyr токена/доступа из VPN
- После фикса:
  - перевести нужную запись обратно в `queued`
  - повторить `post-vpn-smoke.ps1`

## 5) После успешного smoke

- Вернуть рабочий интервал на 15 минут (если требуется для prod-режима).
- Зафиксировать результат в changelog/задаче:
  - timestamp проверки
  - `test_result_id`
  - итог `sync_queue`/`sync_audit`
