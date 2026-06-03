# Переводы docstring (EN -> RU) для zephyr_weekly_report.py
EN_TO_RU: dict[str, str] = {
    "Lazy-create Confluence week folder pages (Week wNN) under the root parent.": (
        "Ленивое создание недельных папок Confluence (Week wNN) под корневым родителем."
    ),
    "Return True if this process holds the lock; False if another instance is running.": (
        "True, если процесс удерживает lock; False, если уже работает другой экземпляр."
    ),
    "Pick latest Jira issue among those that have build in description point A.": (
        "Выбрать последний Jira issue с билдом в точке A описания."
    ),
    "Return {'name': str, 'effective_date': date|None, 'week_start': date|None}.": (
        "Вернуть dict с name, effective_date и week_start для ветки A/B теста."
    ),
    "Return key/value pairs from repo ``.env`` (last assignment per key wins).": (
        "Пары ключ/значение из ``.env`` репозитория (последнее присваивание побеждает)."
    ),
    "Apply ZEPHYR_CONFLUENCE_TITLE_PREFIX so HTML <title> cannot bypass the prefix.": (
        "Применить ZEPHYR_CONFLUENCE_TITLE_PREFIX к заголовку страницы Confluence."
    ),
    "When false, only match pages that are direct children of the target parent.": (
        "Если false — искать страницы только среди прямых детей целевого родителя."
    ),
    "Load persisted rollup snapshot or return an empty payload.": (
        "Загрузить сохранённый snapshot rollup или вернуть пустую структуру."
    ),
    "Merge two analytics dicts; per bug×build cell counts use max (idempotent re-runs).": (
        "Слить два analytics; в ячейке баг×билд — max (идемпотентные перезапуски)."
    ),
    "Merge current run analytics into the persisted all-time snapshot.": (
        "Влить analytics текущего прогона в all-time snapshot."
    ),
    "Seed snapshot keys from on-disk build logs and duplicate_rollup_keys.json.": (
        "Заполнить ключи snapshot из build_log на диске и duplicate_rollup_keys.json."
    ),
    "When snapshot has no keys, seed base state from on-disk build logs.": (
        "Если в snapshot нет ключей — инициализировать из build_log на диске."
    ),
    "Order build columns by calendar day so backfills do not scramble matrix/hot-bugs.": (
        "Упорядочить колонки билдов по календарному дню (для matrix и hot_bugs)."
    ),
    "Refresh totals_by_build and hot_bugs from matrix after a merge.": (
        "Пересчитать totals_by_build и hot_bugs из matrix после merge."
    ),
    "Collect Jira keys for rollup metadata: rolling window + persisted all-time snapshot.": (
        "Собрать Jira-ключи для rollup: скользящее окно + all-time snapshot."
    ),
    "Write a single bugs index page (last N weeks + all time) in weekly defect format.": (
        "Записать индекс багов (последние N недель + all time) в формате weekly defect."
    ),
    "Batch-fetch Jira issue metadata for the given keys.": (
        "Пакетно загрузить метаданные Jira issue для списка ключей."
    ),
    "Aggregate per-bug analytics across the week.": (
        "Агрегировать analytics по багам за неделю."
    ),
    "Write to multiple text streams (console + log file).": (
        "Писать в несколько потоков (консоль и файл лога)."
    ),
}
