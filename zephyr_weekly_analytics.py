"""Страница weekly analytics: реэкспорт функций из zephyr_weekly_report.

Тонкая обёртка для отдельного импорта HTML/wiki отчёта аналитики по циклам.
"""

from __future__ import annotations

from zephyr_weekly_report import (
    _export_weekly_analytics_enabled,
    _weekly_analytics_trend_data,
    _weekly_cycle_matrix_data_rolling,
    _weekly_report_include_analytics_enabled,
    render_weekly_analytics_html,
    render_weekly_analytics_wiki,
    write_weekly_analytics_reports,
)

__all__ = [
    "render_weekly_analytics_html",
    "render_weekly_analytics_wiki",
    "write_weekly_analytics_reports",
    "_weekly_analytics_trend_data",
    "_weekly_cycle_matrix_data_rolling",
    "_weekly_report_include_analytics_enabled",
    "_export_weekly_analytics_enabled",
]
