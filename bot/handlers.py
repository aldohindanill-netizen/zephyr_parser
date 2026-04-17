"""FSM handlers implementing the full Zephyr result submission flow.

Conversation flow:
  /start  →  choose folder  →  choose test run  →  choose test case
          →  choose status  →  add comment (optional)  →  confirm  →  done
"""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

import config
import zephyr

log = logging.getLogger(__name__)
router = Router()


# ---------------------------------------------------------------------------
# Redis session helpers
# ---------------------------------------------------------------------------

_redis: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            password=config.REDIS_PASSWORD,
            db=config.REDIS_DB,
            decode_responses=True,
        )
    return _redis


async def save_session(user_id: int, data: dict[str, Any]) -> None:
    await get_redis().set(
        f"session:{user_id}", json.dumps(data, ensure_ascii=False),
        ex=config.REDIS_SESSION_TTL,
    )


async def load_session(user_id: int) -> dict[str, Any]:
    raw = await get_redis().get(f"session:{user_id}")
    return json.loads(raw) if raw else {}


async def clear_session(user_id: int) -> None:
    await get_redis().delete(f"session:{user_id}")


# ---------------------------------------------------------------------------
# FSM states
# ---------------------------------------------------------------------------

class Form(StatesGroup):
    choose_folder = State()
    choose_run = State()
    choose_item = State()
    choose_status = State()
    enter_comment = State()
    confirm = State()


# ---------------------------------------------------------------------------
# Keyboard builders
# ---------------------------------------------------------------------------

def _kb(buttons: list[tuple[str, str]], cols: int = 1) -> InlineKeyboardMarkup:
    """Build an inline keyboard from [(label, callback_data), ...]."""
    rows = []
    row: list[InlineKeyboardButton] = []
    for label, data in buttons:
        row.append(InlineKeyboardButton(text=label, callback_data=data))
        if len(row) == cols:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------------------------------------------------------
# /start  →  choose folder
# ---------------------------------------------------------------------------

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await clear_session(message.from_user.id)
    await message.answer("Загружаю список папок…")

    try:
        folders = await zephyr.list_folders()
    except Exception as exc:
        log.exception("list_folders failed")
        await message.answer(f"Ошибка при загрузке папок:\n<code>{exc}</code>", parse_mode="HTML")
        return

    if not folders:
        await message.answer("Папки не найдены. Проверьте конфигурацию ZEPHYR_ROOT_FOLDER_IDS.")
        return

    buttons = [(f.name, f"folder:{f.id}:{f.name}") for f in folders]
    await message.answer("Выберите папку:", reply_markup=_kb(buttons))
    await state.set_state(Form.choose_folder)


# ---------------------------------------------------------------------------
# choose folder  →  choose test run
# ---------------------------------------------------------------------------

@router.callback_query(Form.choose_folder, F.data.startswith("folder:"))
async def on_folder(callback: CallbackQuery, state: FSMContext) -> None:
    _, folder_id, folder_name = callback.data.split(":", 2)
    await save_session(callback.from_user.id, {
        "folder_id": folder_id,
        "folder_name": folder_name,
    })
    await callback.message.edit_text(f"📁 {folder_name}\n\nЗагружаю тест-раны…")

    try:
        runs = await zephyr.list_runs(folder_id)
    except Exception as exc:
        log.exception("list_runs failed")
        await callback.message.edit_text(f"Ошибка:\n<code>{exc}</code>", parse_mode="HTML")
        return

    if not runs:
        await callback.message.edit_text("Тест-раны не найдены в этой папке.")
        await state.clear()
        return

    buttons = [(f"{r.name} ({r.status})", f"run:{r.id}:{r.name}") for r in runs]
    await callback.message.edit_text(
        f"📁 <b>{folder_name}</b>\n\nВыберите тест-ран:",
        reply_markup=_kb(buttons),
        parse_mode="HTML",
    )
    await state.set_state(Form.choose_run)
    await callback.answer()


# ---------------------------------------------------------------------------
# choose test run  →  choose test case
# ---------------------------------------------------------------------------

@router.callback_query(Form.choose_run, F.data.startswith("run:"))
async def on_run(callback: CallbackQuery, state: FSMContext) -> None:
    _, run_id, run_name = callback.data.split(":", 2)
    session = await load_session(callback.from_user.id)
    session.update({"run_id": run_id, "run_name": run_name})
    await save_session(callback.from_user.id, session)

    await callback.message.edit_text(
        f"📁 <b>{session['folder_name']}</b>\n"
        f"🔄 <b>{run_name}</b>\n\n"
        "Загружаю тест-кейсы…",
        parse_mode="HTML",
    )

    try:
        items = await zephyr.list_run_items(run_id)
    except Exception as exc:
        log.exception("list_run_items failed")
        await callback.message.edit_text(f"Ошибка:\n<code>{exc}</code>", parse_mode="HTML")
        return

    if not items:
        await callback.message.edit_text("Тест-кейсы не найдены в этом тест-ране.")
        await state.clear()
        return

    buttons = [
        (f"{i.case_key} — {i.case_name}", f"item:{i.item_id}:{i.case_key}:{i.case_name}")
        for i in items
    ]
    await callback.message.edit_text(
        f"📁 <b>{session['folder_name']}</b> › 🔄 <b>{run_name}</b>\n\n"
        "Выберите тест-кейс:",
        reply_markup=_kb(buttons),
        parse_mode="HTML",
    )
    await state.set_state(Form.choose_item)
    await callback.answer()


# ---------------------------------------------------------------------------
# choose test case  →  choose status
# ---------------------------------------------------------------------------

@router.callback_query(Form.choose_item, F.data.startswith("item:"))
async def on_item(callback: CallbackQuery, state: FSMContext) -> None:
    parts = callback.data.split(":", 3)
    _, item_id, case_key, case_name = parts
    session = await load_session(callback.from_user.id)
    session.update({"item_id": item_id, "case_key": case_key, "case_name": case_name})
    await save_session(callback.from_user.id, session)

    try:
        statuses = await zephyr.list_statuses()
    except Exception as exc:
        log.exception("list_statuses failed")
        await callback.message.edit_text(f"Ошибка:\n<code>{exc}</code>", parse_mode="HTML")
        return

    buttons = [(s.name, f"status:{s.id}:{s.name}") for s in statuses]
    await callback.message.edit_text(
        f"📁 <b>{session['folder_name']}</b> › 🔄 <b>{session['run_name']}</b>\n"
        f"🧪 <b>{case_key}</b> — {case_name}\n\n"
        "Выберите статус:",
        reply_markup=_kb(buttons, cols=2),
        parse_mode="HTML",
    )
    await state.set_state(Form.choose_status)
    await callback.answer()


# ---------------------------------------------------------------------------
# choose status  →  enter comment (or skip)
# ---------------------------------------------------------------------------

@router.callback_query(Form.choose_status, F.data.startswith("status:"))
async def on_status(callback: CallbackQuery, state: FSMContext) -> None:
    _, status_id, status_name = callback.data.split(":", 2)
    session = await load_session(callback.from_user.id)
    session.update({"status_id": status_id, "status_name": status_name})
    await save_session(callback.from_user.id, session)

    await callback.message.edit_text(
        f"📁 <b>{session['folder_name']}</b> › 🔄 <b>{session['run_name']}</b>\n"
        f"🧪 <b>{session['case_key']}</b>\n"
        f"✅ Статус: <b>{status_name}</b>\n\n"
        "Добавьте комментарий (или /skip чтобы пропустить):",
        parse_mode="HTML",
    )
    await state.set_state(Form.enter_comment)
    await callback.answer()


@router.message(Form.enter_comment, Command("skip"))
async def on_comment_skip(message: Message, state: FSMContext) -> None:
    await _show_confirm(message, state, comment=None)


@router.message(Form.enter_comment, F.text)
async def on_comment_text(message: Message, state: FSMContext) -> None:
    await _show_confirm(message, state, comment=message.text.strip())


async def _show_confirm(message: Message, state: FSMContext, comment: str | None) -> None:
    session = await load_session(message.from_user.id)
    session["comment"] = comment
    await save_session(message.from_user.id, session)

    comment_line = f"💬 Комментарий: {comment}" if comment else "💬 Комментарий: —"
    text = (
        f"<b>Подтвердите отправку результата:</b>\n\n"
        f"📁 Папка: {session['folder_name']}\n"
        f"🔄 Тест-ран: {session['run_name']}\n"
        f"🧪 Кейс: {session['case_key']} — {session['case_name']}\n"
        f"✅ Статус: {session['status_name']}\n"
        f"{comment_line}"
    )
    kb = _kb([("✅ Отправить в Zephyr", "confirm:yes"), ("❌ Отмена", "confirm:no")])
    await message.answer(text, reply_markup=kb, parse_mode="HTML")
    await state.set_state(Form.confirm)


# ---------------------------------------------------------------------------
# confirm  →  upload to Zephyr
# ---------------------------------------------------------------------------

@router.callback_query(Form.confirm, F.data == "confirm:yes")
async def on_confirm_yes(callback: CallbackQuery, state: FSMContext) -> None:
    session = await load_session(callback.from_user.id)
    await callback.message.edit_text("Отправляю результат в Zephyr…")

    try:
        await zephyr.upload_result(
            test_run_id=session["run_id"],
            item_id=session["item_id"],
            status_id=session["status_id"],
            comment=session.get("comment"),
        )
    except Exception as exc:
        log.exception("upload_result failed")
        await callback.message.edit_text(
            f"❌ Ошибка при отправке:\n<code>{exc}</code>\n\n"
            "Попробуйте снова /start",
            parse_mode="HTML",
        )
        await state.clear()
        return

    await callback.message.edit_text(
        f"✅ <b>Результат успешно загружен в Zephyr!</b>\n\n"
        f"📁 {session['folder_name']}\n"
        f"🔄 {session['run_name']}\n"
        f"🧪 {session['case_key']} — {session['status_name']}\n\n"
        "Для нового ввода — /start",
        parse_mode="HTML",
    )
    await clear_session(callback.from_user.id)
    await state.clear()
    await callback.answer()


@router.callback_query(Form.confirm, F.data == "confirm:no")
async def on_confirm_no(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.message.edit_text("Отменено. Для нового ввода — /start")
    await clear_session(callback.from_user.id)
    await state.clear()
    await callback.answer()
