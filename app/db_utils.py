import logging
from datetime import datetime, timezone, timedelta, time
from typing import List
from zoneinfo import ZoneInfo

from app.db_connect import db_connect
from app.pyrus_api import remove_bot_from_subscribers
from app.utils import now_utc, to_iso

from conf.config import settings

logger = logging.getLogger(__name__)

def init_db():
    conn = db_connect()
    try:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS active_tasks (
            task_id INTEGER PRIMARY KEY,
            due TEXT NOT NULL,
            next_run_at TEXT NOT NULL,
            processing INTEGER DEFAULT 0,
            locked_at TEXT,
            step INTEGER DEFAULT 1
        )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_next_run ON active_tasks(next_run_at)")
        conn.commit()
    finally:
        conn.close()


def cleanup_task(task_id: int, token: str, reason: str):
    delete_task(task_id)
    remove_bot_from_subscribers(task_id, token)
    logger.info("Task %s removed from DB and unsubscribed (reason: %s).", task_id, reason)

def insert_task(task_id: str, due_iso: str, next_run: str):
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO active_tasks (task_id, due, next_run_at, processing, step) VALUES (?, ?, ?, 0, 1)",
            (task_id, due_iso, next_run)
        )
        conn.commit()
    finally:
        conn.close()

def has_task(task_id: int) -> bool:
    """Проверяет, есть ли task_id в active_tasks."""
    conn = db_connect()
    try:
        cur = conn.execute(
            "SELECT 1 FROM active_tasks WHERE task_id = ? LIMIT 1",
            (task_id,)
        )
        return bool(cur.fetchone())
    finally:
        conn.close()

def parse_iso_to_utc(s: str) -> datetime:
    """Парсит ISO-строку в tz-aware UTC datetime.
    Возвращает datetime в UTC или бросает ValueError/TypeError при некорректном формате.
    Поддерживает строки с 'Z', с '+00:00' и без указания зоны (в этом случае предполагается UTC).
    """
    if s is None:
        raise ValueError("empty datetime string")

    if isinstance(s, datetime):
        dt = s
    else:
        # fromisoformat не понимает 'Z' — заменяем
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Если нет смещения (наивная строка типа "2025-08-28T09:30:01"), считаем её UTC
        # Простая эвристика: если в строке нет '+' и нет '-' в части времени (последние 6 символов),
        # то добавим +00:00
        if ("+" not in s and "-" not in s[-6:]):
            s = s + "+00:00"

        dt = datetime.fromisoformat(s)  # может бросить ValueError

    # Сделаем aware и приведём к UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc) # type: ignore
    return dt.astimezone(timezone.utc)

def _parse_iso_to_utc(s: str) -> datetime:
    """Парсит ISO-строку в tz-aware UTC datetime.
    Возвращает datetime в UTC или бросает ValueError/TypeError при некорректном формате.
    Поддерживает строки с 'Z', с '+00:00' и без указания зоны (в этом случае предполагается UTC).
    """
    if s is None:
        raise ValueError("empty datetime string")

    if isinstance(s, datetime):
        dt = s
    else:
        # fromisoformat не понимает 'Z' — заменяем
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Если нет смещения (наивная строка типа "2025-08-28T09:30:01"), считаем её UTC
        # Простая эвристика: если в строке нет '+' и нет '-' в части времени (последние 6 символов),
        # то добавим +00:00
        if ("+" not in s and "-" not in s[-6:]):
            s = s + "+00:00"

        dt = datetime.fromisoformat(s)  # может бросить ValueError

    # Сделаем aware и приведём к UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc) # type: ignore
    return dt.astimezone(timezone.utc)


def fetch_candidates(limit: int = 100) -> List[int]:
    """
    Возвращает task_id задач ready к выполнению (dt <= now).
    Подход: берем небольшой запас строк из БД, парсим даты и фильтруем в Python.
    Это безопаснее при разнородных форматах, но медленнее, чем сравнение в SQL.
    """
    conn = db_connect()
    try:
        # берём с запасом — чтобы не сделать N запросов; tweak size при необходимости
        cur = conn.execute(
            "SELECT task_id, next_run_at FROM active_tasks WHERE processing = 0 ORDER BY next_run_at LIMIT ?",
            (limit * 5,)
        )
        rows = cur.fetchall()
        now = datetime.now(timezone.utc)
        out = []
        for r in rows:
            s = r["next_run_at"]
            try:
                dt = _parse_iso_to_utc(s)
            except (ValueError, TypeError):
                # можно логировать ошибку парсинга, но не ломать цикл
                continue

            if dt <= now:
                out.append(r["task_id"])
                if len(out) >= limit:
                    break

        return out
    finally:
        conn.close()

def try_lock_task(task_id: int) -> bool:
    """Атомарно пометить processing=1 и locked_at, если он был 0."""
    conn = db_connect()
    try:
        cur = conn.execute(
            "UPDATE active_tasks SET processing = 1, locked_at = ? WHERE task_id = ? AND processing = 0",
            (to_iso(now_utc()), task_id)
        )
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()

def delete_task(task_id: int):
    conn = db_connect()
    try:
        conn.execute("DELETE FROM active_tasks WHERE task_id = ?", (task_id,))
        conn.commit()
    finally:
        conn.close()

def bump_step_and_reschedule(task_id: int, step: int, tz_name: str = "Europe/Moscow"):
    """
    Обновляет step и ставит next_run_at на ближайший будущий 10:40 по tz_name (MSK по умолчанию).
    Всегда игнорирует любые относительные offsets — всегда назначаем 10:40 (сегодня или завтра).
    """
    conn = db_connect()
    try:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc

        now_local = datetime.now(tz)
        today_1040 = datetime.combine(now_local.date(), time(10, 40), tzinfo=tz)

        if now_local < today_1040:
            next_local = today_1040
        else:
            next_local = today_1040 + timedelta(days=1)

        # сохранить в UTC (как у вас было раньше)
        next_run_utc = next_local.astimezone(timezone.utc)

        conn.execute(
            "UPDATE active_tasks SET step=?, next_run_at = ?, processing = 0, locked_at = NULL WHERE task_id = ?",
            (step, to_iso(next_run_utc), task_id)
        )
        conn.commit()
    finally:
        conn.close()

def set_step(task_id: int, step: int):
    conn = db_connect()
    try:
        conn.execute("UPDATE active_tasks SET step = ? WHERE task_id = ?", (step, task_id))
        conn.commit()
    finally:
        conn.close()

def get_task_row(task_id: int):
    conn = db_connect()
    try:
        cur = conn.execute("SELECT * FROM active_tasks WHERE task_id = ?", (task_id,))
        return cur.fetchone()
    finally:
        conn.close()


def recover_stale_locks():
    expiry = now_utc() - timedelta(minutes=settings.LOCK_EXPIRY_MINUTES)
    conn = db_connect()
    try:
        cur = conn.execute("SELECT task_id FROM active_tasks WHERE processing = 1 AND locked_at <= ?", (to_iso(expiry),))
        stale = [r["task_id"] for r in cur.fetchall()]
        if stale:
            logger.info("Recovering stale locks for tasks: %s", stale)
            conn.executemany("UPDATE active_tasks SET processing = 0, locked_at = NULL WHERE task_id = ?", [(tid,) for tid in stale])
            conn.commit()
    finally:
        conn.close()