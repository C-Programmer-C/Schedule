import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Union, Any
from conf.config import settings
from flask import jsonify

logger = logging.getLogger(__name__)

def now_utc():
    return datetime.now(timezone.utc)

def check_client(fields: Iterable[Mapping[str, Any]],
                 client_field_id: int = settings.CLIENT_FIELD_ID) -> bool:
    """
    Вернёт True, если среди полей есть поле с id == client_field_id,
    у которого в value присутствует непустой task_id.
    Иначе вернёт False.
    """
    for field in fields:
        if not isinstance(field, Mapping):
            continue

        if field.get("id") != client_field_id:
            continue

        value = field.get("value") or {}
        if not isinstance(value, Mapping):
            continue

        task_id = value.get("task_id")
        if task_id:
            return True
    return False
                
            
def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def parse_iso_or_date(due: Union[str, datetime]) -> Optional[datetime]:
    """Разбирает строку ISO или YYYY-MM-DD в aware datetime (UTC)."""
    if not due:
        return None

    if isinstance(due, datetime):
        dt = due
    else:
        s = due.strip()
        # fromisoformat не принимает 'Z', заменим его
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            # пробуем только дату YYYY-MM-DD
            dt = datetime.strptime(s, "%Y-%m-%d")
    # Сделаем aware и переведём в UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def create_iso_date_with_duration(dt: str, duration_minutes) -> str:
    due_date = datetime.fromisoformat(dt)
    return (due_date + timedelta(minutes=duration_minutes)).isoformat()


def add_interval_to_due(
    due: Union[str, datetime],
    days: int = 0,
    hours: int = 0,
    minutes: int = 0,
    seconds: int = 0,
) -> Optional[str]:
    """
    Прибавляет интервал к due и возвращает ISO строку в UTC.
    Возвращает None если due пустой/None.
    """
    dt = parse_iso_or_date(due)
    if dt is None:
        return None

    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    new_dt = dt + delta
    # Возвращаем ISO с +00:00
    return new_dt.astimezone(timezone.utc).isoformat()


def normalize_due(due: str) -> Optional[str]:
    """
    Преобразует due в ISO формат с временем в UTC.
    
    Поддерживает:
    - только дату "YYYY-MM-DD" -> добавляется 00:00:00
    - ISO с временем "YYYY-MM-DDTHH:MM:SS" или с часовым поясом
    
    Возвращает ISO строку в UTC или None, если due пустой.
    """
    if not due:
        return None

    try:
        # Обработка ISO формата с Z или с часовым поясом
        if due.endswith("Z"):
            dt = datetime.fromisoformat(due.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(due)
    except ValueError:
        # Если не ISO, пробуем как YYYY-MM-DD
        dt = datetime.strptime(due, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)

    # Переводим в UTC, если есть tzinfo
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)

    return dt.isoformat()


def last_comment_has_bot(comments: List[Dict]) -> bool:
    if not comments:
        return False
    bot_id = settings.BOT_ID
    last_comment = comments[-1]
    subscribers = last_comment.get("subscribers_added") or []
    return any(sub.get("id") == bot_id for sub in subscribers if isinstance(sub, dict))


def log_and_abort(message, task_id=None, code=400):
    logger.warning(f"task {task_id} {message}.")
    return jsonify({"error": message}), code


def build_mention_span(person_id: int, fullname: str) -> str:
    """Построить span-упоминание для человека."""
    return (
        f'<span data-personid="{person_id}" data-type="user-mention">{fullname}</span>'
    )
def collect_manager_ids(managers_info: dict) -> List[dict]:
    """
    Собрать поле id у каждого менеджера.
    Возвращает список id менеджеров.
    """
    if not managers_info:
        return []
    first_manager_info = managers_info.get("first_manager")
    second_manager_info = managers_info.get("second_manager")
    if not first_manager_info or not second_manager_info:
        return []
    first_manager_id = first_manager_info.get("id")
    second_manager_id = second_manager_info.get("id")

    if not isinstance(first_manager_id, int) or not isinstance(second_manager_id, int):
        return []

    return [{"id": i} for i in (first_manager_id, second_manager_id) if i is not None]


def collect_manager_mentions(members_info: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Собрать упоминания для первого и второго менеджера (если есть и полны данные).
    Возвращает список упоминаний (может быть пустым, 1 или 2 элемента).
    """
    mentions: List[str] = []
    for key in ("first_manager", "second_manager"):
        mgr = members_info.get(key) or {}
        mgr_id = mgr.get("id")
        mgr_fullname = mgr.get("fullname")
        if mgr_id and mgr_fullname:
            mentions.append(build_mention_span(mgr_id, mgr_fullname))
    return mentions


def parse_and_compare_due(task_id: str, new_due: str, due: str) -> bool:
    """
    Преобразует даты в datetime и сравнивает их.

    :param task_id: ID задачи для логирования
    :param new_due: новая дата из API или задачи
    :param due: дата из базы
    :return: True, если даты есть и отличаются, False иначе
    """
    try:
        new_due_dt = datetime.fromisoformat(new_due) if new_due else None
        due_dt = datetime.fromisoformat(due) if due else None
    except ValueError as e:
        logger.exception(f"Invalid date format for task #{task_id}: {e}")
        return False

    if new_due_dt and due_dt and new_due_dt != due_dt:
        logger.info(f"Due date differs for task #{task_id}.")
        return True

    return False
