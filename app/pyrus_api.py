import functools
import logging
import time
from typing import Type, List
import requests
from app.lock_utils import unlock_task
from conf.config import settings
from app.utils import build_mention_span, collect_manager_mentions, collect_manager_ids

AUTH_URL = "https://accounts.pyrus.com/api/v4/auth"


logger = logging.getLogger(__name__)

class APIError(RuntimeError):
    """Ошибка при получении токена."""

def retry_on_exception(tries: int = 2,
                       delay: float = 30.0,
                       exceptions: tuple[Type[BaseException], ...] = (Exception,),
                       unlock_on_fail: bool = False):
    """
    Декоратор: выполнить функцию tries раз, если она падает одним из exceptions.
    Между попытками ждать delay секунд.
    Если unlock_on_fail=True, то после всех неудачных попыток вызвать unlock_task(task_id).
    """
    if tries < 1:
        raise ValueError(f"Было получено {tries} попыток, когда их должно быть >= 1.")

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, tries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    logger.warning(f"Attempt {attempt}/{tries} failed for {func.__name__}: {e!r}")
                    if attempt < tries:
                        time.sleep(delay)

            # здесь все попытки провалились
            if unlock_on_fail:
                task_id = kwargs.get("task_id") or (args[0] if args else None)
                if task_id is not None:
                    try:
                        unlock_task(task_id)
                        logger.info("Task %s has been unlocked after all retries failed.", task_id)
                    except Exception as ue:
                        logger.error("Failed to unlock task %s after retries: %s", task_id, ue)
            if last_exc:
                raise last_exc
        return wrapper
    return decorator


def build_comments_api_url(task_id):
    return f"https://api.pyrus.com/v4/tasks/{task_id}/comments"

def build_task_api_url(task_id):
    return f"https://api.pyrus.com/v4/tasks/{task_id}"

def build_member_api_url(task_id):
    return f"https://api.pyrus.com/v4/members/{task_id}"

def parse_json_response(resp: requests.Response, context: str = "") -> dict:
    try:
        return resp.json()
    except ValueError as e:
        snippet = resp.text[:300].replace("\n", " ")
        msg = f"Couldn't parse the JSON in the response {context or 'API'}: {resp.status_code} {snippet}"
        raise RuntimeError(msg) from e


def get_task(task_id: int, token: str, timeout: int = 30, check: bool = False):
    """
    Получить задачу по task_id.

    :return:
        - словарь задачи
        - при check=True: True/False/None
            True  → задача есть
            False → задача удалена/доступ запрещён (403)
            None  → ошибка сети / неизвестно
    """
    url = build_task_api_url(task_id)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except requests.HTTPError as e:
        if e.response.status_code == 403:
            return False if check else None
        if check:
            return None
        raise APIError(f"Couldn't get task #{task_id}: {e}") from e
    except requests.RequestException as e:
        logger.warning("Network error while getting task %s: %s", task_id, e)
        if check:
            return None
        raise APIError(f"Couldn't get task #{task_id}: {e}")

    data = parse_json_response(resp, context="task")
    task = data.get("task")
    error_msg = data.get("error", "")

    if check:
        if "access_denied_task" in error_msg.lower():
            return False
        return bool(task)

    if task:
        return task

    if "access_denied_task" in error_msg.lower():
        return None

    raise RuntimeError(f"Failed to parse task #{task_id}: {data}")

@retry_on_exception(tries=3, delay=30,
                    exceptions=(RuntimeError, requests.RequestException), unlock_on_fail=True)
def remove_bot_from_subscribers(task_id: int, token: str, timeout: int = 30):
    headers = {"Authorization": f"Bearer {token}"}
    url = build_comments_api_url(task_id)
    bot_id = settings.BOT_ID
    body = {
    "subscribers_removed": [
        {
            "id": bot_id
        }
    ]
    }
    try:
        resp = requests.post(url, headers=headers, timeout=timeout, json=body)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise APIError(f"Couldn't removed bot from subscribers for the issue #{task_id}: {e}") from e

    data = parse_json_response(resp, context="comments")

    if "task" in data and data["task"]:
        logger.info("bot successfully removed from subscribers.")
        return True

    raise APIError(f"Couldn't send comment: invalid API response #{task_id}: {data}")

@retry_on_exception(tries=3, delay=30,
                    exceptions=(RuntimeError, requests.RequestException), unlock_on_fail=True)
def bot_is_subscriber(task_id: int, token: str, timeout: int = 30) -> bool:
    task = get_task(task_id, token, timeout)
    if not isinstance(task, dict):
        logger.warning("Could not retrieve task %s or task is not a dictionary.", task_id)
        raise APIError(f"Could not retrieve task details for task #{task_id}")
    
    subscribers = task.get("subscribers", [])
    if not subscribers:
        logger.warning("Task #%s has no subscribers: %s", task_id, task)
        raise APIError(f"The API response does not contain subscribers for task #{task_id}")

    for subscriber in subscribers:
        person_id = subscriber.get("person", {}).get("id")
        if person_id == settings.BOT_ID:
            logger.info("Bot is a subscriber for task %s", task_id)
            return True

    logger.info("Bot is NOT a subscriber for task %s", task_id)
    return False


@retry_on_exception(tries=3, delay=30,
                    exceptions=(RuntimeError, requests.RequestException))
def get_token(login: str, security_key: str, timeout: int = 30) -> str:
    payload = {"login": login, "security_key": security_key}

    try:
        resp = requests.post(AUTH_URL, json=payload, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise APIError(f"Couldn't get a token: {e}") from e

    data = parse_json_response(resp, context="auth")

    token = data.get("access_token")

    if not token:
        raise APIError(f"The response does not contain a token: {data}")
    return token


@retry_on_exception(tries=3, delay=30.0,
                    exceptions=(APIError, requests.RequestException), unlock_on_fail=True)
def is_task_closed(task_id: int, token: str, timeout: int = 30) -> bool:
    task = get_task(task_id, token, timeout)
    if not isinstance(task, dict):
        logger.warning(f"Could not retrieve task #{task_id} or task is not a dictionary.")
        raise APIError(f"Could not retrieve task details for task #{task_id}")
    return bool(task.get("close_date") or task.get("is_closed"))

@retry_on_exception(
    tries=3,
    delay=30.0,
    exceptions=(APIError, requests.RequestException),
    unlock_on_fail=True
)
def get_member(task_id: int, token: str, timeout: int = 30) -> dict:
    """Получить информацию о сотруднике по ID задачи."""
    url = build_member_api_url(task_id)
    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise APIError(f"Couldn't get an employee #{task_id}: {e}") from e

    data = parse_json_response(resp, context="member")

    user_id = data.get("id")
    first_name = data.get("first_name")
    last_name = data.get("last_name")

    if not user_id:
        raise APIError(f"The API response in task #{task_id} does not contain the employee's 'id'.: {data}")

    fullname = " ".join(filter(None, [first_name, last_name]))
    if not fullname:
        raise APIError(f"The API response in task #{task_id} does not contain the employee's full name: {data}")

    return {
        "id": user_id,
        "fullname": fullname
    }


@retry_on_exception(
    tries=3,
    delay=30.0,
    exceptions=(APIError, requests.RequestException),
    unlock_on_fail=True
)
def get_due(task_id: int, token: str, timeout: int = 30):
    """Получить срок выполнения задачи (due) по её ID."""
    task = get_task(task_id, token, timeout)
    if not isinstance(task, dict):
        logger.warning("Could not retrieve task %s or task is not a dictionary.", task_id)
        raise APIError(f"Could not retrieve task details for task #{task_id}")
    due = task.get("due")
    if not due:
        raise APIError(f"Couldn't get the deadline for the issue #{task_id}: {task}")

    return due

@retry_on_exception(
    tries=3,
    delay=30.0,
    exceptions=(APIError, requests.RequestException),
    unlock_on_fail=True
)
def get_responsible(task_id: int, token: str, timeout: int = 30) -> dict:
    """Получить информацию об ответственном сотруднике по задаче."""
    task = get_task(task_id, token, timeout)
    if not isinstance(task, dict):
        logger.warning("Could not retrieve task %s or task is not a dictionary.", task_id)
        raise APIError(f"Could not retrieve task details for task #{task_id}")
    responsible = task.get("responsible")
    if not responsible:
        raise APIError(f"The API response does not contain the person responsible for the task #{task_id}: {task}")

    user_id = responsible.get("id")
    first_name = responsible.get("first_name")
    last_name = responsible.get("last_name")

    if not user_id:
        raise APIError(f"The API response in task #{task_id} does not contain the employee's 'id'.: {responsible}")

    fullname = " ".join(filter(None, [first_name, last_name]))
    if not fullname:
        raise APIError(f"The API response in task #{task_id} does not contain the employee's full name: {responsible}")

    return {
        "id": user_id,
        "fullname": fullname
    }

@retry_on_exception(tries=3, delay=30,
                    exceptions=(RuntimeError, requests.RequestException), unlock_on_fail=True)
def add_managers_to_subscribers(task_id: int, token: str, ids_approvals: List[dict[str, int]], timeout: int = 30):
    headers = {"Authorization": f"Bearer {token}"}
    url = build_comments_api_url(task_id)

    body = {
        "subscribers_added": ids_approvals
    }
    try:
        resp = requests.post(url, headers=headers, timeout=timeout, json=body)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise APIError(f"Couldn't add managers_to_subscribers for the issue #{task_id}: {e}") from e

    data = parse_json_response(resp, context="comments")

    if "task" in data and data["task"]:
        logger.info(f"managers successfully added to subscribers in task #{task_id}.")
        return True

    raise APIError(f"Couldn't add managers: invalid API response #{task_id}: {data}")

@retry_on_exception(tries=3, delay=30,
                    exceptions=(RuntimeError, requests.RequestException), unlock_on_fail=True)
def update_client(parent_task_id: int, token: str, task_id: int, timeout: int = 30):
    headers = {"Authorization": f"Bearer {token}"}
    url = build_comments_api_url(task_id)
    
    body = {
        "field_updates": [
            {
                "id": settings.CLIENT_FIELD_ID,
                "value": parent_task_id
            }
        ]
    }
    
    try:
        resp = requests.post(url, headers=headers, timeout=timeout, json=body)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise APIError(f"Couldn't update client for the issue #{task_id}: {e}") from e
    
    data = parse_json_response(resp, context="comments")
    if "task" in data and data["task"]:
        logger.info(f"client successfully updated in task #{task_id}.")
        return True
    
    raise APIError(f"Couldn't update client: invalid API response #{task_id}: {data}")
    
@retry_on_exception(
    tries=3,
    delay=30.0,
    exceptions=(APIError, requests.RequestException),
    unlock_on_fail=True
)
def send_comment(token: str, task_id: int, text: str, members_info: dict, timeout: int = 30) -> bool:
    """Отправить комментарий в задачу с упоминанием сотрудника."""
    headers = {"Authorization": f"Bearer {token}"}
    url = build_comments_api_url(task_id)

    manager_mentions = None

    managers_info = members_info.get("manager") or {}

    if managers_info:

        managers_ids = collect_manager_ids(managers_info)

        if not managers_ids:
            raise APIError(f"managers_ids list is empty for the task #{task_id}")

        res = add_managers_to_subscribers(task_id, token, managers_ids)

        if not res:
            raise APIError(f"Request is not done in the issue #{task_id}")

        manager_mentions = collect_manager_mentions(managers_info)

    user_info = members_info.get("user") or {}

    user_id = user_info.get("id") or members_info.get("id")
    user_fullname = user_info.get("fullname") or members_info.get("fullname")

    if not user_id or not user_fullname:
        raise APIError(f"Information about the user is missing in the issue #{task_id}")

    user_mention = build_mention_span(user_id, user_fullname)

    mentions_part = ", ".join([user_mention] + manager_mentions) if manager_mentions else user_mention

    formatted_text = f"{mentions_part}, {text}"

    body = {"formatted_text": formatted_text}
    if not formatted_text:
        raise RuntimeError(f"An error occurred when forming the request body for creating a comment in the issue. #{task_id}")

    try:
        resp = requests.post(url, headers=headers, timeout=timeout, json=body)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise APIError(f"Couldn't send a comment for the issue #{task_id}: {e}") from e

    data = parse_json_response(resp, context="comments")

    if "task" in data and data["task"]:
        return True

    raise APIError(f"Couldn't send comment: invalid API response #{task_id}: {data}")