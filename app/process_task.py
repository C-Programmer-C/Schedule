import logging

from app.lock_utils import unlock_task
from app.pyrus_api import get_responsible, get_member, bot_is_subscriber, remove_bot_from_subscribers, get_task, \
    APIError
from app.texts import Texts
from app.db_utils import cleanup_task
from conf.config import settings
from app.db_utils import delete_task, get_task_row, bump_step_and_reschedule
from app.pyrus_api import send_comment, is_task_closed


logger = logging.getLogger(__name__)

def process_task(task_id: int, token: str):
    logger.info("Worker picked task %s", task_id)

    row = get_task_row(task_id)
    if not row:
        logger.info("Task %s deleted remotely.", task_id)
        return

    try:
        task_exists = get_task(task_id, token, check=True)

        if task_exists is False:
            delete_task(task_id)
            logger.info("task %s not found (deleted remotely), removed from DB.", task_id)
            return

        if task_exists is None:
            unlock_task(task_id)
            logger.info("task %s check skipped due to network error.", task_id)
            return

        if is_task_closed(task_id, token) or not bot_is_subscriber(task_id, token):
            cleanup_task(task_id, token, reason="Task closed or bot not subscribed")
            return

        step = row["step"] or 0
        logger.debug("Task %s current step=%s", task_id, step)

        if step in (1, 2, 3):
            user_info = get_responsible(task_id, token)
            send_comment(token, task_id, Texts.TEXT_TO_EMPLOYEE, user_info)
            bump_step_and_reschedule(task_id, step + 1)
            return

        if step == 4:
            first_manager_info = get_member(settings.FIRST_MANAGER_ID, token)
            second_manager_info = get_member(settings.SECOND_MANAGER_ID, token)
            if not first_manager_info or not second_manager_info:
                raise APIError("Manager info not found")

            manager_info = {
                "first_manager": first_manager_info,
                "second_manager": second_manager_info
            }
            user_info = get_responsible(task_id, token)
            send_comment(token, task_id, Texts.TEXT_TO_EMPLOYEE_WITH_MANAGER,
                         {"manager": manager_info, "user": user_info})
            remove_bot_from_subscribers(task_id, token)
            delete_task(task_id)
            logger.info("Task %s processed with manager and deleted from DB (final step).", task_id)
            return

    except Exception:
        logger.exception("Unhandled error while processing task %s", task_id)