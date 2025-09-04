import logging
from app.db_utils import delete_task
from app.pyrus_api import remove_bot_from_subscribers

logger = logging.getLogger(__name__)

def cleanup_task(task_id: int, token: str, reason: str):
    delete_task(task_id)
    remove_bot_from_subscribers(task_id, token)
    logger.info("Task %s removed from DB and unsubscribed (reason: %s).", task_id, reason)