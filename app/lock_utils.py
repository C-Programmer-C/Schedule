import logging

from app.db_utils import db_connect

logger = logging.getLogger(__name__)

def unlock_task(task_id: int):
    conn = db_connect()
    try:
        conn.execute(
            "UPDATE active_tasks SET processing = 0, locked_at = NULL WHERE task_id = ?",
            (task_id,)
        )
        conn.commit()
        logger.info("Task %s has been unlocked.", task_id)
    finally:
        conn.close()