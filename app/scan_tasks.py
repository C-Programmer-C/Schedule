import logging
from conf.logging_config import conf_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

from app.db_utils import db_connect, fetch_candidates, try_lock_task
from app.process_task import process_task
from app.pyrus_api import get_token
from app.utils import now_utc, to_iso
from conf.config import settings

conf_logger()
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)


def recover_stale_locks():
    """Recovers stale task locks."""
    expiry = now_utc() - timedelta(minutes=settings.LOCK_EXPIRY_MINUTES)
    conn = db_connect()
    try:
        cur = conn.execute(
            "SELECT task_id FROM active_tasks WHERE processing = 1 AND locked_at <= ?",
            (to_iso(expiry),),
        )
        stale = [r["task_id"] for r in cur.fetchall()]
        if stale:
            logger.info("Recovering stale locks for tasks: %s", stale)
            conn.executemany(
                "UPDATE active_tasks SET processing = 0, locked_at = NULL WHERE task_id = ?",
                [(tid,) for tid in stale],
            )
            conn.commit()
    finally:
        conn.close()


def scanner_job():
    """
    The main scanner job, executed on a schedule.
    Fetches tasks from the database and submits them for processing.
    """
    try:
        auth_token = get_token(settings.LOGIN, settings.SECURITY_KEY)
        logger.debug("Token successfully received.")
    except Exception:
        logger.exception("Failed to get access token.")
        return

    try:
        recover_stale_locks()
        candidates = fetch_candidates(settings.LIMIT_PROCESS_TASKS)
        if not candidates:
            logger.debug("No tasks found for processing.")
            return

        futures = {}
        for task_id in candidates:
            if try_lock_task(task_id):
                fut = executor.submit(process_task, task_id, auth_token)
                futures[fut] = task_id
            else:
                logger.info(
                    "Task #%s is already being processed while trying to lock.", task_id
                )

        for fut in as_completed(futures):
            tid = futures[fut]
            try:
                fut.result()
                logger.info("Task #%s finished successfully.", tid)
            except Exception:
                logger.exception("Error during processing of task #%s.", tid)
    except Exception:
        logger.exception("Failed to search for tasks.")
