import logging
from app.pyrus_api import APIError, get_token, update_client
from app.utils import check_client, log_and_abort
from conf.config import settings

logger = logging.getLogger(__name__)

def set_user_to_task(task: dict, task_id: int):
    try:
        fields = task.get("fields")
        if not fields:
            return log_and_abort("fields not found")
        
        has_client = check_client(fields)
        if has_client:
            logger.warning(f"client already is existing in task #{task_id}")
            return
        parent_task_id = task.get("parent_task_id")
        if not parent_task_id:
            logger.warning(f"parent_task_id is missing in task #{task_id}")
            return
    except Exception:
        
        raise
    
    try:
        token = get_token(settings.LOGIN_ADNIN, settings.SECURITY_KEY_ADMIN)
    except APIError:
        raise
    
    try:
        update_client(parent_task_id, token, task_id)
    except APIError:
        raise
    