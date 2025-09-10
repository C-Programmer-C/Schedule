import logging
from app.pyrus_api import APIError, get_token, update_client
from app.utils import check_client, log_and_abort
from conf.config import settings

logger = logging.getLogger(__name__)

def set_user_to_task(parent_task_id: int, task_id: int):

    try:
        token = get_token(settings.LOGIN_ADNIN, settings.SECURITY_KEY_ADMIN)
    except APIError:
        raise
    
    try:
        update_client(parent_task_id, token, task_id)
    except APIError:
        raise
    