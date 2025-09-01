import re
from flask import request
from app.utils import log_and_abort

def verify_signature(body: bytes, signature: str) -> bool:
    """
    Проверка HMAC-SHA1: Pyrus присылает X-Pyrus-Sig = HMAC-SHA1(secret + body)
    """
    mac = hmac.new(settings.SECURITY_KEY.encode(), body, hashlib.sha1)
    return hmac.compare_digest(mac.hexdigest(), signature)


def validate_pyrus_request(request, secret):
    """
    Проверяет User-Agent, X-Pyrus-Sig и X-Pyrus-Retry.
    В случае успеха возвращает сырые байты тела (request.get_data(cache=True)).
    В случае ошибки вызывает log_and_abort(msg) и возвращает его результат.
    Ожидается, что log_and_abort формирует и возвращает корректный Flask-ответ.
    """
    # raw body — обязательно именно байты, с кэшем, чтобы потом можно было request.get_json()
    raw = request.get_data(cache=True)

    # 1) User-Agent: Pyrus-Bot-4
    ua = request.headers.get('User-Agent', '')
    m = re.match(r'^Pyrus-Bot-(d+)$', ua)
    if not m:
        return log_and_abort("invalid user agent")
    if int(m.group(1)) != 4:
        return log_and_abort("unsupported Pyrus API version")

    # 2) X-Pyrus-Sig: подпись (возможный формат "sha1=...") и её проверка
    sig = request.headers.get('X-Pyrus-Sig', '')
    if not sig:
        return log_and_abort("missing signature")
    # убрать возможный префикс "sha1="
    if sig.startswith('sha1='):
        sig = sig.split('=', 1)[1]

    if not _is_signature_correct(raw, secret, sig):
        return log_and_abort("invalid signature")

    # 3) X-Pyrus-Retry: одно из "1/3", "2/3", "3/3"
    retry = request.headers.get('X-Pyrus-Retry', '')
    if retry not in ALLOWED_RETRIES:
        return log_and_abort("invalid retry header")

    return True