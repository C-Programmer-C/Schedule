import sqlite3

from conf.config import settings


def db_connect():
    conn = sqlite3.connect(settings.DATABASE_PATH, timeout=30, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn