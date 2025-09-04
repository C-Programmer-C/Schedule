from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    LOGIN: str
    SECURITY_KEY: str
    FIRST_MANAGER_ID: int
    SECOND_MANAGER_ID: int
    DATABASE_PATH: str
    MAX_WORKERS: int
    LOCK_EXPIRY_MINUTES: int
    SCAN_INTERVAL: int
    LIMIT_PROCESS_TASKS: int
    BOT_ID: int
    PORT: int = 8080
    SUBJECT_FORM_ID: int
    CLIENT_FIELD_ID: int
    LOGIN_ADNIN: str
    SECURITY_KEY_ADMIN: str
    
    class Config:
        env_file = str(Path(__file__).resolve().parent.parent / ".env")

settings = Settings() # type: ignore

PROJECT_ROOT = Path(__file__).resolve().parent.parent

db_path = Path(settings.DATABASE_PATH).expanduser()
if not db_path.is_absolute():
    db_path = (PROJECT_ROOT / db_path).resolve()