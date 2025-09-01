import logging
import os
import re
import sys
from logging.handlers import RotatingFileHandler


class StripAnsiFilter(logging.Filter):
    ANSI_ESCAPE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')
    def filter(self, record):
        record.msg = self.ANSI_ESCAPE.sub('', str(record.msg))
        return True

def conf_logger(log_path=None):
    # по умолчанию лог рядом с этим скриптом
    if log_path is None:
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../app.log')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # создаём хэндлер для файла
    file_handler = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(StripAnsiFilter())

    # консольный хэндлер
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # очищаем старые хэндлеры и добавляем новые
    root_logger.handlers = []
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    logging.getLogger('apscheduler').setLevel(logging.WARNING)
    
    # принудительно создаём файл и записываем первый лог
    root_logger.debug("Logger initialized, log file created")