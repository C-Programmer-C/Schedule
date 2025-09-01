# NotificationScheduler

## 🚀 Установка и запуск

### 1. Клонировать проект

git clone https://github.com/your-repo/NotificationScheduler.git
cd NotificationScheduler

### 2. Создать виртуальное окружение
python -m venv venv

### 3. Активировать виртуальное окружение
Linux / macOS:
source venv/bin/activate

Windows (PowerShell):
.\venv\Scripts\activate

### 4. Установить зависимости
pip install -r requirements.txt

### 5. Настроить переменные окружения
Создать файл .env в корне проекта.

### 6. Запуск приложения
Проект состоит из двух процессов, которые должны работать параллельно:
  Основной сервер (Flask + Waitress):
  python app/main.py
  Фоновый воркер (обработка задач):
  python app/scan_tasks.py
