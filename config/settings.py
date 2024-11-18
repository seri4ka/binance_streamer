import os
from dotenv import load_dotenv

# Определение пути к корневому каталогу проекта
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, ".env")

# Загрузка переменных из .env
load_dotenv(dotenv_path=ENV_PATH)

# Получение значений переменных
API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

# Проверка на наличие ключей
if not API_KEY or not API_SECRET:
    raise ValueError("API_KEY and API_SECRET must be set in the .env file")
