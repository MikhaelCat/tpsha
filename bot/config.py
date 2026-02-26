import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# DeepSeek API
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
API_TIMEOUT = 30

# Database
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
DB_NAME = os.getenv("POSTGRES_DB", "video_stats")
DB_HOST = os.getenv("POSTGRES_HOST", "db")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Security
MAX_QUERY_LENGTH = 500

ALLOWED_KEYWORDS = [
    'сколько', 'видео', 'просмотров', 'лайков', 'комментариев',
    'жалоб', 'креатора', 'выросло', 'выросли', 'прирост',
    'разных', 'новых', 'всего', 'больше', 'меньше', 'системе',
    'времени', 'включительно', 'с', 'по', 'на', 'сумме'
]

FORBIDDEN_WORDS = [
    'drop', 'delete', 'insert', 'update', 'truncate',
    'alter', 'create', 'grant', 'revoke', 'exec',
    'password', 'token', 'secret', 'key', 'admin'
]