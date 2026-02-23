import asyncio
import logging
import os
import re
import aiohttp
import json
from typing import Optional

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Параметры подключения к БД
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
DB_NAME = os.getenv("POSTGRES_DB", "video_stats")
DB_HOST = os.getenv("POSTGRES_HOST", "db")  # В Docker имя сервиса 'db'
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# DeepSeek API
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"

class DeepSeekLLM:
    """Класс для работы с DeepSeek API"""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
    
    async def generate_sql(self, query: str, schema_info: str, examples: str) -> Optional[str]:
        """Генерация SQL-запроса через DeepSeek"""
        prompt = f"""
Ты — SQL-эксперт. Конвертируй запрос на русском языке в SQL-запрос для PostgreSQL.
Верни ТОЛЬКО SQL-код без объяснений и без тройных кавычек.

Схема базы данных:
{schema_info}

Примеры:
{examples}

Вопрос пользователя: {query}

SQL:"""

        payload = {
            "model": "deepseek-chat",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 500
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(DEEPSEEK_URL, headers=self.headers, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()
                        sql = data["choices"][0]["message"]["content"].strip()
                        # Очистка от маркеров кода если модель их добавит
                        sql = re.sub(r'```sql\s*', '', sql)
                        sql = re.sub(r'```', '', sql)
                        return sql.strip()
                    else:
                        logger.error(f"DeepSeek API error: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"DeepSeek request failed: {e}")
            return None

class QueryParser:
    """Резервный парсер на основе правил (если API недоступен)"""
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[str]:
        months = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        match = re.search(r'(\d{1,2})\s+(' + '|'.join(months.keys()) + r')\s+(\d{4})', date_str.lower())
        if match:
            day = match.group(1).zfill(2)
            month = months[match.group(2)]
            year = match.group(3)
            return f"{year}-{month}-{day}"
        return None

    def parse_query(self, query: str) -> Optional[str]:
        q = query.lower()
        
        if "сколько всего видео" in q:
            return "SELECT COUNT(*) FROM videos"
        
        if "сколько видео у креатора" in q and "вышло" in q:
            creator_match = re.search(r'id\s*([a-zA-Z0-9_-]+)', q)
            if creator_match:
                creator_id = creator_match.group(1)
                date_range_match = re.search(r'с\s+(.+?)\s+по\s+(.+?)\s+(?:включительно|за)', q)
                if date_range_match:
                    date_from = self.parse_date(date_range_match.group(1))
                    date_to = self.parse_date(date_range_match.group(2))
                    if date_from and date_to:
                        return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{creator_id}' AND DATE(video_created_at) BETWEEN '{date_from}' AND '{date_to}'"
        
        if "набрало больше" in q and "просмотров" in q:
            view_match = re.search(r'(\d+)\s+просмотров', q)
            if view_match:
                threshold = int(view_match.group(1))
                return f"SELECT COUNT(*) FROM videos WHERE views_count > {threshold}"
        
        if ("на сколько" in q or "сумме" in q) and "выросли" in q and "просмотров" in q:
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', q)
            if date_match:
                date = self.parse_date(date_match.group(1))
                if date:
                    return f"SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '{date}'"
        
        if "сколько разных видео" in q and "новые просмотры" in q:
            date_match = re.search(r'(\d{1,2}\s+\w+\s+\d{4})', q)
            if date_match:
                date = self.parse_date(date_match.group(1))
                if date:
                    return f"SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '{date}' AND delta_views_count > 0"
        
        return None

class VideoStatsBot:
    def __init__(self): 
        self.bot = Bot(token=BOT_TOKEN)
        self.dp = Dispatcher()
        self.db_pool = None
        self.llm = DeepSeekLLM(DEEPSEEK_API_KEY) if DEEPSEEK_API_KEY else None
        self.parser = QueryParser()
        
        self.schema_info = """
Таблица videos:
- id: TEXT (первичный ключ)
- creator_id: TEXT
- video_created_at: TIMESTAMPTZ (дата публикации)
- views_count, likes_count, comments_count, reports_count: INTEGER

Таблица video_snapshots:
- id: TEXT (первичный ключ)
- video_id: TEXT (ссылка на videos.id)
- delta_views_count, delta_likes_count: INTEGER (прирост за час)
- created_at: TIMESTAMPTZ (время замера)
"""
        
        self.examples = """
Вопрос: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько видео у креатора с id 123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = '123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';
"""

    async def connect_to_db(self):
        try:
            self.db_pool = await asyncpg.create_pool(
                host=DB_HOST, port=DB_PORT, user=DB_USER,
                password=DB_PASSWORD, database=DB_NAME
            )
            logger.info("Подключение к БД успешно")
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise

    async def close_db_connection(self):
        if self.db_pool:
            await self.db_pool.close()

    async def check_db_connection(self):
        try:
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT COUNT(*) FROM videos LIMIT 1")
                return True
        except Exception:
            return False

    async def process_query(self, query: str) -> int:
        sql = None
        
        # Сначала пробуем через LLM
        if self.llm:
            sql = await self.llm.generate_sql(query, self.schema_info, self.examples)
            logger.info(f"LLM сгенерировал SQL: {sql}")
        
        # Если LLM не сработал — используем резервный парсер
        if not sql:
            logger.warning("LLM не вернул SQL, используем резервный парсер")
            sql = self.parser.parse_query(query)
        
        if not sql:
            logger.warning(f"Не удалось распарсить запрос: {query}")
            return 0
        
        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval(sql)
                return result if result is not None else 0
        except Exception as e:
            logger.error(f"Ошибка выполнения SQL: {e}")
            return 0

    def setup_handlers(self):
        @self.dp.message(CommandStart())
        async def start_handler(message: types.Message):
            await message.answer(
                "Привет! Я бот для анализа статистики видео.\n"
                "Примеры вопросов:\n"
                "- Сколько всего видео есть в системе?\n"
                "- Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025?\n"
                "- Сколько видео набрало больше 100000 просмотров?\n"
                "- На сколько просмотров выросли все видео 28 ноября 2025?"
            )
        
        @self.dp.message()
        async def handle_message(message: types.Message):
            try:
                result = await self.process_query(message.text)
                await message.answer(str(result))
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                await message.answer("Ошибка обработки запроса.")

    async def run(self):
        await self.connect_to_db()
        
        for i in range(30):
            if await self.check_db_connection():
                break
            logger.info(f"Ожидание БД... ({i+1}/30)")
            await asyncio.sleep(2)
        
        self.setup_handlers()
        logger.info("Запуск бота...")
        await self.dp.start_polling(self.bot)

async def main():
    bot = VideoStatsBot()
    try:
        await bot.run()
    finally:
        await bot.close_db_connection()

if __name__ == "__main__":  
    asyncio.run(main())