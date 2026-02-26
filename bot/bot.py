import sys
import os
import asyncio
import logging
from typing import Tuple, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from .config import BOT_TOKEN, DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
from .llm import DeepSeekLLM
from .parser import QueryParser
from .security import validator
import asyncpg

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MESSAGES = {
    'start': """Привет! Я бот для анализа статистики видео.
Примеры вопросов:
• Сколько всего видео есть в системе?
• Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025?
• Сколько видео набрало больше 100000 просмотров?
• На сколько просмотров выросли все видео 28 ноября 2025?
• Сколько разных видео получали новые просмотры 27 ноября 2025?""",
    'invalid_intent': "Я отвечаю только на вопросы о статистике видео. Попробуйте переформулировать вопрос.",
    'invalid_sql': "Не удалось обработать запрос. Попробуйте другой вопрос.",
    'error': "Произошла ошибка. Попробуйте позже.",
    'too_long': "Вопрос слишком длинный. Сформулируйте короче."
}


class VideoStatsBot:
    def __init__(self):
        self.bot = Bot(token=BOT_TOKEN)
        self.dp = Dispatcher()
        self.db_pool = None
        self.llm = DeepSeekLLM() if os.getenv("DEEPSEEK_API_KEY") else None
        self.parser = QueryParser()
        
        self.schema = """
Таблица videos: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count
Таблица video_snapshots: id, video_id, delta_views_count, delta_likes_count, created_at
"""
        self.examples = """
Вопрос: Сколько всего видео? 
SQL: SELECT COUNT(*) FROM videos;

Вопрос: Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025?
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: Сколько видео набрало больше 100000 просмотров?
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: Сколько разных видео получали новые просмотры 27 ноября 2025?
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;
"""

    async def connect_to_db(self):
        self.db_pool = await asyncpg.create_pool(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, database=DB_NAME
        )
        logger.info("Подключение к БД успешно")

    async def close_db_connection(self):
        if self.db_pool:
            await self.db_pool.close()

    async def check_db_connection(self):
        try:
            async with self.db_pool.acquire() as conn:
                await conn.fetchval("SELECT COUNT(*) FROM videos LIMIT 1")
                return True
        except Exception as e:
            logger.error(f"Ошибка проверки БД: {e}")
            return False

    async def process_query(self, query: str) -> Tuple[int, bool, str]:
        if not query:
            return 0, False, MESSAGES['invalid_intent']
    
        logger.info(f"Получен запрос: {query[:100]}")
        
        # проверка длины
        if not validator.validate_query_length(query):
            logger.warning("Запрос слишком длинный")
            return 0, False, MESSAGES['too_long']
        
        # очистка ввода
        query = validator.sanitize_input(query)
        
        # проверка на запрещённые слова
        if not validator.check_forbidden_words(query):
            logger.warning(f"Запрещённые слова в запросе")
            return 0, False, MESSAGES['invalid_intent']
        
        # проверка интента
        if not validator.validate_intent(query):
            logger.warning(f"Интент не распознан")
            return 0, False, MESSAGES['invalid_intent']
        
        # генерация SQL 
        sql = None
        method = "NONE"
        
        # сначала пробуем LLM
        if self.llm:
            logger.info("Запрос к DeepSeek API...")
            sql = await self.llm.generate_sql(query, self.schema, self.examples)
            if sql:
                method = "DEEPSEEK"
                logger.info(f"LLM вернул SQL")
        
        # если LLM не справился — пробуем простой парсер
        if not sql:
            logger.info("Пробуем локальный парсер...")
            sql = self.parser.parse_query(query)
            if sql:
                method = "PARSER"
                logger.info(f"Парсер вернул SQL")
        
        # валидация SQL
        if not sql:
            logger.error(f"Не удалось сгенерировать SQL для: {query[:100]}")
            return 0, False, MESSAGES['invalid_sql']
        
        sql_valid, sql_msg = validator.validate_sql(sql)
        if not sql_valid:
            logger.warning(f"Invalid SQL: {sql_msg} | Query: {sql[:100]}")
            return 0, False, MESSAGES['invalid_sql']
        
        logger.info(f"[{method}] SQL: {sql}")
        
        # выполнение запроса
        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval(sql)
                logger.info(f" Результат: {result}")
                return (result if result else 0), True, str(result)
        except Exception as e:
            logger.error(f"SQL error: {e} | SQL: {sql}")
            return 0, False, MESSAGES['error']

    def setup_handlers(self):
        @self.dp.message(CommandStart())
        async def start_handler(message: types.Message):
            await message.answer(MESSAGES['start'])
        
        @self.dp.message()
        async def handle_message(message: types.Message):
            result, success, response_msg = await self.process_query(message.text)
            await message.answer(response_msg)

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

    async def close(self):
        await self.close_db_connection()
        await self.bot.close()


async def main():
    bot = VideoStatsBot()
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())