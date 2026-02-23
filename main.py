import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Any
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from dotenv import load_dotenv
import os
import re

# Попытка импорта библиотек GigaChain (может отсутствовать в зависимости от окружения)
try:
    from gigachain_core.prompts import PromptTemplate
    from gigachain_community.llms import GigaChat
    from gigachain_core.output_parsers import BaseOutputParser
    GIGACHAIN_AVAILABLE = True
except ImportError:
    GIGACHAIN_AVAILABLE = False
    # Заглушки для работы без установленных библиотек GigaChain
    class BaseOutputParser:
        pass
    class GigaChat:
        def __init__(self, **kwargs):
            pass
        async def ainvoke(self, prompt):
            return ""

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Параметры подключения к базе данных
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
DB_NAME = os.getenv("POSTGRES_DB", "video_stats")
DB_HOST = os.getenv("POSTGRES_HOST", "db")  # В Docker имя сервиса 'db'
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

# Токен Telegram бота
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

class SQLQueryOutputParser(BaseOutputParser):
    """Пользовательский парсер для извлечения SQL-запроса из ответа LLM"""
    def parse(self, text: str) -> str:
        # Ищем SQL-запрос в тройных кавычках
        pattern = r'```sql\s*(.*?)\s*```'
        match = re.search(pattern, text, re.DOTALL)
        
        if match:
            sql_query = match.group(1).strip()
            return sql_query
        
        # Если не найдено в кавычках, ищем базовый SELECT
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if line.upper().startswith('SELECT'):
                return line
        
        return text.strip()

def get_sql_prompt(query: str) -> str:
    """Генерация промпта для конвертации естественного языка в SQL"""
    # Исправлены опечатки в описании схемы
    schema_info = """
База данных содержит две таблицы:
1. Таблица videos:
   - id: TEXT (первичный ключ) - идентификатор видео
   - creator_id: TEXT - идентификатор создателя
   - video_created_at: TIMESTAMP WITH TIME ZONE - когда видео было опубликовано
   - views_count: INTEGER - общее количество просмотров видео
   - likes_count: INTEGER - общее количество лайков видео
   - comments_count: INTEGER - общее количество комментариев видео
   - reports_count: INTEGER - общее количество жалоб на видео
   - created_at: TIMESTAMP WITH TIME ZONE - когда запись была создана
   - updated_at: TIMESTAMP WITH TIME ZONE - когда запись была обновлена

2. Таблица video_snapshots:
   - id: TEXT (первичный ключ) - идентификатор снимка
   - video_id: TEXT - ссылка на videos(id)
   - views_count: INTEGER - количество просмотров на момент снимка
   - likes_count: INTEGER - количество лайков на момент снимка
   - comments_count: INTEGER - количество комментариев на момент снимка
   - reports_count: INTEGER - количество жалоб на момент снимка
   - delta_views_count: INTEGER - изменение просмотров с предыдущего снимка
   - delta_likes_count: INTEGER - изменение лайков с предыдущего снимка
   - delta_comments_count: INTEGER - изменение комментариев с предыдущего снимка
   - delta_reports_count: INTEGER - изменение жалоб с предыдущего снимка
   - created_at: TIMESTAMP WITH TIME ZONE - когда был сделан снимок
   - updated_at: TIMESTAMP WITH TIME ZONE - когда запись была обновлена

Важные заметки:
- Даты в базе хранятся в формате ISO (YYYY-MM-DDTHH:MM:SS+TZ)
- При фильтрации по конкретной дате используйте функцию DATE() для извлечения части даты
- Для диапазонов дат используйте оператор BETWEEN
- Всегда возвращайте только одно число в качестве результата
- Используйте соответствующие JOIN при необходимости
- Для вопросов "сколько всего видео" используйте COUNT(*) из таблицы videos
- Для вопросов "общая сумма" используйте SUM() из соответствующих колонок
- Для вопросов о "росте" или "приросте" используйте delta колонки из video_snapshots
"""

    examples = """
Примеры:

Вопрос: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько видео у креатора с id 123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = '123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: "Сколько видео набрало больше 100 000 просмотров за всё время?"
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;
"""

    prompt_template = f"""
Конвертируй следующий запрос на естественном русском языке в SQL-запрос PostgreSQL.
Верни только SQL-запрос внутри тройных кавычек ```sql ... ```.

{schema_info}

{examples}

Вопрос: {query}
"""

    return prompt_template

class VideoStatsBot:
    def __init__(self):  # Исправлено: было def init(self):
        """Инициализация бота и подключение к БД"""
        self.bot = Bot(token=BOT_TOKEN)
        self.dp = Dispatcher()
        self.db_pool = None
        
        # Инициализация GigaChat LLM
        credentials = os.getenv("GIGACHAT_CREDENTIALS")
        scope = os.getenv("GIGACHAT_SCOPE")
        
        if credentials and scope and GIGACHAIN_AVAILABLE:
            self.llm = GigaChat(
                credentials=credentials,
                scope=scope,
                verify_ssl_certs=False
            )
        else:
            # Если GigaChat не настроен, используем заглушку
            logger.warning("Учетные данные GigaChat не найдены. Используется режим без LLM.")
            self.llm = None

    async def connect_to_db(self):
        """Создание пула соединений к базе данных"""
        try:
            self.db_pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            logger.info("Успешное подключение к базе данных")
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            raise

    async def close_db_connection(self):
        """Закрытие пула соединений к базе данных"""
        if self.db_pool:
            await self.db_pool.close()

    async def check_db_connection(self):
        """Проверка работоспособности подключения к базе данных"""
        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.fetchval("SELECT COUNT(*) FROM videos LIMIT 1")
                logger.info("Подключение к БД ОК, таблица videos доступна")
                return True
        except Exception as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            return False

    async def process_natural_language_query(self, query: str) -> int:
        """Обработка запроса на естественном языке и возврат числового результата"""
        # Если LLM не настроен, используем простую логику на основе правил
        if not self.llm:
            query_lower = query.lower()
            
            if "сколько всего видео" in query_lower:
                async with self.db_pool.acquire() as conn:
                    result = await conn.fetchval("SELECT COUNT(*) FROM videos")
                    return result or 0
            
            elif "сколько видео у креатора" in query_lower:
                creator_match = re.search(r"id\s+([a-zA-Z0-9]+)", query)
                if creator_match:
                    creator_id = creator_match.group(1)
                    async with self.db_pool.acquire() as conn:
                        result = await conn.fetchval(
                            "SELECT COUNT(*) FROM videos WHERE creator_id = $1", 
                            creator_id
                        )
                        return result or 0
            
            elif "набрало больше" in query_lower and "просмотров" in query_lower:
                view_match = re.search(r"(\d+)\s+просмотров", query)
                if view_match:
                    view_threshold = int(view_match.group(1))
                    async with self.db_pool.acquire() as conn:
                        result = await conn.fetchval(
                            "SELECT COUNT(*) FROM videos WHERE views_count > $1", 
                            view_threshold
                        )
                        return result or 0
            
            return 0
        
        # Использование LLM для конвертации естественного языка в SQL
        prompt = get_sql_prompt(query)
        llm_response = await self.llm.ainvoke(prompt)
        
        # Парсинг SQL-запроса из ответа LLM
        parser = SQLQueryOutputParser()
        sql_query = parser.parse(llm_response)
        
        logger.info(f"Сгенерированный SQL-запрос: {sql_query}")
        
        # Выполнение SQL-запроса
        async with self.db_pool.acquire() as conn:
            result = await conn.fetchval(sql_query)
            return result if result is not None else 0

    def setup_handlers(self):
        """Настройка обработчиков сообщений бота"""
        @self.dp.message(CommandStart())
        async def start_handler(message: types.Message):
            welcome_text = (
                "Привет! Я бот для анализа статистики видео.\n"
                "Задайте мне вопрос на русском языке, и я верну вам численный ответ.\n\n"
                "Примеры вопросов:\n"
                "- Сколько всего видео есть в системе?\n"
                "- Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?\n"
                "- Сколько видео набрало больше 100 000 просмотров за всё время?\n"
                "- На сколько просмотров в сумме выросли все видео 28 ноября 2025?\n"
                "- Сколько разных видео получали новые просмотры 27 ноября 2025?"
            )
            await message.answer(welcome_text)
        
        @self.dp.message()
        async def handle_user_query(message: types.Message):
            user_query = message.text
            
            try:
                result = await self.process_natural_language_query(user_query)
                await message.answer(str(result))
            except Exception as e:
                logger.error(f"Ошибка обработки запроса: {e}")
                await message.answer("Произошла ошибка при обработке запроса. Попробуйте еще раз.")

    async def run(self):
        """Запуск бота"""
        await self.connect_to_db()
        
        # Ожидание готовности базы данных и загрузки данных
        max_retries = 30
        retry_count = 0
        while retry_count < max_retries:
            if await self.check_db_connection():
                logger.info("База данных готова и подключена")
                break
            else:
                logger.info(f"Ожидание базы данных... ({retry_count + 1}/{max_retries})")
                await asyncio.sleep(2)
                retry_count += 1
        else:
            logger.error("Не удалось подключиться к базе данных после максимального числа попыток")
            return
        
        self.setup_handlers()
        
        logger.info("Запуск опроса бота...")
        await self.dp.start_polling(self.bot)

async def main():
    bot_instance = VideoStatsBot()
    try:
        await bot_instance.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    finally:
        await bot_instance.close_db_connection()

if __name__ == "__main__":  # Исправлено: было if name == "main":
    asyncio.run(main())