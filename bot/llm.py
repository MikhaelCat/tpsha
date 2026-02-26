import aiohttp
import logging
import os
import sys
import re
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .config import DEEPSEEK_API_KEY, DEEPSEEK_URL, API_TIMEOUT

logger = logging.getLogger(__name__)

class DeepSeekLLM:
    def __init__(self):
        self.api_key = DEEPSEEK_API_KEY
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        self.system_prompt = """Ты — SQL-генератор для базы PostgreSQL со статистикой видео.
Твоя единственная задача — преобразовывать вопросы на русском языке в SQL-запросы.

ПРАВИЛА:
1. Возвращай ТОЛЬКО чистый SQL-код, без объяснений, без маркеров ```sql
2. Используй только SELECT-запросы
3. Все строковые значения в SQL оборачивай в одинарные кавычки
4. Для дат используй формат 'YYYY-MM-DD' и функцию DATE(column)
5. Если вопрос не о статистике видео — верни пустую строку

Схема БД:
{schema}

Примеры вопросов и ответов:
{examples}"""

    async def generate_sql(self, query: str, schema: str, examples: str) -> Optional[str]:
        system_prompt = self.system_prompt.format(schema=schema, examples=examples)
        
        user_prompt = f"""Вопрос: {query}

Важно:
- Если вопрос не о статистике видео → верни пустую строку
- Верни ТОЛЬКО SQL, без лишних символов
- Только SELECT-запросы"""

        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 500
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    DEEPSEEK_URL,
                    headers=self.headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=API_TIMEOUT)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        sql = data["choices"][0]["message"]["content"].strip()
                        
                        # очистка от маркеров
                        sql = re.sub(r'```sql?\n?', '', sql, flags=re.IGNORECASE)
                        sql = sql.replace('```', '').strip()
                        
                        # проверка на SELECT
                        if sql.upper().startswith("SELECT"):
                            return sql
                        logger.warning(f"LLM вернул не SELECT: {sql[:100]}")
                        return None
                    else:
                        logger.error(f"DeepSeek API error: {response.status}")
                        return None
        except Exception as e:
            logger.error(f"DeepSeek request failed: {e}")
            return None