import re
from typing import Tuple
from .config import MAX_QUERY_LENGTH, ALLOWED_KEYWORDS, FORBIDDEN_WORDS

class QueryValidator:
    def validate_query_length(self, query: str) -> bool:
        return len(query) <= MAX_QUERY_LENGTH
    
    def sanitize_input(self, query: str) -> str:
        return ' '.join(query.split())
    
    def check_forbidden_words(self, query: str) -> bool:
        """Проверяет запрос на запрещённые слова (регистронезависимо)"""
        q_lower = query.lower()
        for word in FORBIDDEN_WORDS:
            if re.search(rf'\b{re.escape(word)}\b', q_lower, re.IGNORECASE):
                return False
        return True
    
    def validate_intent(self, query: str) -> bool:
        """Проверяет, что вопрос относится к статистике видео"""
        q_lower = query.lower()
        return any(kw in q_lower for kw in ALLOWED_KEYWORDS)
    
    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """Валидация SQL-запроса."""
        if not sql:
            return False, "Пустой SQL"
        
        sql_upper = sql.upper().strip()
        
        if not re.match(r'^\s*SELECT\b', sql_upper):
            return False, "Разрешены только SELECT-запросы"
        
        for forbidden in FORBIDDEN_WORDS:
            if re.search(rf'\b{re.escape(forbidden)}\b', sql, re.IGNORECASE):
                return False, f"Обнаружена запрещённая операция: {forbidden.upper()}"
        
        if sql.count(';') > 1:
            return False, "Обнаружено несколько запросов"
        
        if '--' in sql or '/*' in sql:
            return False, "Обнаружены SQL-комментарии"
        
        return True, "OK"

# глобальный экземпляр
validator = QueryValidator()