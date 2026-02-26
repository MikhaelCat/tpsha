import re
from typing import Optional

class QueryParser:
    """Минимальная парсер-заглушка. 
    Вся логика теперь в LLM, но для экстренных случаев."""
    
    def parse_query(self, query: str) -> Optional[str]:
        q = query.lower().strip()
        
        # самые базовые запросы 
        if "сколько всего видео" in q:
            return "SELECT COUNT(*) FROM videos"
        
        return None  # всё остальное =LLM