"""
Скрипт для загрузки данных о видео из JSON-файла в базу данных PostgreSQL
"""
import json
import asyncio
import asyncpg
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

def parse_datetime(date_str: str) -> Optional[datetime]:
    """Преобразование ISO-строки в datetime объект"""
    if not date_str:
        return None
    try:
        date_str = date_str.replace('Z', '+00:00')
        return datetime.fromisoformat(date_str)
    except (ValueError, AttributeError):
        return None

async def load_videos_to_db(db_pool: asyncpg.Pool, videos_data: List[Dict[Any, Any]]):
    """Загрузка данных о видео в базу данных"""
    print(f"Загрузка {len(videos_data)} видео в базу данных...")
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for i, video in enumerate(videos_data):
                await conn.execute("""
                    INSERT INTO videos (
                        id, creator_id, video_created_at, views_count,
                        likes_count, comments_count, reports_count,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (id) DO UPDATE SET
                        creator_id = EXCLUDED.creator_id,
                        video_created_at = EXCLUDED.video_created_at,
                        views_count = EXCLUDED.views_count,
                        likes_count = EXCLUDED.likes_count,
                        comments_count = EXCLUDED.comments_count,
                        reports_count = EXCLUDED.reports_count,
                        updated_at = EXCLUDED.updated_at
                """,
                    str(video['id']),
                    str(video['creator_id']),
                    parse_datetime(video['video_created_at']),
                    video.get('views_count', 0),
                    video.get('likes_count', 0),
                    video.get('comments_count', 0),
                    video.get('reports_count', 0),
                    parse_datetime(video.get('created_at')),
                    parse_datetime(video.get('updated_at'))
                )
                
                snapshots = video.get('snapshots', [])
                for snapshot in snapshots:
                    await conn.execute("""
                        INSERT INTO video_snapshots (
                            id, video_id, views_count, likes_count,
                            comments_count, reports_count,
                            delta_views_count, delta_likes_count,
                            delta_comments_count, delta_reports_count,
                            created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        ON CONFLICT (id) DO UPDATE SET
                            video_id = EXCLUDED.video_id,
                            views_count = EXCLUDED.views_count,
                            likes_count = EXCLUDED.likes_count,
                            comments_count = EXCLUDED.comments_count,
                            reports_count = EXCLUDED.reports_count,
                            delta_views_count = EXCLUDED.delta_views_count,
                            delta_likes_count = EXCLUDED.delta_likes_count,
                            delta_comments_count = EXCLUDED.delta_comments_count,
                            delta_reports_count = EXCLUDED.delta_reports_count,
                            updated_at = EXCLUDED.updated_at
                    """,
                        str(snapshot['id']),
                        str(snapshot['video_id']),
                        snapshot.get('views_count', 0),
                        snapshot.get('likes_count', 0),
                        snapshot.get('comments_count', 0),
                        snapshot.get('reports_count', 0),
                        snapshot.get('delta_views_count', 0),
                        snapshot.get('delta_likes_count', 0),
                        snapshot.get('delta_comments_count', 0),
                        snapshot.get('delta_reports_count', 0),
                        parse_datetime(snapshot.get('created_at')),
                        parse_datetime(snapshot.get('updated_at'))
                    )
                
                if (i + 1) % 100 == 0:
                    print(f"Обработано {i + 1}/{len(videos_data)} видео...")

async def main():
    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
    DB_NAME = os.getenv("POSTGRES_DB", "video_stats")
    DB_HOST = os.getenv("POSTGRES_HOST", "db")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    
    print("Подключение к базе данных...")
    db_pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME
    )
    
    try:
        print("Чтение JSON-файла...")
        with open('/tmp/videos.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        videos_data = data['videos']
        print(f"Найдено {len(videos_data)} видео в JSON-файле")
        
        await load_videos_to_db(db_pool, videos_data)
        print("Загрузка данных успешно завершена!")
    
    except Exception as e:
        print(f"Ошибка во время загрузки данных: {e}")
        raise
    
    finally:
        await db_pool.close()

if __name__ == "__main__":
    asyncio.run(main())