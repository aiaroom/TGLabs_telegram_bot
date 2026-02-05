import json 
import asyncio
import asyncpg
from datetime import datetime

def parse_datetime(dt_str: str) -> datetime:
    """Безопасный парсинг ISO 8601 с поддержкой Z и +00:00"""
    if dt_str.endswith('Z'):
        dt_str = dt_str[:-1] + '+00:00'
    return datetime.fromisoformat(dt_str)

async def connect_db():
    try:
        conn = await asyncpg.connect(
            host='0.0.0.0',
            port=5432,
            user='postgres',
            password='postgres',
            database='video_analytics'
        )
        print("Подключились к БД")
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

async def read_json_file(filename: str):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            videos = data.get('videos', [])
            print(f"Прочитали {len(videos)} видео из файла")
            return videos
    except FileNotFoundError:
        print(f"Файл не найден: {filename}")
        return None
    except json.JSONDecodeError as e:
        print(f"Ошибка формата JSON: {e}")
        return None
    except Exception as e:
        print(f"Ошибка чтения файла: {e}")
        return None

# Вставляем одно видео
async def insert_video(conn, video):
    try:
        await conn.execute('''
            INSERT INTO videos (
                id, creator_id, video_created_at,
                views_count, likes_count, comments_count, reports_count,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING
        ''',
            video['id'],
            video['creator_id'],
            parse_datetime(video['video_created_at']),
            video.get('views_count', 0),
            video.get('likes_count', 0),
            video.get('comments_count', 0),
            video.get('reports_count', 0),
            parse_datetime(video['created_at']),
            parse_datetime(video['updated_at'])
        )
        return True
    except KeyError as e:
        print(f"Не хватает поля в видео {video.get('id', 'UNKNOWN')}: {e}")
        return False
    except Exception as e:
        print(f"Ошибка вставки видео {video.get('id', 'UNKNOWN')}: {e}")
        return False


# Вставляем один снапшот
async def insert_snapshot(conn, snapshot, video_id):
    try:
        await conn.execute('''
            INSERT INTO video_snapshots (
                id, video_id,
                views_count, likes_count, comments_count, reports_count,
                delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (id) DO NOTHING
        ''',
            snapshot['id'],
            video_id,
            snapshot.get('views_count', 0),
            snapshot.get('likes_count', 0),
            snapshot.get('comments_count', 0),
            snapshot.get('reports_count', 0),
            snapshot.get('delta_views_count', 0),
            snapshot.get('delta_likes_count', 0),
            snapshot.get('delta_comments_count', 0),
            snapshot.get('delta_reports_count', 0),
            parse_datetime(snapshot['created_at']),
            parse_datetime(snapshot['updated_at'])
        )
        return True
    except KeyError as e:
        print(f"Не хватает поля в снапшоте видео {video_id}: {e}")
        return False
    except Exception as e:
        print(f"Ошибка вставки снапшота видео {video_id}: {e}")
        return False


# Обрабатываем одно видео со всеми снапшотами
async def process_video(conn, video):
    video_id = video.get('id', 'UNKNOWN')
    
    # Вставляем видео
    if not await insert_video(conn, video):
        return False, 0
    
    # Вставляем снапшоты
    snapshots = video.get('snapshots', [])
    success_count = 0
    
    for snapshot in snapshots:
        if await insert_snapshot(conn, snapshot, video_id):
            success_count += 1
    
    return True, success_count


# Основная функция
async def load_data():
    # 1. Подключаемся к БД
    conn = await connect_db()
    if not conn:
        return
    
    # 2. Читаем файл
    videos = await read_json_file('./data/videos.json')
    if not videos:
        await conn.close()
        return
    
    # 3. Загружаем данные
    video_ok = 0
    snapshot_ok = 0
    
    for i, video in enumerate(videos, 1):
        print(f"\nВидео {i}/{len(videos)}: {video.get('id', 'без ID')}")
        ok, snaps = await process_video(conn, video)
        if ok:
            video_ok += 1
            snapshot_ok += snaps
    
    # 4. Закрываем соединение
    await conn.close()
    print("\nЗагрузка завершена!")
    print(f"   Видео: {video_ok}/{len(videos)}")
    print(f"   Снапшотов: {snapshot_ok}")

if __name__ == "__main__":
    asyncio.run(load_data())