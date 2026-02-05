import asyncpg
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.pool = None
    
    async def connect(self):
        """Создание пула подключений к PostgreSQL"""
        self.pool = await asyncpg.create_pool(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', 5432)),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres'),
            database=os.getenv('DB_NAME', 'video_analytics'),
            min_size=1,
            max_size=10
        )
        print("Подключено к PostgreSQL")
    
    async def close(self):
        """Закрытие пула подключений"""
        if self.pool:
            await self.pool.close()
            print("Отключено от PostgreSQL")
    
    async def execute_query(self, sql: str) -> int:
        """
        Выполнение SQL-запроса и возврат одного числа
        """
        try:
            # Логируем запрос для отладки
            print(f"🔍 SQL: {sql}")
            
            async with self.pool.acquire() as conn:
                result = await conn.fetch(sql)
                
                if not result:
                    return 0
                
                # Берём первое значение из первой строки
                value = result[0][0]
                return int(value) if value is not None else 0
                
        except Exception as e:
            print(f"Ошибка SQL: {e}")
            print(f"апрос: {sql}")
            raise