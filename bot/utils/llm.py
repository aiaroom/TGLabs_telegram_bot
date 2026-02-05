import aiohttp
import re
import os
from dotenv import load_dotenv

load_dotenv()

class YandexGPTClient:
    def __init__(self):
        self.api_key = os.getenv('YANDEXGPT_API_KEY')
        self.folder_id = os.getenv('YANDEXGPT_FOLDER_ID')
        self.url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    
    async def generate_sql(self, user_query: str) -> str:
        """Генерация SQL-запроса из текстового вопроса"""
        
        # Нормализуем даты в запросе (русский → ISO)
        normalized_query = self._normalize_dates(user_query)
        
        # Формируем промпт
        prompt = self._build_prompt(normalized_query)
        
        # Отправляем запрос к YandexGPT
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.url,
                headers={"Authorization": f"Api-Key {self.api_key}"},
                json={
                    "modelUri": f"gpt://{self.folder_id}/yandexgpt/latest",
                    "completionOptions": {
                        "stream": False,
                        "temperature": 0.0,  # минимум галлюцинаций
                        "maxTokens": "300"
                    },
                    "messages": [{"role": "user", "text": prompt}]
                },
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                data = await response.json()
                
                if response.status != 200:
                    raise Exception(f"YandexGPT error: {data}")
                
                # Извлекаем текст ответа
                raw_sql = data["result"]["alternatives"][0]["message"]["text"]
                return self._extract_sql(raw_sql)
    
    def _build_prompt(self, user_query: str) -> str:
        """Создание промпта с описанием схемы БД"""
        return f"""Ты — система аналитики видео. Преобразуй вопрос пользователя в ОДИН корректный SQL-запрос на PostgreSQL.

СХЕМА БАЗЫ ДАННЫХ:

Таблица "videos" (итоговая статистика по каждому видео):
- id (BIGINT) — идентификатор видео
- creator_id (BIGINT) — идентификатор креатора
- video_created_at (TIMESTAMP) — дата публикации видео
- views_count (INTEGER) — финальное количество просмотров
- likes_count (INTEGER) — финальное количество лайков
- comments_count (INTEGER) — финальное количество комментариев
- reports_count (INTEGER) — финальное количество жалоб

Таблица "video_snapshots" (почасовые замеры статистики):
- id (BIGSERIAL) — идентификатор снапшота
- video_id (BIGINT) — ссылка на видео
- views_count (INTEGER) — текущие просмотры на момент замера
- likes_count (INTEGER) — текущие лайки на момент замера
- comments_count (INTEGER) — текущие комментарии на момент замера
- reports_count (INTEGER) — текущие жалобы на момент замера
- delta_views_count (INTEGER) — прирост просмотров с прошлого замера
- delta_likes_count (INTEGER) — прирост лайков с прошлого замера
- delta_comments_count (INTEGER) — прирост комментариев с прошлого замера
- delta_reports_count (INTEGER) — прирост жалоб с прошлого замера
- created_at (TIMESTAMP) — время замера (раз в час)

ПРАВИЛА ГЕНЕРАЦИИ ЗАПРОСА:
1. В ответе должен быть ТОЛЬКО один SQL-запрос без лишнего текста
2. Запрос должен возвращать РОВНО ОДНО ЧИСЛО (через COUNT, SUM, AVG)
3. Для фильтрации по датам используй: DATE(column) = 'ГГГГ-ММ-ДД' или BETWEEN
4. "Сколько видео" → используй таблицу videos + COUNT(*)
5. "На сколько выросло/прирост" → используй таблицу video_snapshots + SUM(delta_*_count)
6. "Разные видео" → используй COUNT(DISTINCT video_id)
7. Даты в запросах уже нормализованы в формат ГГГГ-ММ-ДД

ПРИМЕРЫ:

Вопрос: "Сколько всего видео есть в системе?"
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько видео у креатора с id 123 вышло с 2025-11-01 по 2025-11-05?"
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 123 AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: "Сколько видео набрало больше 100000 просмотров?"
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: "На сколько просмотров выросли все видео 2025-11-28?"
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: "Сколько разных видео получали новые просмотры 2025-11-27?"
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

Теперь обработай этот вопрос:

Вопрос: {user_query}
SQL:"""
    
    def _normalize_dates(self, text: str) -> str:
        """Преобразование дат с русского на ISO: '28 ноября 2025' → '2025-11-28'"""
        months = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }
        
        # Замена "28 ноября 2025" → "2025-11-28"
        import re
        text = re.sub(
            r'(\d{1,2})\s+([а-яА-ЯёЁ]+?)\s+(\d{4})',
            lambda m: f"{m.group(3)}-{months.get(m.group(2).lower(), '01')}-{m.group(1).zfill(2)}",
            text
        )
        
        # Замена "с 1 по 5 ноября 2025" → "с 2025-11-01 по 2025-11-05"
        text = re.sub(
            r'с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+([а-яА-ЯёЁ]+?)\s+(\d{4})',
            lambda m: f"с {m.group(4)}-{months.get(m.group(3).lower(), '01')}-{m.group(1).zfill(2)} по {m.group(4)}-{months.get(m.group(3).lower(), '01')}-{m.group(2).zfill(2)}",
            text
        )
        
        return text
    
    def _extract_sql(self, text: str) -> str:
        """Извлечение чистого SQL из ответа модели"""
        # Удаляем ```sql и ```
        text = re.sub(r'```sql\s*', '', text)
        text = re.sub(r'```\s*', '', text)
        text = text.strip()
        
        # Если есть несколько запросов - берём первый
        if ';' in text:
            text = text.split(';')[0] + ';'
        else:
            text = text + ';'
        
        return text.strip()
