from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import Command
from utils.llm import YandexGPTClient
from utils.database import Database

router = Router()
llm_client = YandexGPTClient()
db = Database()

@router.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "📊 Бот аналитики видео готов!\n"
        "Задавай вопросы на русском языке, я верну одно число.\n"
        "Примеры:\n"
        "• Сколько всего видео есть в системе?\n"
        "• Сколько видео набрало больше 100000 просмотров?\n"
        "• На сколько просмотров выросли все видео 28 ноября 2025?"
    )

@router.message(F.text)
async def handle_query(message: Message):
    user_query = message.text.strip()
    
    try:
        # Отправляем "печатает..." статус
        await message.bot.send_chat_action(message.chat.id, "typing")
        
        # Генерация SQL через YandexGPT
        sql_query = await llm_client.generate_sql(user_query)
        
        # Выполнение запроса к БД
        result = await db.execute_query(sql_query)
        
        # Отправка результата (только число!)
        await message.answer(str(result))
        
    except Exception as e:
        print(f"❌ Ошибка обработки запроса: {e}")
        await message.answer("❌ Ошибка при обработке запроса")