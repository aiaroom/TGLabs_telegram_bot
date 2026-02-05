import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from dotenv import load_dotenv
import os
from handlers.bot_handler import router, db
from aiogram.client.default import DefaultBotProperties

load_dotenv()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Инициализация бота
    bot = Bot(
        token=os.getenv('TELEGRAM_BOT_TOKEN'),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()
    
    # Подключение роутеров
    dp.include_router(router)
    
    # Подключение к БД
    await db.connect()
    
    logger.info("Бот запущен и готов к работе")
    
    # Запуск поллинга
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
