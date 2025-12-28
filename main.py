import asyncio
import random
import os
import aiohttp
from contextlib import asynccontextmanager
from dotenv import load_dotenv

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, types
from aiogram.types import WebAppInfo
from aiogram.utils.keyboard import InlineKeyboardBuilder

# Загружаем переменные из .env
load_dotenv()

TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
GITHUB_URL = os.getenv("GITHUB_URL")

# Проверка загрузки ключей
if not TOKEN or not TMDB_API_KEY:
    exit("❌ Ошибка: Переменные BOT_TOKEN или TMDB_API_KEY не найдены в .env")

bot = Bot(token=TOKEN)
dp = Dispatcher()
http_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = aiohttp.ClientSession()
    polling_task = asyncio.create_task(dp.start_polling(bot))
    print(f"🚀 Сервер запущен! Работаем с: {GITHUB_URL}")
    yield
    await http_client.close()
    polling_task.cancel()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@dp.message()
async def welcome(message: types.Message):
    builder = InlineKeyboardBuilder()
    # v={random} гарантирует загрузку свежей версии Mini App
    url = f"{GITHUB_URL}?v={random.randint(1, 99999)}"
    builder.button(text="🎬 Подобрать фильм", web_app=WebAppInfo(url=url))
    await message.answer("Привет! Нажми кнопку, чтобы открыть подборщик:", reply_markup=builder.as_markup())

@app.get("/get_movie")
async def get_movie():
    page = random.randint(1, 15)
    url = f"https://api.themoviedb.org/3/movie/popular?api_key={TMDB_API_KEY}&language=ru-RU&page={page}"
    async with http_client.get(url) as resp:
        if resp.status == 200:
            data = await resp.json()
            movie = random.choice(data['results'])
            return {"title": movie['title'], "poster": movie['poster_path']}
    return {"error": "TMDB unreachable"}

@app.get("/proxy_image")
async def proxy_image(path: str):
    tmdb_url = f"https://image.tmdb.org/p/w500{path}"
    async with http_client.get(tmdb_url) as resp:
        if resp.status == 200:
            content = await resp.read()
            return Response(content=content, media_type="image/jpeg")
    return Response(status_code=404)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)