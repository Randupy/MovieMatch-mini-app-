import asyncio
import random
import os
import aiohttp
import aiosqlite
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiogram import Bot

load_dotenv()

# --- Конфигурация ---
TOKEN = os.getenv("BOT_TOKEN")
KP_API_KEY = os.getenv("KP_API_KEY")
DB_NAME = "movies.db"
bot = Bot(token=TOKEN)

http_client = None


# --- Модели данных ---
class LikeRequest(BaseModel):
    user_id: int
    movie_id: int  # Важно: ID для точного матча
    movie_title: str
    poster_url: str


class RoomAction(BaseModel):
    user_id: int
    room_id: str = None


# --- Фоновые задачи ---
async def cleanup_rooms():
    while True:
        try:
            await asyncio.sleep(300)  # Проверка каждые 5 минут
            async with aiosqlite.connect(DB_NAME) as db:
                # Удаляем комнаты без активности более 30 минут
                await db.execute("""
                    DELETE FROM rooms 
                    WHERE (strftime('%s','now') - strftime('%s', last_activity)) > 1800
                """)
                await db.commit()
        except Exception as e:
            print(f"Ошибка очистки комнат: {e}")


# --- Жизненный цикл (Lifespan) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = aiohttp.ClientSession()

    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Таблица лайков с movie_id
        await db.execute("""
            CREATE TABLE IF NOT EXISTS likes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                movie_id INTEGER,
                movie_title TEXT,
                poster_url TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 2. Таблица комнат
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_id TEXT PRIMARY KEY,
                user1_id INTEGER,
                user2_id INTEGER DEFAULT NULL,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

    cleanup_task = asyncio.create_task(cleanup_rooms())
    print("🚀 База готова, сервер запущен!")

    yield

    cleanup_task.cancel()
    await http_client.close()
    if bot.session:
        await bot.session.close()


# --- Приложение ---
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Эндпоинты ---

@app.get("/get_movie")
async def get_movie():
    headers = {"X-API-KEY": KP_API_KEY}
    try:
        # 1. Получаем ID фильма
        page = random.randint(1, 5)
        list_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/collections?type=TOP_POPULAR_ALL&page={page}"

        async with http_client.get(list_url, headers=headers) as resp:
            if resp.status != 200:
                return {"title": "Ошибка API", "description": "Не удалось получить список"}
            items_data = await resp.json()
            movie_item = random.choice(items_data.get("items", []))
            movie_id = movie_item.get("kinopoiskId")

        # 2. Получаем детали
        details_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{movie_id}"
        async with http_client.get(details_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                genres_list = [g['genre'] for g in data.get('genres', [])]
                genres_str = ", ".join(genres_list[:2]).capitalize()

                return {
                    "id": movie_id,  # <--- ОТПРАВЛЯЕМ ID НА ФРОНТЕНД
                    "title": data.get("nameRu") or data.get("nameEn") or "Без названия",
                    "poster": data.get("posterUrl") or data.get("posterUrlPreview"),
                    "rating": str(data.get("ratingKinopoisk") or data.get("rating") or "0.0"),
                    "description": data.get("description") or "Описание пока не добавлено.",
                    "year": str(data.get("year") or "----"),
                    "genres": genres_str or "Кино"
                }
    except Exception as e:
        print(f"Error: {e}")
        return {"title": "Ошибка сервера", "description": str(e)}
    return {"title": "Фильм не найден", "description": "Попробуйте еще раз"}


@app.post("/like")
async def save_like(req: LikeRequest):
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Сохраняем лайк
        await db.execute(
            "INSERT INTO likes (user_id, movie_id, movie_title, poster_url) VALUES (?, ?, ?, ?)",
            (req.user_id, req.movie_id, req.movie_title, req.poster_url)
        )

        # 2. Обновляем активность комнаты
        await db.execute("""
            UPDATE rooms SET last_activity = CURRENT_TIMESTAMP 
            WHERE user1_id = ? OR user2_id = ?
        """, (req.user_id, req.user_id))

        # 3. Ищем МАТЧ по ID
        async with db.execute("""
            SELECT r.user1_id, r.user2_id FROM rooms r
            JOIN likes l ON (l.user_id = r.user1_id OR l.user_id = r.user2_id)
            WHERE (r.user1_id = ? OR r.user2_id = ?) 
            AND l.movie_id = ? AND l.user_id != ?
        """, (req.user_id, req.user_id, req.movie_id, req.user_id)) as cursor:
            match = await cursor.fetchone()

            if match:
                partner_id = match[0] if match[0] != req.user_id else match[1]
                if partner_id:
                    text = f"🍿 У вас МАТЧ! Вы оба хотите посмотреть: {req.movie_title}\nhttps://www.kinopoisk.ru/film/{req.movie_id}/"
                    try:
                        await bot.send_message(req.user_id, text)
                        await bot.send_message(partner_id, text)
                    except Exception as e:
                        print(f"Ошибка отправки: {e}")
                return {"status": "match", "movie": req.movie_title}

        await db.commit()
    return {"status": "success"}


@app.get("/get_likes/{user_id}")
async def get_likes(user_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT movie_title, poster_url FROM likes WHERE user_id = ?", (user_id,)) as cursor:
            rows = await cursor.fetchall()
            return [{"title": r["movie_title"], "poster": r["poster_url"]} for r in rows]


@app.post("/create_room")
async def create_room(req: RoomAction):
    room_code = str(random.randint(1000, 9999))
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO rooms (room_id, user1_id) VALUES (?, ?)", (room_code, req.user_id))
        await db.commit()
    return {"room_id": room_code}


@app.post("/join_room")
async def join_room(req: RoomAction):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT * FROM rooms WHERE room_id = ? AND user2_id IS NULL", (req.room_id,)) as cur:
            room = await cur.fetchone()
            if room:
                await db.execute("UPDATE rooms SET user2_id = ?, last_activity = CURRENT_TIMESTAMP WHERE room_id = ?",
                                 (req.user_id, req.room_id))
                await db.commit()
                return {"status": "success"}
    return {"status": "error", "message": "Комната не найдена или занята"}


if __name__ == "__main__":
    import uvicorn

    # Удали старый файл movies.db перед запуском, чтобы схема обновилась!
    if os.path.exists(DB_NAME):
        print("⚠️ Рекомендуется удалить старый movies.db для обновления структуры таблиц.")

    uvicorn.run(app, host="0.0.0.0", port=8080)
