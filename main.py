import asyncio
import random
import os
import aiohttp
import aiosqlite
import datetime
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import BotCommand, BotCommandScopeDefault
from aiogram.utils.keyboard import InlineKeyboardBuilder

load_dotenv()

# --- Конфигурация ---
TOKEN = os.getenv("BOT_TOKEN")
KP_API_KEY = os.getenv("KP_API_KEY")
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", 0))
DB_NAME = "movies.db"

# Инициализация объектов
bot = Bot(token=TOKEN)
dp = Dispatcher()
http_client = None


# --- Модели данных API ---
class LikeRequest(BaseModel):
    user_id: int
    movie_id: int
    movie_title: str
    poster_url: str


class RoomAction(BaseModel):
    user_id: int
    user_name: str = "Игрок"
    room_id: str = None
    genre: str = None


# --- Модели для Web-Админки и Поддержки ---
class TicketReply(BaseModel):
    admin_id: int
    ticket_id: int
    text: str


class BroadcastRequest(BaseModel):
    admin_id: int
    text: str


class TicketRequest(BaseModel):
    user_id: int
    message: str
    msg_type: str = "text"


# --- Вспомогательные функции ---
def get_now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def is_admin(user_id):
    if user_id == SUPER_ADMIN_ID: return True
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone() is not None


# --- Фоновые задачи ---
async def cleanup_rooms():
    while True:
        try:
            await asyncio.sleep(300)
            async with aiosqlite.connect(DB_NAME) as db:
                await db.execute(
                    "DELETE FROM rooms WHERE (strftime('%s','now') - strftime('%s', last_activity)) > 1800")
                await db.commit()
        except Exception as e:
            print(f"Ошибка очистки комнат: {e}")


# --- Жизненный цикл (Lifespan) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    http_client = aiohttp.ClientSession()

    async with aiosqlite.connect(DB_NAME) as db:
        # Таблицы приложения
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
        # НОВАЯ ТАБЛИЦА: Просмотренные фильмы (чтобы не показывать повторно)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen_movies (
                user_id INTEGER,
                movie_id INTEGER,
                UNIQUE(user_id, movie_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                room_id TEXT PRIMARY KEY,
                user1_id INTEGER,
                user1_name TEXT,
                user2_id INTEGER DEFAULT NULL,
                user2_name TEXT DEFAULT NULL,
                genre TEXT DEFAULT NULL,
                last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Таблицы админки
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY, 
                username TEXT, 
                first_name TEXT, 
                joined_date TIMESTAMP,
                last_active TIMESTAMP,
                is_blocked INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                user_id INTEGER, 
                message TEXT, 
                msg_type TEXT DEFAULT 'text',
                status TEXT DEFAULT 'open', 
                admin_reply TEXT DEFAULT NULL,
                created_at TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY, 
                added_at TIMESTAMP
            )
        """)

        if SUPER_ADMIN_ID:
            await db.execute("INSERT OR IGNORE INTO admins (user_id, added_at) VALUES (?, ?)",
                             (SUPER_ADMIN_ID, get_now()))
        await db.commit()

    polling_task = asyncio.create_task(dp.start_polling(bot))
    cleanup_task = asyncio.create_task(cleanup_rooms())
    print("🚀 Сервер и Бот запущены!")

    yield

    polling_task.cancel()
    cleanup_task.cancel()
    await http_client.close()
    if bot.session: await bot.session.close()



# --- FastAPI Приложение ---
app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
#      API ЭНДПОИНТЫ (ДЛЯ WEB APP)
# ==========================================

@app.get("/get_movie")
async def get_movie(user_id: int, genre: str = None):
    headers = {"X-API-KEY": KP_API_KEY}

    # 1. Получаем список уже просмотренных ID
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT movie_id FROM seen_movies WHERE user_id = ?", (user_id,)) as cursor:
            seen_rows = await cursor.fetchall()
            seen_ids = {row[0] for row in seen_rows}

    try:
        # Генерируем пул из 60 страниц (1200 фильмов).
        # Перемешиваем, чтобы каждый раз искать в разных местах.
        pages_to_try = list(range(1, 61))
        random.shuffle(pages_to_try)

        movie_id = None

        # Пробуем проверить до 15 разных страниц из перемешанного списка
        for page in pages_to_try[:15]:
            base_url = "https://kinopoiskapiunofficial.tech/api/v2.2/films/collections?type=TOP_POPULAR_ALL"

            if genre and genre != "all":
                # Сортировка по количеству голосов (NUM_VOTE) - самое релевантное
                base_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films?genres={genre}&order=NUM_VOTE&type=FILM&ratingFrom=6&ratingTo=10&yearFrom=2000&yearTo=3000"

            list_url = f"{base_url}&page={page}"

            async with http_client.get(list_url, headers=headers) as resp:
                # Если словили лимит запросов (429) или ошибку, ждем и пропускаем итерацию
                if resp.status != 200:
                    await asyncio.sleep(0.2)  # Небольшая пауза перед следующей попыткой
                    continue

                items_data = await resp.json()
                items = items_data.get("items", [])

                # Фильтруем: оставляем только те, которых нет в seen_ids
                available_movies = [m for m in items if m.get("kinopoiskId") not in seen_ids]

                if available_movies:
                    movie_item = random.choice(available_movies)
                    movie_id = movie_item.get("kinopoiskId")
                    break  # Фильм найден, выходим из цикла

            # ВАЖНО: Минимальная задержка между запросами в цикле,
            # чтобы API не возвращал 429 ошибку, из-за которой бот думает, что фильмов нет.
            await asyncio.sleep(0.15)

        if not movie_id:
            return {"title": "Фильмы закончились", "description": "Попробуйте сменить жанр или сбросить историю."}

        # Получаем детали фильма
        details_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{movie_id}"
        async with http_client.get(details_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                genres = ", ".join([g['genre'] for g in data.get('genres', [])][:2]).capitalize()
                return {
                    "id": movie_id,
                    "title": data.get("nameRu") or data.get("nameEn") or "Без названия",
                    "poster": data.get("posterUrl") or data.get("posterUrlPreview"),
                    "rating": str(data.get("ratingKinopoisk") or data.get("rating") or "0.0"),
                    "description": data.get("description") or "Описание отсутствует.",
                    "year": str(data.get("year") or "----"),
                    "genres": genres or "Кино"
                }
    except Exception as e:
        print(f"Error: {e}")
        return {"title": "Ошибка сети", "description": "Попробуйте еще раз"}

    return {"title": "Ошибка", "description": "Не удалось загрузить фильм"}


@app.post("/like")
async def save_like(req: LikeRequest):
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Сохраняем в лайки
        await db.execute("INSERT INTO likes (user_id, movie_id, movie_title, poster_url) VALUES (?, ?, ?, ?)",
                         (req.user_id, req.movie_id, req.movie_title, req.poster_url))

        # 2. Сохраняем в просмотренные (чтобы не показывать снова)
        await db.execute("INSERT OR IGNORE INTO seen_movies (user_id, movie_id) VALUES (?, ?)",
                         (req.user_id, req.movie_id))

        now = get_now()
        await db.execute(
            "INSERT INTO users (user_id, last_active) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET last_active=?",
            (req.user_id, now, now))
        await db.execute("UPDATE rooms SET last_activity = CURRENT_TIMESTAMP WHERE user1_id = ? OR user2_id = ?",
                         (req.user_id, req.user_id))

        async with db.execute("""
            SELECT r.user1_id, r.user2_id FROM rooms r
            JOIN likes l ON (l.user_id = r.user1_id OR l.user_id = r.user2_id)
            WHERE (r.user1_id = ? OR r.user2_id = ?) AND l.movie_id = ? AND l.user_id != ?
        """, (req.user_id, req.user_id, req.movie_id, req.user_id)) as cursor:
            match = await cursor.fetchone()
            if match:
                partner_id = match[0] if match[0] != req.user_id else match[1]
                if partner_id:
                    text = f"🍿 <b>У ВАС МАТЧ!</b>\nФильм: {req.movie_title}\n<a href='https://www.kinopoisk.ru/film/{req.movie_id}/'>Смотреть</a>"
                    try:
                        await bot.send_message(req.user_id, text, parse_mode="HTML")
                        await bot.send_message(partner_id, text, parse_mode="HTML")
                    except:
                        pass
                return {"status": "match", "movie": req.movie_title}
        await db.commit()
    return {"status": "success"}


@app.post("/dislike")
async def save_dislike(req: LikeRequest):
    """Просто добавляем фильм в просмотренные, но не в лайки"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO seen_movies (user_id, movie_id) VALUES (?, ?)",
                         (req.user_id, req.movie_id))
        await db.commit()
    return {"status": "skipped"}


@app.get("/get_likes/{user_id}")
async def get_likes(user_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT movie_title, poster_url FROM likes WHERE user_id = ? ORDER BY id DESC",
                              (user_id,)) as c:
            return [{"title": r["movie_title"], "poster": r["poster_url"]} for r in await c.fetchall()]


@app.post("/create_room")
async def create_room(req: RoomAction):
    code = str(random.randint(1000, 9999))
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO rooms (room_id, user1_id, user1_name, genre) VALUES (?, ?, ?, ?)",
                         (code, req.user_id, req.user_name, req.genre))
        await db.commit()
    return {"room_id": code}


@app.post("/join_room")
async def join_room(req: RoomAction):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user1_name, genre FROM rooms WHERE room_id = ? AND user2_id IS NULL",
                              (req.room_id,)) as c:
            room = await c.fetchone()
            if room:
                await db.execute(
                    "UPDATE rooms SET user2_id = ?, user2_name = ?, last_activity = CURRENT_TIMESTAMP WHERE room_id = ?",
                    (req.user_id, req.user_name, req.room_id))
                await db.commit()
                return {"status": "success", "partner_name": room[0], "genre": room[1]}
    return {"status": "error", "message": "Комната занята или не найдена"}


@app.get("/check_room/{room_id}")
async def check_room_status(room_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user2_name FROM rooms WHERE room_id = ? AND user2_id IS NOT NULL",
                              (room_id,)) as c:
            res = await c.fetchone()
            if res:
                return {"status": "joined", "partner_name": res[0]}
    return {"status": "waiting"}


@app.post("/leave_room")
async def leave_room(req: RoomAction):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM rooms WHERE room_id = ? AND (user1_id = ? OR user2_id = ?)",
                         (req.room_id, req.user_id, req.user_id))
        await db.commit()
    return {"status": "success"}


# ==========================================
#      ПОДДЕРЖКА ПОЛЬЗОВАТЕЛЕЙ (ОБНОВЛЕНО)
# ==========================================

@app.post("/create_ticket")
async def create_ticket(req: TicketRequest):
    """Создает текстовый тикет"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO tickets (user_id, message, msg_type, created_at) VALUES (?, ?, ?, ?)",
            (req.user_id, req.message, "text", get_now())
        )
        await db.commit()

    # Уведомляем суперадмина
    try:
        await bot.send_message(
            SUPER_ADMIN_ID,
            f"🆘 <b>Новый тикет!</b>\nОт: <code>{req.user_id}</code>\n\n📝 {req.message}",
            parse_mode="HTML"
        )
    except:
        pass

    return {"status": "success"}


@app.get("/my_tickets/{user_id}")
async def get_my_tickets(user_id: int):
    """Возвращает историю тикетов вместе с ответами админа"""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
                "SELECT id, message, status, admin_reply, created_at FROM tickets WHERE user_id = ? ORDER BY id DESC",
                (user_id,)
        ) as c:
            # Возвращаем admin_reply пользователю
            return [dict(r) for r in await c.fetchall()]


# ==========================================
#      АДМИН API (ДЛЯ WEB APP)
# ==========================================

@app.get("/admin/check/{user_id}")
async def check_admin_rights(user_id: int):
    return {"is_admin": await is_admin(user_id)}


@app.get("/admin/stats/{user_id}")
async def get_admin_stats(user_id: int):
    if not await is_admin(user_id): raise HTTPException(403)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c: u_count = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM likes") as c: l_count = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM rooms") as c: r_count = (await c.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM tickets WHERE status='open'") as c: t_count = (await c.fetchone())[
            0]
    return {"users": u_count, "likes": l_count, "rooms": r_count, "tickets": t_count}


@app.get("/admin/tickets/{user_id}")
async def get_web_tickets(user_id: int):
    if not await is_admin(user_id): raise HTTPException(403)
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
                "SELECT id, user_id, message, created_at FROM tickets WHERE status='open'") as c:
            return [dict(r) for r in await c.fetchall()]


@app.post("/admin/reply_ticket")
async def reply_ticket_web(req: TicketReply):
    if not await is_admin(req.admin_id): raise HTTPException(403)
    async with aiosqlite.connect(DB_NAME) as db:
        # Проверяем наличие тикета
        async with db.execute("SELECT user_id, message FROM tickets WHERE id=?", (req.ticket_id,)) as c:
            ticket = await c.fetchone()

        if ticket:
            try:
                # Отправляем в Telegram
                await bot.send_message(ticket[0], f"📨 <b>Ответ поддержки:</b>\n\nВ: <i>{ticket[1]}</i>\n\n👉 {req.text}",
                                       parse_mode="HTML")

                # Сохраняем ответ в базу и закрываем тикет
                await db.execute("UPDATE tickets SET status='closed', admin_reply=? WHERE id=?",
                                 (req.text, req.ticket_id))
                await db.commit()
                return {"status": "success"}
            except:
                # Если бот заблокирован пользователем, все равно сохраняем ответ в базе,
                # чтобы он увидел его в приложении
                await db.execute("UPDATE tickets SET status='closed', admin_reply=? WHERE id=?",
                                 (req.text, req.ticket_id))
                await db.commit()
                return {"status": "success", "warning": "Telegram blocked, saved to DB"}

    return {"status": "error", "message": "Тикет не найден"}


@app.post("/admin/broadcast")
async def broadcast_web(req: BroadcastRequest):
    if not await is_admin(req.admin_id): raise HTTPException(403)
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as c:
            users = await c.fetchall()
    count = 0
    for u in users:
        try:
            await bot.send_message(u[0], req.text, parse_mode="HTML")
            count += 1
            await asyncio.sleep(0.05)
        except:
            pass
    return {"status": "success", "sent": count}


# ==========================================
#      AIOGRAM (Только запуск приложения)
# ==========================================

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO users (user_id, username, first_name, joined_date, last_active) VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET last_active=?",
            (message.from_user.id, message.from_user.username, message.from_user.first_name, get_now(), get_now(),
             get_now()))
        await db.commit()

    # ⚠️ ССЫЛКА НА ПРИЛОЖЕНИЕ (Замени на актуальную при перезапуске ngrok!)
    kb = InlineKeyboardBuilder()
    kb.button(text="🔥 Открыть MovieMatch",
              web_app=types.WebAppInfo(url="https://larviparous-intercondylic-sherilyn.ngrok-free.dev"))

    await message.answer("👋 Привет! Жми кнопку ниже, чтобы начать.", reply_markup=kb.as_markup())


if __name__ == "__main__":
    import uvicorn

    if os.path.exists(DB_NAME): print("⚠️ DB found")
    uvicorn.run(app, host="0.0.0.0", port=8080)
