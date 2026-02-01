import asyncio
import random
import os
import aiohttp
import aiosqlite
import datetime
import dateparser
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import BotCommand, BotCommandScopeDefault
from aiogram.utils.keyboard import InlineKeyboardBuilder
from datetime import datetime as dt, timedelta # Добавляем 'as dt' для новых

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
    description: str = ""  # Добавили поле
    rating: str = "0.0"  # Добавили поле
    year: str = ""  # Добавили поле
    genres: str = ""  # Добавили поле


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
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                description TEXT,
                rating TEXT,
                year TEXT,
                genres TEXT
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

    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Получаем данные комнаты и партнера
        async with db.execute(
                "SELECT user1_id, user2_id, genre FROM rooms WHERE user1_id = ? OR user2_id = ?",
                (user_id, user_id)
        ) as cursor:
            room = await cursor.fetchone()

        partner_id = None
        if room:
            partner_id = room[1] if room[0] == user_id else room[0]
            if not genre or genre == "all":
                genre = room[2]

        # 2. Собираем список исключений (что вы уже видели)
        async with db.execute("SELECT movie_id FROM seen_movies WHERE user_id = ?", (user_id,)) as cursor:
            my_seen = {row[0] for row in await cursor.fetchall()}

        # 3. Находим лайки партнера, которые мы еще НЕ видели
        partner_likes = []
        if partner_id:
            async with db.execute(
                    "SELECT movie_id FROM likes WHERE user_id = ? AND movie_id NOT IN (SELECT movie_id FROM seen_movies WHERE user_id = ?)",
                    (partner_id, user_id)
            ) as cursor:
                partner_likes = [row[0] for row in await cursor.fetchall()]

    try:
        # 4. Формируем запрос к API для получения "свежих" фильмов
        if genre and genre != "all":
            base_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films?genres={genre}&order=NUM_VOTE&type=FILM&ratingFrom=6"
        else:
            base_url = "https://kinopoiskapiunofficial.tech/api/v2.2/films/collections?type=TOP_POPULAR_ALL"

        potential_movies = []

        # Берем фильмы с первых 3-х страниц для разнообразия
        for page in range(1, 4):
            async with http_client.get(f"{base_url}&page={page}", headers=headers) as resp:
                if resp.status != 200: continue
                data = await resp.json()
                items = data.get("items", []) or data.get("films", [])
                for m in items:
                    mid = m.get("kinopoiskId") or m.get("filmId")
                    if mid and mid not in my_seen:
                        potential_movies.append(mid)
            if len(potential_movies) > 40: break

        # 5. СМЕШИВАЕМ: Лайки партнера + Новые фильмы
        # Чтобы не было предсказуемости, перемешиваем весь список
        final_pool = list(set(partner_likes + potential_movies))
        random.shuffle(final_pool)

        if not final_pool:
            return {"title": "Фильмы закончились", "description": "Попробуйте сменить жанр!"}

        # 6. Берем первый ID из перемешанного пула и получаем детали
        movie_id = final_pool[0]
        details_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{movie_id}"

        async with http_client.get(details_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                g_list = [g['genre'] for g in data.get('genres', [])]
                return {
                    "id": movie_id,
                    "title": data.get("nameRu") or data.get("nameEn") or "Без названия",
                    "poster": data.get("posterUrl") or data.get("posterUrlPreview"),
                    "rating": str(data.get("ratingKinopoisk") or data.get("rating") or "0.0"),
                    "description": data.get("description") or "Описание отсутствует.",
                    "year": str(data.get("year") or "----"),
                    "genres": ", ".join(g_list[:2]).capitalize() or "Кино"
                }
    except Exception as e:
        print(f"Error in get_movie: {e}")

    return {"title": "Ошибка", "description": "Не удалось загрузить фильм."}


@app.get("/check_matches/{user_id}")
async def check_matches(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        # Ищем комнату, в которой состоит пользователь
        async with db.execute(
                "SELECT user1_id, user2_id FROM rooms WHERE user1_id = ? OR user2_id = ?",
                (user_id, user_id)
        ) as c:
            room = await c.fetchone()

        if not room:
            return {"status": "none"}

        # Определяем ID партнера
        partner_id = room[1] if room[0] == user_id else room[0]
        if not partner_id:
            return {"status": "none"}

        # Ищем фильмы, которые лайкнули ОБА (мэтчи)
        # Мы проверяем лайки за последние 10 секунд, чтобы не спамить старыми мэтчами
        query = """
            SELECT l1.movie_title 
            FROM likes l1
            JOIN likes l2 ON l1.movie_id = l2.movie_id
            WHERE l1.user_id = ? AND l2.user_id = ?
            AND l1.timestamp > datetime('now', '-10 seconds')
            ORDER BY l1.id DESC LIMIT 1
        """
        async with db.execute(query, (user_id, partner_id)) as c:
            match = await c.fetchone()
            if match:
                return {"status": "match", "movie": match[0]}

    return {"status": "none"}

@app.post("/like")
async def save_like(req: LikeRequest):
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Сохраняем лайк со ВСЕМИ данными (используем INSERT OR IGNORE чтобы не было дублей)
        await db.execute("""
            INSERT OR IGNORE INTO likes 
            (user_id, movie_id, movie_title, poster_url, description, rating, year, genres) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (req.user_id, req.movie_id, req.movie_title, req.poster_url,
              req.description, req.rating, req.year, req.genres))

        # Сохраняем в список просмотренных
        await db.execute("INSERT OR IGNORE INTO seen_movies (user_id, movie_id) VALUES (?, ?)",
                         (req.user_id, req.movie_id))

        now = get_now()
        await db.execute("UPDATE users SET last_active = ? WHERE user_id = ?", (now, req.user_id))

        # 2. Ищем комнату и проверяем мэтч у партнера (Твоя оригинальная логика)
        async with db.execute("SELECT user1_id, user2_id FROM rooms WHERE user1_id = ? OR user2_id = ?",
                              (req.user_id, req.user_id)) as c:
            room = await c.fetchone()

        if room:
            partner_id = room[1] if room[0] == req.user_id else room[0]
            if partner_id:
                async with db.execute("SELECT id FROM likes WHERE user_id = ? AND movie_id = ?",
                                      (partner_id, req.movie_id)) as c:
                    is_match = await c.fetchone()

                if is_match:
                    text = f"🍿 <b>У ВАС МЭТЧ!</b>\nФильм: {req.movie_title}"
                    try:
                        await bot.send_message(req.user_id, text, parse_mode="HTML")
                        await bot.send_message(partner_id, text, parse_mode="HTML")
                    except:
                        pass
                    await db.commit()
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
async def get_likes(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row  # Это позволит обращаться к колонкам по именам
        async with db.execute("SELECT * FROM likes WHERE user_id = ? ORDER BY id DESC", (user_id,)) as c:
            rows = await c.fetchall()
            # Превращаем строки базы в список словарей (JSON)
            return [dict(r) for r in rows]


@app.delete("/remove_like")
async def remove_like(user_id: int, movie_id: int):
    """
    Удаляет лайк из базы данных.
    Параметры передаются в URL: /remove_like?user_id=123&movie_id=456
    """
    print(f"DEBUG: Попытка удаления лайка: user={user_id}, movie={movie_id}")

    try:
        async with aiosqlite.connect(DB_NAME) as db:
            # Сначала проверяем, есть ли такой лайк вообще
            async with db.execute(
                    "SELECT 1 FROM likes WHERE user_id = ? AND movie_id = ?",
                    (user_id, movie_id)
            ) as cursor:
                exists = await cursor.fetchone()

            if not exists:
                print(f"DEBUG: Лайк не найден в базе")
                return {"status": "not_found", "message": "Лайк не найден"}

            # Удаляем
            await db.execute(
                "DELETE FROM likes WHERE user_id = ? AND movie_id = ?",
                (user_id, movie_id)
            )
            await db.commit()

            print(f"DEBUG: Лайк успешно удален")
            return {"status": "success"}

    except Exception as e:
        print(f"ERROR в remove_like: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/create_room")
async def create_room(req: RoomAction):
    async with aiosqlite.connect(DB_NAME) as db:
        # ПРИНУДИТЕЛЬНАЯ ОЧИСТКА: Удаляем все комнаты, где пользователь был участником
        # Это позволяет пересоздать комнату без ручного выхода
        await db.execute(
            "DELETE FROM rooms WHERE user1_id = ? OR user2_id = ?",
            (req.user_id, req.user_id)
        )

        # Генерация нового кода
        code = str(random.randint(1000, 9999))

        # Создание новой комнаты
        await db.execute(
            "INSERT INTO rooms (room_id, user1_id, user1_name, genre) VALUES (?, ?, ?, ?)",
            (code, req.user_id, req.user_name, req.genre)
        )
        await db.commit()

    return {"room_id": code}


@app.post("/join_room")
async def join_room(req: RoomAction):
    async with aiosqlite.connect(DB_NAME) as db:
        # ПРОВЕРКА 1: Нельзя войти к самому себе
        async with db.execute("SELECT user1_id, genre, user1_name FROM rooms WHERE room_id = ?", (req.room_id,)) as c:
            room = await c.fetchone()
            if not room:
                return {"status": "error", "message": "Комната не найдена"}
            if room[0] == req.user_id:
                return {"status": "error", "message": "Вы не можете войти в свою же комнату"}

        # ПРОВЕРКА 2: Не занята ли комната вторым игроком
        async with db.execute("SELECT user1_name, genre FROM rooms WHERE room_id = ? AND user2_id IS NULL",
                              (req.room_id,)) as c:
            available_room = await c.fetchone()
            if available_room:
                await db.execute(
                    "UPDATE rooms SET user2_id = ?, user2_name = ?, last_activity = CURRENT_TIMESTAMP WHERE room_id = ?",
                    (req.user_id, req.user_name, req.room_id))
                await db.commit()
                return {"status": "success", "partner_name": available_room[0], "genre": available_room[1]}
    return {"status": "error", "message": "Комната уже заполнена"}


@app.get("/check_room/{room_id}")
async def check_room_status(room_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        # Достаем ID и имена обоих участников
        async with db.execute(
                "SELECT user1_id, user2_id, user1_name, user2_name FROM rooms WHERE room_id = ?",
                (room_id,)
        ) as c:
            res = await c.fetchone()

            if res:
                u1_id, u2_id, u1_name, u2_name = res
                # Если гость (user2_id) уже подключился
                if u2_id is not None:
                    return {
                        "status": "joined",
                        "user1_name": u1_name,
                        "user2_name": u2_name,
                        "user1_id": u1_id,
                        "user2_id": u2_id
                    }

    return {"status": "waiting"}


@app.post("/leave_room")
async def leave_room(req: RoomAction):
    async with aiosqlite.connect(DB_NAME) as db:
        # 1. Сначала проверяем, кто именно выходит: создатель (user1) или гость (user2)
        async with db.execute(
                "SELECT user1_id FROM rooms WHERE room_id = ?",
                (req.room_id,)
        ) as cursor:
            room = await cursor.fetchone()

        if room:
            creator_id = room[0]
            if req.user_id == creator_id:
                # Если выходит создатель — удаляем комнату полностью
                await db.execute("DELETE FROM rooms WHERE room_id = ?", (req.room_id,))
            else:
                # Если выходит гость — просто очищаем его данные, оставляя комнату в статусе 'waiting'
                await db.execute(
                    "UPDATE rooms SET user2_id = NULL, user2_name = NULL WHERE room_id = ?",
                    (req.room_id,)
                )

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
                await bot.send_message(ticket[0], f"📨 <b>Ответ поддержки:</b>\n\nВы: <i>{ticket[1]}</i>\n\n👉 {req.text}",
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


# Состояния
admin_states = {}
broadcast_data = {}


@dp.message(Command("broadcast"))
async def start_broadcast(message: types.Message):
    if message.from_user.id != SUPER_ADMIN_ID: return
    admin_states[message.from_user.id] = 'waiting_msg'
    await message.answer("🚀 Режим рассылки!\n\n"
"Отправьте сообщение (текст, фото, кружок и т.д.), которое нужно разослать.\n"
"Чтобы отменить: /cancel")


@dp.message(Command("cancel"))
async def cancel_br(message: types.Message):
    admin_states.pop(message.from_user.id, None)
    broadcast_data.pop(message.from_user.id, None)
    await message.answer("❌ Отменено.")


@dp.message()
async def handle_broadcast(message: types.Message):
    uid = message.from_user.id
    # Проверяем, что пишет админ и что он в процессе создания рассылки
    if uid != SUPER_ADMIN_ID or uid not in admin_states:
        return

    state = admin_states[uid]

    if state == 'waiting_msg':
        broadcast_data[uid] = message
        admin_states[uid] = 'waiting_time'
        await message.answer(
            "⏳ Когда отправить?\n\n"
            "Примеры:\n"
            " `0` — мгновенно\n"
            " `18:00` — сегодня в шесть вечера\n"
            " `31.01 12:00` — конкретный день\n"
            " `через 3 дня` — (используя dateparser)"
        )

    elif state == 'waiting_time':
        # Используем dt (datetime as dt) для корректной работы с датами
        now = dt.now()
        target_time = None

        if message.text == "0":
            target_time = now
        else:
            # Парсим время через dateparser
            target_time = dateparser.parse(message.text, settings={'PREFER_DATES_FROM': 'future'})

            if not target_time:
                await message.answer("⚠️ Не понял формат. Попробуй еще раз (например, `15:30` или `01.02 10:00`)")
                return

        # Если время получилось в прошлом (например, ввели 10:00, а сейчас уже 11:00), прибавляем день
        if target_time < now:
            target_time += timedelta(days=1)

        msg_to_send = broadcast_data[uid]
        # Очищаем состояния сразу, чтобы админ мог пользоваться ботом дальше
        admin_states.pop(uid)
        broadcast_data.pop(uid)

        wait_seconds = (target_time - now).total_seconds()

        await message.answer(f"✅ Запланировано на: `{target_time.strftime('%d.%m %H:%M')}`\n"
                             f"(Ожидание: {int(wait_seconds // 3600)}ч {int((wait_seconds % 3600) // 60)}м)")

        # Фоновое ожидание перед отправкой
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)

        # ПРОЦЕСС РАССЫЛКИ
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT user_id FROM users") as c:
                users = await c.fetchall()

        success, blocked, errors = 0, 0, 0
        for u in users:
            try:
                # Копируем сообщение (текст, медиа, кружки и т.д.)
                await msg_to_send.copy_to(chat_id=u[0])
                success += 1
                await asyncio.sleep(0.05) # Плавная отправка, чтобы не поймать флуд-контроль
            except Exception as e:
                if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                    blocked += 1
                else:
                    errors += 1

        # Отчет админу по завершении
        await bot.send_message(uid,
                               f"📊 Результаты рассылки:\n"
                               f"✅ Доставлено: {success}\n"
                               f"🚫 Заблокировали бота: {blocked}\n"
                               f"⚠️ Ошибки: {errors}"
                               )

if __name__ == "__main__":
    import uvicorn

    if os.path.exists(DB_NAME): print("⚠️ DB found")
    uvicorn.run(app, host="0.0.0.0", port=8080)
