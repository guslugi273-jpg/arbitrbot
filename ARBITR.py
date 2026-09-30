import asyncio
import csv
import io
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# =====================================================================
# КОНФИГУРАЦИЯ СИСТЕМЫ
# =====================================================================
BOT_TOKEN = "8979491056:AAEamiXQj9EZrl34ggOaPpJphHMaqMOXcYg"
CRYPTO_PAY_TOKEN = "640413:AAozTIOPhVCXP62brvl6Bt8kL0vp9ticohx"  # Токен из @CryptoBot (/net -> Mainnet / Testnet)
XROCKET_PAY_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBJZCI6IjMwMzE5OSIsImp0aSI6ImFwcDozMDMxOTk6NWQ1Yzk0MmMtODRjMy00ZjdjLWI1ODYtMzc4Mzc3MTg3MGZmIiwiaWF0IjoxNzkwNzUzOTYxfQ.4uSC0RvbOZR7VPr_CyHZX4J_zii4yboKt3n9qfxvfHo"# Токен из @xrocket (/pay -> API Keys)

CHANNEL_SIGNALS_ID = -1004368321305             # ID закрытого VIP-канала
REQUIRED_CHANNEL_ID = "@arbitrnewwws"    # Публичный ТГК для обязательной подписки
ADMIN_IDS = [8066395175]                         # Telegram ID администраторов

PRICES = {
    "week": {"usd": 7.0, "days": 7, "name": "1 Неделя ($7)"},
    "month": {"usd": 30.0, "days": 30, "name": "1 Месяц ($30)"}
}

MEME_KEYWORDS = {
    'doge', 'shib', 'pepe', 'wif', 'bonk', 'floki', 'bome', 'mew', 
    'popcat', 'turbo', 'neiro', 'brett', 'mog', 'myro', 'meme', 'lunc'
}

EXCHANGE_CLASSES = {
    'binance': ccxt.binance,
    'bybit': ccxt.bybit,
    'okx': ccxt.okx,
    'gate': ccxt.gate,
    'kucoin': ccxt.kucoin,
}

DB_NAME = "bot_database.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

# =====================================================================
# БАЗА ДАННЫХ (aiosqlite)
# =====================================================================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                sub_expiry TIMESTAMP,
                notified_24h INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id TEXT,
                provider TEXT,
                user_id INTEGER,
                amount REAL,
                plan TEXT,
                status TEXT DEFAULT 'active'
            )
        """)
        await db.commit()

async def get_user(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, username, sub_expiry, notified_24h FROM users WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchone()

async def update_or_create_user(user_id: int, username: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO users (user_id, username) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
        """, (user_id, username))
        await db.commit()

async def add_subscription(user_id: int, days: int):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT sub_expiry FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            
        now = datetime.now()
        if row and row[0]:
            current_expiry = datetime.fromisoformat(row[0])
            new_expiry = (current_expiry if current_expiry > now else now) + timedelta(days=days)
        else:
            new_expiry = now + timedelta(days=days)

        await db.execute("UPDATE users SET sub_expiry = ?, notified_24h = 0 WHERE user_id = ?", (new_expiry.isoformat(), user_id))
        await db.commit()
        return new_expiry

async def revoke_subscription(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET sub_expiry = NULL WHERE user_id = ?", (user_id,))
        await db.commit()

# =====================================================================
# ПЛАТЕЖНЫЕ ШЛЮЗЫ: CRYPTOBOT & XROCKET
# =====================================================================
class CryptoPayAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {"Crypto-Pay-API-Token": self.token}
        self.base_url = "https://pay.crypt.bot/api/"

    async def create_invoice(self, amount: float, payload: str) -> Optional[Dict]:
        url = f"{self.base_url}createInvoice"
        data = {"asset": "USDT", "amount": str(amount), "description": "Подписка на сканер арбитража", "payload": payload}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=data) as resp:
                    res = await resp.json()
                    if res.get("ok"):
                        return {
                            "invoice_id": str(res["result"]["invoice_id"]),
                            "pay_url": res["result"]["pay_url"]
                        }
        except Exception as e:
            logging.error(f"CryptoPay error: {e}")
        return None

    async def get_invoice(self, invoice_id: str) -> Optional[Dict]:
        url = f"{self.base_url}getInvoices"
        params = {"invoice_ids": invoice_id}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as resp:
                    res = await resp.json()
                    if res.get("ok") and res["result"]["items"]:
                        return res["result"]["items"][0]
        except Exception as e:
            logging.error(f"CryptoPay getInvoice error: {e}")
        return None


class XRocketPayAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {
            "Rocket-Pay-Key": self.token,
            "Content-Type": "application/json"
        }
        self.base_url = "https://pay.xrocket.tg/"

    async def create_invoice(self, amount: float, payload: str) -> Optional[Dict]:
        url = f"{self.base_url}tg-pay/invoice"
        data = {
            "amount": amount,
            "currency": "USDT",
            "description": "Подписка на сканер арбитража",
            "numPayments": 1,
            "payload": payload
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=data) as resp:
                    res = await resp.json()
                    if res.get("success") and res.get("data"):
                        return {
                            "invoice_id": str(res["data"]["id"]),
                            "pay_url": res["data"]["link"]
                        }
        except Exception as e:
            logging.error(f"XRocket error: {e}")
        return None

    async def get_invoice(self, invoice_id: str) -> Optional[Dict]:
        url = f"{self.base_url}tg-pay/invoice/{invoice_id}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers) as resp:
                    res = await resp.json()
                    if res.get("success") and res.get("data"):
                        return res["data"]
        except Exception as e:
            logging.error(f"XRocket getInvoice error: {e}")
        return None

crypto_pay = CryptoPayAPI(CRYPTO_PAY_TOKEN)
xrocket_pay = XRocketPayAPI(XROCKET_PAY_TOKEN)

# =====================================================================
# СКАНИРОВАНИЕ БИРЖ (5 сек через fetch_tickers)
# =====================================================================
async def fetch_top_100_clean_coins() -> List[str]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1}
    clean_symbols = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data:
                        symbol = item.get('symbol', '').upper()
                        coin_id = item.get('id', '').lower()
                        name = item.get('name', '').lower()
                        if not any(kw in coin_id or kw in name or kw == symbol.lower() for kw in MEME_KEYWORDS):
                            clean_symbols.append(f"{symbol}/USDT")
    except Exception as e:
        logging.error(f"CoinGecko error: {e}")
    return clean_symbols or ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT"]

async def fetch_exchange_tickers(ex_name: str, ex_obj) -> Optional[Dict]:
    try:
        tickers = await asyncio.wait_for(ex_obj.fetch_tickers(), timeout=4.0)
        return {ex_name: tickers}
    except Exception:
        return None

async def scan_market_5s(exchanges: Dict) -> List[Dict]:
    target_coins = set(await fetch_top_100_clean_coins())
    
    tasks = [fetch_exchange_tickers(name, ex) for name, ex in exchanges.items()]
    results = await asyncio.gather(*tasks)
    
    all_tickers = {}
    for res in results:
        if res:
            all_tickers.update(res)

    if len(all_tickers) < 2:
        return []

    signals = []
    for symbol in target_coins:
        coin_prices = {}
        for ex_name, tickers in all_tickers.items():
            if symbol in tickers:
                t = tickers[symbol]
                if t.get('bid') and t.get('ask') and t['bid'] > 0 and t['ask'] > 0:
                    coin_prices[ex_name] = {'bid': float(t['bid']), 'ask': float(t['ask'])}

        if len(coin_prices) < 2:
            continue

        best_buy, min_ask = min(coin_prices.items(), key=lambda x: x[1]['ask'])
        best_sell, max_bid = max(coin_prices.items(), key=lambda x: x[1]['bid'])

        if best_buy != best_sell:
            gross_spread = ((max_bid['bid'] - min_ask['ask']) / min_ask['ask']) * 100
            net_spread = gross_spread - 0.20

            if net_spread > 0.25:
                signals.append({
                    'symbol': symbol,
                    'buy_ex': best_buy.upper(),
                    'buy_price': min_ask['ask'],
                    'sell_ex': best_sell.upper(),
                    'sell_price': max_bid['bid'],
                    'gross_spread': gross_spread,
                    'net_spread': net_spread
                })

    return signals

# =====================================================================
# ФОНОВЫЕ ЗАДАЧИ
# =====================================================================
async def background_scanner_5s(bot: Bot, exchanges: Dict):
    """Сканирование рынка каждые 5 секунд."""
    while True:
        try:
            signals = await scan_market_5s(exchanges)
            for sig in signals:
                text = (
                    f"⚡️ <b>БЫСТРЫЙ СИГНАЛ: {sig['symbol']}</b>\n\n"
                    f"🟢 Покупка: <b>{sig['buy_ex']}</b> ({sig['buy_price']:.5f} USDT)\n"
                    f"🔴 Продажа: <b>{sig['sell_ex']}</b> ({sig['sell_price']:.5f} USDT)\n\n"
                    f"📊 Грязный спред: <code>{sig['gross_spread']:.2f}%</code>\n"
                    f"📈 <b>ЧИСТЫЙ СПРЕД: {sig['net_spread']:.2f}%</b>"
                )
                await bot.send_message(CHANNEL_SIGNALS_ID, text)
        except Exception as e:
            logging.error(f"Ошибка фонового сканера: {e}")
        await asyncio.sleep(5)

async def background_billing_checker(bot: Bot):
    """Фоновый опрос CryptoBot и xRocket без вебхуков."""
    while True:
        try:
            now = datetime.now()
            async with aiosqlite.connect(DB_NAME) as db:
                # 1. Проверка активных инвойсов
                async with db.execute("SELECT id, invoice_id, provider, user_id, plan FROM invoices WHERE status = 'active'") as cursor:
                    invoices = await cursor.fetchall()

                for row_id, inv_id, provider, u_id, plan in invoices:
                    is_paid = False
                    
                    if provider == "cryptobot":
                        inv = await crypto_pay.get_invoice(inv_id)
                        if inv and inv.get("status") == "paid":
                            is_paid = True
                    elif provider == "xrocket":
                        inv = await xrocket_pay.get_invoice(inv_id)
                        if inv and inv.get("status") in ["PAID", "paid"]:
                            is_paid = True

                    if is_paid:
                        exp_date = await add_subscription(u_id, PRICES[plan]["days"])
                        await db.execute("UPDATE invoices SET status = 'paid' WHERE id = ?", (row_id,))
                        await db.commit()
                        
                        provider_title = "@CryptoBot" if provider == "cryptobot" else "@xrocket"
                        await bot.send_message(
                            u_id,
                            f"🎉 <b>Оплата через {provider_title} успешно получена!</b>\n\n"
                            f"Вам активирована подписка: <b>{PRICES[plan]['name']}</b>.\n"
                            f"Срок действия до: <code>{exp_date.strftime('%Y-%m-%d %H:%M')}</code>"
                        )

                # 2. Предупреждение за 24 часа до окончания
                target_time = now + timedelta(hours=24)
                async with db.execute("SELECT user_id, sub_expiry FROM users WHERE notified_24h = 0 AND sub_expiry IS NOT NULL") as cursor:
                    users = await cursor.fetchall()

                for u_id, exp_str in users:
                    exp_dt = datetime.fromisoformat(exp_str)
                    if now < exp_dt <= target_time:
                        try:
                            await bot.send_message(u_id, "⏰ <b>Напоминание!</b> Ваша подписка закончится через 24 часа. Продлите её в меню бота.")
                            await db.execute("UPDATE users SET notified_24h = 1 WHERE user_id = ?", (u_id,))
                            await db.commit()
                        except Exception:
                            pass
        except Exception as e:
            logging.error(f"Billing Error: {e}")
        await asyncio.sleep(15)

# =====================================================================
# ТЕКСТЫ И КНОПКИ (Single Message UI)
# =====================================================================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
router = Router()

async def check_channel_sub(user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
        return m.status in ['creator', 'administrator', 'member']
    except Exception:
        return False

def get_main_menu_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="💎 Подписка & Тарифы", callback_data="menu_buy")],
        [InlineKeyboardButton(text="👤 Мой Кабинет", callback_data="menu_profile"), InlineKeyboardButton(text="💬 Поддержка", url="https://t.me/your_support")],
        [InlineKeyboardButton(text="📢 Наш Канал", url=f"https://t.me/{REQUIRED_CHANNEL_ID.replace('@', '')}")]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="⚙ Админ Панель", callback_data="menu_admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# =====================================================================
# ОБРАБОТЧИКИ КОМАНД И МЕНЮ
# =====================================================================
@router.message(Command("start"))
async def cmd_start(message: Message):
    await update_or_create_user(message.from_user.id, message.from_user.username or "User")
    
    if not await check_channel_sub(message.from_user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{REQUIRED_CHANNEL_ID.replace('@', '')}")],
            [InlineKeyboardButton(text="✅ Проверить подписку", callback_data="check_sub")]
        ])
        await message.answer("🔒 <b>Доступ ограничен!</b>\n\nЧтобы пользоваться ботом, нужно быть подписанным на наш канал.", reply_markup=kb)
        return

    is_admin = message.from_user.id in ADMIN_IDS
    text = (
        "👋 <b>Главное меню Crypto Arbitrage Bot</b>\n\n"
        "Бот анализирует цены на 5 ведущих биржах каждые 5 секунд и находит чистые арбитражные связки без мемкоинов.\n\n"
        "ℹ️ <i>Выберите нужное действие в меню ниже.</i>"
    )
    await message.answer(text, reply_markup=get_main_menu_kb(is_admin))

@router.callback_query(F.data == "menu_main")
async def cb_menu_main(call: CallbackQuery):
    is_admin = call.from_user.id in ADMIN_IDS
    text = (
        "👋 <b>Главное меню Crypto Arbitrage Bot</b>\n\n"
        "Бот анализирует цены на 5 ведущих биржах каждые 5 секунд и находит чистые арбитражные связки без мемкоинов.\n\n"
        "ℹ️ <i>Выберите нужное действие в меню ниже.</i>"
    )
    await call.message.edit_text(text, reply_markup=get_main_menu_kb(is_admin))

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    if await check_channel_sub(call.from_user.id):
        is_admin = call.from_user.id in ADMIN_IDS
        await call.message.edit_text("✅ <b>Подписка на канал подтверждена!</b>", reply_markup=get_main_menu_kb(is_admin))
    else:
        await call.answer("❌ Вы ещё не подписались на обязательный канал!", show_alert=True)

@router.callback_query(F.data == "menu_profile")
async def cb_profile(call: CallbackQuery):
    u = await get_user(call.from_user.id)
    sub_status = "❌ Не активна"
    if u and u[2]:
        exp = datetime.fromisoformat(u[2])
        if exp > datetime.now():
            sub_status = f"✅ Активна до {exp.strftime('%Y-%m-%d %H:%M')}"

    text = (
        f"👤 <b>Личный кабинет пользователя</b>\n\n"
        f"📋 <b>Ваш ID:</b> <code>{call.from_user.id}</code>\n"
        f"🏷 <b>Юзернейм:</b> @{call.from_user.username or 'отсутствует'}\n"
        f"💎 <b>Статус подписки:</b> {sub_status}\n\n"
        f"ℹ️ <i>Отображает текущий доступ к закрытому VIP-каналу с сигналами.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад в меню", callback_data="menu_main")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "menu_buy")
async def cb_buy(call: CallbackQuery):
    text = (
        "💎 <b>Покупка подписки на арбитражный сканер</b>\n\n"
        "После оплаты подписка выдается автоматически, а бот высылает инвайт в VIP-канал.\n\n"
        "📌 <b>Тарифные планы:</b>\n"
        "• <b>1 Неделя</b> — 7.00 $ (USDT)\n"
        "• <b>1 Месяц</b> — 30.00 $ (USDT)\n\n"
        "ℹ️ <i>Нажмите на нужный тариф для выбора способа оплаты (@CryptoBot или @xrocket).</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 1 Неделя — $7", callback_data="select_plan_week")],
        [InlineKeyboardButton(text="💳 1 Месяц — $30", callback_data="select_plan_month")],
        [InlineKeyboardButton(text="◀ Назад в меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

# Выбор платежного сервиса (@CryptoBot или @xrocket)
@router.callback_query(F.data.startswith("select_plan_"))
async def cb_select_gateway(call: CallbackQuery):
    plan_key = call.data.replace("select_plan_", "")
    plan = PRICES[plan_key]

    text = (
        f"💳 <b>Выбран тариф: {plan['name']}</b>\n\n"
        f"Выберите удобный сервис для оплаты:\n"
        f"• <b>CryptoBot</b> — оплата через @CryptoBot\n"
        f"• <b>xRocket</b> — оплата через @xrocket\n\n"
        f"ℹ️ <i>Оба способа работают автоматически без участия администратора.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🤖 Оплатить через @CryptoBot", callback_data=f"pay_cryptobot_{plan_key}")],
        [InlineKeyboardButton(text="🚀 Оплатить через @xrocket", callback_data=f"pay_xrocket_{plan_key}")],
        [InlineKeyboardButton(text="◀️ Назад к тарифам", callback_data="menu_buy")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

# Процесс выписки счета
@router.callback_query(F.data.startswith("pay_"))
async def cb_pay_process(call: CallbackQuery):
    parts = call.data.split("_")
    provider = parts[1]  # 'cryptobot' или 'xrocket'
    plan_key = parts[2]  # 'week' или 'month'
    plan = PRICES[plan_key]

    invoice = None
    if provider == "cryptobot":
        invoice = await crypto_pay.create_invoice(plan["usd"], f"{call.from_user.id}:{plan_key}")
    elif provider == "xrocket":
        invoice = await xrocket_pay.create_invoice(plan["usd"], f"{call.from_user.id}:{plan_key}")

    if not invoice:
        await call.answer("Ошибка при создании счета. Проверьте правильность токена шлюза.", show_alert=True)
        return

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO invoices (invoice_id, provider, user_id, amount, plan) VALUES (?, ?, ?, ?, ?)",
            (invoice["invoice_id"], provider, call.from_user.id, plan["usd"], plan_key)
        )
        await db.commit()

    provider_name = "@CryptoBot" if provider == "cryptobot" else "@xrocket"
    text = (
        f"🧾 <b>Счет на оплату сформирован ({provider_name})</b>\n\n"
        f"Тариф: <b>{plan['name']}</b>\n"
        f"Сумма: <b>{plan['usd']} USDT</b>\n\n"
        f"ℹ️ <i>Нажмите «Оплатить» ниже. После завершения транзакции подписка активируется в течение 15 секунд.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔗 Оплатить в {provider_name}", url=invoice["pay_url"])],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="menu_buy")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

# =====================================================================
# АДМИН ПАНЕЛЬ (/admin И INLINE)
# =====================================================================
async def build_admin_text_and_kb():
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c1:
            total_users = (await c1.fetchone())[0]
        
        now = datetime.now().isoformat()
        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_expiry > ?", (now,)) as c2:
            active_subs = (await c2.fetchone())[0]
            
        async with db.execute("SELECT SUM(amount) FROM invoices WHERE status = 'paid'") as c3:
            total_revenue = (await c3.fetchone())[0] or 0.0

    text = (
        f"⚙️ <b>Панель Администратора Code-01</b>\n\n"
        f"📊 <b>Статистика системы:</b>\n"
        f"• Всего пользователей в БД: <b>{total_users}</b>\n"
        f"• Активных подписок: <b>{active_subs}</b>\n"
        f"• Общая выручка: <b>{total_revenue:.2f} $</b>\n\n"
        f"⚙️ <b>Команды управления:</b>\n"
        f"👉 <code>/grant [user_id] [дней]</code> — Выдать подписку вручную\n"
        f"👉 <code>/revoke [user_id]</code> — Забрать подписку\n"
        f"👉 <code>/broadcast [текст]</code> — Сделать рассылку\n\n"
        f"ℹ️ <i>Нажмите кнопку ниже для выгрузки всей базы пользователей.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Выгрузить CSV логи", callback_data="admin_export")],
        [InlineKeyboardButton(text="◀️ Вернуться в главное меню", callback_data="menu_main")]
    ])
    return text, kb

@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    text, kb = await build_admin_text_and_kb()
    await message.answer(text, reply_markup=kb)

@router.callback_query(F.data == "menu_admin")
async def cb_admin_menu(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Доступ запрещен!", show_alert=True)
        return
    text, kb = await build_admin_text_and_kb()
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "admin_export")
async def cb_admin_export(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["User ID", "Username", "Subscription Expiry", "Status"])

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, username, sub_expiry FROM users") as cursor:
            rows = await cursor.fetchall()
            now = datetime.now()
            for u_id, uname, exp_str in rows:
                status = "Expired"
                if exp_str and datetime.fromisoformat(exp_str) > now:
                    status = "Active"
                writer.writerow([u_id, uname, exp_str or "N/A", status])

    file_bytes = output.getvalue().encode('utf-8')
    input_file = BufferedInputFile(file_bytes, filename="users_export.csv")
    await call.message.answer_document(input_file, caption="📊 Полная выгрузка базы подписок.")
    await call.answer("Файл выгружен!")

@router.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, target_id, days = message.text.split()
        exp = await add_subscription(int(target_id), int(days))
        await message.answer(f"✅ Выдана подписка юзеру <code>{target_id}</code> на {days} дн. До: {exp.strftime('%Y-%m-%d %H:%M')}")
        await bot.send_message(int(target_id), f"🎁 Администратор выдал вам подписку на {days} дней!")
    except Exception:
        await message.answer("❌ Формат: <code>/grant [user_id] [дней]</code>")

@router.message(Command("revoke"))
async def cmd_revoke(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, target_id = message.text.split()
        await revoke_subscription(int(target_id))
        await message.answer(f"🚫 Подписка пользователя <code>{target_id}</code> аннулирована.")
        await bot.send_message(int(target_id), "⚠️ Ваша подписка была аннулирована администратором.")
    except Exception:
        await message.answer("❌ Формат: <code>/revoke [user_id]</code>")

@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    text_to_send = message.text.replace("/broadcast", "").strip()
    if not text_to_send:
        await message.answer("❌ Формат: <code>/broadcast Ваш текст</code>")
        return

    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            users = await cursor.fetchall()

    count = 0
    for (u_id,) in users:
        try:
            await bot.send_message(u_id, f"📢 <b>Объявление:</b>\n\n{text_to_send}")
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await message.answer(f"✅ Рассылка завершена. Доставлено {count} пользователям.")

# =====================================================================
# ТОЧКА ВХОДА
# =====================================================================
async def main():
    await init_db()
    dp.include_router(router)
    
    exchanges = {name: cls({'enableRateLimit': True}) for name, cls in EXCHANGE_CLASSES.items()}

    asyncio.create_task(background_scanner_5s(bot, exchanges))
    asyncio.create_task(background_billing_checker(bot))

    logging.info("Бот запущен с поддержкой CryptoBot и xRocket...")
    try:
        await dp.start_polling(bot)
    finally:
        for ex in exchanges.values():
            await ex.close()

if __name__ == "__main__":
    asyncio.run(main())
