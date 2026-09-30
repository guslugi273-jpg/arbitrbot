import asyncio
import csv
import html
import io
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import aiohttp
import aiosqlite
import ccxt.async_support as ccxt

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
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
CRYPTO_PAY_TOKEN = "640413:AAozTIOPhVCXP62brvl6Bt8kL0vp9ticohx"

CHANNEL_SIGNALS_ID = -1004368321305             # ID закрытого VIP-канала
REQUIRED_CHANNEL_ID = "@arbitrnewwws"           # Публичный ТГК для обязательной подписки
ADMIN_IDS = [8066395175]                         # Telegram ID администраторов

PRICES = {
    "week": {"usd": 7.0, "days": 7, "name": "7 ДНЕЙ (PRO)"},
    "month": {"usd": 30.0, "days": 30, "name": "30 ДНЕЙ (VIP)"}
}

MEME_KEYWORDS = {
    'doge', 'shib', 'pepe', 'wif', 'bonk', 'floki', 'bome', 'mew', 
    'popcat', 'turbo', 'neiro', 'brett', 'mog', 'myro', 'meme', 'lunc'
}

EXCHANGE_NAMES = ['binance', 'bybit', 'okx', 'gate', 'kucoin']
EXCHANGE_CLASSES = {
    'binance': ccxt.binance,
    'bybit': ccxt.bybit,
    'okx': ccxt.okx,
    'gate': ccxt.gate,
    'kucoin': ccxt.kucoin,
}

DB_NAME = "bot_database.db"
LATEST_SIGNALS: List[Dict] = []
LAST_ALERT_TIMES: Dict[str, int] = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

class Form(StatesGroup):
    waiting_for_support = State()
    waiting_for_broadcast = State()
    waiting_for_trade_pair = State()
    waiting_for_trade_amount = State()
    waiting_for_trade_profit = State()
    waiting_for_calc_input = State()

# =====================================================================
# ХЕЛПЕРЫ ССЫЛОК И ТОРГОВЛИ
# =====================================================================
def get_trade_url(exchange: str, symbol: str) -> str:
    base = symbol.split('/')[0].upper()
    ex = exchange.lower()
    
    if ex == 'binance':
        return f"https://www.binance.com/ru/trade/{base}_USDT?type=spot"
    elif ex == 'bybit':
        return f"https://www.bybit.com/trade/usdt/{base}"
    elif ex == 'okx':
        return f"https://www.okx.com/trade-spot/{base.lower()}-usdt"
    elif ex == 'gate':
        return f"https://www.gate.io/trade/{base}_USDT"
    elif ex == 'kucoin':
        return f"https://www.kucoin.com/trade/{base}-USDT"
    return "https://t.me"

# =====================================================================
# БАЗА ДАННЫХ (aiosqlite)
# =====================================================================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                sub_expiry INTEGER DEFAULT 0,
                notified_24h INTEGER DEFAULT 0,
                min_spread REAL DEFAULT 0.3,
                enabled_exchanges TEXT DEFAULT '["binance","bybit","okx","gate","kucoin"]'
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id TEXT UNIQUE,
                provider TEXT,
                user_id INTEGER,
                amount REAL,
                plan TEXT,
                status TEXT DEFAULT 'active'
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                pair_info TEXT,
                amount_usd REAL,
                profit_usd REAL,
                roi_percent REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('support_username', '@support_arbitrage')")
        await db.commit()

        # Автоматическая миграция подписок из ISO-строк в INTEGER (при необходимости)
        async with db.execute("SELECT user_id, sub_expiry FROM users WHERE sub_expiry IS NOT NULL") as cursor:
            rows = await cursor.fetchall()
            for u_id, exp_val in rows:
                if isinstance(exp_val, str) and not exp_val.isdigit():
                    try:
                        dt = datetime.fromisoformat(exp_val)
                        ts = int(dt.timestamp())
                        await db.execute("UPDATE users SET sub_expiry = ? WHERE user_id = ?", (ts, u_id))
                    except Exception:
                        await db.execute("UPDATE users SET sub_expiry = 0 WHERE user_id = ?", (u_id,))
            await db.commit()

async def get_setting(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else default

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, value))
        await db.commit()

async def get_sub_expiry(user_id: int) -> int:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT sub_expiry FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row and row[0] is not None:
                try:
                    return int(row[0])
                except (ValueError, TypeError):
                    return 0
            return 0

async def get_user_data(user_id: int) -> dict:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, username, sub_expiry, min_spread, enabled_exchanges FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                sub_exp = int(row[2]) if row[2] is not None and str(row[2]).isdigit() else 0
                return {
                    "user_id": row[0],
                    "username": row[1],
                    "sub_expiry": sub_exp,
                    "min_spread": row[3] or 0.3,
                    "exchanges": json.loads(row[4]) if row[4] else EXCHANGE_NAMES
                }
            return {"user_id": user_id, "username": "User", "sub_expiry": 0, "min_spread": 0.3, "exchanges": EXCHANGE_NAMES}

async def update_user(user_id: int, username: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO users (user_id, username) VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
        """, (user_id, username))
        await db.commit()

async def update_user_settings(user_id: int, min_spread: float = None, exchanges: list = None):
    async with aiosqlite.connect(DB_NAME) as db:
        if min_spread is not None:
            await db.execute("UPDATE users SET min_spread = ? WHERE user_id = ?", (min_spread, user_id))
        if exchanges is not None:
            await db.execute("UPDATE users SET enabled_exchanges = ? WHERE user_id = ?", (json.dumps(exchanges), user_id))
        await db.commit()

async def add_subscription(user_id: int, days: int) -> int:
    now = int(time.time())
    current_expiry = await get_sub_expiry(user_id)
    base = max(now, current_expiry)
    new_expiry = base + (days * 86400)

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO users (user_id, username, sub_expiry, notified_24h) 
            VALUES (?, 'User', ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET sub_expiry = excluded.sub_expiry, notified_24h = 0
        """, (user_id, new_expiry))
        await db.commit()
    return new_expiry

async def revoke_subscription(user_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE users SET sub_expiry = 0 WHERE user_id = ?", (user_id,))
        await db.commit()

async def is_user_subscribed(user_id: int) -> bool:
    expiry = await get_sub_expiry(user_id)
    return expiry > int(time.time())

# =====================================================================
# ФУНКЦИИ ДНЕВНИКА СДЕЛОК
# =====================================================================
async def save_trade(user_id: int, pair_info: str, amount_usd: float, profit_usd: float):
    roi = (profit_usd / amount_usd * 100) if amount_usd > 0 else 0.0
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
            INSERT INTO trades (user_id, pair_info, amount_usd, profit_usd, roi_percent)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, pair_info, amount_usd, profit_usd, roi))
        await db.commit()

async def get_user_trade_stats(user_id: int) -> dict:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT COUNT(*), COALESCE(SUM(amount_usd), 0), COALESCE(SUM(profit_usd), 0), COALESCE(AVG(roi_percent), 0)
            FROM trades WHERE user_id = ?
        """, (user_id,)) as cursor:
            row = await cursor.fetchone()
            return {
                "count": row[0],
                "total_volume": round(row[1], 2),
                "total_profit": round(row[2], 2),
                "avg_roi": round(row[3], 2)
            }

async def get_recent_trades(user_id: int, limit: int = 5) -> list:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("""
            SELECT pair_info, amount_usd, profit_usd, roi_percent, created_at
            FROM trades WHERE user_id = ? ORDER BY id DESC LIMIT ?
        """, (user_id, limit)) as cursor:
            return await cursor.fetchall()

# =====================================================================
# ПЛАТЕЖНЫЙ ШЛЮЗ: CRYPTOBOT
# =====================================================================
class CryptoPayAPI:
    def __init__(self, token: str):
        self.token = token
        self.headers = {"Crypto-Pay-API-Token": self.token}
        self.base_url = "https://pay.crypt.bot/api/"

    async def create_invoice(self, amount: float, payload: str) -> Optional[Dict]:
        url = f"{self.base_url}createInvoice"
        data = {"asset": "USDT", "amount": str(amount), "description": "PRO Access Scanner", "payload": payload}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=data, timeout=10) as resp:
                    res = await resp.json()
                    if res.get("ok"):
                        return {"invoice_id": str(res["result"]["invoice_id"]), "pay_url": res["result"]["pay_url"]}
        except Exception as e:
            logging.error(f"CryptoPay error: {e}")
        return None

    async def get_invoice(self, invoice_id: str) -> Optional[Dict]:
        url = f"{self.base_url}getInvoices"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params={"invoice_ids": invoice_id}, timeout=10) as resp:
                    res = await resp.json()
                    if res.get("ok") and res["result"]["items"]:
                        return res["result"]["items"][0]
        except Exception as e:
            logging.error(f"CryptoPay getInvoice error: {e}")
        return None

crypto_pay = CryptoPayAPI(CRYPTO_PAY_TOKEN)

# =====================================================================
# СКАНИРОВАНИЕ БИРЖ
# =====================================================================
async def fetch_top_clean_coins(session: aiohttp.ClientSession) -> List[str]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 80, "page": 1}
    clean_symbols = []
    try:
        async with session.get(url, params=params, timeout=8) as resp:
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
    return clean_symbols or ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "AVAX/USDT", "NEAR/USDT", "SUI/USDT", "APT/USDT"]

async def fetch_exchange_tickers(ex_name: str, ex_obj) -> Optional[Dict]:
    try:
        tickers = await asyncio.wait_for(ex_obj.fetch_tickers(), timeout=3.5)
        return {ex_name: tickers}
    except Exception:
        return None

async def scan_market_5s(exchanges: Dict, session: aiohttp.ClientSession) -> List[Dict]:
    global LATEST_SIGNALS
    target_coins = set(await fetch_top_clean_coins(session))
    
    tasks = [fetch_exchange_tickers(name, ex) for name, ex in exchanges.items()]
    results = await asyncio.gather(*tasks)
    
    all_tickers = {}
    for res in results:
        if res:
            all_tickers.update(res)

    if len(all_tickers) < 2:
        return []

    signals = []
    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")
    
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

            if net_spread >= 0.25:
                est_profit_1k = round((1000 * (net_spread / 100)), 2)
                signals.append({
                    'symbol': symbol,
                    'buy_ex': best_buy.upper(),
                    'buy_price': min_ask['ask'],
                    'buy_url': get_trade_url(best_buy, symbol),
                    'sell_ex': best_sell.upper(),
                    'sell_price': max_bid['bid'],
                    'sell_url': get_trade_url(best_sell, symbol),
                    'gross_spread': round(gross_spread, 2),
                    'net_spread': round(net_spread, 2),
                    'est_profit': est_profit_1k,
                    'time': now_utc
                })

    LATEST_SIGNALS = signals
    return signals

# =====================================================================
# ФОНОВЫЕ ЗАДАЧИ
# =====================================================================
async def background_scanner_5s(bot: Bot, exchanges: Dict):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                signals = await scan_market_5s(exchanges, session)
                now_ts = int(time.time())
                for sig in signals:
                    symbol = sig['symbol']
                    # Кулдаун 120 сек на повторные алерты по одной паре
                    if sig['net_spread'] >= 0.40 and (now_ts - LAST_ALERT_TIMES.get(symbol, 0) > 120):
                        LAST_ALERT_TIMES[symbol] = now_ts
                        text = (
                            f"🚨 <b>ARBITRAGE ALERT | {sig['symbol']}</b>\n"
                            f"────────────────────────\n"
                            f"🟢 <b>BUY:</b> <a href='{sig['buy_url']}'>{sig['buy_ex']}</a> ➔ <code>{sig['buy_price']:.5f} USDT</code>\n"
                            f"🔴 <b>SELL:</b> <a href='{sig['sell_url']}'>{sig['sell_ex']}</a> ➔ <code>{sig['sell_price']:.5f} USDT</code>\n"
                            f"────────────────────────\n"
                            f"📈 <b>Чистый спред:</b> <code>+{sig['net_spread']}%</code>\n"
                            f"💵 <b>Профит с $1,000:</b> <code>~${sig['est_profit']} USDT</code>\n"
                            f"⏱ <b>Время:</b> <code>{sig['time']} UTC</code>\n"
                            f"🌐 <b>Сеть вывода:</b> <code>AVAILABLE [TRC20/ERC20]</code>"
                        )
                        kb = InlineKeyboardMarkup(inline_keyboard=[
                            [
                                InlineKeyboardButton(text=f"🟢 Купить на {sig['buy_ex']}", url=sig['buy_url']),
                                InlineKeyboardButton(text=f"🔴 Продать на {sig['sell_ex']}", url=sig['sell_url'])
                            ]
                        ])
                        await bot.send_message(CHANNEL_SIGNALS_ID, text, reply_markup=kb, disable_web_page_preview=True)
            except Exception as e:
                logging.error(f"Scanner background error: {e}")
            await asyncio.sleep(5)

async def background_billing_checker(bot: Bot):
    while True:
        try:
            now_ts = int(time.time())
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT id, invoice_id, provider, user_id, plan FROM invoices WHERE status = 'active'") as cursor:
                    invoices = await cursor.fetchall()

                for row_id, inv_id, provider, u_id, plan in invoices:
                    is_paid = False
                    if provider == "cryptobot":
                        inv = await crypto_pay.get_invoice(inv_id)
                        if inv and inv.get("status") == "paid":
                            is_paid = True

                    if is_paid:
                        # Атомарно меняем статус, чтобы исключить начисление дважды
                        cursor_upd = await db.execute("UPDATE invoices SET status = 'paid' WHERE id = ? AND status = 'active'", (row_id,))
                        await db.commit()
                        
                        if cursor_upd.rowcount > 0:
                            new_exp_ts = await add_subscription(u_id, PRICES[plan]["days"])
                            exp_dt_str = datetime.fromtimestamp(new_exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
                            
                            is_admin = u_id in ADMIN_IDS
                            kb = await get_main_menu_kb(u_id, is_admin)
                            try:
                                await bot.send_message(
                                    u_id,
                                    f"✅ <b>ОПЛАТА ПОДТВЕРЖДЕНА!</b>\n\n"
                                    f"Активирован тариф: <b>{PRICES[plan]['name']}</b>\n"
                                    f"Срок действия до: <code>{exp_dt_str}</code>\n\n"
                                    f"🔓 Все фильтры, быстрые сигналы и VIP-канал открыты.",
                                    reply_markup=kb
                                )
                            except Exception:
                                pass

                # Уведомления за 24 часа
                target_time = now_ts + 86400
                async with db.execute("SELECT user_id, sub_expiry FROM users WHERE notified_24h = 0 AND sub_expiry > ?", (now_ts,)) as cursor:
                    users = await cursor.fetchall()

                for u_id, exp_ts in users:
                    if exp_ts and now_ts < exp_ts <= target_time:
                        try:
                            await bot.send_message(u_id, "⚠️ <b>ВНИМАНИЕ!</b> До окончания вашей PRO-подписки осталось менее 24 часов. Продлите доступ, чтобы не потерять сигналы.")
                            await db.execute("UPDATE users SET notified_24h = 1 WHERE user_id = ?", (u_id,))
                            await db.commit()
                        except Exception:
                            pass
        except Exception as e:
            logging.error(f"Billing Error: {e}")
        await asyncio.sleep(10)

# =====================================================================
# UI / ИНТЕРФЕЙС И КНОПКИ
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

async def get_main_menu_kb(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    has_sub = await is_user_subscribed(user_id)
    support_user = await get_setting("support_username", "@support_arbitrage")
    clean_support = support_user.replace("@", "")

    buttons = []
    if has_sub:
        buttons.append([
            InlineKeyboardButton(text="⚡ LIVE СКАНЕР", callback_data="view_fast_signals"),
            InlineKeyboardButton(text="⚙️ ФИЛЬТРЫ", callback_data="menu_settings")
        ])
        buttons.append([InlineKeyboardButton(text="📊 ДНЕВНИК & КАЛЬКУЛЯТОР", callback_data="menu_trades")])
        buttons.append([InlineKeyboardButton(text="💳 ПРОДЛИТЬ ПОДПИСКУ", callback_data="menu_buy")])
    else:
        buttons.append([InlineKeyboardButton(text="🔥 АКТИВИРОВАТЬ PRO ДОСТУП", callback_data="menu_buy")])
        buttons.append([InlineKeyboardButton(text="📊 ДНЕВНИК & КАЛЬКУЛЯТОР", callback_data="menu_trades")])

    buttons.append([
        InlineKeyboardButton(text="👤 ЛИЧНЫЙ КАБИНЕТ", callback_data="menu_profile"),
        InlineKeyboardButton(text="📖 ИНСТРУКЦИЯ", callback_data="menu_guide")
    ])
    buttons.append([
        InlineKeyboardButton(text="💬 ПОДДЕРЖКА", url=f"https://t.me/{clean_support}"),
        InlineKeyboardButton(text="📢 НАШ КАНАЛ", url=f"https://t.me/{REQUIRED_CHANNEL_ID.replace('@', '')}")
    ])

    if is_admin:
        buttons.append([InlineKeyboardButton(text="🛠 АДМИН ПАНЕЛЬ", callback_data="menu_admin")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

# =====================================================================
# ОБРАБОТКА КОМАНД И МЕНЮ
# =====================================================================
@router.message(Command("start"))
async def cmd_start(message: Message):
    await update_user(message.from_user.id, message.from_user.username or "User")
    
    if not await check_channel_sub(message.from_user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 ПОДПИСАТЬСЯ НА КАНАЛ", url=f"https://t.me/{REQUIRED_CHANNEL_ID.replace('@', '')}")],
            [InlineKeyboardButton(text="🔄 ПРОВЕРИТЬ ПОДПИСКУ", callback_data="check_sub")]
        ])
        await message.answer(
            "🔒 <b>ДОСТУП ОГРАНИЧЕН</b>\n\n"
            "Для использования сканера подпишитесь на официальный канал сообщества.",
            reply_markup=kb
        )
        return

    is_admin = message.from_user.id in ADMIN_IDS
    kb = await get_main_menu_kb(message.from_user.id, is_admin)
    
    text = (
        "⚡️ <b>CRYPTO ARBITRAGE SCANNER PRO v3.4</b>\n"
        "────────────────────────\n"
        "🟢 <b>Статус системы:</b> <code>ONLINE [5 EXCHANGES]</code>\n"
        "⏱ <b>Интервал обновления:</b> <code>5 сек</code>\n"
        "🛡 <b>Фильтр мемкоинов:</b> <code>АКТИВЕН</code>\n\n"
        "Сканер отслеживает межбиржевой спред в реальном времени. Выберите действие в меню ниже."
    )
    await message.answer(text, reply_markup=kb)

@router.callback_query(F.data == "menu_main")
async def cb_menu_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = call.from_user.id in ADMIN_IDS
    kb = await get_main_menu_kb(call.from_user.id, is_admin)
    text = (
        "⚡️ <b>CRYPTO ARBITRAGE SCANNER PRO v3.4</b>\n"
        "────────────────────────\n"
        "🟢 <b>Статус системы:</b> <code>ONLINE [5 EXCHANGES]</code>\n"
        "⏱ <b>Интервал обновления:</b> <code>5 сек</code>\n"
        "🛡 <b>Фильтр мемкоинов:</b> <code>АКТИВЕН</code>\n\n"
        "Сканер отслеживает межбиржевой спред в реальном времени. Выберите действие в меню ниже."
    )
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    if await check_channel_sub(call.from_user.id):
        is_admin = call.from_user.id in ADMIN_IDS
        kb = await get_main_menu_kb(call.from_user.id, is_admin)
        await call.message.edit_text("✅ <b>Подписка подтверждена! Добро пожаловать.</b>", reply_markup=kb)
    else:
        await call.answer("❌ Вы не подписались на канал!", show_alert=True)

@router.callback_query(F.data == "menu_profile")
async def cb_profile(call: CallbackQuery):
    u = await get_user_data(call.from_user.id)
    stats = await get_user_trade_stats(call.from_user.id)
    
    now_ts = int(time.time())
    exp_ts = u.get("sub_expiry", 0)
    
    if exp_ts > now_ts:
        sub_badge = "🟢 PRO VIP"
        exp_info = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
    else:
        sub_badge = "🔴 НЕ АКТИВНА"
        exp_info = "Отсутствует"

    username_safe = html.escape(call.from_user.username or 'не задан')
    text = (
        f"👤 <b>ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ</b>\n"
        f"────────────────────────\n"
        f"🆔 <b>ID:</b> <code>{call.from_user.id}</code>\n"
        f"👤 <b>Юзернейм:</b> @{username_safe}\n"
        f"💎 <b>Статус подписки:</b> {sub_badge}\n"
        f"⏳ <b>Действительна до:</b> <code>{exp_info}</code>\n"
        f"⚙️ <b>Мин. спред:</b> <code>{u['min_spread']}%</code>\n\n"
        f"📊 <b>СТАТИСТИКА ТОРГОВЛИ:</b>\n"
        f"• Выполнено сделок: <b>{stats['count']}</b>\n"
        f"• Общий оборот: <b>${stats['total_volume']} USDT</b>\n"
        f"• Чистый профит: <b>+${stats['total_profit']} USDT</b>\n"
        f"• Средний ROI: <b>+{stats['avg_roi']}%</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]])
    await call.message.edit_text(text, reply_markup=kb)

# =====================================================================
# ДНЕВНИК СДЕЛОК И КАЛЬКУЛЯТОР
# =====================================================================
@router.callback_query(F.data == "menu_trades")
async def cb_trades_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    stats = await get_user_trade_stats(call.from_user.id)
    recent = await get_recent_trades(call.from_user.id, limit=3)

    history_text = ""
    if recent:
        history_text = "\n<b>📜 ПОСЛЕДНИЕ СДЕЛКИ:</b>\n"
        for p_info, amt, prof, roi, dt in recent:
            history_text += f"• <code>{html.escape(str(p_info))}</code> | Вход: <b>${amt}</b> | Профит: <b>+${prof} (+{roi}%)</b>\n"

    text = (
        f"📊 <b>ДНЕВНИК СДЕЛОК И КАЛЬКУЛЯТОР</b>\n"
        f"────────────────────────\n"
        f"💼 <b>Общий оборот:</b> <code>${stats['total_volume']} USDT</code>\n"
        f"💰 <b>Чистый заработок:</b> <code>+${stats['total_profit']} USDT</code>\n"
        f"📈 <b>Всего сделок:</b> <code>{stats['count']}</code>\n"
        f"{history_text}\n"
        f"Выберите нужное действие ниже:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ ЗАПИСАТЬ СДЕЛКУ", callback_data="trade_add"),
            InlineKeyboardButton(text="🧮 КАЛЬКУЛЯТОР", callback_data="trade_calc")
        ],
        [InlineKeyboardButton(text="📜 ПОЛНАЯ ИСТОРИЯ", callback_data="trade_history")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "trade_add")
async def cb_trade_add_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_trade_pair)
    text = (
        "➕ <b>ДОБАВЛЕНИЕ СДЕЛКИ (Шаг 1/3)</b>\n"
        "────────────────────────\n"
        "Введите парную информацию или название монеты и бирж.\n\n"
        "<i>Пример: <code>SOL Binance -> Bybit</code> или <code>BTC/USDT</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_trade_pair)
async def process_trade_pair(message: Message, state: FSMContext):
    await state.update_data(pair_info=message.text.strip())
    await state.set_state(Form.waiting_for_trade_amount)
    
    text = (
        "➕ <b>ДОБАВЛЕНИЕ СДЕЛКИ (Шаг 2/3)</b>\n"
        "────────────────────────\n"
        "Введите **сумму покупки / депозит** в USDT ($):\n\n"
        "<i>Пример: <code>1000</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_trades")]])
    await message.answer(text, reply_markup=kb)

@router.message(Form.waiting_for_trade_amount)
async def process_trade_amount(message: Message, state: FSMContext):
    try:
        amt = float(message.text.replace(',', '.').strip())
        if amt <= 0:
            raise ValueError()
        await state.update_data(amount_usd=amt)
        await state.set_state(Form.waiting_for_trade_profit)
        
        text = (
            "➕ <b>ДОБАВЛЕНИЕ СДЕЛКИ (Шаг 3/3)</b>\n"
            "────────────────────────\n"
            "Введите **полученный чистый профит** в USDT ($):\n\n"
            "<i>Пример: <code>18.5</code></i>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_trades")]])
        await message.answer(text, reply_markup=kb)
    except Exception:
        await message.answer("❌ Введите корректное число (например: 1000)!")

@router.message(Form.waiting_for_trade_profit)
async def process_trade_profit(message: Message, state: FSMContext):
    try:
        prof = float(message.text.replace(',', '.').strip())
        data = await state.get_data()
        await save_trade(message.from_user.id, data['pair_info'], data['amount_usd'], prof)
        
        roi = round((prof / data['amount_usd'] * 100), 2)
        await state.clear()
        
        text = (
            "✅ <b>СДЕЛКА УСПЕШНО СОХРАНЕНА!</b>\n"
            "────────────────────────\n"
            f"📌 Пара/Биржи: <b>{html.escape(data['pair_info'])}</b>\n"
            f"💵 Депозит: <b>${data['amount_usd']} USDT</b>\n"
            f"📈 Чистый профит: <b>+${prof} USDT (+{roi}%)</b>\n\n"
            "Данные внесены в ваш личный профиль и дневник!"
        )
        is_admin = message.from_user.id in ADMIN_IDS
        kb = await get_main_menu_kb(message.from_user.id, is_admin)
        await message.answer(text, reply_markup=kb)
    except Exception:
        await message.answer("❌ Введите корректное число профита (например: 15.5)!")

# =====================================================================
# БЫСТРЫЙ КАЛЬКУЛЯТОР
# =====================================================================
@router.callback_query(F.data == "trade_calc")
async def cb_calc_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_calc_input)
    text = (
        "🧮 <b>АРБИТРАЖНЫЙ КАЛЬКУЛЯТОР</b>\n"
        "────────────────────────\n"
        "Рассчитайте чистую прибыль с учетом стандартных комиссий бирж (0.1% Buy + 0.1% Sell).\n\n"
        "Отправьте 3 числа через пробел:\n"
        "<code>[Депозит $] [Цена покупки] [Цена продажи]</code>\n\n"
        "<i>Пример: <code>1000 65000 65800</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Назад", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_calc_input)
async def process_calc_input(message: Message, state: FSMContext):
    try:
        parts = message.text.replace(',', '.').split()
        if len(parts) != 3:
            raise ValueError()
        
        capital = float(parts[0])
        buy_p = float(parts[1])
        sell_p = float(parts[2])
        
        if capital <= 0 or buy_p <= 0 or sell_p <= 0:
            raise ValueError()

        # Расчет
        coins_bought = (capital * 0.999) / buy_p  
        usd_received = (coins_bought * sell_p) * 0.999 
        
        net_profit = round(usd_received - capital, 2)
        roi = round((net_profit / capital * 100), 2)
        gross_spread = round(((sell_p - buy_p) / buy_p * 100), 2)
        
        await state.clear()
        
        status_emoji = "🟢" if net_profit > 0 else "🔴"
        text = (
            f"🧮 <b>РЕЗУЛЬТАТ РАСЧЕТА</b>\n"
            f"────────────────────────\n"
            f"💵 Депозит: <b>${capital:.2f} USDT</b>\n"
            f"📈 Грязный спред: <b>+{gross_spread}%</b>\n"
            f"⚙️ Учтенная комиссия бирж: <b>-0.2%</b>\n"
            f"────────────────────────\n"
            f"{status_emoji} Чистый профит: <b>+${net_profit} USDT</b>\n"
            f"📊 Чистый ROI: <b>+{roi}%</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Рассчитать еще", callback_data="trade_calc")],
            [InlineKeyboardButton(text="◀️ В меню дневника", callback_data="menu_trades")]
        ])
        await message.answer(text, reply_markup=kb)
    except Exception:
        await message.answer("❌ Неверный формат! Введите 3 положительных числа через пробел.\n<i>Пример: 1000 65000 65800</i>")

@router.callback_query(F.data == "trade_history")
async def cb_trade_history(call: CallbackQuery):
    trades = await get_recent_trades(call.from_user.id, limit=15)
    if not trades:
        text = "📜 <b>ИСТОРИЯ СДЕЛОК ПУСТА</b>\n\nВы еще не зафиксировали ни одной сделки."
    else:
        text = "📜 <b>ПОЛНАЯ ИСТОРИЯ СДЕЛОК (Последние 15)</b>\n────────────────────────\n\n"
        for p_info, amt, prof, roi, dt in trades:
            text += f"🔹 <b>{html.escape(str(p_info))}</b>\n   Депозит: ${amt} | Профит: <b>+${prof} USDT (+{roi}%)</b>\n\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)

# =====================================================================
# НАСТРОЙКИ И ФИЛЬТРЫ
# =====================================================================
@router.callback_query(F.data == "menu_settings")
async def cb_settings(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("❌ Настройки доступны только для пользователей с PRO подпиской!", show_alert=True)
        return

    u = await get_user_data(call.from_user.id)
    text = (
        "⚙️ <b>НАСТРОЙКИ СКАНЕРА</b>\n"
        "────────────────────────\n"
        "Укажите минимальный процент чистейшего спреда и список используемых бирж для фильтрации связок."
    )
    
    spread_btns = []
    for sp in [0.3, 0.5, 1.0, 2.0]:
        mark = "✅ " if u["min_spread"] == sp else ""
        spread_btns.append(InlineKeyboardButton(text=f"{mark}{sp}%", callback_data=f"set_spread_{sp}"))

    ex_btns = []
    for ex in EXCHANGE_NAMES:
        enabled = ex in u["exchanges"]
        mark = "🟩 " if enabled else "🟥 "
        ex_btns.append(InlineKeyboardButton(text=f"{mark}{ex.upper()}", callback_data=f"toggle_ex_{ex}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        spread_btns,
        [ex_btns[0], ex_btns[1], ex_btns[2]],
        [ex_btns[3], ex_btns[4]],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data.startswith("set_spread_"))
async def cb_set_spread(call: CallbackQuery):
    val = float(call.data.replace("set_spread_", ""))
    await update_user_settings(call.from_user.id, min_spread=val)
    await cb_settings(call)

@router.callback_query(F.data.startswith("toggle_ex_"))
async def cb_toggle_ex(call: CallbackQuery):
    ex = call.data.replace("toggle_ex_", "")
    u = await get_user_data(call.from_user.id)
    exs = u["exchanges"]
    if ex in exs:
        if len(exs) > 2:
            exs.remove(ex)
    else:
        exs.append(ex)
    await update_user_settings(call.from_user.id, exchanges=exs)
    await cb_settings(call)

# =====================================================================
# ПРОСМОТР СИГНАЛОВ И ИНСТРУКЦИЙ
# =====================================================================
@router.callback_query(F.data == "view_fast_signals")
async def cb_view_fast_signals(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("❌ Доступно только при активной PRO-подписке!", show_alert=True)
        return

    u = await get_user_data(call.from_user.id)
    filtered = [
        s for s in LATEST_SIGNALS 
        if s['net_spread'] >= u['min_spread'] 
        and s['buy_ex'].lower() in u['exchanges'] 
        and s['sell_ex'].lower() in u['exchanges']
    ]

    if not filtered:
        text = (
            "⚡️ <b>LIVE SCANNER | ПОИСК СВЯЗОК</b>\n"
            "────────────────────────\n"
            "🔍 В данный момент нет связок, подходящих под ваши фильтры.\n"
            f"📌 Ваши параметры: Мин. спред <b>{u['min_spread']}%</b>\n\n"
            "<i>Сканер проверяет рынок каждые 5 секунд...</i>"
        )
    else:
        text = "⚡️ <b>АКТУАЛЬНЫЕ МЕЖБИРЖЕВЫЕ СВЯЗКИ:</b>\n────────────────────────\n\n"
        for sig in filtered[:4]:
            text += (
                f"🔹 <b>{sig['symbol']}</b> (Обновлено: <code>{sig['time']}</code>)\n"
                f"🟢 Покупка: <a href='{sig['buy_url']}'><b>{sig['buy_ex']}</b></a> ➔ <code>{sig['buy_price']:.5f} USDT</code>\n"
                f"🔴 Продажа: <a href='{sig['sell_url']}'><b>{sig['sell_ex']}</b></a> ➔ <code>{sig['sell_price']:.5f} USDT</code>\n"
                f"📈 Спред: <b>+{sig['net_spread']}%</b> | Профит с $1k: <b>~${sig['est_profit']}</b>\n\n"
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="view_fast_signals")],
        [InlineKeyboardButton(text="◀️ Назад в меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)

@router.callback_query(F.data == "menu_guide")
async def cb_guide(call: CallbackQuery):
    text = (
        "📖 <b>РЕГЛАМЕНТ ТОРГОВЛИ И ИНСТРУКЦИЯ</b>\n"
        "────────────────────────\n"
        "1. Получите сигнал в бота или VIP-канале.\n"
        "2. Перейдите по ссылке <b>BUY</b> и купите токен на первой бирже по маркету/лимиту.\n"
        "3. Перейдите в раздел вывода (Withdraw) и отправьте монеты на вторую биржу.\n"
        "4. После зачисления на второй бирже нажмите <b>SELL</b> и продайте токен обратно в USDT.\n\n"
        "⚠️ <b>ВАЖНО:</b> Всегда сверяйте статус сетей ввода/вывода (Deposit/Withdraw) перед проведением крупной сделки!"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]])
    await call.message.edit_text(text, reply_markup=kb)

# =====================================================================
# ОПЛАТА И ТАРИФЫ (ОПЛАТА XROCKET УДАЛЕНА)
# =====================================================================
@router.callback_query(F.data == "menu_buy")
async def cb_buy(call: CallbackQuery):
    text = (
        "💎 <b>ВЫБОР ТАРИФА PRO ДОСТУПА</b>\n"
        "────────────────────────\n"
        "В стоимость входит:\n"
        "• Мгновенный доступ к сканеру и приватным сигналам.\n"
        "• Гибкие настройки фильтров под ваш капитал.\n"
        "• Автоматические уведомления в закрытый VIP-канал.\n\n"
        "📌 <b>Доступные планы:</b>\n"
        "• <b>7 ДНЕЙ (PRO)</b> — 7.00 USDT\n"
        "• <b>30 ДНЕЙ (VIP)</b> — 30.00 USDT\n"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 7 ДНЕЙ — $7", callback_data="select_plan_week")],
        [InlineKeyboardButton(text="💳 30 ДНЕЙ — $30", callback_data="select_plan_month")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data.startswith("select_plan_"))
async def cb_select_plan(call: CallbackQuery):
    plan_key = call.data.replace("select_plan_", "")
    plan = PRICES[plan_key]

    invoice = await crypto_pay.create_invoice(plan["usd"], f"{call.from_user.id}:{plan_key}")

    if not invoice:
        await call.answer("Ошибка генерации чека. Попробуйте позже.", show_alert=True)
        return

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO invoices (invoice_id, provider, user_id, amount, plan) VALUES (?, 'cryptobot', ?, ?, ?)"
            " ON CONFLICT(invoice_id) DO NOTHING",
            (invoice["invoice_id"], call.from_user.id, plan["usd"], plan_key)
        )
        await db.commit()

    text = (
        f"🧾 <b>СЧЕТ НА ОПЛАТУ СФОРМИРОВАН</b>\n"
        f"────────────────────────\n"
        f"Шлюз: <b>@CryptoBot</b>\n"
        f"Тариф: <b>{plan['name']}</b>\n"
        f"К оплате: <b>{plan['usd']} USDT</b>\n\n"
        f"<i>Нажмите кнопку ниже для перехода к оплате. Подписка активируется автоматически сразу после подтверждения транзакции.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🔗 ОПЛАТИТЬ {plan['usd']} USDT", url=invoice["pay_url"])],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="menu_buy")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

# =====================================================================
# АДМИН-ПАНЕЛЬ
# =====================================================================
@router.callback_query(F.data == "menu_admin")
async def cb_admin_panel(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return

    support_user = await get_setting("support_username", "@support_arbitrage")
    now_ts = int(time.time())
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as c1:
            total_users = (await c1.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_expiry > ?", (now_ts,)) as c2:
            active_subs = (await c2.fetchone())[0]
        async with db.execute("SELECT SUM(amount) FROM invoices WHERE status = 'paid'") as c3:
            total_revenue = (await c3.fetchone())[0] or 0.0

    text = (
        f"⚙️ <b>ADMIN DASHBOARD</b>\n"
        f"────────────────────────\n"
        f"📊 <b>Всего юзеров:</b> <code>{total_users}</code>\n"
        f"💎 <b>Активных PRO:</b> <code>{active_subs}</code>\n"
        f"💰 <b>Выручка:</b> <code>${total_revenue:.2f} USDT</code>\n"
        f"💬 <b>Поддержка:</b> <code>{html.escape(support_user)}</code>\n\n"
        f"<b>Быстрые команды:</b>\n"
        f"• <code>/grant [ID] [Дней]</code> — Выдать доступ\n"
        f"• <code>/revoke [ID]</code> — Забрать доступ\n"
        f"• <code>/set_support [@username]</code> — Сменить контакт"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Массовая рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="💬 Сменить аккаунт поддержки", callback_data="admin_change_support")],
        [InlineKeyboardButton(text="📥 Экспорт базы (CSV)", callback_data="admin_export")],
        [InlineKeyboardButton(text="◀️ В главное меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "admin_change_support")
async def cb_admin_change_sup(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_support)
    await call.message.answer("✏️ Введите новый юзернейм поддержки (начинайте с @):")
    await call.answer()

@router.message(Form.waiting_for_support)
async def process_support_set(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    txt = message.text.strip()
    if not txt.startswith("@"):
        txt = "@" + txt
    await set_setting("support_username", txt)
    await state.clear()
    await message.answer(f"✅ Поддержка изменена на <b>{html.escape(txt)}</b>")

@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_broadcast)
    await call.message.answer("✏️ Введите текст рассылки (поддерживается HTML разметка):")
    await call.answer()

@router.message(Form.waiting_for_broadcast)
async def process_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    txt = message.text
    await state.clear()
    
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            users = await cursor.fetchall()

    sent = 0
    for (u_id,) in users:
        try:
            await bot.send_message(u_id, f"📢 <b>ОБЪЯВЛЕНИЕ СИСТЕМЫ:</b>\n\n{txt}")
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass
    await message.answer(f"✅ Рассылка успешно отправлена {sent} пользователям.")

@router.callback_query(F.data == "admin_export")
async def cb_admin_export(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["User ID", "Username", "Subscription Expiry (UTC)", "Status"])

    now_ts = int(time.time())
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, username, sub_expiry FROM users") as cursor:
            rows = await cursor.fetchall()
            for u_id, uname, exp_ts in rows:
                status = "Expired"
                exp_str = "N/A"
                if exp_ts and int(exp_ts) > 0:
                    exp_dt = datetime.fromtimestamp(int(exp_ts), tz=timezone.utc)
                    exp_str = exp_dt.strftime('%d.%m.%Y %H:%M UTC')
                    if int(exp_ts) > now_ts:
                        status = "Active"
                writer.writerow([u_id, uname or "N/A", exp_str, status])

    file_bytes = output.getvalue().encode('utf-8')
    input_file = BufferedInputFile(file_bytes, filename="users_database.csv")
    await call.message.answer_document(input_file, caption="📊 Полный экспорт пользователей системы.")
    await call.answer()

@router.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        parts = message.text.split()
        if len(parts) != 3:
            raise ValueError()
        target_user_id = int(parts[1])
        days = int(parts[2])
        if days <= 0:
            raise ValueError()

        exp_ts = await add_subscription(target_user_id, days)
        exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
        
        await message.answer(f"✅ Выдана подписка юзеру <code>{target_user_id}</code> на {days} дн. До: <b>{exp_str}</b>")
        try:
            kb = await get_main_menu_kb(target_user_id, target_user_id in ADMIN_IDS)
            await bot.send_message(
                target_user_id,
                f"🎉 <b>Вам активирован PRO доступ на {days} дней!</b>\n\n"
                f"Срок действия: до <code>{exp_str}</code>",
                reply_markup=kb
            )
        except Exception:
            pass
    except Exception:
        await message.answer("❌ Формат: <code>/grant [user_id] [дней]</code>")

@router.message(Command("revoke"))
async def cmd_revoke(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            raise ValueError()
        target_user_id = int(parts[1])
        await revoke_subscription(target_user_id)
        await message.answer(f"🚫 Подписка пользователя <code>{target_user_id}</code> аннулирована.")
    except Exception:
        await message.answer("❌ Формат: <code>/revoke [user_id]</code>")

@router.message(Command("set_support"))
async def cmd_set_support(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        parts = message.text.split()
        if len(parts) != 2:
            raise ValueError()
        new_sup = parts[1].strip()
        if not new_sup.startswith("@"):
            new_sup = "@" + new_sup
        await set_setting("support_username", new_sup)
        await message.answer(f"✅ Аккаунт поддержки обновлен: <b>{html.escape(new_sup)}</b>")
    except Exception:
        await message.answer("❌ Формат: <code>/set_support @username</code>")

# =====================================================================
# ЗАПУСК
# =====================================================================
async def main():
    await init_db()
    dp.include_router(router)
    
    exchanges = {name: cls({'enableRateLimit': True}) for name, cls in EXCHANGE_CLASSES.items()}

    asyncio.create_task(background_scanner_5s(bot, exchanges))
    asyncio.create_task(background_billing_checker(bot))

    logging.info("Арбитражный сканер PRO успешно запущен...")
    try:
        await dp.start_polling(bot)
    finally:
        for ex in exchanges.values():
            await ex.close()

if __name__ == "__main__":
    asyncio.run(main())
