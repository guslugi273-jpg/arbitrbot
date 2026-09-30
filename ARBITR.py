import asyncio
import csv
import html
import io
import json
import logging
import time
from datetime import datetime, timezone
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

# --- Конфигурация ---
BOT_TOKEN = "8979491056:AAEamiXQj9EZrl34ggOaPpJphHMaqMOXcYg"
CRYPTO_PAY_TOKEN = "640413:AAozTIOPhVCXP62brvl6Bt8kL0vp9ticohx"

CHANNEL_SIGNALS_ID = -1004368321305
REQUIRED_CHANNEL_ID = "@arbitrnewwws"
ADMIN_IDS = [8066395175]

PRICES = {
    "week": {"usd": 7.0, "days": 7, "name": "PRO (7 дней)"},
    "month": {"usd": 30.0, "days": 30, "name": "VIP (30 дней)"}
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
EXCHANGE_NAMES = list(EXCHANGE_CLASSES.keys())

DB_NAME = "bot_database.db"
LATEST_SIGNALS: List[Dict] = []
LAST_ALERT_TIMES: Dict[str, int] = {}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class Form(StatesGroup):
    waiting_for_support = State()
    waiting_for_broadcast = State()
    waiting_for_trade_pair = State()
    waiting_for_trade_amount = State()
    waiting_for_trade_profit = State()
    waiting_for_calc_input = State()


# --- Вспомогательные функции ---
def parse_expiry(val) -> int:
    """Безопасный парсинг даты окончания подписки."""
    if not val:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, str):
        v = val.strip()
        if not v:
            return 0
        try:
            return int(float(v))
        except ValueError:
            pass
        try:
            return int(datetime.fromisoformat(v).timestamp())
        except ValueError:
            pass
    return 0

def get_trade_url(exchange: str, symbol: str) -> str:
    base = symbol.split('/')[0].upper()
    ex = exchange.lower()
    urls = {
        'binance': f"https://www.binance.com/ru/trade/{base}_USDT?type=spot",
        'bybit': f"https://www.bybit.com/trade/usdt/{base}",
        'okx': f"https://www.okx.com/trade-spot/{base.lower()}-usdt",
        'gate': f"https://www.gate.io/trade/{base}_USDT",
        'kucoin': f"https://www.kucoin.com/trade/{base}-USDT"
    }
    return urls.get(ex, "https://t.me")


# --- Работа с базой данных ---
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

        # Валидация подписок при старте
        async with db.execute("SELECT user_id, sub_expiry FROM users WHERE sub_expiry IS NOT NULL AND sub_expiry != 0") as cursor:
            rows = await cursor.fetchall()
            for u_id, exp_val in rows:
                parsed = parse_expiry(exp_val)
                if parsed > 0 and parsed != exp_val:
                    await db.execute("UPDATE users SET sub_expiry = ? WHERE user_id = ?", (parsed, u_id))
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
            return parse_expiry(row[0]) if row else 0

async def get_user_data(user_id: int) -> dict:
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, username, sub_expiry, min_spread, enabled_exchanges FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return {
                    "user_id": row[0],
                    "username": row[1],
                    "sub_expiry": parse_expiry(row[2]),
                    "min_spread": row[3] if row[3] is not None else 0.3,
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
    current = await get_sub_expiry(user_id)
    new_expiry = max(now, current) + (days * 86400)

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
    if user_id in ADMIN_IDS:
        return True
    return (await get_sub_expiry(user_id)) > int(time.time())

# --- История сделок ---
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


# --- Оплата CryptoPay ---
class CryptoPayAPI:
    def __init__(self, token: str):
        self.headers = {"Crypto-Pay-API-Token": token}
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
            logging.error(f"CryptoPay get error: {e}")
        return None

crypto_pay = CryptoPayAPI(CRYPTO_PAY_TOKEN)


# --- Мониторинг рынка ---
async def fetch_top_clean_coins(session: aiohttp.ClientSession) -> List[str]:
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 80, "page": 1}
    clean_symbols = []
    try:
        async with session.get(url, params=params, timeout=8) as resp:
            if resp.status == 200:
                data = await resp.json()
                for item in data:
                    sym = item.get('symbol', '').upper()
                    cid = item.get('id', '').lower()
                    name = item.get('name', '').lower()
                    if not any(kw in cid or kw in name or kw == sym.lower() for kw in MEME_KEYWORDS):
                        clean_symbols.append(f"{sym}/USDT")
    except Exception as e:
        logging.error(f"CoinGecko error: {e}")
    return clean_symbols or ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "XRP/USDT", "ADA/USDT", "AVAX/USDT", "NEAR/USDT", "SUI/USDT"]

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
            gross = ((max_bid['bid'] - min_ask['ask']) / min_ask['ask']) * 100
            net = gross - 0.20

            if net >= 0.25:
                profit_1k = round((1000 * (net / 100)), 2)
                signals.append({
                    'symbol': symbol,
                    'buy_ex': best_buy.upper(),
                    'buy_price': min_ask['ask'],
                    'buy_url': get_trade_url(best_buy, symbol),
                    'sell_ex': best_sell.upper(),
                    'sell_price': max_bid['bid'],
                    'sell_url': get_trade_url(best_sell, symbol),
                    'gross_spread': round(gross, 2),
                    'net_spread': round(net, 2),
                    'est_profit': profit_1k,
                    'time': now_utc
                })

    LATEST_SIGNALS = signals
    return signals


# --- Фоновые процессы ---
async def background_scanner_5s(bot: Bot, exchanges: Dict):
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                signals = await scan_market_5s(exchanges, session)
                now_ts = int(time.time())
                for sig in signals:
                    symbol = sig['symbol']
                    if sig['net_spread'] >= 0.40 and (now_ts - LAST_ALERT_TIMES.get(symbol, 0) > 120):
                        LAST_ALERT_TIMES[symbol] = now_ts
                        text = (
                            f"🚨 <b>#{sig['symbol'].replace('/USDT', '')}</b> | <code>+{sig['net_spread']}%</code>\n"
                            f"──────────────\n"
                            f"🟢 <b>Покупка:</b> <a href='{sig['buy_url']}'>{sig['buy_ex']}</a> ➔ <code>{sig['buy_price']:.5f}</code>\n"
                            f"🔴 <b>Продажа:</b> <a href='{sig['sell_url']}'>{sig['sell_ex']}</a> ➔ <code>{sig['sell_price']:.5f}</code>\n"
                            f"──────────────\n"
                            f"💵 <b>Профит с $1,000:</b> <code>+${sig['est_profit']} USDT</code>\n"
                            f"⏱ <b>Обновлено:</b> <code>{sig['time']} UTC</code>"
                        )
                        kb = InlineKeyboardMarkup(inline_keyboard=[
                            [
                                InlineKeyboardButton(text=f"Купить ({sig['buy_ex']})", url=sig['buy_url']),
                                InlineKeyboardButton(text=f"Продать ({sig['sell_ex']})", url=sig['sell_url'])
                            ]
                        ])
                        await bot.send_message(CHANNEL_SIGNALS_ID, text, reply_markup=kb, disable_web_page_preview=True)
            except Exception as e:
                logging.error(f"Scanner bg error: {e}")
            await asyncio.sleep(5)

async def background_billing_checker(bot: Bot):
    while True:
        try:
            now_ts = int(time.time())
            async with aiosqlite.connect(DB_NAME) as db:
                async with db.execute("SELECT id, invoice_id, provider, user_id, plan FROM invoices WHERE status = 'active'") as cursor:
                    invoices = await cursor.fetchall()

                for row_id, inv_id, provider, u_id, plan in invoices:
                    try:
                        if provider == "cryptobot":
                            inv = await crypto_pay.get_invoice(inv_id)
                            if inv:
                                st = inv.get("status")
                                if st == "paid":
                                    upd = await db.execute("UPDATE invoices SET status = 'paid' WHERE id = ? AND status = 'active'", (row_id,))
                                    await db.commit()
                                    if upd.rowcount > 0:
                                        new_exp = await add_subscription(u_id, PRICES[plan]["days"])
                                        exp_str = datetime.fromtimestamp(new_exp, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
                                        kb = await get_main_menu_kb(u_id, u_id in ADMIN_IDS)
                                        try:
                                            await bot.send_message(
                                                u_id,
                                                f"🎉 <b>Оплата успешно получена!</b>\n\n"
                                                f"Тариф: <b>{PRICES[plan]['name']}</b>\n"
                                                f"Активен до: <code>{exp_str}</code>\n\n"
                                                f"Все функции сканера и VIP-канал доступны.",
                                                reply_markup=kb
                                            )
                                        except Exception:
                                            pass
                                elif st == "expired":
                                    await db.execute("UPDATE invoices SET status = 'expired' WHERE id = ?", (row_id,))
                                    await db.commit()
                    except Exception as inv_err:
                        logging.error(f"Invoice check error {inv_id}: {inv_err}")

                # Уведомление за 24 часа
                async with db.execute("SELECT user_id, sub_expiry FROM users WHERE notified_24h = 0 AND sub_expiry > ?", (now_ts,)) as cursor:
                    users = await cursor.fetchall()

                for u_id, exp_ts in users:
                    parsed_exp = parse_expiry(exp_ts)
                    if parsed_exp and now_ts < parsed_exp <= (now_ts + 86400):
                        try:
                            await bot.send_message(u_id, "⏳ <b>Напоминание:</b> До окончания подписки осталось меньше 24 часов.")
                            await db.execute("UPDATE users SET notified_24h = 1 WHERE user_id = ?", (u_id,))
                            await db.commit()
                        except Exception:
                            pass
        except Exception as e:
            logging.error(f"Billing bg error: {e}")
        await asyncio.sleep(10)


# --- Инициализация Aiogram ---
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
    support_user = (await get_setting("support_username", "@support_arbitrage")).replace("@", "")

    btns = []
    if has_sub:
        btns.append([
            InlineKeyboardButton(text="⚡ Сканер сигналов", callback_data="view_fast_signals"),
            InlineKeyboardButton(text="⚙️ Фильтры", callback_data="menu_settings")
        ])
    else:
        btns.append([InlineKeyboardButton(text="💎 Оформить подписку", callback_data="menu_buy")])

    btns.append([
        InlineKeyboardButton(text="📊 Дневник сделок", callback_data="menu_trades"),
        InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile")
    ])
    btns.append([
        InlineKeyboardButton(text="📖 Инструкция", callback_data="menu_guide"),
        InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{support_user}")
    ])

    if is_admin:
        btns.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="menu_admin")])

    return InlineKeyboardMarkup(inline_keyboard=btns)


# --- Обработчики команд и меню ---
@router.message(Command("start"))
async def cmd_start(message: Message):
    await update_user(message.from_user.id, message.from_user.username or "User")
    
    if not await check_channel_sub(message.from_user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться на канал", url=f"https://t.me/{REQUIRED_CHANNEL_ID.replace('@', '')}")],
            [InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")]
        ])
        await message.answer(
            "👋 <b>Добро пожаловать!</b>\n\n"
            "Для доступа к боту необходимо подписаться на наш основной канал.",
            reply_markup=kb
        )
        return

    is_admin = message.from_user.id in ADMIN_IDS
    kb = await get_main_menu_kb(message.from_user.id, is_admin)
    text = (
        "💎 <b>Arbitrage Hub</b>\n"
        "──────────────\n"
        "🟢 <b>Статус:</b> <code>ONLINE</code>\n"
        "🔄 <b>Биржи:</b> Binance, Bybit, OKX, Gate, KuCoin\n"
        "⏱ <b>Обновление:</b> 5 секунд\n\n"
        "Выберите нужный раздел в меню ниже:"
    )
    await message.answer(text, reply_markup=kb)

@router.callback_query(F.data == "menu_main")
async def cb_menu_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = call.from_user.id in ADMIN_IDS
    kb = await get_main_menu_kb(call.from_user.id, is_admin)
    text = (
        "💎 <b>Arbitrage Hub</b>\n"
        "──────────────\n"
        "🟢 <b>Статус:</b> <code>ONLINE</code>\n"
        "🔄 <b>Биржи:</b> Binance, Bybit, OKX, Gate, KuCoin\n"
        "⏱ <b>Обновление:</b> 5 секунд\n\n"
        "Выберите нужный раздел в меню ниже:"
    )
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    if await check_channel_sub(call.from_user.id):
        is_admin = call.from_user.id in ADMIN_IDS
        kb = await get_main_menu_kb(call.from_user.id, is_admin)
        await call.message.edit_text("✅ Подписка подтверждена!", reply_markup=kb)
    else:
        await call.answer("❌ Подписка не найдена. Проверьте еще раз!", show_alert=True)

@router.callback_query(F.data == "menu_profile")
async def cb_profile(call: CallbackQuery):
    u = await get_user_data(call.from_user.id)
    stats = await get_user_trade_stats(call.from_user.id)
    now_ts = int(time.time())
    exp_ts = u.get("sub_expiry", 0)

    if call.from_user.id in ADMIN_IDS or exp_ts > now_ts:
        sub_badge = "PRO"
        exp_info = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC') if exp_ts > now_ts else "Бессрочно (Admin)"
    else:
        sub_badge = "Не активна"
        exp_info = "—"

    uname = html.escape(call.from_user.username or 'не указан')
    text = (
        f"👤 <b>Личный кабинет</b>\n"
        f"──────────────\n"
        f"🆔 ID: <code>{call.from_user.id}</code>\n"
        f"Юзернейм: @{uname}\n"
        f"Подписка: <b>{sub_badge}</b>\n"
        f"Действует до: <code>{exp_info}</code>\n"
        f"Мин. спред: <code>{u['min_spread']}%</code>\n\n"
        f"📊 <b>Ваша статистика:</b>\n"
        f"• Сделок закрыто: <b>{stats['count']}</b>\n"
        f"• Общий оборот: <b>${stats['total_volume']} USDT</b>\n"
        f"• Чистый профит: <b>+${stats['total_profit']} USDT</b>\n"
        f"• Средний ROI: <b>+{stats['avg_roi']}%</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]])
    await call.message.edit_text(text, reply_markup=kb)


# --- Дневник и Калькулятор ---
@router.callback_query(F.data == "menu_trades")
async def cb_trades_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    stats = await get_user_trade_stats(call.from_user.id)
    recent = await get_recent_trades(call.from_user.id, limit=3)

    history = ""
    if recent:
        history = "\n<b>Последние записи:</b>\n"
        for p_info, amt, prof, roi, _ in recent:
            history += f"• <code>{html.escape(str(p_info))}</code> | Вход: ${amt} | Профит: <b>+${prof} (+{roi}%)</b>\n"

    text = (
        f"📊 <b>Дневник сделок & Калькулятор</b>\n"
        f"──────────────\n"
        f"💼 Оборот: <code>${stats['total_volume']} USDT</code>\n"
        f"💰 Профит: <code>+${stats['total_profit']} USDT</code>\n"
        f"📈 Сделок: <code>{stats['count']}</code>\n"
        f"{history}\n"
        f"Выберите действие:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Новая запись", callback_data="trade_add"),
            InlineKeyboardButton(text="🧮 Калькулятор", callback_data="trade_calc")
        ],
        [InlineKeyboardButton(text="📜 Вся история", callback_data="trade_history")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "trade_add")
async def cb_trade_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_trade_pair)
    text = (
        "📝 <b>Запись сделки (1/3)</b>\n\n"
        "Введите связку или монету:\n"
        "<i>Например: <code>SOL Binance -> Bybit</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_trade_pair)
async def process_trade_pair(message: Message, state: FSMContext):
    await state.update_data(pair_info=message.text.strip())
    await state.set_state(Form.waiting_for_trade_amount)
    await message.answer("📝 <b>Запись сделки (2/3)</b>\n\nВведите сумму входа ($):\n<i>Например: <code>1000</code></i>")

@router.message(Form.waiting_for_trade_amount)
async def process_trade_amount(message: Message, state: FSMContext):
    try:
        amt = float(message.text.replace(',', '.').strip())
        if amt <= 0:
            raise ValueError()
        await state.update_data(amount_usd=amt)
        await state.set_state(Form.waiting_for_trade_profit)
        await message.answer("📝 <b>Запись сделки (3/3)</b>\n\nВведите чистый профит ($):\n<i>Например: <code>12.5</code></i>")
    except Exception:
        await message.answer("❌ Введите корректную сумму числом (например: 1000)")

@router.message(Form.waiting_for_trade_profit)
async def process_trade_profit(message: Message, state: FSMContext):
    try:
        prof = float(message.text.replace(',', '.').strip())
        data = await state.get_data()
        await save_trade(message.from_user.id, data['pair_info'], data['amount_usd'], prof)
        
        roi = round((prof / data['amount_usd'] * 100), 2)
        await state.clear()
        
        kb = await get_main_menu_kb(message.from_user.id, message.from_user.id in ADMIN_IDS)
        await message.answer(
            f"✅ <b>Сделка зафиксирована!</b>\n\n"
            f"Пара: <b>{html.escape(data['pair_info'])}</b>\n"
            f"Сумма: <b>${data['amount_usd']}</b>\n"
            f"Профит: <b>+${prof} USDT (+{roi}%)</b>",
            reply_markup=kb
        )
    except Exception:
        await message.answer("❌ Введите числом профит (например: 12.5)")

@router.callback_query(F.data == "trade_calc")
async def cb_calc_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_calc_input)
    text = (
        "🧮 <b>Быстрый калькулятор</b>\n\n"
        "Автоматически учитывает комиссию бирж (0.1% + 0.1%).\n"
        "Отправьте 3 значения через пробел:\n"
        "<code>[Депозит] [Цена покупки] [Цена продажи]</code>\n\n"
        "<i>Пример: <code>1000 142.5 144.1</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_calc_input)
async def process_calc_input(message: Message, state: FSMContext):
    try:
        parts = message.text.replace(',', '.').split()
        if len(parts) != 3:
            raise ValueError()
        
        capital, buy_p, sell_p = map(float, parts)
        if capital <= 0 or buy_p <= 0 or sell_p <= 0:
            raise ValueError()

        bought = (capital * 0.999) / buy_p
        received = (bought * sell_p) * 0.999
        net_profit = round(received - capital, 2)
        roi = round((net_profit / capital * 100), 2)
        gross = round(((sell_p - buy_p) / buy_p * 100), 2)
        
        await state.clear()
        status = "🟢" if net_profit > 0 else "🔴"
        text = (
            f"🧮 <b>Расчет связки</b>\n"
            f"──────────────\n"
            f"Депозит: <b>${capital:.2f} USDT</b>\n"
            f"Грязный спред: <b>+{gross}%</b>\n"
            f"Комиссия бирж: <b>-0.2%</b>\n"
            f"──────────────\n"
            f"{status} Чистый профит: <b>+${net_profit} USDT</b>\n"
            f"📊 Чистый ROI: <b>+{roi}%</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Рассчитать еще", callback_data="trade_calc")],
            [InlineKeyboardButton(text="◀️ В дневник", callback_data="menu_trades")]
        ])
        await message.answer(text, reply_markup=kb)
    except Exception:
        await message.answer("❌ Формат: 3 положительных числа через пробел.\n<i>Пример: 1000 142.5 144.1</i>")

@router.callback_query(F.data == "trade_history")
async def cb_trade_history(call: CallbackQuery):
    trades = await get_recent_trades(call.from_user.id, limit=15)
    if not trades:
        text = "📜 <b>История сделок пуста.</b>"
    else:
        text = "📜 <b>История последних 15 сделок:</b>\n──────────────\n\n"
        for p_info, amt, prof, roi, _ in trades:
            text += f"• <b>{html.escape(str(p_info))}</b>\n  Депозит: ${amt} | Профит: <b>+${prof} USDT (+{roi}%)</b>\n\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)


# --- Настройки ---
@router.callback_query(F.data == "menu_settings")
async def cb_settings(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("❌ Настройки доступны только с PRO подпиской!", show_alert=True)
        return

    u = await get_user_data(call.from_user.id)
    text = (
        "⚙️ <b>Настройки фильтрации</b>\n"
        "──────────────\n"
        "Настройте минимальный спред и отключите ненужные биржи:"
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
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]
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
    if ex in exs and len(exs) > 2:
        exs.remove(ex)
    elif ex not in exs:
        exs.append(ex)
    await update_user_settings(call.from_user.id, exchanges=exs)
    await cb_settings(call)


# --- Сигналы и Инструкция ---
@router.callback_query(F.data == "view_fast_signals")
async def cb_view_fast_signals(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("❌ Доступно только подписчикам PRO!", show_alert=True)
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
            "⚡️ <b>Поиск связок...</b>\n\n"
            "В данный момент нет доступных арбитражных связок под ваши фильтры.\n"
            f"<i>Текущий порог спреда: <b>{u['min_spread']}%</b></i>"
        )
    else:
        text = "⚡️ <b>Актуальные связки в реальном времени:</b>\n──────────────\n\n"
        for sig in filtered[:4]:
            text += (
                f"🔹 <b>{sig['symbol']}</b> (<code>{sig['time']}</code>)\n"
                f"🟢 Покупка: <a href='{sig['buy_url']}'><b>{sig['buy_ex']}</b></a> ➔ <code>{sig['buy_price']:.5f}</code>\n"
                f"🔴 Продажа: <a href='{sig['sell_url']}'><b>{sig['sell_ex']}</b></a> ➔ <code>{sig['sell_price']:.5f}</code>\n"
                f"📈 Спред: <b>+{sig['net_spread']}%</b> | Профит с $1k: <b>~${sig['est_profit']}</b>\n\n"
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="view_fast_signals")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)

@router.callback_query(F.data == "menu_guide")
async def cb_guide(call: CallbackQuery):
    text = (
        "📖 <b>Как работать с арбитражными связками:</b>\n"
        "──────────────\n"
        "1. Выберите связку из списка сканера или VIP-канала.\n"
        "2. Перейдите по кнопке <b>Покупка</b> и купите монету по рыночной цене.\n"
        "3. Переведите монеты на вторую биржу (раздел Withdraw/Deposit).\n"
        "4. Нажмите <b>Продажа</b> и продайте токен обратно в USDT.\n\n"
        "⚠️ <i>Всегда проверяйте доступность сети перевода на обеих биржах перед сделкой!</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]])
    await call.message.edit_text(text, reply_markup=kb)


# --- Оплата ---
@router.callback_query(F.data == "menu_buy")
async def cb_buy(call: CallbackQuery):
    text = (
        "💎 <b>Оформление PRO доступа</b>\n"
        "──────────────\n"
        "Что вы получите:\n"
        "• Мгновенные сигналы в личных сообщениях\n"
        "• Доступ в закрытый VIP-канал\n"
        "• Фильтры по спреду и биржам\n"
        "• Встроенный дневник и калькулятор\n\n"
        "<b>Выберите тариф:</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="PRO (7 дней) — 7.00 USDT", callback_data="select_plan_week")],
        [InlineKeyboardButton(text="VIP (30 дней) — 30.00 USDT", callback_data="select_plan_month")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data.startswith("select_plan_"))
async def cb_select_plan(call: CallbackQuery):
    plan_key = call.data.replace("select_plan_", "")
    plan = PRICES[plan_key]

    invoice = await crypto_pay.create_invoice(plan["usd"], f"{call.from_user.id}:{plan_key}")
    if not invoice:
        await call.answer(" Ошибка создания чека. Попробуйте чуть позже.", show_alert=True)
        return

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "INSERT INTO invoices (invoice_id, provider, user_id, amount, plan) VALUES (?, 'cryptobot', ?, ?, ?)"
            " ON CONFLICT(invoice_id) DO NOTHING",
            (invoice["invoice_id"], call.from_user.id, plan["usd"], plan_key)
        )
        await db.commit()

    text = (
        f"💳 <b>Оплата через @CryptoBot</b>\n"
        f"──────────────\n"
        f"Тариф: <b>{plan['name']}</b>\n"
        f"Сумма: <b>{plan['usd']} USDT</b>\n\n"
        f"Для оплаты нажмите кнопку ниже. Подписка активируется автоматически."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Перейти к оплате (${plan['usd']})", url=invoice["pay_url"])],
        [InlineKeyboardButton(text="◀️ Назад к тарифам", callback_data="menu_buy")]
    ])
    await call.message.edit_text(text, reply_markup=kb)


# --- Админ панель ---
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
        f"🛠 <b>Админ-панель</b>\n"
        f"──────────────\n"
        f"👥 Всего пользователей: <code>{total_users}</code>\n"
        f"💎 Активных PRO: <code>{active_subs}</code>\n"
        f"💰 Доход: <code>${total_revenue:.2f} USDT</code>\n"
        f"💬 Поддержка: <code>{html.escape(support_user)}</code>\n\n"
        f"<b>Быстрый доступ (команды):</b>\n"
        f"• <code>/grant [ID] [Дней]</code>\n"
        f"• <code>/revoke [ID]</code>\n"
        f"• <code>/set_support [@username]</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Массовая рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="💬 Изменить поддержку", callback_data="admin_change_support")],
        [InlineKeyboardButton(text="📥 Выгрузить базу (CSV)", callback_data="admin_export")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "admin_change_support")
async def cb_admin_change_sup(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_support)
    await call.message.answer("✏️ Введите новый юзернейм поддержки (начиная с @):")
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
    await message.answer(f"✅ Юзернейм поддержки обновлен: <b>{html.escape(txt)}</b>")

@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_broadcast)
    await call.message.answer("✏️ Введите текст рассылки (поддерживаются HTML-теги):")
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
            await bot.send_message(u_id, f"📢 <b>Сообщение от администрации:</b>\n\n{txt}")
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass
    await message.answer(f"✅ Рассылка завершена. Доставлено пользователям: {sent}")

@router.callback_query(F.data == "admin_export")
async def cb_admin_export(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["User ID", "Username", "Expiry Date (UTC)", "Status"])

    now_ts = int(time.time())
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT user_id, username, sub_expiry FROM users") as cursor:
            rows = await cursor.fetchall()
            for u_id, uname, exp_ts in rows:
                parsed = parse_expiry(exp_ts)
                exp_str = datetime.fromtimestamp(parsed, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC') if parsed > 0 else "N/A"
                status = "Active" if parsed > now_ts else "Expired"
                writer.writerow([u_id, uname or "N/A", exp_str, status])

    file_bytes = output.getvalue().encode('utf-8')
    input_file = BufferedInputFile(file_bytes, filename="users_export.csv")
    await call.message.answer_document(input_file, caption="📊 Выгрузка базы пользователей.")
    await call.answer()

@router.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, target_id, days = message.text.split()
        exp_ts = await add_subscription(int(target_id), int(days))
        exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
        await message.answer(f"✅ Подписка для <code>{target_id}</code> выдана на {days} дн. До: <b>{exp_str}</b>")
    except Exception:
        await message.answer("❌ Формат: <code>/grant [ID] [Дни]</code>")

@router.message(Command("revoke"))
async def cmd_revoke(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, target_id = message.text.split()
        await revoke_subscription(int(target_id))
        await message.answer(f"🚫 Подписка пользователя <code>{target_id}</code> аннулирована.")
    except Exception:
        await message.answer("❌ Формат: <code>/revoke [ID]</code>")

@router.message(Command("set_support"))
async def cmd_set_support(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, new_sup = message.text.split()
        new_sup = new_sup.strip()
        if not new_sup.startswith("@"):
            new_sup = "@" + new_sup
        await set_setting("support_username", new_sup)
        await message.answer(f"✅ Поддержка обновлена: <b>{html.escape(new_sup)}</b>")
    except Exception:
        await message.answer("❌ Формат: <code>/set_support @username</code>")


# --- Запуск приложения ---
async def main():
    await init_db()
    dp.include_router(router)
    
    exchanges = {name: cls({'enableRateLimit': True}) for name, cls in EXCHANGE_CLASSES.items()}

    asyncio.create_task(background_scanner_5s(bot, exchanges))
    asyncio.create_task(background_billing_checker(bot))

    logging.info("Сканер успешно запущен...")
    try:
        await dp.start_polling(bot)
    finally:
        for ex in exchanges.values():
            await ex.close()

if __name__ == "__main__":
    asyncio.run(main())
