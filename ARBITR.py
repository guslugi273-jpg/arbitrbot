import asyncio
import csv
import html
import io
import json
import logging
import os
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
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

# --- Конфигурация приложения ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "8979491056:AAEamiXQj9EZrl34ggOaPpJphHMaqMOXcYg")
CRYPTO_PAY_TOKEN = os.getenv("CRYPTO_PAY_TOKEN", "640413:AAozTIOPhVCXP62brvl6Bt8kL0vp9ticohx")

CHANNEL_SIGNALS_ID = -1004368321305
REQUIRED_CHANNEL_ID = "@arbitrnewwws"
ADMIN_IDS = [8066395175]

# Зафиксированный юзернейм поддержки (без возможности изменения через админку)
SUPPORT_USERNAME = "piki_wor"

PRICES = {
    "week": {"usd": 7.0, "days": 7, "name": "PRO (7 дней)"},
    "month": {"usd": 30.0, "days": 30, "name": "VIP (30 дней)"}
}

# Ключевые слова мем-коинов и низколиквидных щиткоинов
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


# --- Менеджер Базы Данных ---
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        if not self._db:
            self._db = await aiosqlite.connect(self.db_path)
            await self._db.execute("PRAGMA journal_mode=WAL;")
            await self._db.execute("PRAGMA synchronous=NORMAL;")
            await self._db.execute("PRAGMA busy_timeout=5000;")
            await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if not self._db:
            raise RuntimeError("БД не инициализирована.")
        return self._db

db_mgr = DatabaseManager(DB_NAME)


class Form(StatesGroup):
    waiting_for_broadcast = State()
    waiting_for_grant_id = State()
    waiting_for_grant_days = State()
    waiting_for_revoke_id = State()
    waiting_for_trade_pair = State()
    waiting_for_trade_amount = State()
    waiting_for_trade_profit = State()
    waiting_for_calc_input = State()
    waiting_for_balance = State()
    waiting_for_max_deal = State()


# --- Вспомогательные функции ---
def parse_expiry(val) -> int:
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


# --- Запросы к БД ---
async def init_db():
    db = db_mgr.conn
    await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            sub_expiry INTEGER DEFAULT 0,
            notified_24h INTEGER DEFAULT 0,
            min_spread REAL DEFAULT 0.3,
            balance REAL DEFAULT 50.0,
            max_deal_amount REAL DEFAULT 0.0,
            enabled_exchanges TEXT DEFAULT '["binance","bybit","okx","gate","kucoin"]'
        )
    """)
    for col, col_type in [("balance", "REAL DEFAULT 50.0"), ("max_deal_amount", "REAL DEFAULT 0.0")]:
        try:
            await db.execute(f"ALTER TABLE users ADD COLUMN {col} {col_type}")
        except Exception:
            pass

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
    await db.execute("""
        CREATE TABLE IF NOT EXISTS promo_activations (
            user_id INTEGER,
            code TEXT,
            activated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, code)
        )
    """)
    await db.commit()

async def get_sub_expiry(user_id: int) -> int:
    async with db_mgr.conn.execute("SELECT sub_expiry FROM users WHERE user_id = ?", (user_id,)) as cursor:
        row = await cursor.fetchone()
        return parse_expiry(row[0]) if row else 0

async def get_user_data(user_id: int) -> dict:
    async with db_mgr.conn.execute(
        "SELECT user_id, username, sub_expiry, min_spread, enabled_exchanges, balance, max_deal_amount FROM users WHERE user_id = ?",
        (user_id,)
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            try:
                exchanges = json.loads(row[4]) if row[4] else EXCHANGE_NAMES
            except Exception:
                exchanges = EXCHANGE_NAMES
            return {
                "user_id": row[0],
                "username": row[1] or "User",
                "sub_expiry": parse_expiry(row[2]),
                "min_spread": row[3] if row[3] is not None else 0.3,
                "exchanges": exchanges,
                "balance": row[5] if row[5] is not None else 50.0,
                "max_deal_amount": row[6] if row[6] is not None else 0.0,
            }
        return {
            "user_id": user_id,
            "username": "User",
            "sub_expiry": 0,
            "min_spread": 0.3,
            "exchanges": EXCHANGE_NAMES,
            "balance": 50.0,
            "max_deal_amount": 0.0
        }

async def update_user(user_id: int, username: str):
    await db_mgr.conn.execute("""
        INSERT INTO users (user_id, username) VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET username = excluded.username
    """, (user_id, username))
    await db_mgr.conn.commit()

async def update_user_field(user_id: int, field: str, value):
    await db_mgr.conn.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (value, user_id))
    await db_mgr.conn.commit()

async def update_user_settings(user_id: int, min_spread: float = None, exchanges: list = None):
    if min_spread is not None:
        await db_mgr.conn.execute("UPDATE users SET min_spread = ? WHERE user_id = ?", (min_spread, user_id))
    if exchanges is not None:
        await db_mgr.conn.execute("UPDATE users SET enabled_exchanges = ? WHERE user_id = ?", (json.dumps(exchanges), user_id))
    await db_mgr.conn.commit()

async def add_subscription(user_id: int, days: int) -> int:
    now = int(time.time())
    current = await get_sub_expiry(user_id)
    new_expiry = max(now, current) + (days * 86400)

    await db_mgr.conn.execute("""
        INSERT INTO users (user_id, username, sub_expiry, notified_24h) 
        VALUES (?, 'User', ?, 0)
        ON CONFLICT(user_id) DO UPDATE SET sub_expiry = excluded.sub_expiry, notified_24h = 0
    """, (user_id, new_expiry))
    await db_mgr.conn.commit()
    return new_expiry

async def revoke_subscription(user_id: int):
    await db_mgr.conn.execute("UPDATE users SET sub_expiry = 0 WHERE user_id = ?", (user_id,))
    await db_mgr.conn.commit()

async def is_user_subscribed(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    return (await get_sub_expiry(user_id)) > int(time.time())


# --- Работа с историями сделок ---
async def save_trade(user_id: int, pair_info: str, amount_usd: float, profit_usd: float):
    roi = (profit_usd / amount_usd * 100) if amount_usd > 0 else 0.0
    await db_mgr.conn.execute("""
        INSERT INTO trades (user_id, pair_info, amount_usd, profit_usd, roi_percent)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, pair_info, amount_usd, profit_usd, roi))
    await db_mgr.conn.commit()

async def get_user_trade_stats(user_id: int) -> dict:
    async with db_mgr.conn.execute("""
        SELECT COUNT(*), COALESCE(SUM(amount_usd), 0), COALESCE(SUM(profit_usd), 0), COALESCE(AVG(roi_percent), 0)
        FROM trades WHERE user_id = ?
    """, (user_id,)) as cursor:
        row = await cursor.fetchone()
        return {
            "count": row[0] if row else 0,
            "total_volume": round(row[1], 2) if row else 0.0,
            "total_profit": round(row[2], 2) if row else 0.0,
            "avg_roi": round(row[3], 2) if row else 0.0
        }

async def get_recent_trades(user_id: int, limit: int = 5) -> list:
    async with db_mgr.conn.execute("""
        SELECT pair_info, amount_usd, profit_usd, roi_percent, created_at
        FROM trades WHERE user_id = ? ORDER BY id DESC LIMIT ?
    """, (user_id, limit)) as cursor:
        return await cursor.fetchall()


# --- Оплата CryptoPay ---
class CryptoPayAPI:
    def __init__(self, token: str):
        self.headers = {"Crypto-Pay-API-Token": token}
        self.base_url = "https://pay.crypt.bot/api/"
        self._session: Optional[aiohttp.ClientSession] = None

    async def get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def create_invoice(self, amount: float, payload: str) -> Optional[Dict]:
        url = f"{self.base_url}createInvoice"
        data = {"asset": "USDT", "amount": str(amount), "description": "PRO Access Scanner", "payload": payload}
        try:
            session = await self.get_session()
            async with session.post(url, json=data, timeout=10) as resp:
                res = await resp.json()
                if res.get("ok"):
                    return {"invoice_id": str(res["result"]["invoice_id"]), "pay_url": res["result"]["pay_url"]}
        except Exception as e:
            logging.error(f"CryptoPay error: {e}")
        return None

    async def get_invoice(self, invoice_id: str) -> Optional[Dict]:
        url = f"{self.base_url}getInvoices"
        try:
            session = await self.get_session()
            async with session.get(url, params={"invoice_ids": invoice_id}, timeout=10) as resp:
                res = await resp.json()
                if res.get("ok") and res["result"]["items"]:
                    return res["result"]["items"][0]
        except Exception as e:
            logging.error(f"CryptoPay get error: {e}")
        return None

crypto_pay = CryptoPayAPI(CRYPTO_PAY_TOKEN)


# --- Сканер рынка (ФИЛЬТРАЦИЯ ФАНТОМОВ ВЕСЬМА ЖЁСТКАЯ) ---
async def fetch_exchange_tickers(ex_name: str, ex_obj) -> Optional[Dict]:
    try:
        tickers = await asyncio.wait_for(ex_obj.fetch_tickers(), timeout=6.0)
        return {ex_name: tickers}
    except Exception:
        return None

async def scan_market_5s(exchanges: Dict) -> List[Dict]:
    global LATEST_SIGNALS
    tasks = [fetch_exchange_tickers(name, ex) for name, ex in exchanges.items()]
    results = await asyncio.gather(*tasks)
    
    all_tickers = {}
    for res in results:
        if res:
            all_tickers.update(res)

    if len(all_tickers) < 2:
        return []

    coin_map = {}
    for ex_name, tickers in all_tickers.items():
        if not isinstance(tickers, dict):
            continue
        for symbol, t in tickers.items():
            if not symbol or not symbol.endswith('/USDT'):
                continue
            
            base_coin = symbol.split('/')[0].upper()
            if any(kw in base_coin.lower() for kw in MEME_KEYWORDS):
                continue

            bid = t.get('bid')
            ask = t.get('ask')
            
            if not bid or not ask or bid <= 0 or ask <= 0:
                continue

            # Исключаем аномалии цен и неликвидные монеты
            quote_volume = t.get('quoteVolume') or ((t.get('baseVolume') or 0) * ask)
            if not quote_volume or quote_volume < 50000.0:  # Строгий фильтр объема от $50,000 USDT
                continue

            bid_vol = t.get('bidVolume')
            ask_vol = t.get('askVolume')
            # Фильтр глубины стакана (объем ордеров не менее $50 USDT)
            if bid_vol is not None and (bid_vol * bid) < 50.0:
                continue
            if ask_vol is not None and (ask_vol * ask) < 50.0:
                continue

            if symbol not in coin_map:
                coin_map[symbol] = {}
            coin_map[symbol][ex_name] = {'bid': float(bid), 'ask': float(ask)}

    signals = []
    now_utc = datetime.now(timezone.utc).strftime("%H:%M:%S")

    for symbol, prices in coin_map.items():
        if len(prices) < 2:
            continue

        best_buy_ex, min_ask = min(prices.items(), key=lambda x: x[1]['ask'])
        best_sell_ex, max_bid = max(prices.items(), key=lambda x: x[1]['bid'])

        if best_buy_ex != best_sell_ex:
            buy_p = min_ask['ask']
            sell_p = max_bid['bid']

            if buy_p <= 0 or sell_p <= buy_p:
                continue

            gross = ((sell_p - buy_p) / buy_p) * 100
            net = gross - 0.20  # Минус комиссии двух бирж

            # Спред выше 8.0% на споте крупных бирж — 100% фантом (закрыт ввод/вывод).
            # Фильтруем строгим диапазоном реалистичных аномалий 0.3% - 8.0%
            if 0.3 <= net <= 8.0:
                signals.append({
                    'symbol': symbol,
                    'buy_ex': best_buy_ex.upper(),
                    'buy_price': buy_p,
                    'buy_url': get_trade_url(best_buy_ex, symbol),
                    'sell_ex': best_sell_ex.upper(),
                    'sell_price': sell_p,
                    'sell_url': get_trade_url(best_sell_ex, symbol),
                    'gross_spread': round(gross, 2),
                    'net_spread': round(net, 2),
                    'min_required': 10.0,
                    'time': now_utc
                })

    signals.sort(key=lambda x: x['net_spread'], reverse=True)
    LATEST_SIGNALS = signals
    return signals


# --- Фоновые процессы ---
async def background_scanner_5s(bot: Bot, exchanges: Dict):
    while True:
        try:
            signals = await scan_market_5s(exchanges)
            now_ts = int(time.time())
            for sig in signals:
                symbol = sig['symbol']
                if sig['net_spread'] >= 0.50 and (now_ts - LAST_ALERT_TIMES.get(symbol, 0) > 180):
                    LAST_ALERT_TIMES[symbol] = now_ts
                    # Расчет профита теперь с $100!
                    profit_100 = round((100 * (sig['net_spread'] / 100)), 2)
                    text = (
                        f"<b>Сигнал: {sig['symbol']}</b> (+{sig['net_spread']}%)\n\n"
                        f"• Покупка: <a href='{sig['buy_url']}'>{sig['buy_ex']}</a> — <code>{sig['buy_price']:.5f}</code>\n"
                        f"• Продажа: <a href='{sig['sell_url']}'>{sig['sell_ex']}</a> — <code>{sig['sell_price']:.5f}</code>\n"
                        f"• Ож. профит ($100): <code>+${profit_100} USDT</code>\n"
                        f"• Время: {sig['time']} UTC"
                    )
                    kb = InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text=f"Купить ({sig['buy_ex']})", url=sig['buy_url']),
                            InlineKeyboardButton(text=f"Продать ({sig['sell_ex']})", url=sig['sell_url'])
                        ]
                    ])
                    try:
                        await bot.send_message(CHANNEL_SIGNALS_ID, text, reply_markup=kb, disable_web_page_preview=True)
                    except Exception as send_err:
                        logging.error(f"Channel send error: {send_err}")
        except Exception as e:
            logging.error(f"Scanner bg error: {e}")
        await asyncio.sleep(5)

async def background_billing_checker(bot: Bot):
    while True:
        try:
            now_ts = int(time.time())
            db = db_mgr.conn
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
                                            f"<b>Оплата успешно получена</b>\n\n"
                                            f"Тариф: {PRICES[plan]['name']}\n"
                                            f"Срок действия: <code>{exp_str}</code>\n\n"
                                            f"Все функции сканера и VIP-канала доступны.",
                                            reply_markup=kb
                                        )
                                    except Exception:
                                        pass
                            elif st == "expired":
                                await db.execute("UPDATE invoices SET status = 'expired' WHERE id = ?", (row_id,))
                                await db.commit()
                except Exception as inv_err:
                    logging.error(f"Invoice check error {inv_id}: {inv_err}")

            async with db.execute("SELECT user_id, sub_expiry FROM users WHERE notified_24h = 0 AND sub_expiry > ?", (now_ts,)) as cursor:
                users = await cursor.fetchall()

            for u_id, exp_ts in users:
                parsed_exp = parse_expiry(exp_ts)
                if parsed_exp and now_ts < parsed_exp <= (now_ts + 86400):
                    try:
                        await bot.send_message(u_id, "Срок действия PRO-подписки истекает менее чем через 24 часа.")
                        await db.execute("UPDATE users SET notified_24h = 1 WHERE user_id = ?", (u_id,))
                        await db.commit()
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"Billing bg error: {e}")
        await asyncio.sleep(10)


# --- Aiogram Инициализация ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()


async def check_channel_sub(user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(REQUIRED_CHANNEL_ID, user_id)
        return m.status in ['creator', 'administrator', 'member']
    except Exception:
        return True

async def get_main_menu_kb(user_id: int, is_admin: bool = False) -> InlineKeyboardMarkup:
    has_sub = await is_user_subscribed(user_id)

    btns = []
    if has_sub:
        btns.append([
            InlineKeyboardButton(text="📡 Сканер сигналов", callback_data="view_fast_signals"),
            InlineKeyboardButton(text="⚙️ Фильтры", callback_data="menu_settings")
        ])
    else:
        btns.append([InlineKeyboardButton(text="Оформить PRO", callback_data="menu_buy")])

    btns.append([
        InlineKeyboardButton(text="📊 Дневник сделок", callback_data="menu_trades"),
        InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile")
    ])
    btns.append([
        InlineKeyboardButton(text="📖 Инструкция", callback_data="menu_guide"),
        InlineKeyboardButton(text="👨‍💻 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}")
    ])

    if is_admin:
        btns.append([InlineKeyboardButton(text="👑 Админ-панель", callback_data="menu_admin")])

    return InlineKeyboardMarkup(inline_keyboard=btns)


# --- Обработчики маршрутов ---
@router.message(Command("start"))
async def cmd_start(message: Message):
    await update_user(message.from_user.id, message.from_user.username or "User")
    
    if not await check_channel_sub(message.from_user.id):
        clean_channel = REQUIRED_CHANNEL_ID.replace('@', '')
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться на канал", url=f"https://t.me/{clean_channel}")],
            [InlineKeyboardButton(text="Проверить подписку", callback_data="check_sub")]
        ])
        await message.answer(
            "<b>Приветствуем!</b>\n\nДля доступа к сканеру необходимо быть подписанным на наш канал.",
            reply_markup=kb
        )
        return

    is_admin = message.from_user.id in ADMIN_IDS
    kb = await get_main_menu_kb(message.from_user.id, is_admin)
    text = (
        "<b>Arbitrage Terminal</b>\n\n"
        "• Статус: <code>ONLINE</code>\n"
        "• Мониторинг: Binance, Bybit, OKX, Gate, KuCoin\n"
        "• Частота обновления: 5 сек\n\n"
        "Выберите интересующий раздел:"
    )
    await message.answer(text, reply_markup=kb)

# --- Промокод /free1 ---
@router.message(Command("free1"))
async def cmd_promo_free1(message: Message):
    user_id = message.from_user.id
    code = "free1"
    db = db_mgr.conn

    await update_user(user_id, message.from_user.username or "User")

    async with db.execute("SELECT 1 FROM promo_activations WHERE user_id = ? AND code = ?", (user_id, code)) as cursor:
        already_used = await cursor.fetchone()

    if already_used:
        await message.answer("❌ Вы уже активировали данный промокод!")
        return

    new_exp = await add_subscription(user_id, 3)
    await db.execute("INSERT INTO promo_activations (user_id, code) VALUES (?, ?)", (user_id, code))
    await db.commit()

    exp_str = datetime.fromtimestamp(new_exp, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
    is_admin = user_id in ADMIN_IDS
    kb = await get_main_menu_kb(user_id, is_admin)
    await message.answer(
        f"🎉 <b>Промокод успешно активирован!</b>\n\n"
        f"Вам выдана PRO-подписка на 3 дня.\n"
        f"Действует до: <code>{exp_str}</code>",
        reply_markup=kb
    )

@router.callback_query(F.data == "menu_main")
async def cb_menu_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = call.from_user.id in ADMIN_IDS
    kb = await get_main_menu_kb(call.from_user.id, is_admin)
    text = (
        "<b>Arbitrage Terminal</b>\n\n"
        "• Статус: <code>ONLINE</code>\n"
        "• Мониторинг: Binance, Bybit, OKX, Gate, KuCoin\n"
        "• Частота обновления: 5 сек\n\n"
        "Выберите интересующий раздел:"
    )
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "check_sub")
async def cb_check_sub(call: CallbackQuery):
    if await check_channel_sub(call.from_user.id):
        is_admin = call.from_user.id in ADMIN_IDS
        kb = await get_main_menu_kb(call.from_user.id, is_admin)
        await call.message.edit_text("Подписка подтверждена.", reply_markup=kb)
    else:
        await call.answer("Подписка на канал не обнаружена.", show_alert=True)


# --- ПРОФИЛЬ И НАСТРОЙКА БАЛАНСА ---
@router.callback_query(F.data == "menu_profile")
async def cb_profile(call: CallbackQuery):
    u = await get_user_data(call.from_user.id)
    stats = await get_user_trade_stats(call.from_user.id)
    now_ts = int(time.time())
    exp_ts = u.get("sub_expiry", 0)

    is_admin = call.from_user.id in ADMIN_IDS
    if is_admin:
        sub_badge = "PRO (Administrator)"
        exp_info = "Бессрочно"
    elif exp_ts > now_ts:
        sub_badge = "PRO"
        exp_info = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
    else:
        sub_badge = "Не активна"
        exp_info = "—"

    uname = html.escape(call.from_user.username or 'не указан')
    max_deal_str = f"${u['max_deal_amount']:.2f}" if u['max_deal_amount'] > 0 else "Без ограничений"

    text = (
        f"<b>Личный профиль</b>\n\n"
        f"• ID: <code>{call.from_user.id}</code>\n"
        f"• Юзернейм: @{uname}\n"
        f"• Подписка: <b>{sub_badge}</b>\n"
        f"• Активна до: <code>{exp_info}</code>\n\n"
        f"⚙️ <b>Настройки капитала для сканера:</b>\n"
        f"💰 Ваш баланс: <b>${u['balance']:.2f} USDT</b>\n"
        f"🎯 Макс. лимит на 1 сделку: <b>{max_deal_str}</b>\n"
        f"📊 Мин. спред фильтра: <code>{u['min_spread']}%</code>\n\n"
        f"<b>Статистика торговли:</b>\n"
        f"• Закрыто сделок: {stats['count']}\n"
        f"• Общий оборот: ${stats['total_volume']} USDT\n"
        f"• Чистый профит: +${stats['total_profit']} USDT\n"
        f"• Средний ROI: +{stats['avg_roi']}%"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💰 Изменить баланс", callback_data="profile_edit_balance"),
            InlineKeyboardButton(text="⚙️ Лимит сделки", callback_data="profile_edit_max_deal")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "profile_edit_balance")
async def cb_edit_balance(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_balance)
    text = (
        "<b>Настройка баланса</b>\n\n"
        "Введите ваш текущий баланс в $ (USDT).\n"
        "Сканер будет подбирать смету и сделки строго под этот депозит.\n\n"
        "<i>Пример: <code>50</code> или <code>150.5</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_profile")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_balance)
async def process_balance_input(message: Message, state: FSMContext):
    try:
        val = float(message.text.replace(',', '.').strip())
        if val < 0:
            raise ValueError()
        await update_user_field(message.from_user.id, "balance", val)
        await state.clear()
        
        is_admin = message.from_user.id in ADMIN_IDS
        kb = await get_main_menu_kb(message.from_user.id, is_admin)
        await message.answer(f"✅ Баланс успешно обновлен: <b>${val:.2f} USDT</b>", reply_markup=kb)
    except Exception:
        await message.answer("Введите корректную сумму числом (например: 50):")

@router.callback_query(F.data == "profile_edit_max_deal")
async def cb_edit_max_deal(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_max_deal)
    text = (
        "<b>Максимальный лимит на 1 сделку</b>\n\n"
        "Укажите максимальную сумму в $, которую сканер может выделять на одну связку.\n"
        "Отправьте <code>0</code>, если хотите инвестировать весь ваш баланс.\n\n"
        "<i>Пример: <code>25</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_profile")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_max_deal)
async def process_max_deal_input(message: Message, state: FSMContext):
    try:
        val = float(message.text.replace(',', '.').strip())
        if val < 0:
            raise ValueError()
        await update_user_field(message.from_user.id, "max_deal_amount", val)
        await state.clear()
        
        is_admin = message.from_user.id in ADMIN_IDS
        kb = await get_main_menu_kb(message.from_user.id, is_admin)
        msg_text = f"✅ Лимит на 1 сделку установлен: <b>${val:.2f} USDT</b>" if val > 0 else "✅ Лимит снят (торговля на весь баланс)."
        await message.answer(msg_text, reply_markup=kb)
    except Exception:
        await message.answer("Введите корректную сумму числом (например: 25):")


# --- Дневник и калькулятор ---
@router.callback_query(F.data == "menu_trades")
async def cb_trades_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    stats = await get_user_trade_stats(call.from_user.id)
    recent = await get_recent_trades(call.from_user.id, limit=3)

    history = ""
    if recent:
        history = "\n<b>Недавние записи:</b>\n"
        for p_info, amt, prof, roi, _ in recent:
            history += f"• {html.escape(str(p_info))} | Вход: ${amt} | +${prof} (+{roi}%)\n"

    text = (
        f"<b>Дневник сделок и Калькулятор</b>\n\n"
        f"• Оборот: <code>${stats['total_volume']} USDT</code>\n"
        f"• Профит: <code>+${stats['total_profit']} USDT</code>\n"
        f"• Всего сделок: <code>{stats['count']}</code>\n"
        f"{history}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Новая запись", callback_data="trade_add"),
            InlineKeyboardButton(text="Калькулятор", callback_data="trade_calc")
        ],
        [InlineKeyboardButton(text="Вся история", callback_data="trade_history")],
        [InlineKeyboardButton(text="Главное меню", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data == "trade_add")
async def cb_trade_add(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_trade_pair)
    text = (
        "<b>Добавление сделки (Шаг 1 из 3)</b>\n\n"
        "Укажите связку или торговую пару:\n"
        "<i>Пример: <code>SOL Binance -> Bybit</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)

@router.message(Form.waiting_for_trade_pair)
async def process_trade_pair(message: Message, state: FSMContext):
    await state.update_data(pair_info=message.text.strip())
    await state.set_state(Form.waiting_for_trade_amount)
    await message.answer("<b>Добавление сделки (Шаг 2 из 3)</b>\n\nУкажите сумму входа в $:\n<i>Пример: <code>1000</code></i>")

@router.message(Form.waiting_for_trade_amount)
async def process_trade_amount(message: Message, state: FSMContext):
    try:
        amt = float(message.text.replace(',', '.').strip())
        if amt <= 0:
            raise ValueError()
        await state.update_data(amount_usd=amt)
        await state.set_state(Form.waiting_for_trade_profit)
        await message.answer("<b>Добавление сделки (Шаг 3 из 3)</b>\n\nУкажите чистый профит в $:\n<i>Пример: <code>12.5</code></i>")
    except Exception:
        await message.answer("Укажите корректную сумму числом (например: 1000).")

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
            f"<b>Сделка сохранена</b>\n\n"
            f"• Пара: {html.escape(data['pair_info'])}\n"
            f"• Сумма: ${data['amount_usd']}\n"
            f"• Профит: +${prof} USDT (+{roi}%)",
            reply_markup=kb
        )
    except Exception:
        await message.answer("Укажите чистый профит числом (например: 12.5).")

@router.callback_query(F.data == "trade_calc")
async def cb_calc_start(call: CallbackQuery, state: FSMContext):
    await state.set_state(Form.waiting_for_calc_input)
    text = (
        "<b>Арбитражный калькулятор</b>\n\n"
        "Расчёт комиссии бирж: 0.1% покупка + 0.1% продажа.\n"
        "Отправьте 3 значения через пробел:\n"
        "<code>[Депозит] [Цена покупки] [Цена продажи]</code>\n\n"
        "<i>Пример: <code>1000 142.5 144.1</code></i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="menu_trades")]])
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
        text = (
            f"<b>Результат расчёта</b>\n\n"
            f"• Депозит: ${capital:.2f} USDT\n"
            f"• Грязный спред: +{gross}%\n"
            f"• Комиссия бирж: -0.2%\n\n"
            f"• Чистый профит: <b>+${net_profit} USDT</b>\n"
            f"• Чистый ROI: <b>+{roi}%</b>"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Рассчитать ещё", callback_data="trade_calc")],
            [InlineKeyboardButton(text="К дневнику", callback_data="menu_trades")]
        ])
        await message.answer(text, reply_markup=kb)
    except Exception:
        await message.answer("Неверный формат. Отправьте 3 положительных числа через пробел (например: 1000 142.5 144.1).")

@router.callback_query(F.data == "trade_history")
async def cb_trade_history(call: CallbackQuery):
    trades = await get_recent_trades(call.from_user.id, limit=15)
    if not trades:
        text = "История сделок пуста."
    else:
        text = "<b>История последних сделок:</b>\n\n"
        for p_info, amt, prof, roi, _ in trades:
            text += f"• <b>{html.escape(str(p_info))}</b>\n  Депозит: ${amt} | Профит: +${prof} USDT (+{roi}%)\n\n"

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="menu_trades")]])
    await call.message.edit_text(text, reply_markup=kb)


# --- Настройки фильтров ---
@router.callback_query(F.data == "menu_settings")
async def cb_settings(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("Настройки доступны только пользователям с PRO подпиской.", show_alert=True)
        return

    u = await get_user_data(call.from_user.id)
    text = "<b>Настройка фильтрации</b>\n\nВыберите минимальный спред и активные биржи:"
    
    spread_btns = []
    for sp in [0.3, 0.5, 1.0, 2.0]:
        mark = "• " if u["min_spread"] == sp else ""
        spread_btns.append(InlineKeyboardButton(text=f"{mark}{sp}%", callback_data=f"set_spread_{sp}"))

    ex_btns = []
    for ex in EXCHANGE_NAMES:
        enabled = ex in u["exchanges"]
        mark = "[+] " if enabled else "[-] "
        ex_btns.append(InlineKeyboardButton(text=f"{mark}{ex.upper()}", callback_data=f"toggle_ex_{ex}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[
        spread_btns,
        [ex_btns[0], ex_btns[1], ex_btns[2]],
        [ex_btns[3], ex_btns[4]],
        [InlineKeyboardButton(text="Назад", callback_data="menu_main")]
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


# --- СКАНЕР СИГНАЛОВ ---
@router.callback_query(F.data == "view_fast_signals")
async def cb_view_fast_signals(call: CallbackQuery):
    if not await is_user_subscribed(call.from_user.id):
        await call.answer("Раздел доступен только владельцам PRO подписки.", show_alert=True)
        return

    u = await get_user_data(call.from_user.id)
    user_balance = u["balance"]
    max_deal = u["max_deal_amount"]

    if user_balance <= 0:
        await call.answer("⚠️ Ваш баланс в профиле равен $0. Задайте баланс в разделе 'Профиль'.", show_alert=True)
        return

    effective_trade_amount = user_balance
    if max_deal > 0:
        effective_trade_amount = min(user_balance, max_deal)

    filtered = []
    for s in LATEST_SIGNALS:
        if s['net_spread'] < u['min_spread']:
            continue
        if s['buy_ex'].lower() not in u['exchanges'] or s['sell_ex'].lower() not in u['exchanges']:
            continue
        if s['min_required'] > user_balance:
            continue
            
        filtered.append(s)

    if not filtered:
        text = (
            "<b>Поиск подходящих связок...</b>\n\n"
            f"В данный момент нет доступных связок под ваш баланс <b>${user_balance:.2f} USDT</b>.\n"
            f"• Мин. спред: <code>{u['min_spread']}%</code>\n"
            "• Рынок сканируется каждые 5 секунд."
        )
    else:
        text = (
            f"<b>Активные связки под ваш баланс (${user_balance:.2f} USDT):</b>\n"
            f"<i>Сделки фильтруются строго под ваш депозит.</i>\n\n"
        )
        for sig in filtered[:5]:
            calc_profit = round(effective_trade_amount * (sig['net_spread'] / 100.0), 2)
            
            text += (
                f"• <b>{sig['symbol']}</b> ({sig['time']} UTC)\n"
                f" Покупка: <a href='{sig['buy_url']}'>{sig['buy_ex']}</a> ➔ <code>{sig['buy_price']:.5f}</code>\n"
                f" Продажа: <a href='{sig['sell_url']}'>{sig['sell_ex']}</a> ➔ <code>{sig['sell_price']:.5f}</code>\n"
                f" Спред: <b>+{sig['net_spread']}%</b>\n"
                f" 💰 Вход в сделку: <b>${effective_trade_amount:.2f}</b> ➔ Профит: <b>+${calc_profit} USDT</b>\n\n"
            )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="view_fast_signals")],
        [InlineKeyboardButton(text="👤 Изменить баланс в профиле", callback_data="menu_profile")],
        [InlineKeyboardButton(text="Назад", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb, disable_web_page_preview=True)

@router.callback_query(F.data == "menu_guide")
async def cb_guide(call: CallbackQuery):
    text = (
        "<b>Инструкция по работе со сканером</b>\n\n"
        "1. Выберите доступную связку в сканере или сигнальном канале.\n"
        "2. Перейдите на биржу покупки и приобретите актив по маркету.\n"
        "3. Переведите токен на биржу продажи через соответствующую сеть.\n"
        "4. Продайте актив на целевой бирже в USDT.\n\n"
        "<i>Обращайте внимание на статусы ввода/вывода монет и комиссии сетей перед проведением транзакций.</i>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="menu_main")]])
    await call.message.edit_text(text, reply_markup=kb)


# --- Покупка подписки ---
@router.callback_query(F.data == "menu_buy")
async def cb_buy(call: CallbackQuery):
    text = (
        "<b>Оформление PRO доступа</b>\n\n"
        "Преимущества подписки:\n"
        "• Персональные уведомления о профитных связках\n"
        "• Доступ к VIP-каналу сигналов\n"
        "• Гибкие фильтры по биржам и спреду (от 0.3%)\n"
        "• Доступ к калькулятору и дневнику\n\n"
        "Выберите подходящий тариф:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="PRO (7 дней) — $7.00", callback_data="select_plan_week")],
        [InlineKeyboardButton(text="VIP (30 дней) — $30.00", callback_data="select_plan_month")],
        [InlineKeyboardButton(text="Назад", callback_data="menu_main")]
    ])
    await call.message.edit_text(text, reply_markup=kb)

@router.callback_query(F.data.startswith("select_plan_"))
async def cb_select_plan(call: CallbackQuery):
    plan_key = call.data.replace("select_plan_", "")
    plan = PRICES[plan_key]

    invoice = await crypto_pay.create_invoice(plan["usd"], f"{call.from_user.id}:{plan_key}")
    if not invoice:
        await call.answer("Ошибка при генерации счета. Попробуйте позже.", show_alert=True)
        return

    await db_mgr.conn.execute(
        "INSERT INTO invoices (invoice_id, provider, user_id, amount, plan) VALUES (?, 'cryptobot', ?, ?, ?)"
        " ON CONFLICT(invoice_id) DO NOTHING",
        (invoice["invoice_id"], call.from_user.id, plan["usd"], plan_key)
    )
    await db_mgr.conn.commit()

    text = (
        f"<b>Оплата счета</b>\n\n"
        f"Тариф: <b>{plan['name']}</b>\n"
        f"К оплате: <b>{plan['usd']} USDT</b>\n\n"
        f"После завершения транзакции подписка активируется автоматически."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Оплатить (${plan['usd']})", url=invoice["pay_url"])],
        [InlineKeyboardButton(text="К тарифам", callback_data="menu_buy")]
    ])
    await call.message.edit_text(text, reply_markup=kb)


# --- Панель администратора (ПОЛНОСТЬЮ ИСПРАВЛЕНА) ---
@router.callback_query(F.data == "menu_admin")
async def cb_admin_panel(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        await call.answer("Доступ запрещен.", show_alert=True)
        return

    await state.clear()
    now_ts = int(time.time())
    db = db_mgr.conn
    
    try:
        async with db.execute("SELECT COUNT(*) FROM users") as c1:
            row1 = await c1.fetchone()
            total_users = row1[0] if row1 else 0

        async with db.execute("SELECT COUNT(*) FROM users WHERE sub_expiry > ?", (now_ts,)) as c2:
            row2 = await c2.fetchone()
            active_subs = row2[0] if row2 else 0

        async with db.execute("SELECT COALESCE(SUM(amount), 0.0) FROM invoices WHERE status = 'paid'") as c3:
            row3 = await c3.fetchone()
            total_revenue = row3[0] if row3 and row3[0] is not None else 0.0
    except Exception as e:
        logging.error(f"Admin SQL error: {e}")
        total_users, active_subs, total_revenue = 0, 0, 0.0

    text = (
        f"<b>Панель администратора</b>\n\n"
        f"• Пользователей: {total_users}\n"
        f"• Активных PRO: {active_subs}\n"
        f"• Общий доход: ${total_revenue:.2f} USDT\n"
        f"• Поддержка: @{SUPPORT_USERNAME}\n\n"
        f"Выберите действие:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Выдать PRO", callback_data="admin_grant"),
            InlineKeyboardButton(text="Снять PRO", callback_data="admin_revoke")
        ],
        [InlineKeyboardButton(text="Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Экспорт базы (CSV)", callback_data="admin_export")],
        [InlineKeyboardButton(text="Главное меню", callback_data="menu_main")]
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        await call.message.answer(text, reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "admin_grant")
async def cb_admin_grant_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_grant_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_admin")]])
    await call.message.edit_text("Укажите Telegram ID пользователя:", reply_markup=kb)

@router.message(Form.waiting_for_grant_id)
async def process_grant_id(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        u_id = int(message.text.strip())
        await state.update_data(target_id=u_id)
        await state.set_state(Form.waiting_for_grant_days)
        await message.answer("Укажите срок действия подписки в днях:")
    except Exception:
        await message.answer("Введите корректный числовой Telegram ID.")

@router.message(Form.waiting_for_grant_days)
async def process_grant_days(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        days = int(message.text.strip())
        data = await state.get_data()
        target_id = data['target_id']
        exp_ts = await add_subscription(target_id, days)
        exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
        await state.clear()
        
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В админку", callback_data="menu_admin")]])
        await message.answer(f"PRO подписка для ID <code>{target_id}</code> выдана на {days} дн. До: {exp_str}", reply_markup=kb)
        
        try:
            await bot.send_message(target_id, f"Вам активирована PRO-подписка на {days} дней. Активна до: <code>{exp_str}</code>")
        except Exception:
            pass
    except Exception:
        await message.answer("Укажите количество дней числом.")

@router.callback_query(F.data == "admin_revoke")
async def cb_admin_revoke_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_revoke_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_admin")]])
    await call.message.edit_text("Укажите Telegram ID пользователя для сброса подписки:", reply_markup=kb)

@router.message(Form.waiting_for_revoke_id)
async def process_revoke_id(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        u_id = int(message.text.strip())
        await revoke_subscription(u_id)
        await state.clear()
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В админку", callback_data="menu_admin")]])
        await message.answer(f"Подписка для ID <code>{u_id}</code> аннулирована.", reply_markup=kb)
    except Exception:
        await message.answer("Введите корректный числовой Telegram ID.")

@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(Form.waiting_for_broadcast)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu_admin")]])
    await call.message.edit_text("Введите текст рассылки (поддерживаются HTML-теги):", reply_markup=kb)

@router.message(Form.waiting_for_broadcast)
async def process_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    txt = message.text
    await state.clear()
    
    async with db_mgr.conn.execute("SELECT user_id FROM users") as cursor:
        users = await cursor.fetchall()

    sent = 0
    for (u_id,) in users:
        try:
            await bot.send_message(u_id, f"<b>Объявление:</b>\n\n{txt}")
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="В админку", callback_data="menu_admin")]])
    await message.answer(f"Рассылка завершена. Доставлено сообщений: {sent}", reply_markup=kb)

@router.callback_query(F.data == "admin_export")
async def cb_admin_export(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["User ID", "Username", "Expiry Date (UTC)", "Status", "Balance ($)"])

    now_ts = int(time.time())
    async with db_mgr.conn.execute("SELECT user_id, username, sub_expiry, balance FROM users") as cursor:
        rows = await cursor.fetchall()
        for u_id, uname, exp_ts, bal in rows:
            parsed = parse_expiry(exp_ts)
            exp_str = datetime.fromtimestamp(parsed, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC') if parsed > 0 else "N/A"
            status = "Active" if parsed > now_ts else "Expired"
            writer.writerow([u_id, uname or "N/A", exp_str, status, bal or 0.0])

    file_bytes = output.getvalue().encode('utf-8')
    input_file = BufferedInputFile(file_bytes, filename="users_export.csv")
    await call.message.answer_document(input_file, caption="Экспорт пользователей системы.")
    await call.answer()

# Административные команды
@router.message(Command("grant"))
async def cmd_grant(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, target_id, days = message.text.split()
        exp_ts = await add_subscription(int(target_id), int(days))
        exp_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime('%d.%m.%Y %H:%M UTC')
        await message.answer(f"Подписка для ID <code>{target_id}</code> выдана на {days} дн. Действует до: {exp_str}")
    except Exception:
        await message.answer("Формат: <code>/grant [ID] [Дни]</code>")

@router.message(Command("revoke"))
async def cmd_revoke(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        _, target_id = message.text.split()
        await revoke_subscription(int(target_id))
        await message.answer(f"Подписка пользователя <code>{target_id}</code> аннулирована.")
    except Exception:
        await message.answer("Формат: <code>/revoke [ID]</code>")


# --- Запуск приложения ---
async def main():
    await db_mgr.connect()
    await init_db()
    dp.include_router(router)
    
    exchanges = {name: cls({'enableRateLimit': True}) for name, cls in EXCHANGE_CLASSES.items()}

    asyncio.create_task(background_scanner_5s(bot, exchanges))
    asyncio.create_task(background_billing_checker(bot))

    logging.info("Бот и сканер успешно запущены...")
    try:
        await dp.start_polling(bot)
    finally:
        await crypto_pay.close()
        for ex in exchanges.values():
            try:
                await ex.close()
            except Exception:
                pass
        await db_mgr.close()

if __name__ == "__main__":
    asyncio.run(main())
