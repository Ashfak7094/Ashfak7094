import os, asyncio, logging, time, signal
from collections import deque, defaultdict
from typing import Dict, List, Tuple

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
import aiohttp

# =========================
# CONFIG
# =========================
APIKEY = os.getenv("BINANCEAPI_KEY")
APISECRET = os.getenv("BINANCESECRET")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_USER_ID = os.getenv("TELEGRAM_USER_ID", "")  # DIRECT USER ID

PAPERMODE = os.getenv("PAPERMODE", "true").lower() == "true"

USD_RISK_PCT = 0.002
MAX_TRADES = 5

MIN_PRICE = 0.0003
MAX_PRICE = 0.98
MIN_QUOTE_VOL = 1_500_000

TIMEFRAME = "1m"

SPREAD_THRESHOLD = 0.002
MIN_ORDERBOOK_USD = 100

TARGET_PROFIT_PCT = 0.018
TRAIL_MIN_PCT = 0.012
PARTIAL_TP_PCT = 0.5

COOLDOWN_SECONDS = 300

CYCLE_TIME = 1.2
MAX_SYMBOLS_PER_CYCLE = 10

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("TITAN")

# =========================
# TELEGRAM (DIRECT DM ONLY)
# =========================
async def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_USER_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": int(TELEGRAM_USER_ID),
        "text": msg,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.warning(await resp.text())
    except Exception as e:
        logger.warning(f"Telegram DM error: {e}")

# =========================
# TITAN HFT v6.2
# =========================
class TitanHFTv62:
    def __init__(self):
        self.exchange = ccxt.binance({
            "apiKey": APIKEY,
            "secret": APISECRET,
            "sandbox": PAPERMODE,
            "enableRateLimit": True,
            "rateLimit": 1200,
            "options": {
                "defaultType": "spot",
                "recvWindow": 10000
            }
        })

        self.symbols: List[str] = []
        self.buffers: Dict[str, deque] = {}
        self.trades: Dict[str, Dict] = {}
        self.cooldowns = defaultdict(lambda: 0.0)

        self.balance_cache = 25.0
        self.balance_cache_time = 0.0

        self.shutdown_flag = False
        self.rate_limit_warnings = 0

    # -------------------------
    async def startup(self):
        if not PAPERMODE and not (APIKEY and APISECRET):
            raise RuntimeError("LIVE mode requires API keys")

        await self.exchange.load_markets()
        await self.refresh_symbols()

        mode = "🧪 PAPER" if PAPERMODE else "🔴 LIVE"
        await send_telegram(
            f"🚀 <b>TITAN HFT v6.2</b>\n"
            f"Mode: {mode}\n"
            f"Cycle: {CYCLE_TIME}s × {MAX_SYMBOLS_PER_CYCLE}\n"
            f"Symbols: {len(self.symbols)}"
        )

    # -------------------------
    async def refresh_symbols(self):
        tickers = await self.exchange.fetch_tickers()
        candidates = []

        for sym, t in tickers.items():
            if not sym.endswith("/USDT"):
                continue
            if not (MIN_PRICE <= t.get("last", 0) <= MAX_PRICE):
                continue
            if t.get("quoteVolume", 0) < MIN_QUOTE_VOL:
                continue
            candidates.append((sym.replace("/USDT", ""), t["quoteVolume"]))

        candidates.sort(key=lambda x: x[1], reverse=True)
        self.symbols = [s for s, _ in candidates[:MAX_SYMBOLS_PER_CYCLE + 2]]
        logger.info(f"Loaded {len(self.symbols)} symbols")

    # -------------------------
    async def get_balance(self) -> float:
        now = time.time()
        if now - self.balance_cache_time > 30:
            try:
                bal = await self.exchange.fetch_balance()
                self.balance_cache = float(bal["USDT"]["free"])
                self.balance_cache_time = now
            except:
                pass
        return max(self.balance_cache, 25.0)

    # -------------------------
    def calculate_qty(self, balance: float, price: float, atr: float) -> float:
        risk_usd = balance * USD_RISK_PCT
        stop_dist = atr * 2.2
        pos_value = risk_usd / (stop_dist / price)
        max_exposure = balance * 0.08
        qty = min(pos_value, max_exposure) / price
        return round(qty, 6)

    # -------------------------
    async def compute_signal(self, symbol: str) -> Tuple[bool, float]:
        buf = self.buffers.get(symbol)
        if not buf or len(buf) < 35:
            return False, 0.0

        df = pd.DataFrame(buf, columns=["ts", "o", "h", "l", "c", "v"])
        close = df["c"].iloc[-1]

        rsi_ok = 48 <= ta.rsi(df["c"], 14).iloc[-1] <= 72

        macd = ta.macd(df["c"])
        macd_ok = macd["MACD_12_26_9"].iloc[-1] > macd["MACDs_12_26_9"].iloc[-1] > 0

        vol_ok = df["v"].iloc[-1] > df["v"].tail(21).mean() * 1.6

        atr = ta.atr(df["h"], df["l"], df["c"], 14).iloc[-1]
        atr_ok = 0.0018 <= (atr / close) <= 0.045

        score = sum([rsi_ok, macd_ok, vol_ok, atr_ok])
        return score >= 3, float(atr)

    # -------------------------
    async def sufficient_liquidity(self, symbol: str) -> bool:
        try:
            ob = await self.exchange.fetch_order_book(f"{symbol}/USDT", 5)
            bid, ask = ob["bids"][0][0], ob["asks"][0][0]
            spread = (ask - bid) / bid
            depth = sum(p * q for p, q in ob["asks"][:3])
            return spread <= SPREAD_THRESHOLD and depth >= MIN_ORDERBOOK_USD
        except:
            return False

    # -------------------------
    async def execute_buy(self, symbol: str, price: float, atr: float):
        if symbol in self.trades or len(self.trades) >= MAX_TRADES or time.time() < self.cooldowns[symbol]:
            return

        bal = await self.get_balance()
        qty = self.calculate_qty(bal, price, atr)
        if qty * price < 6:
            return

        if not PAPERMODE:
            order = await self.exchange.create_market_buy_order(f"{symbol}/USDT", qty)
            price = float(order.get("average", price))

        self.trades[symbol] = {
            "entry": price,
            "qty": qty,
            "atr": atr,
            "high": price,
            "breakeven": False
        }

        await send_telegram(f"🟢 <b>BUY {symbol}</b>\nValue: ${qty*price:.2f}")

    # -------------------------
    async def manage_trade(self, symbol: str):
        trade = self.trades.get(symbol)
        if not trade:
            return

        t = await self.exchange.fetch_ticker(f"{symbol}/USDT")
        price = t["last"]
        trade["high"] = max(trade["high"], price)

        if not trade["breakeven"] and price >= trade["entry"] * 1.005:
            trade["breakeven"] = True
            trade["entry"] = price
            await send_telegram(f"⚡ <b>BREAKEVEN {symbol}</b>")

        trail = max(TRAIL_MIN_PCT, trade["atr"] * 2.3 / price)
        stop = trade["high"] * (1 - trail)
        profit = price / trade["entry"] - 1

        if profit >= TARGET_PROFIT_PCT:
            await self.execute_sell(symbol, True, "TP")
        elif price <= stop:
            await self.execute_sell(symbol, False, "TRAIL")

    # -------------------------
    async def execute_sell(self, symbol: str, partial: bool, reason: str):
        trade = self.trades.get(symbol)
        if not trade:
            return

        qty = trade["qty"] * (PARTIAL_TP_PCT if partial else 1.0)

        if not PAPERMODE:
            await self.exchange.create_market_sell_order(f"{symbol}/USDT", qty)

        if partial:
            trade["qty"] -= qty
        else:
            del self.trades[symbol]
            self.cooldowns[symbol] = time.time() + COOLDOWN_SECONDS

        await send_telegram(f"🔴 <b>SELL {symbol}</b>\nReason: {reason}")

    # -------------------------
    async def process_symbol(self, symbol: str):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(f"{symbol}/USDT", TIMEFRAME, limit=50)
            self.buffers.setdefault(symbol, deque(maxlen=60))
            self.buffers[symbol].extend(ohlcv[-10:])

            signal_ok, atr = await self.compute_signal(symbol)
            if signal_ok and await self.sufficient_liquidity(symbol):
                ticker = await self.exchange.fetch_ticker(f"{symbol}/USDT")
                await self.execute_buy(symbol, ticker["last"], atr)

            await self.manage_trade(symbol)

        except Exception as e:
            if "429" in str(e):
                self.rate_limit_warnings += 1
                logger.warning("429 RATE LIMIT")

    # -------------------------
    async def run(self):
        await self.startup()
        while not self.shutdown_flag:
            await asyncio.gather(*(self.process_symbol(s) for s in self.symbols[:MAX_SYMBOLS_PER_CYCLE]))
            await asyncio.sleep(CYCLE_TIME * 2 if self.rate_limit_warnings > 3 else CYCLE_TIME)

    # -------------------------
    async def shutdown(self):
        for s in list(self.trades):
            await self.execute_sell(s, False, "SHUTDOWN")
        await self.exchange.close()

# =========================
# MAIN
# =========================
async def main():
    bot = TitanHFTv62()

    def stop(*_):
        bot.shutdown_flag = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        await bot.run()
    finally:
        await bot.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
