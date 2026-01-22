#!/usr/bin/env python3
import os
import asyncio
import logging
import time
import signal
from collections import deque, defaultdict
from typing import Dict, List, Tuple, Optional

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta
import aiohttp

APIKEY = "25u5Q0ZS0CXK5CK6hrya3zcO6hx28yu5Y0byPQzQAxYBB6pdZolLjLqVCk2ftsdu"
APISECRET = "7aGtW1PtA5Gm4Wqkj0BPuMl1oo4Fad5a8CtTE1ZAuNO7p5zfn1Mrxn5KmvE8QGqY"
PAPERMODE = "true"

MAX_POOL_SIZE = 350
TARGET_SCAN_SIZE = 280
BATCH_SIZE = 80
ROTATE_EVERY_CYCLES = 6

MIN_PRICE = 0.0003
MAX_PRICE = 0.98
MIN_QUOTE_VOL = 1_500_000
TIMEFRAME = "1m"

SPREAD_THRESHOLD = 0.0015
MIN_ORDERBOOK_USD = 200
BID_ASK_DEPTH_RATIO = 0.8

USD_RISK_PCT = 0.002
MAX_TRADES = 6
TARGET_PROFIT_PCT = 0.018
TRAIL_MIN_PCT = 0.012
PARTIAL_TP_PCT = 0.5
COOLDOWN_SECONDS = 300
TAKER_FEE_PCT = 0.001
MAX_SYMBOL_EXPOSURE_PCT = 0.04
MAX_DAILY_LOSS_USD = float(os.getenv("MAX_DAILY_LOSS_USD", "50"))

HARD_STOP_MULT = 2.2
MAX_STOP_PCT = 0.045

CYCLE_TIME_BASE = 0.8
CYCLE_TIME_MIN = 0.35
CYCLE_TIME_MAX = 2.2

SEM_LIMIT = 60

RETRY_BASE = 0.5
RETRY_MAX = 6.0
RETRY_ATTEMPTS = 4

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TITAN")

def fmt_usd(x: float) -> str:
    return f"${x:,.2f}"

def pct(x: float) -> str:
    return f"{x*100:.2f}%"

class TitanHFTv633:
    def __init__(self):
        self.exchange = ccxt.binance({
            "apiKey": APIKEY,
            "secret": APISECRET,
            "sandbox": PAPERMODE,
            "enableRateLimit": True,
            "rateLimit": 1200,
            "options": {"defaultType": "spot", "recvWindow": 10000}
        })
        self.penny_pool: List[str] = []
        self.symbols: List[str] = []
        self.buffers: Dict[str, deque] = {}
        self.trades: Dict[str, Dict] = {}
        self.cooldowns = defaultdict(lambda: 0.0)
        self.balance_cache = 25.0
        self.balance_cache_time = 0.0
        self.day_start_equity: Optional[float] = None
        self.realized_pnl_usd = 0.0
        self.shutdown_flag = False
        self.rate_limit_warnings = 0
        self.sem = asyncio.Semaphore(SEM_LIMIT)
        self.tg_queue: asyncio.Queue = asyncio.Queue()
        self.tg_worker_task: Optional[asyncio.Task] = None
        self.last_batch_latency = 1.0

    async def send_telegram(self, msg: str):
        await self.tg_queue.put(msg)

    async def _telegram_worker(self):
        token = "7938320826:AAG8xlvqdUb42FU-bf9tmVYE1SH8kG_6_FA"
        user_id = 5138274404
        if not token or not user_id:
            logger.warning("Telegram not configured (missing token/user_id)")
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        timeout = aiohttp.ClientTimeout(total=8)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    msg = await asyncio.wait_for(self.tg_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    if self.shutdown_flag and self.tg_queue.empty():
                        break
                    continue
                payload = {
                    "chat_id": int(user_id),
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                }
                delay = 0.7
                for attempt in range(3):
                    try:
                        async with session.post(url, json=payload) as resp:
                            if resp.status == 200:
                                break
                            body = await resp.text()
                            logger.warning(f"Telegram status={resp.status} body={body}")
                    except Exception as e:
                        logger.error(f"Telegram send error: {e}")
                    await asyncio.sleep(min(6.0, delay))
                    delay *= 2
                self.tg_queue.task_done()
        logger.info("Telegram worker exited cleanly")

    async def _safe_call(self, fn, *args, critical: bool = False, **kwargs):
        delay = RETRY_BASE
        for _ in range(RETRY_ATTEMPTS):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                msg = str(e)
                if ("429" in msg or msg.startswith("5")) and not critical:
                    await asyncio.sleep(min(RETRY_MAX, delay))
                    delay *= 2
                    continue
                logger.error(f"{fn.__name__} failed: {e}")
                return None
        return None

    async def shutdown(self):
        for s in list(self.trades):
            await self.execute_sell(s, False, "SHUTDOWN")
        self.shutdown_flag = True
        try:
            await asyncio.wait_for(self.tg_queue.join(), timeout=6)
        except asyncio.TimeoutError:
            logger.warning("Telegram queue drain timeout")
        if self.tg_worker_task:
            await self.tg_worker_task
        await self.exchange.close()

    async def startup(self):
        if not PAPERMODE and not (APIKEY and APISECRET):
            raise RuntimeError("LIVE mode requires API keys")
        await self.exchange.load_markets()
        if not self.tg_worker_task:
            self.tg_worker_task = asyncio.create_task(self._telegram_worker())
        await self.refresh_symbols()
        mode = "🧪 PAPER" if PAPERMODE else "🔴 LIVE"
        await self.send_telegram(
            f"🚀 <b>TITAN HFT v6.3.3</b>\n"
            f"Mode: {mode}\n"
            f"Cycle: adaptive {CYCLE_TIME_MIN}-{CYCLE_TIME_MAX}s × batch {BATCH_SIZE}\n"
            f"Pool={len(self.penny_pool)} | Scan={len(self.symbols)}"
        )
        self.day_start_equity = await self.get_equity()

    async def build_penny_pool(self):
        tickers = await self._safe_call(self.exchange.fetch_tickers)
        if not tickers:
            logger.warning("No tickers returned for penny pool build")
            self.penny_pool = []
            return
        pool = []
        for sym, t in tickers.items():
            if not sym.endswith("/USDT"):
                continue
            last = t.get("last", 0.0) or 0.0
            qv = t.get("quoteVolume", 0.0) or 0.0
            if MIN_PRICE <= last <= MAX_PRICE and qv >= MIN_QUOTE_VOL:
                pool.append((sym.replace("/USDT", ""), qv))
        pool.sort(key=lambda x: x[1], reverse=True)
        self.penny_pool = [s for s, _ in pool[:MAX_POOL_SIZE]]
        logger.info(f"Penny pool built: {len(self.penny_pool)} symbols")

    def select_scan_symbols(self):
        pinned = list(self.trades.keys())
        remaining = [s for s in self.penny_pool if s not in pinned]
        scan = pinned + remaining[:max(0, TARGET_SCAN_SIZE - len(pinned))]
        logger.info(f"Scan set prepared: pinned={len(pinned)} total={len(scan)}")
        return scan

    async def refresh_symbols(self):
        await self.build_penny_pool()
        self.symbols = self.select_scan_symbols()
        await self.send_telegram(
            f"📈 <b>Penny refresh</b>\nPool={len(self.penny_pool)} | Scan={len(self.symbols)}"
        )

    async def get_balance(self) -> float:
        now = time.time()
        if now - self.balance_cache_time > 25:
            bal = await self._safe_call(self.exchange.fetch_balance)
            if bal and "USDT" in bal and "free" in bal["USDT"]:
                try:
                    self.balance_cache = float(bal["USDT"]["free"])
                    self.balance_cache_time = now
                except Exception as e:
                    logger.error(f"Balance parse error: {e}")
        return max(self.balance_cache, 25.0)

    async def get_equity(self) -> float:
        return await self.get_balance()

    def can_open(self, symbol: str, qty: float, price: float, balance: float) -> bool:
        exposure = qty * price
        if exposure > balance * MAX_SYMBOL_EXPOSURE_PCT:
            logger.info(f"Open blocked {symbol}: exposure cap {fmt_usd(exposure)}")
            return False
        if self.day_start_equity is not None:
            current_equity = balance
            if (self.day_start_equity - current_equity) > MAX_DAILY_LOSS_USD:
                logger.info(f"Open blocked {symbol}: daily loss cap reached")
                return False
        return True

    def calculate_qty(self, balance: float, price: float, atr: float) -> float:
        risk_usd = balance * USD_RISK_PCT
        stop_dist = atr * HARD_STOP_MULT
        pos_value = risk_usd / (stop_dist / price) if stop_dist > 0 else 0.0
        max_exposure = balance * MAX_SYMBOL_EXPOSURE_PCT
        qty = min(pos_value, max_exposure) / price
        return round(max(qty, 0.0), 6)

    def _apply_fee(self, px: float) -> float:
        return px * (1 + TAKER_FEE_PCT)

    async def compute_signal(self, symbol: str) -> Tuple[bool, float]:
        buf = self.buffers.get(symbol)
        if not buf or len(buf) < 35:
            return False, 0.0
        df = pd.DataFrame(buf, columns=["ts", "o", "h", "l", "c", "v"])
        close = float(df["c"].iloc[-1])
        rsi = ta.rsi(df["c"], 14).iloc[-1]
        rsi_ok = 48 <= rsi <= 72
        macd = ta.macd(df["c"])
        macd_ok = macd["MACD_12_26_9"].iloc[-1] > macd["MACDs_12_26_9"].iloc[-1] > 0
        vol_ok = df["v"].iloc[-1] > df["v"].tail(21).mean() * 1.6
        atr = float(ta.atr(df["h"], df["l"], df["c"], 14).iloc[-1])
        atr_ok = 0.0018 <= (atr / close) <= MAX_STOP_PCT
        ema50 = ta.ema(df["c"], 50).iloc[-1]
        ema200 = ta.ema(df["c"], 200).iloc[-1]
        trend_ok = ema50 > ema200
        score = sum([rsi_ok, macd_ok, vol_ok, atr_ok, trend_ok])
        return score >= 4, atr

    async def sufficient_liquidity(self, symbol: str) -> bool:
        ob = await self._safe_call(self.exchange.fetch_order_book, f"{symbol}/USDT", 5)
        if not ob or not ob.get("bids") or not ob.get("asks"):
            return False
        bid, ask = ob["bids"][0][0], ob["asks"][0][0]
        spread = (ask - bid) / bid if bid else 1.0
        ask_depth = sum(p * q for p, q in ob["asks"][:3])
        bid_depth = sum(p * q for p, q in ob["bids"][:3])
        depth_ok = bid_depth >= ask_depth * BID_ASK_DEPTH_RATIO
        return spread <= SPREAD_THRESHOLD and ask_depth >= MIN_ORDERBOOK_USD and depth_ok

    async def execute_buy(self, symbol: str, price: float, atr: float):
        if symbol in self.trades or len(self.trades) >= MAX_TRADES or time.time() < self.cooldowns[symbol]:
            return
        bal = await self.get_balance()
        qty = self.calculate_qty(bal, price, atr)
        if qty * price < 6:
            return
        if not self.can_open(symbol, qty, price, bal):
            return
        fill_price = price
        if not PAPERMODE:
            order = await self._safe_call(self.exchange.create_market_buy_order, f"{symbol}/USDT", qty, critical=True)
            if order:
                fill_price = float(order.get("average", price))
        fill_price = self._apply_fee(fill_price)
        hard_stop_dist = min(atr * HARD_STOP_MULT, fill_price * MAX_STOP_PCT)
        initial_stop = fill_price - hard_stop_dist
        self.trades[symbol] = {
            "entry": fill_price,
            "qty": qty,
            "atr": atr,
            "high": fill_price,
            "breakeven": False,
            "breakeven_price": None,
            "realized": 0.0,
            "stop": initial_stop
        }
        await self.send_telegram(
            f"🟢 <b>BUY {symbol}</b>\n"
            f"Qty: {qty:.6f}\n"
            f"Entry: {fill_price:.6f}\n"
            f"Value: {fmt_usd(qty*fill_price)}\n"
            f"ATR: {pct(atr/fill_price)}\n"
            f"Init SL: {initial_stop:.6f}"
        )
        logger.info(f"BUY {symbol} qty={qty:.6f} entry={fill_price:.6f} value={fmt_usd(qty*fill_price)} stop={initial_stop:.6f}")

    async def manage_trade(self, symbol: str):
        trade = self.trades.get(symbol)
        if not trade:
            return
        t = await self._safe_call(self.exchange.fetch_ticker, f"{symbol}/USDT")
        if not t:
            return
        price = float(t["last"])
        trade["high"] = max(trade["high"], price)
        if not trade["breakeven"] and price >= trade["entry"] * 1.005:
            trade["breakeven"] = True
            trade["breakeven_price"] = price
            trade["stop"] = max(trade["stop"], trade["entry"])
            await self.send_telegram(f"⚡ <b>BREAKEVEN {symbol}</b>\nPrice: {price:.6f}\nSL→Entry: {trade['stop']:.6f}")
            logger.info(f"BREAKEVEN {symbol} price={price:.6f} stop={trade['stop']:.6f}")
        trail = max(TRAIL_MIN_PCT, trade["atr"] * 2.3 / price)
        dyn_stop = trade["high"] * (1 - trail)
        trade["stop"] = max(trade["stop"], dyn_stop)
        profit = price / trade["entry"] - 1
        if price <= trade["stop"]:
            await self.execute_sell(symbol, False, "STOP")
            return
        if profit >= TARGET_PROFIT_PCT:
            await self.execute_sell(symbol, True, "TP")

    async def execute_sell(self, symbol: str, partial: bool, reason: str):
        trade = self.trades.get(symbol)
        if not trade:
            return
        qty = trade["qty"] * (PARTIAL_TP_PCT if partial else 1.0)
        if qty <= 0:
            return
        t = await self._safe_call(self.exchange.fetch_ticker, f"{symbol}/USDT")
        if not t:
            return
        price = float(t["last"])
        if not PAPERMODE:
            await self._safe_call(self.exchange.create_market_sell_order, f"{symbol}/USDT", qty, critical=True)
        exit_price = self._apply_fee(price)
        proceeds = qty * exit_price
        cost_basis = qty * trade["entry"]
        realized = proceeds - cost_basis
        trade["realized"] += realized
        self.realized_pnl_usd += realized
        if partial:
            trade["qty"] -= qty
            await self.send_telegram(
                f"🔴 <b>SELL {symbol}</b>\nReason: {reason}\n"
                f"Qty: {qty:.6f}\nExit: {exit_price:.6f}\nRealized: {fmt_usd(realized)}\n"
                f"Remain: {trade['qty']:.6f} | SL: {trade['stop']:.6f}"
            )
            logger.info(f"SELL PARTIAL {symbol} qty={qty:.6f} exit={exit_price:.6f} realized={fmt_usd(realized)} stop={trade['stop']:.6f}")
        else:
            del self.trades[symbol]
            self.cooldowns[symbol] = time.time() + COOLDOWN_SECONDS
            await self.send_telegram(
                f"🔴 <b>SELL {symbol}</b>\nReason: {reason}\n"
                f"Qty: {qty:.6f}\nExit: {exit_price:.6f}\nRealized: {fmt_usd(realized)}\n"
                f"Cooldown: {COOLDOWN_SECONDS}s"
            )
            logger.info(f"SELL FULL {symbol} qty={qty:.6f} exit={exit_price:.6f} realized={fmt_usd(realized)}")

    async def process_symbol(self, symbol: str):
        async with self.sem:
            try:
                ohlcv = await self._safe_call(self.exchange.fetch_ohlcv, f"{symbol}/USDT", TIMEFRAME, limit=50)
                if not ohlcv:
                    return
                self.buffers.setdefault(symbol, deque(maxlen=60))
                self.buffers[symbol].extend(ohlcv[-10:])
                signal_ok, atr = await self.compute_signal(symbol)
                if signal_ok and await self.sufficient_liquidity(symbol):
                    ticker = await self._safe_call(self.exchange.fetch_ticker, f"{symbol}/USDT")
                    if ticker:
                        await self.execute_buy(symbol, float(ticker["last"]), atr)
                await self.manage_trade(symbol)
            except Exception as e:
                if "429" in str(e):
                    self.rate_limit_warnings += 1
                    logger.warning(f"{symbol} 429 RATE LIMIT")
                else:
                    logger.error(f"process_symbol error {symbol}: {e}")

    def _compute_cycle_sleep(self) -> float:
        base = CYCLE_TIME_BASE
        if self.last_batch_latency < 0.35 and self.rate_limit_warnings < 2:
            base = max(CYCLE_TIME_MIN, base * 0.7)
        elif self.rate_limit_warnings > 4:
            base = min(CYCLE_TIME_MAX, base * 1.8)
        return base

    async def run(self):
        await self.startup()
        cycle = 0
        while not self.shutdown_flag:
            if not self.symbols:
                await self.refresh_symbols()
                if not self.symbols:
                    await asyncio.sleep(0.8)
                    continue
            start_ts = time.time()
            n = len(self.symbols)
            idx = (cycle * BATCH_SIZE) % n
            end = idx + BATCH_SIZE
            if end <= n:
                batch = self.symbols[idx:end]
            else:
                batch = self.symbols[idx:] + self.symbols[:end - n]
            try:
                await asyncio.wait_for(
                    asyncio.gather(*(self.process_symbol(s) for s in batch)),
                    timeout=max(2.5, BATCH_SIZE * 0.12)
                )
            except asyncio.TimeoutError:
                self.rate_limit_warnings += 1
                logger.warning("Batch timeout—continuing")
            self.last_batch_latency = max(0.001, time.time() - start_ts)
            cycle += 1
            if cycle % ROTATE_EVERY_CYCLES == 0:
                await self.refresh_symbols()
            if self.rate_limit_warnings > 0 and cycle % 5 == 0:
                self.rate_limit_warnings = max(0, self.rate_limit_warnings - 1)
            sleep_for = self._compute_cycle_sleep()
            await asyncio.sleep(sleep_for * (0.95 + 0.1 * (cycle % 3) / 2))
            if cycle % 20 == 0:
                await self.send_telegram(
                    f"🧭 <b>Cycle {cycle}</b>\nLatency: {self.last_batch_latency:.3f}s\nWarnings: {self.rate_limit_warnings}\nOpen trades: {len(self.trades)}"
                )

async def main():
    bot = TitanHFTv633()
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
