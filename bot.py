#!/usr/bin/env python3
"""
MEXC Pump Sentinel — Monolithic Deployment Artifact
Author: Kirby
Protocol: Modular documentation within a single executable host.
"""

import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Dict, List, Optional, Any, Callable

import aiohttp
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# =============================================================================
# Interfaces & Data Models
# =============================================================================

@dataclass
class MarketSnapshot:
    symbol: str
    timestamp: float
    price: float
    volume_24h: float
    price_change_24h: float
    price_velocity_1m: float
    volume_spike: float
    trade_spike: float
    bid_ask_ratio: float
    breakout: float


@dataclass
class PumpSignal:
    symbol: str
    detected_at: float
    confidence: float
    metrics: Dict[str, float]
    message: str


@dataclass
class SentinelConfig:
    telegram_token: str = "8601112236:AAEtfkKD7ebXkFpsxjvpFBIc1JN_EsAu9Po"
    mexc_base_url: str = "https://api.mexc.com"
    poll_interval_sec: int = 10
    alert_cooldown_sec: int = 300
    min_volume_usdt: float = 50000.0
    price_velocity_threshold: float = 0.015       # 1.5% in 1m
    volume_spike_threshold: float = 3.0           # 3x avg
    trade_freq_spike_threshold: float = 3.0       # 3x avg
    order_book_imbalance_threshold: float = 2.0   # bid/ask ratio
    composite_score_threshold: float = 0.75
    max_concurrent_symbols: int = 200
    log_level: str = "INFO"
    authorized_chat_ids: List[int] = field(default_factory=list)


# =============================================================================
# Error Handling & Edge Case Management
# =============================================================================

class SentinelError(Exception):
    pass


class MEXCAPIError(SentinelError):
    pass


class PumpDetectionError(SentinelError):
    pass


class TelegramError(SentinelError):
    pass


def with_retry(max_retries: int = 3, base_delay: float = 1.0):
    def decorator(func: Callable):
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except MEXCAPIError as e:
                    last_exc = e
                    wait = base_delay * (2 ** (attempt - 1))
                    logging.getLogger("MEXCSentinel").warning(
                        f"Retry {attempt}/{max_retries} after {wait}s: {e}"
                    )
                    await asyncio.sleep(wait)
            raise last_exc
        return wrapper
    return decorator


# =============================================================================
# Configuration Manager with JSON serialization
# =============================================================================

class ConfigurationManager:
    def __init__(self, config_path: Optional[str] = None):
        self.config = SentinelConfig()
        self.config_path = config_path
        if config_path:
            self._load_from_file()
        self._override_from_env()

    def _load_from_file(self):
        if not self.config_path or not os.path.exists(self.config_path):
            return
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for k, v in data.items():
                if hasattr(self.config, k):
                    setattr(self.config, k, v)
        except Exception as e:
            logging.getLogger("MEXCSentinel").warning(f"Config file load failed: {e}")

    def _override_from_env(self):
        if token := os.getenv("TELEGRAM_TOKEN"):
            self.config.telegram_token = token
        if url := os.getenv("MEXC_BASE_URL"):
            self.config.mexc_base_url = url
        if lvl := os.getenv("LOG_LEVEL"):
            self.config.log_level = lvl

    def get(self) -> SentinelConfig:
        return self.config

    def to_json(self) -> str:
        return json.dumps(asdict(self.config), indent=2, ensure_ascii=False)

    def from_json(self, raw: str):
        data = json.loads(raw)
        for k, v in data.items():
            if hasattr(self.config, k):
                setattr(self.config, k, v)

    def save_to_file(self, path: Optional[str] = None):
        target = path or self.config_path
        if not target:
            return
        try:
            with open(target, 'w', encoding='utf-8') as f:
                f.write(self.to_json())
        except OSError as e:
            logging.getLogger("MEXCSentinel").error(f"Config save failed: {e}")


# =============================================================================
# Logging & Diagnostics Module
# =============================================================================

class CircularLogHandler(logging.Handler):
    def __init__(self, capacity: int = 2000):
        super().__init__()
        self.buffer = deque(maxlen=capacity)

    def emit(self, record):
        self.buffer.append(self.format(record))

    def get_logs(self, lines: int = 100) -> str:
        return "\n".join(list(self.buffer)[-lines:])


class Diagnostics:
    def __init__(self, level: str = "INFO"):
        self.logger = logging.getLogger("MEXCSentinel")
        self.logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        if not self.logger.handlers:
            fmt = logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(module)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            ch = logging.StreamHandler(sys.stdout)
            ch.setFormatter(fmt)
            self.logger.addHandler(ch)

            self.circular = CircularLogHandler(capacity=2000)
            self.circular.setFormatter(fmt)
            self.logger.addHandler(self.circular)

    def get_logger(self) -> logging.Logger:
        return self.logger

    def get_recent_logs(self, lines: int = 100) -> str:
        return self.circular.get_logs(lines)


# =============================================================================
# Input/Output Handler
# =============================================================================

class MEXCClient:
    def __init__(self, config: SentinelConfig):
        self.cfg = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(10)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15)
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def _get(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        url = f"{self.cfg.mexc_base_url}{endpoint}"
        async with self.semaphore:
            try:
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 429:
                        raise MEXCAPIError("Rate limited by MEXC")
                    if resp.status >= 400:
                        text = await resp.text()
                        raise MEXCAPIError(f"MEXC HTTP {resp.status}: {text}")
                    return await resp.json()
            except aiohttp.ClientError as e:
                raise MEXCAPIError(f"Connection failure: {e}")
            except Exception as e:
                raise MEXCAPIError(f"Request failed: {e}")

    @with_retry(max_retries=3, base_delay=1.0)
    async def get_24hr_tickers(self) -> List[Dict]:
        data = await self._get("/api/v3/ticker/24hr")
        if not isinstance(data, list):
            raise MEXCAPIError("Unexpected 24hr ticker format")
        return data

    @with_retry(max_retries=3, base_delay=1.0)
    async def get_klines(self, symbol: str, interval: str = "1m", limit: int = 20) -> List[List]:
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        data = await self._get("/api/v3/klines", params)
        if not isinstance(data, list):
            raise MEXCAPIError(f"Unexpected klines format for {symbol}")
        return data

    @with_retry(max_retries=2, base_delay=0.5)
    async def get_order_book(self, symbol: str, limit: int = 50) -> Dict:
        params = {"symbol": symbol, "limit": limit}
        return await self._get("/api/v3/depth", params)

    @with_retry(max_retries=2, base_delay=0.5)
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[Dict]:
        params = {"symbol": symbol, "limit": limit}
        return await self._get("/api/v3/trades", params)


class TelegramIO:
    def __init__(self, app: Application, config: SentinelConfig):
        self.app = app
        self.cfg = config

    async def broadcast(self, message: str):
        for chat_id in self.cfg.authorized_chat_ids:
            try:
                await self.app.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode="HTML"
                )
            except Exception as e:
                logging.getLogger("MEXCSentinel").error(f"Telegram send failed to {chat_id}: {e}")

    async def send_alert(self, signal: PumpSignal):
        await self.broadcast(signal.message)


# =============================================================================
# Core Algorithm Class
# =============================================================================

class PumpDetector:
    def __init__(self, config: SentinelConfig):
        self.cfg = config
        self.last_alert: Dict[str, float] = {}
        self.history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

    def ingest_snapshot(self, snap: MarketSnapshot) -> Optional[PumpSignal]:
        now = snap.timestamp
        sym = snap.symbol

        if now - self.last_alert.get(sym, 0) < self.cfg.alert_cooldown_sec:
            return None

        self.history[sym].append(snap)

        score = 0.0

        pv = abs(snap.price_velocity_1m)
        score += min(pv / self.cfg.price_velocity_threshold, 1.0) * 0.30

        vs = max(snap.volume_spike - 1.0, 0.0)
        norm_vs = vs / max(self.cfg.volume_spike_threshold - 1.0, 0.1)
        score += min(norm_vs, 1.0) * 0.25

        ts = max(snap.trade_spike - 1.0, 0.0)
        norm_ts = ts / max(self.cfg.trade_freq_spike_threshold - 1.0, 0.1)
        score += min(norm_ts, 1.0) * 0.20

        score += min(snap.bid_ask_ratio / self.cfg.order_book_imbalance_threshold, 1.0) * 0.15

        score += snap.breakout * 0.10

        if score >= self.cfg.composite_score_threshold:
            self.last_alert[sym] = now
            metrics = {
                "price_velocity": snap.price_velocity_1m,
                "volume_spike": snap.volume_spike,
                "trade_spike": snap.trade_spike,
                "obi": snap.bid_ask_ratio,
                "breakout": snap.breakout,
                "score": score
            }
            msg = (
                f"⚠️ <b>INCIPIENT PUMP DETECTED</b>\n"
                f"Symbol: <code>{sym}</code>\n"
                f"Confidence: {score:.2f}\n"
                f"Price Vel (1m): {snap.price_velocity_1m:.2%}\n"
                f"Vol Spike: {snap.volume_spike:.1f}x\n"
                f"Trade Spike: {snap.trade_spike:.1f}x\n"
                f"OB Imbalance: {snap.bid_ask_ratio:.2f}\n"
                f"Breakout: {'YES' if snap.breakout else 'NO'}\n"
                f"Time: {datetime.utcfromtimestamp(now).isoformat()}Z"
            )
            return PumpSignal(sym, now, score, metrics, msg)
        return None


# =============================================================================
# Main Entry Point / Host setup
# =============================================================================

def build_main_menu() -> ReplyKeyboardMarkup:
    """
    Constructs the persistent command keyboard.
    Buttons map 1:1 to slash commands.
    """
    keyboard = [
        [KeyboardButton("/start"), KeyboardButton("/monitor")],
        [KeyboardButton("/stop"), KeyboardButton("/status")],
        [KeyboardButton("/config")]
    ]
    return ReplyKeyboardMarkup(
        keyboard,
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False
    )


class SentinelHost:
    def __init__(self):
        self.config_manager = ConfigurationManager()
        self.cfg = self.config_manager.get()
        self.diagnostics = Diagnostics(self.cfg.log_level)
        self.log = self.diagnostics.get_logger()
        self.detector = PumpDetector(self.cfg)
        self.mexc: Optional[MEXCClient] = None
        self.telegram_app: Optional[Application] = None
        self.telegram_io: Optional[TelegramIO] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._tick_lock = asyncio.Lock()
        self.trade_count_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))

    async def setup(self):
        self.mexc = MEXCClient(self.cfg)
        await self.mexc.__aenter__()

        self.telegram_app = Application.builder().token(self.cfg.telegram_token).build()
        self.telegram_io = TelegramIO(self.telegram_app, self.cfg)

        # Command routing
        self.telegram_app.add_handler(CommandHandler("start", self.cmd_start))
        self.telegram_app.add_handler(CommandHandler("status", self.cmd_status))
        self.telegram_app.add_handler(CommandHandler("monitor", self.cmd_monitor))
        self.telegram_app.add_handler(CommandHandler("stop", self.cmd_stop))
        self.telegram_app.add_handler(CommandHandler("config", self.cmd_config))
        # Fallback for plain text that isn't a recognized command
        self.telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.cmd_fallback))

        await self.telegram_app.initialize()
        await self.telegram_app.start()
        asyncio.create_task(self.telegram_app.updater.start_polling())
        self.log.info("Host initialized. Secure line active.")

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if chat_id not in self.cfg.authorized_chat_ids:
            self.cfg.authorized_chat_ids.append(chat_id)
            try:
                self.config_manager.save_to_file()
            except Exception:
                pass
        menu = build_main_menu()
        await update.message.reply_text(
            "MEXC Pump Sentinel online. Authorization logged. Use the menu below.",
            reply_markup=menu
        )

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logs = self.diagnostics.get_recent_logs(20)
        status = (
            f"<b>Sentinel Status</b>\n"
            f"Monitoring: {'ACTIVE' if self._running else 'STANDBY'}\n"
            f"Tracked symbols: {len(self.detector.history)}\n"
            f"Alert cooldown: {self.cfg.alert_cooldown_sec}s\n"
            f"<pre>{logs}</pre>"
        )
        await update.message.reply_text(status, parse_mode="HTML")

    async def cmd_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._running:
            self._running = True
            self._task = asyncio.create_task(self._monitor_loop())
            await update.message.reply_text("Monitor loop engaged. Tracking all MEXC USDT pairs.")
        else:
            await update.message.reply_text("Monitor already active.")

    async def cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self._running:
            self._running = False
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            await update.message.reply_text("Monitor loop disengaged. Returning to standby.")
        else:
            await update.message.reply_text("Monitor was not active.")

    async def cmd_config(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        cfg_json = self.config_manager.to_json()
        payload = f"<pre>{cfg_json[:3500]}</pre>"
        await update.message.reply_text(payload, parse_mode="HTML")

    async def cmd_fallback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handles stray text that isn't a command."""
        await update.message.reply_text(
            "Unknown input. Use the menu buttons or type a slash command.",
            reply_markup=build_main_menu()
        )

    async def _monitor_loop(self):
        self.log.info("Monitor loop started.")
        while self._running:
            async with self._tick_lock:
                try:
                    await self._tick()
                except Exception as e:
                    self.log.error(f"Tick exception: {e}")
            await asyncio.sleep(self.cfg.poll_interval_sec)
        self.log.info("Monitor loop terminated.")

    async def _tick(self):
        tickers = await self.mexc.get_24hr_tickers()
        usdt = [
            t for t in tickers
            if t.get("symbol", "").endswith("USDT")
            and float(t.get("quoteVolume", 0)) >= self.cfg.min_volume_usdt
        ]

        candidates = []
        for t in usdt:
            p_change = float(t.get("priceChangePercent", 0))
            q_vol = float(t.get("quoteVolume", 0))
            if abs(p_change) > 1.0 or q_vol > self.cfg.min_volume_usdt * 3:
                candidates.append(t)

        candidates.sort(
            key=lambda x: abs(float(x.get("priceChangePercent", 0))),
            reverse=True
        )
        candidates = candidates[:self.cfg.max_concurrent_symbols]

        now = time.time()
        for t in candidates:
            sym = t["symbol"]
            try:
                klines = await self.mexc.get_klines(sym, "1m", 20)
                if not klines or len(klines) < 5:
                    continue

                curr = klines[-1]
                open_p = float(curr[1])
                close_p = float(curr[4])
                high_p = float(curr[2])
                low_p = float(curr[3])
                vol = float(curr[5])

                p_vel = (close_p - open_p) / open_p if open_p > 0 else 0

                prev_vols = [float(c[5]) for c in klines[:-1]]
                avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
                vol_spike = (vol / avg_vol) if avg_vol > 0 else 0

                lookback = [float(c[2]) for c in klines[-10:-1]]
                resistance = max(lookback) if lookback else 0
                breakout = 1.0 if (resistance > 0 and close_p > resistance * 1.005) else 0.0

                ob = await self.mexc.get_order_book(sym, 50)
                bids = ob.get("bids", [])
                asks = ob.get("asks", [])
                bid_vol = sum(float(b[1]) for b in bids)
                ask_vol = sum(float(a[1]) for a in asks)
                obi = (bid_vol / ask_vol) if ask_vol > 0 else 0

                trades = await self.mexc.get_recent_trades(sym, 100)
                cutoff_ms = (now - 60) * 1000
                recent_count = sum(1 for tr in trades if float(tr.get("time", 0)) > cutoff_ms)

                hist = self.trade_count_history[sym]
                avg_count = sum(hist) / len(hist) if hist else 10.0
                trade_spike = (recent_count / avg_count) if avg_count > 0 else 0
                hist.append(recent_count)

                snap = MarketSnapshot(
                    symbol=sym,
                    timestamp=now,
                    price=close_p,
                    volume_24h=float(t.get("volume", 0)),
                    price_change_24h=float(t.get("priceChangePercent", 0)),
                    price_velocity_1m=p_vel,
                    volume_spike=vol_spike,
                    trade_spike=trade_spike,
                    bid_ask_ratio=obi,
                    breakout=breakout
                )

                signal = self.detector.ingest_snapshot(snap)
                if signal:
                    self.log.warning(
                        f"ALERT {sym} | Score:{signal.confidence:.2f} "
                        f"Vel:{p_vel:.2%} Vol:{vol_spike:.1f}x Trade:{trade_spike:.1f}x"
                    )
                    await self.telegram_io.send_alert(signal)

            except MEXCAPIError as e:
                self.log.debug(f"MEXC API error scanning {sym}: {e}")
                continue
            except Exception as e:
                self.log.error(f"Unhandled scan error for {sym}: {e}")
                continue

    async def run(self):
        await self.setup()
        self.log.info("Sentinel Host operational. Awaiting commands.")
        while True:
            await asyncio.sleep(3600)

    async def shutdown(self):
        self._running = False
        if self._task:
            self._task.cancel()
        if self.telegram_app:
            await self.telegram_app.updater.stop()
            await self.telegram_app.stop()
            await self.telegram_app.shutdown()
        if self.mexc:
            await self.mexc.__aexit__(None, None, None)


async def main():
    host = SentinelHost()
    try:
        await host.run()
    except (KeyboardInterrupt, SystemExit):
        await host.shutdown()


if __name__ == "__main__":
    asyncio.run(main())