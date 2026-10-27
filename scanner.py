"""
Binance Futures Crypto Scanner Engine
Scans USDT perpetual contracts matching:
- 24h Volume >= MIN_VOLUME
- Price < MAX_PRICE (default 1.0)
- Price < 1D EMA(99)
- Price > 4H EMA(99)
- Calculates ATL (All-Time Low) with intelligent caching
"""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional
import ccxt.async_support as ccxt
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CryptoScanner")


class CryptoScanner:
    def __init__(
        self,
        min_volume: float = 5_000_000.0,
        max_price: float = 1.0,
        ema_span: int = 99,
        use_closed_candle: bool = False,
        interval_seconds: int = 15,
        concurrency: int = 20,
    ):
        self.min_volume = min_volume
        self.max_price = max_price
        self.ema_span = ema_span
        self.use_closed_candle = use_closed_candle
        self.interval_seconds = interval_seconds
        self.concurrency = concurrency
        self.paused = False

        self.exchange: Optional[ccxt.binance] = None
        self.semaphore = asyncio.Semaphore(concurrency)
        self.atl_cache: Dict[str, float] = {}

        # Scanner state
        self.is_scanning = False
        self.last_scan_time: Optional[str] = None
        self.last_scan_duration: float = 0.0
        self.total_candidates: int = 0
        self.matches: List[Dict] = []
        self.logs: List[Dict] = []
        self.countdown: int = interval_seconds

        # Listeners / callbacks for WebSocket or SSE updates
        self.listeners: List[Callable] = []
        self._loop_task: Optional[asyncio.Task] = None
        self._countdown_task: Optional[asyncio.Task] = None
        self._manual_trigger_event = asyncio.Event()

    def add_listener(self, callback: Callable):
        if callback not in self.listeners:
            self.listeners.append(callback)

    def remove_listener(self, callback: Callable):
        if callback in self.listeners:
            self.listeners.remove(callback)

    async def _notify(self, event_type: str, data: Optional[Dict] = None):
        payload = {
            "event": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data or self.get_state(),
        }
        dead_listeners = []
        for cb in self.listeners:
            try:
                res = cb(payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error(f"Listener notification error: {e}")
                dead_listeners.append(cb)
        for dead in dead_listeners:
            self.remove_listener(dead)

    def add_log(self, message: str, level: str = "info"):
        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "message": message,
            "level": level,
        }
        self.logs.append(entry)
        if len(self.logs) > 60:
            self.logs.pop(0)
        logger.info(message)

    async def get_exchange(self) -> ccxt.binance:
        if self.exchange is None:
            self.exchange = ccxt.binance({
                "enableRateLimit": True,
                "options": {
                    "defaultType": "future",
                    "adjustForTimeDifference": True,
                },
                "timeout": 15000,
            })
            await self.exchange.load_markets()
            self.add_log(f"Connected to Binance Futures ({len(self.exchange.markets)} markets loaded)")
        return self.exchange

    async def _analyze_symbol(self, ex: ccxt.binance, symbol: str, volume_24h: float) -> Optional[Dict]:
        async with self.semaphore:
            try:
                # 1. Fetch 1D OHLCV
                ohlcv_1d = await ex.fetch_ohlcv(symbol, timeframe="1d", limit=100)
                if not ohlcv_1d or len(ohlcv_1d) < 100:
                    return None

                df_1d = pd.DataFrame(ohlcv_1d, columns=["timestamp", "open", "high", "low", "close", "volume"])
                candle_index = -2 if self.use_closed_candle else -1

                ema_1d = float(df_1d["close"].ewm(span=self.ema_span, adjust=False).mean().iloc[candle_index])
                price_1d = float(df_1d["close"].iloc[candle_index])

                # Condition 0: Price must be below max_price (default 1.0)
                if price_1d >= self.max_price:
                    return None

                # Condition 1: Price must be BELOW 1D EMA(99)
                if price_1d >= ema_1d:
                    return None

                # 2. Fetch 4H OHLCV (only reached if Conditions 0 & 1 pass)
                ohlcv_4h = await ex.fetch_ohlcv(symbol, timeframe="4h", limit=100)
                if not ohlcv_4h or len(ohlcv_4h) < 100:
                    return None

                df_4h = pd.DataFrame(ohlcv_4h, columns=["timestamp", "open", "high", "low", "close", "volume"])
                ema_4h = float(df_4h["close"].ewm(span=self.ema_span, adjust=False).mean().iloc[candle_index])
                price_4h = float(df_4h["close"].iloc[candle_index])

                # Condition 2: Price must be ABOVE 4H EMA(99)
                if price_4h <= ema_4h:
                    return None

                # 3. Determine All-Time Low (ATL) with intelligent caching
                atl = self.atl_cache.get(symbol)
                if atl is None or atl <= 0:
                    ohlcv_monthly = await ex.fetch_ohlcv(symbol, timeframe="1M", limit=1000)
                    if ohlcv_monthly:
                        atl = float(min(c[3] for c in ohlcv_monthly))
                    else:
                        atl = float(df_1d["low"].min())
                    self.atl_cache[symbol] = atl
                else:
                    # Update ATL if current price or recent low made a new ATL
                    atl = min(atl, price_4h)
                    self.atl_cache[symbol] = atl

                # Calculate helpful metrics
                dist_atl_pct = ((price_4h - atl) / atl * 100.0) if atl > 0 else 0.0
                dist_ema_4h_pct = ((price_4h - ema_4h) / ema_4h * 100.0) if ema_4h > 0 else 0.0
                dist_ema_1d_pct = ((price_4h - ema_1d) / ema_1d * 100.0) if ema_1d > 0 else 0.0

                # Spread position: where price is between 4H EMA (support) and 1D EMA (resistance target)
                spread = ema_1d - ema_4h
                range_pct = ((price_4h - ema_4h) / spread * 100.0) if spread > 0 else 50.0
                range_pct = max(0.0, min(100.0, range_pct))

                clean_sym = symbol.split(":")[0].replace("/", "")
                base_asset = symbol.split("/")[0]

                return {
                    "symbol": symbol,
                    "clean_symbol": clean_sym,
                    "tv_symbol": f"BINANCE:{clean_sym}.P",
                    "base_asset": base_asset,
                    "price": price_4h,
                    "atl": atl,
                    "dist_atl_pct": round(dist_atl_pct, 2),
                    "ema_4h": ema_4h,
                    "dist_ema_4h_pct": round(dist_ema_4h_pct, 2),
                    "ema_1d": ema_1d,
                    "dist_ema_1d_pct": round(dist_ema_1d_pct, 2),
                    "volume_24h": volume_24h,
                    "volume_m": round(volume_24h / 1_000_000.0, 2),
                    "range_position_pct": round(range_pct, 1),
                    "scanned_at": datetime.now(timezone.utc).isoformat(),
                }
            except Exception as e:
                # Keep scanner moving
                return None

    async def scan_once(self) -> List[Dict]:
        if self.is_scanning:
            return self.matches

        self.is_scanning = True
        self.countdown = self.interval_seconds
        await self._notify("scan_started")

        start_time = time.time()
        try:
            ex = await self.get_exchange()
            tickers = await ex.fetch_tickers()

            # Filter active linear USDT futures with volume >= min_volume and price < max_price
            candidates = []
            volumes = {}

            for s, m in ex.markets.items():
                if m.get("active") and m.get("linear") and m.get("quote") == "USDT":
                    ticker = tickers.get(s, {})
                    vol = ticker.get("quoteVolume", 0) or 0
                    last_price = ticker.get("last") or 0

                    if vol >= self.min_volume and (last_price < self.max_price or last_price == 0):
                        candidates.append(s)
                        volumes[s] = vol

            self.total_candidates = len(candidates)
            self.add_log(f"Scanning {len(candidates)} high-volume sub-${self.max_price} candidates...")

            tasks = [self._analyze_symbol(ex, s, volumes[s]) for s in candidates]
            results = await asyncio.gather(*tasks)

            matches = [r for r in results if r is not None]
            matches.sort(key=lambda x: x["volume_24h"], reverse=True)

            self.matches = matches
            self.last_scan_duration = round(time.time() - start_time, 2)
            self.last_scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            symbols_found = [m["clean_symbol"] for m in matches]
            self.add_log(
                f"Scan complete in {self.last_scan_duration}s. Found {len(matches)} matches: {', '.join(symbols_found) if symbols_found else 'None'}"
            )

        except Exception as e:
            self.add_log(f"Scanner error: {e}", level="error")
        finally:
            self.is_scanning = False
            self.countdown = self.interval_seconds
            await self._notify("scan_completed")

        return self.matches

    async def _countdown_loop(self):
        while True:
            await asyncio.sleep(1)
            if not self.paused and not self.is_scanning:
                if self.countdown > 0:
                    self.countdown -= 1
                    # Broadcast heartbeat countdown to clients
                    await self._notify("tick", {"countdown": self.countdown})

    async def run_loop(self):
        # Start the countdown timer task
        self._countdown_task = asyncio.create_task(self._countdown_loop())

        while True:
            if not self.paused:
                await self.scan_once()

            # Wait for interval or until manual trigger is signaled
            self.countdown = self.interval_seconds
            for _ in range(self.interval_seconds):
                try:
                    await asyncio.wait_for(self._manual_trigger_event.wait(), timeout=1.0)
                    self._manual_trigger_event.clear()
                    break
                except asyncio.TimeoutError:
                    if self.paused:
                        # If paused, keep waiting without triggering next scan
                        break

    def trigger_scan_now(self):
        self._manual_trigger_event.set()

    def update_config(
        self,
        min_volume: Optional[float] = None,
        max_price: Optional[float] = None,
        ema_span: Optional[int] = None,
        use_closed_candle: Optional[bool] = None,
        interval_seconds: Optional[int] = None,
        paused: Optional[bool] = None,
    ):
        if min_volume is not None:
            self.min_volume = float(min_volume)
        if max_price is not None:
            self.max_price = float(max_price)
        if ema_span is not None:
            self.ema_span = int(ema_span)
        if use_closed_candle is not None:
            self.use_closed_candle = bool(use_closed_candle)
        if interval_seconds is not None:
            self.interval_seconds = max(5, int(interval_seconds))
            self.countdown = min(self.countdown, self.interval_seconds)
        if paused is not None:
            self.paused = bool(paused)

        self.add_log(f"Config updated: vol>={self.min_volume/1e6:.1f}M, max_price={self.max_price}, interval={self.interval_seconds}s, paused={self.paused}")

    def get_state(self) -> Dict:
        tv_watchlist = ", ".join([f"BINANCE:{m['clean_symbol']}.P" for m in self.matches])
        clean_symbols = ", ".join([m["clean_symbol"] for m in self.matches])

        return {
            "status": "scanning" if self.is_scanning else ("paused" if self.paused else "running"),
            "is_scanning": self.is_scanning,
            "paused": self.paused,
            "countdown": self.countdown,
            "last_scan_time": self.last_scan_time,
            "last_scan_duration": self.last_scan_duration,
            "total_candidates": self.total_candidates,
            "matches_count": len(self.matches),
            "matches": self.matches,
            "tv_watchlist": tv_watchlist,
            "clean_symbols": clean_symbols,
            "config": {
                "min_volume": self.min_volume,
                "max_price": self.max_price,
                "ema_span": self.ema_span,
                "use_closed_candle": self.use_closed_candle,
                "interval_seconds": self.interval_seconds,
            },
            "logs": self.logs[-20:],
        }

    async def close(self):
        if self._loop_task:
            self._loop_task.cancel()
        if self._countdown_task:
            self._countdown_task.cancel()
        if self.exchange:
            await self.exchange.close()
            self.exchange = None
