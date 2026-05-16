#!/usr/bin/env python3
"""
MULTI-CRYPTO 5M DIP BUYER — LIVE READY
=====================================
Features added:
- Average fill price from top 3 order book levels
- Skip trade if <80% fillable at those levels
- Daily trade limits (500 total, 200 per crypto)
- Max 10 consecutive trade losses
- Daily loss limit -$20
- No profit target (run indefinitely)
- No trading hours restriction
- All limits set high to avoid triggering during paper testing

FIX APPLIED (2026-05-15):
- Stop loss P&L now uses actual exit price instead of assuming total loss
  Original: loss = -(entry_price * size + entry_price * size * FEE_RATE)
  Fixed:   loss = (exit_price - entry_price) * size - total_fees
  This is more accurate when stop loss triggers above price=0.
  All other exit logic (take profit, expiry, session close) unchanged.
"""

import json
import time
import asyncio
import websockets
import requests
import logging
import pytz
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_API = "https://gamma-api.polymarket.com/events"

CRYPTOS = ["sol", "xrp", "doge", "hype"]

# Strategy config
CRASH_THRESHOLD = 0.20
MAX_ENTRY_TIME = 120
MAX_HOLD_SECONDS = 90
STOP_LOSS_PCT = 0.30
TAKE_PROFIT_PCT = 1.00
FEE_BPS = 200
FEE_RATE = FEE_BPS / 10000.0
MIN_CRASH_SPEED = 0.60

# Trend detection
TREND_LOOKBACK_MARKETS = 5
TREND_FLIP_THRESHOLD = 3

# Risk management — LIVE READY
MAX_DAILY_TRADES = 500
MAX_DAILY_TRADES_PER_CRYPTO = 200
MAX_CONSECUTIVE_TRADE_LOSSES = 10
DAILY_LOSS_LIMIT = -20.0
# No profit target — run indefinitely

# Partial fill handling
MIN_FILL_RATIO = 0.80  # Need 80% of desired size available

# WebSocket
WS_HEARTBEAT_INTERVAL = 10.0
WS_DATA_TIMEOUT = 120.0
WS_RECONNECT_DELAY_BASE = 5.0
WS_RECONNECT_DELAY_MAX = 60.0

DISCOVER_INTERVAL = 15


@dataclass
class MarketInfo:
    slug: str
    title: str
    token_ids: List[str]
    outcomes: List[str]
    condition_id: str
    crypto: str
    window_start_et: datetime
    window_end_et: datetime


@dataclass
class DipPosition:
    pos_id: str
    side: str
    entry_price: float
    size: float
    entry_time: float
    exit_price: Optional[float] = None
    exit_time: Optional[float] = None
    pnl: Optional[float] = None
    status: str = "OPEN"


@dataclass
class PriceRecord:
    time: float
    up_price: float
    down_price: float


@dataclass
class MarketSession:
    info: MarketInfo
    books: Dict[str, dict] = field(default_factory=dict)
    prices: List[PriceRecord] = field(default_factory=list)
    positions: List[DipPosition] = field(default_factory=list)
    start_time: float = 0.0
    messages_received: int = 0
    last_update: float = 0.0


def discover_crypto_5m(crypto: str) -> Optional[MarketInfo]:
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    minute = (now_et.minute // 5) * 5
    base = now_et.replace(minute=minute, second=0, microsecond=0)

    for offset in [0, 5, -5]:
        window = base + timedelta(minutes=offset)
        ts = int(window.astimezone(timezone.utc).timestamp())

        for slug_format in [f"{crypto}-updown-5m-{ts}", f"{crypto}-up-or-down-5m-{ts}"]:
            try:
                url = f"{GAMMA_API}?slug={slug_format}"
                resp = requests.get(url, timeout=5)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                if not isinstance(data, list) or len(data) == 0:
                    continue

                event = data[0]
                if not event.get('active') or event.get('closed'):
                    continue

                markets = event.get('markets', [])
                if not markets:
                    continue

                m = markets[0]
                clob_raw = m.get('clobTokenIds', '[]')
                try:
                    token_ids = json.loads(clob_raw) if isinstance(clob_raw, str) else clob_raw
                except:
                    token_ids = []

                if len(token_ids) != 2:
                    continue

                outcomes = m.get('outcomes', ['Up', 'Down'])
                end_et = window + timedelta(minutes=5)
                remaining = (end_et - now_et).total_seconds()

                if remaining < 30:
                    continue

                return MarketInfo(
                    slug=slug_format,
                    title=event.get('title', 'Unknown'),
                    token_ids=token_ids,
                    outcomes=outcomes,
                    condition_id=m.get('conditionId', ''),
                    crypto=crypto,
                    window_start_et=window,
                    window_end_et=end_et
                )

            except Exception as e:
                logger.debug(f"Discovery error for {slug_format}: {e}")

    return None


class WebSocketManager:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.reconnect_count = 0
        self.last_data_time = 0.0
        self.subscribed_tokens: set = set()
        self._running = False
        self._conn_id = 0
        self._pending_tokens: List[str] = []
        self._lock = asyncio.Lock()
        self._event_callback = None

    def set_callback(self, callback):
        self._event_callback = callback

    def queue_tokens(self, token_ids: List[str]):
        self._pending_tokens = list(dict.fromkeys(token_ids))

    async def run(self):
        self._running = True
        while self._running:
            try:
                self._conn_id += 1
                cid = self._conn_id
                logger.info(f"🔌 [Conn #{cid}] Connecting...")

                async with websockets.connect(
                    WS_URL,
                    ping_interval=None,
                    close_timeout=5,
                    max_size=10_000_000
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    self.last_data_time = time.time()
                    self.reconnect_count = 0
                    logger.info(f"✅ [Conn #{cid}] Connected!")

                    async with self._lock:
                        if self._pending_tokens:
                            await self._subscribe_now(self._pending_tokens)

                    heartbeat_task = asyncio.create_task(self._heartbeat())
                    await asyncio.sleep(0.5)

                    try:
                        async for raw in ws:
                            self.last_data_time = time.time()
                            await self._handle_message(raw)
                    except websockets.exceptions.ConnectionClosedError as e:
                        logger.warning(f"⚠️ [Conn #{cid}] Closed: {e}")
                    except websockets.exceptions.ConnectionClosedOK:
                        logger.info(f"ℹ️ [Conn #{cid}] Closed normally")

                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass

            except Exception as e:
                logger.error(f"❌ [Conn #{cid}] Error: {e}")
                self.connected = False

            finally:
                self.connected = False
                if not self._running:
                    break

                delay = min(
                    WS_RECONNECT_DELAY_BASE * (2 ** min(self.reconnect_count, 4)),
                    WS_RECONNECT_DELAY_MAX
                )
                self.reconnect_count += 1
                logger.info(f"⏳ Reconnect in {delay:.1f}s (attempt #{self.reconnect_count})")
                await asyncio.sleep(delay)

    async def _subscribe_now(self, token_ids: List[str]):
        if not self.ws or not self.connected or not token_ids:
            return

        new_tokens = [t for t in token_ids if t not in self.subscribed_tokens]
        if not new_tokens:
            return

        if self.subscribed_tokens:
            msg = {
                "assets_ids": new_tokens,
                "operation": "subscribe",
                "custom_feature_enabled": True
            }
        else:
            msg = {
                "assets_ids": new_tokens,
                "type": "market",
                "custom_feature_enabled": True
            }

        try:
            await self.ws.send(json.dumps(msg))
            self.subscribed_tokens.update(new_tokens)
            logger.info(f"📡 Subscribed {len(new_tokens)} tokens (total: {len(self.subscribed_tokens)})")
        except Exception as e:
            logger.error(f"Subscribe error: {e}")

    async def subscribe(self, token_ids: List[str]):
        async with self._lock:
            self._pending_tokens = list(dict.fromkeys(token_ids))
            if self.connected and self.ws:
                await self._subscribe_now(self._pending_tokens)

    async def _heartbeat(self):
        while self._running and self.connected:
            try:
                if self.ws:
                    await self.ws.send("PING")
                    await asyncio.sleep(WS_HEARTBEAT_INTERVAL)
            except Exception:
                break

    async def _handle_message(self, raw: str):
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self._process_event(item)
        elif isinstance(data, dict):
            await self._process_event(data)

    async def _process_event(self, data: dict):
        et = data.get('event_type', '')
        tid = data.get('asset_id')

        if self._event_callback and tid:
            await self._event_callback(data)

    def stop(self):
        self._running = False
        if self.ws:
            asyncio.create_task(self.ws.close())


class MultiCryptoTrader:
    def __init__(self):
        self.ws = WebSocketManager()
        self.sessions: Dict[str, MarketSession] = {}
        self.all_sessions: List[dict] = []
        self.session_history: List[dict] = []
        self.start_time = time.time()
        self._running = False

        # Per-crypto trend tracking
        self.crypto_trends: Dict[str, dict] = {
            c: {
                'up_crashes': [],
                'down_crashes': [],
                'current_trend': "UNKNOWN",
                'choppy_count': 0,
                'last_choppy_state': False
            }
            for c in CRYPTOS
        }

        # Risk management tracking
        self.daily_trades = 0
        self.daily_trades_per_crypto: Dict[str, int] = {c: 0 for c in CRYPTOS}
        self.consecutive_losses = 0
        self.daily_pnl = 0.0
        self.last_reset_day = datetime.now(timezone.utc).day
        self.trading_stopped = False
        self.stop_reason = None

    def _reset_daily_counters(self):
        current_day = datetime.now(timezone.utc).day
        if current_day != self.last_reset_day:
            logger.info(f"📅 NEW DAY — Resetting daily counters")
            self.daily_trades = 0
            self.daily_trades_per_crypto = {c: 0 for c in CRYPTOS}
            self.consecutive_losses = 0
            self.daily_pnl = 0.0
            self.last_reset_day = current_day
            self.trading_stopped = False
            self.stop_reason = None

    def _check_limits(self) -> bool:
        """Returns True if trading is allowed, False if stopped."""
        if self.trading_stopped:
            return False

        # Daily loss limit
        if self.daily_pnl <= DAILY_LOSS_LIMIT:
            self.trading_stopped = True
            self.stop_reason = f"Daily loss limit reached: ${self.daily_pnl:.4f}"
            logger.error(f"🛑 TRADING STOPPED: {self.stop_reason}")
            return False

        # Max daily trades
        if self.daily_trades >= MAX_DAILY_TRADES:
            self.trading_stopped = True
            self.stop_reason = f"Max daily trades reached: {self.daily_trades}"
            logger.error(f"🛑 TRADING STOPPED: {self.stop_reason}")
            return False

        # Consecutive losses
        if self.consecutive_losses >= MAX_CONSECUTIVE_TRADE_LOSSES:
            self.trading_stopped = True
            self.stop_reason = f"Max consecutive losses reached: {self.consecutive_losses}"
            logger.error(f"🛑 TRADING STOPPED: {self.stop_reason}")
            return False

        return True

    async def run(self):
        logger.info("=" * 70)
        logger.info("MULTI-CRYPTO 5M DIP BUYER — LIVE READY")
        logger.info("Features: Avg fill price, daily limits, loss controls")
        logger.info("=" * 70)

        self._running = True
        self.ws.set_callback(self._process_ws_event)

        ws_task = asyncio.create_task(self.ws.run())
        await asyncio.sleep(2)

        trade_task = asyncio.create_task(self._trade_loop())
        status_task = asyncio.create_task(self._status_loop())

        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

        logger.info("⏹️ Shutting down...")
        self._running = False
        self.ws.stop()

        for task in [trade_task, status_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

        self._final_summary()
        self._save_all()

    async def _process_ws_event(self, data: dict):
        et = data.get('event_type', '')
        tid = data.get('asset_id')

        if not tid:
            return

        for crypto, session in self.sessions.items():
            if tid in session.info.token_ids:
                session.messages_received += 1
                session.last_update = time.time()

                if tid not in session.books:
                    session.books[tid] = {'bids': [], 'asks': [], 'last_update': 0}

                if et == 'book':
                    bids = data.get('bids', [])
                    asks = data.get('asks', [])
                    session.books[tid] = {
                        'bids': [{'price': float(b['price']), 'size': float(b['size'])} for b in bids],
                        'asks': [{'price': float(a['price']), 'size': float(a['size'])} for a in asks],
                        'last_update': time.time()
                    }

                elif et == 'best_bid_ask':
                    ba = data.get('best_ask')
                    bb = data.get('best_bid')
                    if ba is not None:
                        ba_price = float(ba)
                        if not session.books[tid]['asks']:
                            session.books[tid]['asks'].append({'price': ba_price, 'size': 1.0})
                        else:
                            session.books[tid]['asks'][0]['price'] = ba_price
                    if bb is not None:
                        bb_price = float(bb)
                        if not session.books[tid]['bids']:
                            session.books[tid]['bids'].append({'price': bb_price, 'size': 1.0})
                        else:
                            session.books[tid]['bids'][0]['price'] = bb_price
                    session.books[tid]['last_update'] = time.time()
                break

    async def _trade_loop(self):
        last_discover = 0

        while self._running:
            try:
                now = time.time()

                # Reset daily counters if new day
                self._reset_daily_counters()

                if now - last_discover > DISCOVER_INTERVAL:
                    last_discover = now

                    for crypto in CRYPTOS:
                        market = discover_crypto_5m(crypto)

                        if market:
                            existing = self.sessions.get(crypto)
                            if not existing or existing.info.slug != market.slug:
                                if existing:
                                    await self._close_session(existing)

                                session = MarketSession(
                                    info=market,
                                    start_time=time.time()
                                )
                                self.sessions[crypto] = session

                                self.ws.queue_tokens(market.token_ids)
                                await self.ws.subscribe(market.token_ids)

                                logger.info(f"\n🎯 NEW {crypto.upper()} MARKET: {market.slug}")
                                trend = self.crypto_trends[crypto]['current_trend']
                                logger.info(f" Trend: {trend}")

                # Trade all active sessions
                for crypto, session in list(self.sessions.items()):
                    if session:
                        await self._trade_session(session, crypto)

                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Trade loop error: {e}")
                await asyncio.sleep(1)

    async def _trade_session(self, session: MarketSession, crypto: str):
        elapsed = time.time() - session.start_time

        tid0, tid1 = session.info.token_ids[0], session.info.token_ids[1]
        book0 = session.books.get(tid0, {'asks': []})
        book1 = session.books.get(tid1, {'asks': []})

        up_ask = book0['asks'][0] if book0['asks'] else None
        down_ask = book1['asks'][0] if book1['asks'] else None

        if not up_ask or not down_ask:
            return

        up_price = up_ask['price']
        down_price = down_ask['price']

        session.prices.append(PriceRecord(time=elapsed, up_price=up_price, down_price=down_price))

        # === MANAGE OPEN POSITIONS ===
        for pos in session.positions[:]:
            if pos.status != "OPEN":
                continue

            current_price = up_price if pos.side == 'UP' else down_price

            if current_price <= pos.entry_price * (1 - STOP_LOSS_PCT):
                pos.exit_price = current_price
                pos.exit_time = elapsed
                # FIX: Use actual exit price for stop loss P&L instead of assuming total loss
                gross_pnl = (current_price - pos.entry_price) * pos.size
                fees = (pos.entry_price * pos.size * FEE_RATE) + (current_price * pos.size * FEE_RATE)
                loss = gross_pnl - fees
                pos.pnl = loss
                pos.status = "CLOSED_LOSS"
                self.daily_pnl += loss
                self.consecutive_losses += 1
                logger.info(f"🛑 STOP LOSS | {crypto.upper()} {pos.side}@{pos.entry_price:.4f} → {current_price:.4f} | LOSS: ${loss:.4f} | Daily P&L: ${self.daily_pnl:+.4f} | Consecutive losses: {self.consecutive_losses}")

            elif current_price >= pos.entry_price * (1 + TAKE_PROFIT_PCT):
                pos.exit_price = current_price
                pos.exit_time = elapsed
                gross_profit = (current_price - pos.entry_price) * pos.size
                fees = (pos.entry_price * pos.size * FEE_RATE) + (current_price * pos.size * FEE_RATE)
                net = gross_profit - fees
                pos.pnl = net
                pos.status = "CLOSED_PROFIT"
                self.daily_pnl += net
                self.consecutive_losses = 0  # Reset on win
                logger.info(f"🎯 TAKE PROFIT | {crypto.upper()} {pos.side}@{pos.entry_price:.4f} → {current_price:.4f} | PROFIT: ${net:.4f} | Daily P&L: ${self.daily_pnl:+.4f}")

            elif elapsed - pos.entry_time > MAX_HOLD_SECONDS:
                pos.exit_price = current_price
                pos.exit_time = elapsed
                gross_pnl = (current_price - pos.entry_price) * pos.size
                fees = (pos.entry_price * pos.size * FEE_RATE) + (current_price * pos.size * FEE_RATE)
                net = gross_pnl - fees
                pos.pnl = net
                pos.status = "EXPIRED"
                if net < 0:
                    self.daily_pnl += net
                    self.consecutive_losses += 1
                else:
                    self.daily_pnl += net
                    self.consecutive_losses = 0
                logger.info(f"⏰ EXPIRED | {crypto.upper()} {pos.side}@{pos.entry_price:.4f} → {current_price:.4f} | P&L: ${net:+.4f} | Daily P&L: ${self.daily_pnl:+.4f}")

        # === CHECK FOR DIP ENTRY ===
        if elapsed < MAX_ENTRY_TIME:
            # Check limits before entering
            if not self._check_limits():
                return

            # Check per-crypto daily trade limit
            if self.daily_trades_per_crypto[crypto] >= MAX_DAILY_TRADES_PER_CRYPTO:
                return

            up_crashed = up_price <= CRASH_THRESHOLD
            down_crashed = down_price <= CRASH_THRESHOLD

            trend_data = self.crypto_trends[crypto]

            if up_crashed and session.info.slug not in trend_data['up_crashes']:
                trend_data['up_crashes'].append(session.info.slug)
            if down_crashed and session.info.slug not in trend_data['down_crashes']:
                trend_data['down_crashes'].append(session.info.slug)

            self._update_trend(crypto)

            buy_side = None
            current_trend = trend_data['current_trend']

            if current_trend == "UP_TREND" and up_crashed:
                buy_side = 'UP'
            elif current_trend == "DOWN_TREND" and down_crashed:
                buy_side = 'DOWN'
            elif current_trend == "CHOPPY":
                if up_crashed and not any(p.side == 'UP' and p.status == "OPEN" for p in session.positions):
                    buy_side = 'UP'
                elif down_crashed and not any(p.side == 'DOWN' and p.status == "OPEN" for p in session.positions):
                    buy_side = 'DOWN'
            elif current_trend == "UNKNOWN":
                if up_crashed:
                    buy_side = 'UP'
                elif down_crashed:
                    buy_side = 'DOWN'

            if buy_side:
                entry_price = up_price if buy_side == 'UP' else down_price
                has_existing = any(p.side == buy_side and p.status == "OPEN" for p in session.positions)
                if not has_existing:
                    crash_confirmed = self._check_crash(session.prices, buy_side, entry_price)
                    if crash_confirmed:
                        bounce_confirmed = self._check_bounce(session.prices, buy_side)
                        if bounce_confirmed:
                            size = 5.0 if current_trend == "CHOPPY" else 10.0

                            # === PARTIAL FILL HANDLING ===
                            tid = tid0 if buy_side == 'UP' else tid1
                            book = session.books.get(tid, {'asks': []})
                            asks = book['asks']

                            if not asks:
                                return

                            # Calculate average fill price from top 3 levels
                            total_cost = 0.0
                            remaining = size
                            levels_used = 0

                            for ask in asks[:3]:  # Top 3 levels only
                                if remaining <= 0:
                                    break
                                fill = min(remaining, ask['size'])
                                total_cost += fill * ask['price']
                                remaining -= fill
                                levels_used += 1

                            filled_size = size - remaining
                            fill_ratio = filled_size / size

                            if fill_ratio < MIN_FILL_RATIO:
                                logger.info(f"⚠️ INSUFFICIENT LIQUIDITY | {crypto.upper()} {buy_side} | Filled: {filled_size:.1f}/{size} ({fill_ratio*100:.0f}%) | Skipping")
                                return

                            avg_entry = total_cost / filled_size
                            slippage = avg_entry - asks[0]['price']

                            # Use actual filled size (might be slightly less due to rounding)
                            actual_size = filled_size

                            pos = DipPosition(
                                pos_id=f"{session.info.slug}_{buy_side}_{int(time.time()*1000)}",
                                side=buy_side,
                                entry_price=avg_entry,
                                size=actual_size,
                                entry_time=elapsed
                            )
                            session.positions.append(pos)
                            self.daily_trades += 1
                            self.daily_trades_per_crypto[crypto] += 1
                            logger.info(f"🚀 DIP BUY | {crypto.upper()} {buy_side} @ {avg_entry:.4f} (slippage={slippage:.4f}, size={actual_size}, filled={fill_ratio*100:.0f}%, t={elapsed:.0f}s) | Daily trades: {self.daily_trades}/{MAX_DAILY_TRADES}")

    def _update_trend(self, crypto: str):
        trend_data = self.crypto_trends[crypto]
        recent_up = len(trend_data['up_crashes'][-TREND_LOOKBACK_MARKETS:])
        recent_down = len(trend_data['down_crashes'][-TREND_LOOKBACK_MARKETS:])

        new_trend = trend_data['current_trend']

        if recent_up >= TREND_FLIP_THRESHOLD and recent_down == 0:
            new_trend = "UP_TREND"
        elif recent_down >= TREND_FLIP_THRESHOLD and recent_up == 0:
            new_trend = "DOWN_TREND"
        elif recent_up > 0 and recent_down > 0:
            new_trend = "CHOPPY"
        else:
            new_trend = "UNKNOWN"

        if new_trend != trend_data['current_trend']:
            logger.info(f"📊 {crypto.upper()} TREND: {trend_data['current_trend']} → {new_trend}")
            trend_data['current_trend'] = new_trend

        if new_trend == "CHOPPY" and not trend_data['last_choppy_state']:
            trend_data['choppy_count'] += 1
            trend_data['last_choppy_state'] = True
        elif new_trend != "CHOPPY":
            trend_data['last_choppy_state'] = False

    def _check_crash(self, prices: List[PriceRecord], side: str, current_price: float) -> bool:
        if len(prices) < 5:
            return False

        cutoff = prices[-1].time - 45
        recent = [p for p in prices if p.time >= cutoff]

        if len(recent) < 3:
            return False

        if side == 'UP':
            max_price = max(p.up_price for p in recent)
        else:
            max_price = max(p.down_price for p in recent)

        if max_price <= 0:
            return False

        drop_pct = (max_price - current_price) / max_price
        return drop_pct >= MIN_CRASH_SPEED

    def _check_bounce(self, prices: List[PriceRecord], side: str) -> bool:
        if len(prices) < 3:
            return True

        last3 = prices[-3:]
        if side == 'UP':
            p1, p2, p3 = last3[0].up_price, last3[1].up_price, last3[2].up_price
        else:
            p1, p2, p3 = last3[0].down_price, last3[1].down_price, last3[2].down_price

        return p2 < p3

    async def _close_session(self, session: MarketSession):
        crypto = session.info.crypto
        logger.info(f"\n🏁 {crypto.upper()} SESSION END: {session.info.slug}")

        tid0, tid1 = session.info.token_ids[0], session.info.token_ids[1]
        book0 = session.books.get(tid0, {'asks': []})
        book1 = session.books.get(tid1, {'asks': []})
        up_price = book0['asks'][0]['price'] if book0['asks'] else 0
        down_price = book1['asks'][0]['price'] if book1['asks'] else 0

        for pos in session.positions:
            if pos.status == "OPEN":
                current_price = up_price if pos.side == 'UP' else down_price
                pos.exit_price = current_price
                pos.exit_time = time.time() - session.start_time
                gross_pnl = (current_price - pos.entry_price) * pos.size
                fees = (pos.entry_price * pos.size * FEE_RATE) + (current_price * pos.size * FEE_RATE)
                net = gross_pnl - fees
                pos.pnl = net
                pos.status = "EXPIRED"
                self.daily_pnl += net
                if net < 0:
                    self.consecutive_losses += 1

        profits = sum(p.pnl for p in session.positions if p.pnl and p.pnl > 0)
        losses = sum(p.pnl for p in session.positions if p.pnl and p.pnl < 0)
        net_pnl = profits + losses

        logger.info(f" Trades: {len(session.positions)} | Profits: ${profits:+.4f} | Losses: ${losses:+.4f}")
        logger.info(f" TRUE NET P&L: ${net_pnl:+.4f} | Daily P&L: ${self.daily_pnl:+.4f}")

        up_min = min((p.up_price for p in session.prices), default=1.0)
        down_min = min((p.down_price for p in session.prices), default=1.0)
        trend_data = self.crypto_trends[crypto]
        if up_min <= CRASH_THRESHOLD and session.info.slug not in trend_data['up_crashes']:
            trend_data['up_crashes'].append(session.info.slug)
        if down_min <= CRASH_THRESHOLD and session.info.slug not in trend_data['down_crashes']:
            trend_data['down_crashes'].append(session.info.slug)

        self.session_history.append({
            'slug': session.info.slug,
            'crypto': crypto,
            'title': session.info.title,
            'start_et': session.info.window_start_et.isoformat(),
            'trades': len(session.positions),
            'profits': profits,
            'losses': losses,
            'net_pnl': net_pnl,
            'trend': trend_data['current_trend'],
            'messages': session.messages_received,
            'trade_details': [
                {
                    'side': p.side,
                    'entry': p.entry_price,
                    'exit': p.exit_price,
                    'size': p.size,
                    'pnl': p.pnl,
                    'status': p.status
                }
                for p in session.positions
            ]
        })

        if crypto in self.sessions:
            del self.sessions[crypto]

    async def _status_loop(self):
        while self._running:
            await asyncio.sleep(30)
            self._write_status()

    def _write_status(self):
        now = datetime.now(timezone.utc)

        total_trades = sum(h['trades'] for h in self.session_history)
        total_profits = sum(h['profits'] for h in self.session_history)
        total_losses = sum(h['losses'] for h in self.session_history)
        total_net = total_profits + total_losses

        win_sessions = sum(1 for h in self.session_history if h['net_pnl'] > 0)
        loss_sessions = sum(1 for h in self.session_history if h['net_pnl'] < 0)
        total_sessions = len(self.session_history)

        lines = []
        lines.append("=" * 70)
        lines.append("MULTI-CRYPTO 5M DIP BUYER — LIVE READY")
        lines.append(f"Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append("=" * 70)
        lines.append("")

        # Risk status
        lines.append("🛡️ RISK STATUS")
        lines.append("-" * 40)
        lines.append(f" Daily P&L: ${self.daily_pnl:+.4f} (limit: ${DAILY_LOSS_LIMIT})")
        lines.append(f" Daily trades: {self.daily_trades}/{MAX_DAILY_TRADES}")
        lines.append(f" Consecutive loss: {self.consecutive_losses}/{MAX_CONSECUTIVE_TRADE_LOSSES}")
        if self.trading_stopped:
            lines.append(f" ⚠️ TRADING STOPPED: {self.stop_reason}")
        lines.append("")

        lines.append("📊 OVERALL PERFORMANCE")
        lines.append("-" * 40)
        lines.append(f" Uptime: {int(time.time() - self.start_time)} seconds")
        lines.append(f" Total sessions: {total_sessions}")
        lines.append(f" Winning sessions: {win_sessions}")
        lines.append(f" Losing sessions: {loss_sessions}")
        lines.append(f" Win rate: {win_sessions/max(total_sessions,1)*100:.1f}%")
        lines.append("")
        lines.append(f" Total trades: {total_trades}")
        lines.append(f" Total profits: ${total_profits:+.4f}")
        lines.append(f" Total losses: ${total_losses:+.4f}")
        lines.append(f" TRUE NET P&L: ${total_net:+.4f}")
        lines.append(f" Avg per session: ${total_net/max(total_sessions,1):+.4f}")
        lines.append("")

        lines.append("📈 PER-CRYPTO BREAKDOWN")
        lines.append("-" * 40)
        for crypto in CRYPTOS:
            crypto_sessions = [h for h in self.session_history if h['crypto'] == crypto]
            c_trades = sum(h['trades'] for h in crypto_sessions)
            c_profits = sum(h['profits'] for h in crypto_sessions)
            c_losses = sum(h['losses'] for h in crypto_sessions)
            c_net = c_profits + c_losses
            trend_data = self.crypto_trends[crypto]
            daily_crypto = self.daily_trades_per_crypto[crypto]
            lines.append(f" {crypto.upper():4s} | Trades:{c_trades} | P&L:${c_net:+.4f} | Daily:{daily_crypto}/{MAX_DAILY_TRADES_PER_CRYPTO} | Trend:{trend_data['current_trend']:12s}")
        lines.append("")

        lines.append("🎯 ACTIVE SESSIONS")
        lines.append("-" * 40)
        if self.sessions:
            for crypto, session in self.sessions.items():
                elapsed = int(time.time() - session.start_time)
                tid0, tid1 = session.info.token_ids[0], session.info.token_ids[1]
                book0 = session.books.get(tid0, {'asks': []})
                book1 = session.books.get(tid1, {'asks': []})
                up = book0['asks'][0]['price'] if book0['asks'] else None
                down = book1['asks'][0]['price'] if book1['asks'] else None
                open_pos = len([p for p in session.positions if p.status == "OPEN"])
                closed_profit = len([p for p in session.positions if p.status == "CLOSED_PROFIT"])
                closed_loss = len([p for p in session.positions if p.status == "CLOSED_LOSS"])
                session_pnl = sum(p.pnl for p in session.positions if p.pnl is not None)

                up_str = f"{up:.4f}" if up else "N/A"
                down_str = f"{down:.4f}" if down else "N/A"
                lines.append(f" {crypto.upper():4s} | {session.info.slug[-20:]} | {elapsed}s")
                lines.append(f" UP:{up_str} DOWN:{down_str} | Open:{open_pos} Wins:{closed_profit} Losses:{closed_loss} | P&L:${session_pnl:+.4f}")
        else:
            lines.append(" No active sessions")
        lines.append("")

        if self.session_history:
            lines.append("📋 RECENT SESSIONS (last 15)")
            lines.append("-" * 40)
            for h in self.session_history[-15:]:
                status = "✅" if h['net_pnl'] > 0 else "❌" if h['net_pnl'] < 0 else "➖"
                lines.append(f" {status} {h['crypto'].upper():4s} {h['slug'][-20:]} | {h['trend']:12s} | Trades:{h['trades']} | NET:${h['net_pnl']:+.4f}")
            lines.append("")

        lines.append("=" * 70)

        try:
            with open('/root/polymarket_bot/live_dip_status.txt', 'w') as f:
                f.write('\n'.join(lines))
        except Exception as e:
            logger.error(f"Status write error: {e}")

    def _final_summary(self):
        self._write_status()

        logger.info("\n" + "=" * 70)
        logger.info("FINAL LIVE DIP BUYER SUMMARY")
        logger.info("=" * 70)

        total_trades = 0
        total_profits = 0.0
        total_losses = 0.0
        total_net = 0.0

        for hist in self.session_history:
            trades = hist['trades']
            profits = hist['profits']
            losses = hist['losses']
            net = hist['net_pnl']

            total_trades += trades
            total_profits += profits
            total_losses += losses
            total_net += net

            status = '✅' if net > 0 else '❌' if net < 0 else '➖'
            logger.info(f"\n{hist['crypto'].upper()} {hist['slug'][-20:]}:")
            logger.info(f" {status} Trades:{trades} | Profit:${profits:+.4f} | Loss:${losses:+.4f} | NET:${net:+.4f}")

        logger.info(f"\n{'='*70}")
        logger.info(f"TOTALS across {len(self.session_history)} sessions:")
        logger.info(f" Total trades: {total_trades}")
        logger.info(f" Total profits: ${total_profits:+.4f}")
        logger.info(f" Total losses: ${total_losses:+.4f}")
        logger.info(f" TRUE NET P&L: ${total_net:+.4f}")
        logger.info(f" Avg per session: ${total_net / max(len(self.session_history), 1):+.4f}")
        logger.info(f" Win rate: {sum(1 for h in self.session_history if h['net_pnl'] > 0)}/{len(self.session_history)}")
        logger.info(f"{'='*70}")

    def _save_all(self):
        data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'uptime_seconds': time.time() - self.start_time,
            'total_sessions': len(self.all_sessions),
            'daily_pnl': self.daily_pnl,
            'daily_trades': self.daily_trades,
            'consecutive_losses': self.consecutive_losses,
            'trading_stopped': self.trading_stopped,
            'stop_reason': self.stop_reason,
            'crypto_trends': {
                c: {
                    'up_crashes': len(self.crypto_trends[c]['up_crashes']),
                    'down_crashes': len(self.crypto_trends[c]['down_crashes']),
                    'current_trend': self.crypto_trends[c]['current_trend'],
                    'choppy_count': self.crypto_trends[c]['choppy_count']
                }
                for c in CRYPTOS
            },
            'session_history': self.session_history
        }

        try:
            with open('/root/polymarket_bot/live_dip_buyer_data.json', 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"\n💾 Saved to live_dip_buyer_data.json")
        except Exception as e:
            logger.error(f"Save error: {e}")

    def stop(self):
        self._running = False
        self.ws.stop()


async def main():
    trader = MultiCryptoTrader()
    try:
        await trader.run()
    except KeyboardInterrupt:
        logger.info("👋 Interrupted")
    except Exception as e:
        logger.error(f"Fatal: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
