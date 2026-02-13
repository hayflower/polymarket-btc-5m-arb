"""
Polymarket BTC 5-Minute Up/Down Arbitrage Bot
==============================================

Strategy reverse-engineered from two Polymarket traders:

gabagool22 (0x6031b...f96d):
  - Buys BOTH "Up" and "Down" tokens when combined cost < $1.00
  - Scales in with 15-30 small orders over ~40 seconds
  - Enters ~2 minutes into each 5-minute window
  - Holds all positions to expiry (no early exits)
  - Profit = $1.00 - combined_cost per share pair (2-7% per window)

Wee-Playroom (0xf247...5216):
  - Directional with partial hedge (larger size on favored side)
  - Actively sells positions during the window
  - Less profitable per-trade: often overpays (combined > $1.00)

This bot implements gabagool22's arbitrage as the core strategy.

INFRASTRUCTURE (verified Feb 2026):
  - CLOB origin server: AWS eu-west-2 (London, UK)
    * Confirmed by Polymarket's own geoblock docs referencing eu-west-2
    * Backup/failover: eu-west-1 (Ireland)
    * Fronted by Cloudflare Anycast (masks origin IP)
    * NJ/Equinix NY4 claims are FALSE -- confused with trad-fi infra
  - UK itself is GEOBLOCKED -- you cannot trade from a London IP
  - Recommended VPS: Amsterdam (5-12ms) or Zurich (community-best)
  - Rate limits: 150/s for /book, 350/s burst for /order
  - Fees: 0% on most markets; crypto 5-min may have fee_rate_bps
  - Price range: 0.01 - 0.99 (API rejects outside)
  - Tick sizes: 0.001 (common), changes dynamically near 0/1
  - Min order: ~5 shares
  - py-clob-client known bugs: silent order failures (#258),
    stale tick size cache (#122), decimal issues (#253)

REALISTIC LIMITATIONS:
  - Arb windows are competitive and close in SECONDS
  - gabagool22 alone does $132M volume -- these are not empty markets
  - Slippage between book read and order fill eats margins
  - WebSocket feeds stop after ~20 min (bot uses REST polling)
  - You compete against co-located bots with <5ms latency
"""

import os
import sys
import json
import time
import math
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from collections import defaultdict, deque

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log"),
    ],
)
log = logging.getLogger("btc5m")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"
WS_CLOB = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
CHAIN_ID = 137  # Polygon

# Strategy params
MAX_COMBINED_COST = float(os.getenv("MAX_COMBINED_COST", "0.97"))
MIN_PROFIT_MARGIN = float(os.getenv("MIN_PROFIT_MARGIN", "0.02"))
ORDER_SIZE_USDC = float(os.getenv("ORDER_SIZE_USDC", "50"))
MAX_POSITION_PER_WINDOW = float(os.getenv("MAX_POSITION_PER_WINDOW", "500"))
DIRECTIONAL_BIAS = float(os.getenv("DIRECTIONAL_BIAS", "0.0"))
BANKROLL = float(os.getenv("BANKROLL", "500"))  # total capital

# Execution params
NUM_SLICES = int(os.getenv("NUM_SLICES", "10"))
SLICE_DELAY_SEC = float(os.getenv("SLICE_DELAY_SEC", "1.5"))
ENTRY_DELAY_SEC = int(os.getenv("ENTRY_DELAY_SEC", "90"))  # wait 90s into window
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))
BOOK_POLL_BUDGET = 100  # max /book requests per 10s window (limit is 1500)
REPRICE_INTERVAL = int(os.getenv("REPRICE_INTERVAL", "3"))  # re-read book every N slices

# Fee calculation for crypto 5-min markets
FEE_RATE_BPS = int(os.getenv("FEE_RATE_BPS", "0"))  # check market's feeRateBps field

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Market:
    condition_id: str
    title: str
    slug: str
    up_token_id: str
    down_token_id: str
    end_date: str
    event_slug: str
    tick_size: float = 0.001
    neg_risk: bool = False
    fee_rate_bps: int = 0

    @property
    def end_timestamp(self) -> float:
        try:
            dt = datetime.fromisoformat(self.end_date.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, AttributeError):
            return 0.0

    @property
    def start_timestamp(self) -> float:
        return self.end_timestamp - 300


@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class OrderBookSnapshot:
    up_asks: list[BookLevel] = field(default_factory=list)
    up_bids: list[BookLevel] = field(default_factory=list)
    down_asks: list[BookLevel] = field(default_factory=list)
    down_bids: list[BookLevel] = field(default_factory=list)
    timestamp: float = 0.0

    @property
    def up_best_ask(self) -> float:
        return self.up_asks[0].price if self.up_asks else 1.0

    @property
    def down_best_ask(self) -> float:
        return self.down_asks[0].price if self.down_asks else 1.0

    @property
    def up_best_bid(self) -> float:
        return self.up_bids[0].price if self.up_bids else 0.0

    @property
    def down_best_bid(self) -> float:
        return self.down_bids[0].price if self.down_bids else 0.0

    @property
    def combined_ask(self) -> float:
        return self.up_best_ask + self.down_best_ask

    @property
    def up_ask_depth(self) -> float:
        return sum(l.size for l in self.up_asks)

    @property
    def down_ask_depth(self) -> float:
        return sum(l.size for l in self.down_asks)

    def fillable_at_price(self, side: str, max_price: float) -> float:
        """How many shares can be filled at or below max_price."""
        asks = self.up_asks if side == "up" else self.down_asks
        total = 0.0
        for level in asks:
            if level.price <= max_price:
                total += level.size
            else:
                break
        return total

    def vwap(self, side: str, num_shares: float) -> float:
        """Volume-weighted average price to fill num_shares."""
        asks = self.up_asks if side == "up" else self.down_asks
        filled = 0.0
        cost = 0.0
        for level in asks:
            take = min(level.size, num_shares - filled)
            cost += take * level.price
            filled += take
            if filled >= num_shares:
                break
        return cost / filled if filled > 0 else 1.0


@dataclass
class Position:
    market_title: str
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    orders_placed: int = 0
    orders_filled: int = 0

    @property
    def total_invested(self) -> float:
        return self.up_cost + self.down_cost

    @property
    def pairs(self) -> float:
        return min(self.up_shares, self.down_shares)

    @property
    def combined_avg_cost(self) -> float:
        if self.pairs == 0:
            return 1.0
        return self.total_invested / self.pairs

    @property
    def expected_payout(self) -> float:
        return self.pairs * 1.0

    @property
    def expected_profit(self) -> float:
        return self.expected_payout - self.total_invested

    @property
    def excess_shares(self) -> float:
        return abs(self.up_shares - self.down_shares)


# ---------------------------------------------------------------------------
# Fee calculation
# ---------------------------------------------------------------------------

def calculate_fee(price: float, fee_rate_bps: int) -> float:
    """fee(p) = p * (1-p) * (fee_rate_bps / 10000). Max ~1.56% at p=0.50."""
    if fee_rate_bps == 0:
        return 0.0
    r = fee_rate_bps / 10_000
    return price * (1 - price) * r


# ---------------------------------------------------------------------------
# Price / size rounding
# ---------------------------------------------------------------------------

def round_price(price: float, tick_size: float) -> float:
    """Round price to valid tick. CLOB rejects invalid precision."""
    d = Decimal(str(price)).quantize(Decimal(str(tick_size)), rounding=ROUND_DOWN)
    return max(0.01, min(0.99, float(d)))


def round_size(size: float) -> float:
    """Round size to 2 decimals (CLOB requirement for all tick sizes)."""
    return float(Decimal(str(size)).quantize(Decimal("0.01"), rounding=ROUND_DOWN))


# ---------------------------------------------------------------------------
# Rate limiter (uses deque for O(1) cleanup instead of list rebuild)
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, max_requests: int, window_sec: float = 10.0):
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self) -> bool:
        now = time.time()
        cutoff = now - self.window_sec
        with self._lock:
            while self._timestamps and self._timestamps[0] <= cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self.max_requests:
                return False
            self._timestamps.append(now)
            return True

    def wait_and_acquire(self):
        while not self.acquire():
            time.sleep(0.05)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

class PolymarketAPI:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.book_limiter = RateLimiter(BOOK_POLL_BUDGET, 10.0)
        self.gamma_limiter = RateLimiter(400, 10.0)
        self.price_limiter = RateLimiter(1000, 10.0)
        self._request_count = 0
        self._error_count = 0
        self._pool = ThreadPoolExecutor(max_workers=4)

    def _get(self, url: str, params: dict | None = None,
             limiter: RateLimiter | None = None, timeout: int = 5) -> dict | list | None:
        if limiter:
            limiter.wait_and_acquire()
        try:
            self._request_count += 1
            resp = self.session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            self._error_count += 1
            try:
                status = resp.status_code
            except UnboundLocalError:
                status = 0
            if status == 429:
                log.warning(f"Rate limited on {url}. Backing off 2s.")
                time.sleep(2)
            else:
                log.error(f"HTTP {status} on {url}")
            return None
        except requests.exceptions.RequestException as e:
            self._error_count += 1
            log.error(f"Request failed {url}: {e}")
            return None

    def get_order_book(self, token_id: str) -> dict:
        return self._get(f"{CLOB_BASE}/book", {"token_id": token_id}, self.book_limiter) or {}

    def get_both_books(self, up_token: str, down_token: str) -> tuple[dict, dict]:
        """Fetch both order books concurrently to minimize snapshot skew."""
        fut_up = self._pool.submit(self.get_order_book, up_token)
        fut_down = self._pool.submit(self.get_order_book, down_token)
        return fut_up.result(timeout=5), fut_down.result(timeout=5)

    def get_price(self, token_id: str, side: str = "buy") -> float | None:
        result = self._get(f"{CLOB_BASE}/price", {"token_id": token_id, "side": side}, self.price_limiter)
        return float(result.get("price", 0)) if result else None

    def get_midpoint(self, token_id: str) -> float | None:
        result = self._get(f"{CLOB_BASE}/midpoint", {"token_id": token_id}, self.price_limiter)
        return float(result.get("mid", 0)) if result else None

    def search_btc_5m_markets(self) -> list[dict]:
        markets = []
        events = self._get(f"{GAMMA_BASE}/events",
                           {"limit": 100, "active": "true", "closed": "false"},
                           self.gamma_limiter, timeout=10)
        if events:
            for event in events:
                if self._is_btc_5m(event.get("title", ""), event.get("ticker", "")):
                    markets.append(event)
        if markets:
            return markets

        raw = self._get(f"{GAMMA_BASE}/markets",
                        {"limit": 100, "active": "true", "closed": "false"},
                        self.gamma_limiter, timeout=10)
        if raw:
            for m in raw:
                if self._is_btc_5m(m.get("title", ""), m.get("groupItemTitle", "")):
                    markets.append(m)
        return markets

    @staticmethod
    def _is_btc_5m(title: str, ticker: str) -> bool:
        t = title.lower()
        k = ticker.lower()
        btc = "btc" in t or "bitcoin" in t or "btc" in k
        five = "5m" in k or "5-minute" in t or ("5" in t and "minute" in t) or "5m" in t
        updown = "up" in t and "down" in t
        return btc and (five or updown)

    def get_user_activity(self, wallet: str, limit: int = 100, offset: int = 0) -> list[dict]:
        return self._get(f"{DATA_BASE}/activity",
                         {"user": wallet, "limit": limit, "offset": offset},
                         timeout=10) or []

    @property
    def stats(self) -> str:
        return f"requests={self._request_count} errors={self._error_count}"


# ---------------------------------------------------------------------------
# Order book parsing
# ---------------------------------------------------------------------------

def parse_book(raw: dict) -> tuple[list[BookLevel], list[BookLevel]]:
    asks = sorted([BookLevel(float(a["price"]), float(a["size"])) for a in raw.get("asks", [])],
                  key=lambda x: x.price)
    bids = sorted([BookLevel(float(b["price"]), float(b["size"])) for b in raw.get("bids", [])],
                  key=lambda x: x.price, reverse=True)
    return asks, bids


def get_snapshot(api: PolymarketAPI, market: Market) -> OrderBookSnapshot:
    """Fetch both books concurrently to reduce snapshot skew."""
    up_raw, down_raw = api.get_both_books(market.up_token_id, market.down_token_id)
    up_asks, up_bids = parse_book(up_raw)
    down_asks, down_bids = parse_book(down_raw)
    return OrderBookSnapshot(
        up_asks=up_asks, up_bids=up_bids,
        down_asks=down_asks, down_bids=down_bids,
        timestamp=time.time(),
    )


# ---------------------------------------------------------------------------
# Strategy engine
# ---------------------------------------------------------------------------

class ArbitrageStrategy:
    """
    Buy both Up and Down when combined < $1.00 after fees.

    Uses VWAP (volume-weighted average price) to estimate actual fill
    cost across multiple book levels, not just top-of-book.
    """

    def __init__(self, max_combined: float, min_margin: float,
                 bias: float = 0.0, fee_rate_bps: int = 0):
        self.max_combined = max_combined
        self.min_margin = min_margin
        self.bias = bias
        self.fee_rate_bps = fee_rate_bps
        self.opportunities_seen = 0
        self.opportunities_traded = 0

    def evaluate(self, snapshot: OrderBookSnapshot,
                 available_capital: float = ORDER_SIZE_USDC) -> dict | None:
        up_ask = snapshot.up_best_ask
        down_ask = snapshot.down_best_ask

        up_fee = calculate_fee(up_ask, self.fee_rate_bps)
        down_fee = calculate_fee(down_ask, self.fee_rate_bps)
        total_fee = up_fee + down_fee

        combined_raw = up_ask + down_ask
        combined_net = combined_raw + total_fee
        margin_net = 1.0 - combined_net

        log.info(
            f"  Up={up_ask:.4f} Down={down_ask:.4f} "
            f"Raw={combined_raw:.4f} Fees={total_fee:.4f} "
            f"Net={combined_net:.4f} Margin={margin_net:.4f}"
        )

        if combined_net >= self.max_combined:
            return None
        if margin_net < self.min_margin:
            return None

        self.opportunities_seen += 1

        # Walk the book for realistic depth
        up_fillable = snapshot.fillable_at_price("up", up_ask + 0.02)
        down_fillable = snapshot.fillable_at_price("down", down_ask + 0.02)
        max_pairs = min(up_fillable, down_fillable)

        # Budget: use the smaller of configured size and available capital
        effective_budget = min(available_capital, ORDER_SIZE_USDC, MAX_POSITION_PER_WINDOW)
        budget_pairs = effective_budget / combined_net
        target_pairs = min(max_pairs, budget_pairs)

        if target_pairs < 5:
            log.info(f"  Insufficient depth: {max_pairs:.0f} fillable, need 5+")
            return None

        # VWAP check: actual fill cost may be worse than top-of-book
        up_vwap = snapshot.vwap("up", target_pairs)
        down_vwap = snapshot.vwap("down", target_pairs)
        vwap_combined = up_vwap + down_vwap + total_fee
        vwap_margin = 1.0 - vwap_combined

        if vwap_margin < self.min_margin:
            log.info(f"  VWAP margin too thin: {vwap_margin:.4f} (top-of-book was {margin_net:.4f})")
            return None

        # Directional bias
        up_shares = target_pairs
        down_shares = target_pairs
        if self.bias != 0.0:
            shift = min(abs(self.bias), 0.3)
            if self.bias > 0:
                up_shares *= (1 + shift)
            else:
                down_shares *= (1 + shift)

        up_shares = round_size(up_shares)
        down_shares = round_size(down_shares)

        total_cost = (up_shares * (up_ask + up_fee) + down_shares * (down_ask + down_fee))
        pairs = min(up_shares, down_shares)
        expected_profit = pairs - total_cost

        return {
            "up_price": up_ask,
            "down_price": down_ask,
            "combined_raw": combined_raw,
            "combined_net": combined_net,
            "margin_raw": 1.0 - combined_raw,
            "margin_net": margin_net,
            "vwap_margin": vwap_margin,
            "fees_per_pair": total_fee,
            "up_shares": up_shares,
            "down_shares": down_shares,
            "total_cost": total_cost,
            "expected_profit": expected_profit,
            "fillable_depth": max_pairs,
        }


# ---------------------------------------------------------------------------
# Order executor
# ---------------------------------------------------------------------------

class OrderExecutor:
    """
    Known py-clob-client issues to watch:
    - Orders return success but never execute on-chain (#258)
    - Tick size cache goes stale (#122)
    - Price 0.999 rejected; valid range 0.01-0.99 (#242)
    - Decimal issues in market order amounts (#253)
    """

    def __init__(self, private_key: str):
        self.private_key = private_key
        self.client = None
        self.dry_run = True
        self._order_count = 0
        self._fill_count = 0

    def initialize(self) -> bool:
        if not self.private_key or "YOUR" in self.private_key:
            log.warning("No private key. DRY RUN mode enabled.")
            return False
        try:
            from py_clob_client.client import ClobClient
            self.client = ClobClient(CLOB_BASE, key=self.private_key, chain_id=CHAIN_ID)
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            self.dry_run = False
            log.info("CLOB client initialized. LIVE trading enabled.")
            return True
        except ImportError:
            log.error("py-clob-client not installed. pip install py-clob-client")
            return False
        except Exception as e:
            log.error(f"CLOB init failed: {e}")
            return False

    def place_limit_buy(self, token_id: str, price: float, size: float,
                        tick_size: float = 0.001) -> dict | None:
        price = round_price(price, tick_size)
        size = round_size(size)
        if size < 5:
            return None

        self._order_count += 1
        if self.dry_run:
            log.info(f"  [DRY] BUY {size} @ {price:.4f} (...{token_id[-8:]})")
            return {"status": "dry_run", "price": price, "size": size}

        try:
            from py_clob_client.order_builder.constants import BUY
            order = self.client.create_order({
                "token_id": token_id, "price": price, "size": size, "side": BUY,
            })
            result = self.client.post_order(order)
            status = result.get("status", "unknown")
            log.info(f"  Order #{self._order_count}: {status}")
            if status == "matched" or result.get("orderID"):
                self._fill_count += 1
            else:
                log.warning(f"  Possible non-fill: {result}")
            return result
        except Exception as e:
            log.error(f"  Order failed: {e}")
            return None

    @property
    def fill_rate(self) -> str:
        if self._order_count == 0:
            return "0/0"
        return f"{self._fill_count}/{self._order_count} ({self._fill_count/self._order_count*100:.0f}%)"


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------

def parse_market(raw: dict, event: dict | None = None) -> Market | None:
    tokens = raw.get("clobTokenIds", [])
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except json.JSONDecodeError:
            return None
    if len(tokens) < 2:
        return None

    outcomes = raw.get("outcomes", [])
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except json.JSONDecodeError:
            outcomes = ["Up", "Down"]

    up_idx, down_idx = 0, 1
    for i, o in enumerate(outcomes):
        if str(o).lower() == "up":
            up_idx = i
        elif str(o).lower() == "down":
            down_idx = i

    return Market(
        condition_id=raw.get("conditionId", ""),
        title=raw.get("question", raw.get("title", "Unknown")),
        slug=raw.get("slug", ""),
        up_token_id=tokens[up_idx],
        down_token_id=tokens[down_idx],
        end_date=raw.get("endDate", raw.get("endDateIso", "")),
        event_slug=(event or raw).get("slug", ""),
        tick_size=float(raw.get("minimumTickSize", 0.001)),
        neg_risk=raw.get("negRisk", False),
        fee_rate_bps=int(raw.get("feeRateBps", 0)),
    )


def discover_markets(api: PolymarketAPI) -> list[Market]:
    raw_results = api.search_btc_5m_markets()
    markets = []
    for item in raw_results:
        nested = item.get("markets", [])
        if nested:
            for m in nested:
                if not m.get("closed"):
                    parsed = parse_market(m, event=item)
                    if parsed:
                        markets.append(parsed)
        else:
            parsed = parse_market(item)
            if parsed:
                markets.append(parsed)
    markets.sort(key=lambda m: m.end_timestamp)
    return markets


def pick_next_market(markets: list[Market], traded: set[str]) -> Market | None:
    now = time.time()
    for m in markets:
        if m.condition_id in traded:
            continue
        if m.end_timestamp and m.end_timestamp < now:
            continue
        if m.start_timestamp and m.start_timestamp > now + 300:
            continue
        return m
    return None


# ---------------------------------------------------------------------------
# Execution engine (with live re-pricing)
# ---------------------------------------------------------------------------

def execute_arb(executor: OrderExecutor, api: PolymarketAPI,
                market: Market, plan: dict) -> Position:
    """
    Scaled entry with periodic re-pricing.
    Every REPRICE_INTERVAL slices, re-reads the order book to get fresh
    prices instead of using the stale initial snapshot.
    """
    position = Position(market_title=market.title)

    up_per_slice = round_size(plan["up_shares"] / NUM_SLICES)
    down_per_slice = round_size(plan["down_shares"] / NUM_SLICES)
    if up_per_slice < 5:
        up_per_slice = 5.0
    if down_per_slice < 5:
        down_per_slice = 5.0

    actual_slices = min(
        NUM_SLICES,
        int(plan["up_shares"] / up_per_slice),
        int(plan["down_shares"] / down_per_slice),
    )

    current_up_price = plan["up_price"]
    current_down_price = plan["down_price"]

    log.info(f"  Executing {actual_slices} slices: {up_per_slice} Up + {down_per_slice} Down each")

    for i in range(actual_slices):
        # Re-price periodically (every REPRICE_INTERVAL slices)
        if i > 0 and i % REPRICE_INTERVAL == 0:
            fresh = get_snapshot(api, market)
            new_combined = fresh.up_best_ask + fresh.down_best_ask
            if new_combined < MAX_COMBINED_COST:
                current_up_price = fresh.up_best_ask
                current_down_price = fresh.down_best_ask
                log.info(f"  Re-priced slice {i}: Up={current_up_price:.4f} Down={current_down_price:.4f} "
                         f"Combined={new_combined:.4f}")
            else:
                log.warning(f"  Re-price: combined {new_combined:.4f} >= {MAX_COMBINED_COST}, "
                            f"stopping early at slice {i}")
                break

        # Buy Up
        result = executor.place_limit_buy(market.up_token_id, current_up_price,
                                          up_per_slice, market.tick_size)
        if result:
            position.up_shares += up_per_slice
            position.up_cost += up_per_slice * current_up_price
            position.orders_placed += 1
            if result.get("status") not in ("dry_run", None):
                position.orders_filled += 1

        # Buy Down
        result = executor.place_limit_buy(market.down_token_id, current_down_price,
                                          down_per_slice, market.tick_size)
        if result:
            position.down_shares += down_per_slice
            position.down_cost += down_per_slice * current_down_price
            position.orders_placed += 1
            if result.get("status") not in ("dry_run", None):
                position.orders_filled += 1

        if i < actual_slices - 1:
            time.sleep(SLICE_DELAY_SEC)

        # Bail if market closing
        if market.end_timestamp and time.time() > market.end_timestamp - 15:
            log.warning(f"  Market closing in <15s, stopping at slice {i+1}")
            break

    return position


# ---------------------------------------------------------------------------
# Capital tracker
# ---------------------------------------------------------------------------

class CapitalTracker:
    """Track deployed vs available capital to prevent overallocation."""

    def __init__(self, bankroll: float):
        self.bankroll = bankroll
        self.deployed = 0.0  # capital currently locked in open positions
        self.realized_pnl = 0.0

    @property
    def available(self) -> float:
        return self.bankroll + self.realized_pnl - self.deployed

    def deploy(self, amount: float):
        self.deployed += amount

    def resolve(self, invested: float, payout: float):
        """Called when a market resolves. Frees capital + records PnL."""
        self.deployed -= invested
        self.realized_pnl += (payout - invested)

    def __repr__(self):
        return (f"Capital(bankroll=${self.bankroll:.2f} deployed=${self.deployed:.2f} "
                f"available=${self.available:.2f} pnl=${self.realized_pnl:.2f})")


# ---------------------------------------------------------------------------
# Main bot loop
# ---------------------------------------------------------------------------

def run_bot():
    capital = CapitalTracker(BANKROLL)

    log.info("=" * 60)
    log.info("Polymarket BTC 5-Min Arbitrage Bot")
    log.info(f"  Bankroll:           ${BANKROLL}")
    log.info(f"  Max per window:     ${ORDER_SIZE_USDC}")
    log.info(f"  Max combined cost:  {MAX_COMBINED_COST}")
    log.info(f"  Min profit margin:  {MIN_PROFIT_MARGIN}")
    log.info(f"  Slices:             {NUM_SLICES} (re-price every {REPRICE_INTERVAL})")
    log.info(f"  Entry delay:        {ENTRY_DELAY_SEC}s into window")
    log.info(f"  Directional bias:   {DIRECTIONAL_BIAS}")
    log.info("=" * 60)
    log.info("")
    log.info("SERVER: AWS eu-west-2 (London) behind Cloudflare")
    log.info("  UK is GEOBLOCKED -- use Amsterdam or Zurich VPS")
    log.info("  Run 'python bot.py latency' to test your connection")
    log.info("")

    api = PolymarketAPI()
    strategy = ArbitrageStrategy(MAX_COMBINED_COST, MIN_PROFIT_MARGIN,
                                 DIRECTIONAL_BIAS, FEE_RATE_BPS)
    executor = OrderExecutor(PRIVATE_KEY)
    executor.initialize()

    traded_markets: set[str] = set()
    positions: list[Position] = []
    windows_scanned = 0

    while True:
        try:
            log.info(f"Scanning... {capital}")
            all_markets = discover_markets(api)
            log.info(f"  Found {len(all_markets)} active BTC 5m markets")

            market = pick_next_market(all_markets, traded_markets)
            if not market:
                log.info("  No untraded market available. Waiting 30s...")
                time.sleep(30)
                continue

            log.info(f"  Target: {market.title}")
            log.info(f"  Ends: {market.end_date} | Fee bps: {market.fee_rate_bps}")
            if market.fee_rate_bps:
                strategy.fee_rate_bps = market.fee_rate_bps

            # Check capital
            if capital.available < 10:
                log.warning(f"  Insufficient capital: ${capital.available:.2f} available. Waiting...")
                time.sleep(60)
                continue

            # Wait for optimal entry (~90s into the window, like gabagool22)
            now = time.time()
            if market.start_timestamp:
                target_entry = market.start_timestamp + ENTRY_DELAY_SEC
                wait_time = target_entry - now
                if 0 < wait_time < 300:
                    log.info(f"  Waiting {wait_time:.0f}s for optimal entry timing...")
                    time.sleep(wait_time)

            # Poll for arb opportunity
            windows_scanned += 1
            log.info(f"  Polling order book (window #{windows_scanned})...")
            deadline = market.end_timestamp - 30 if market.end_timestamp else time.time() + 120
            plan = None

            while time.time() < deadline:
                snapshot = get_snapshot(api, market)
                plan = strategy.evaluate(snapshot, available_capital=capital.available)
                if plan:
                    break
                time.sleep(POLL_INTERVAL_SEC)

            traded_markets.add(market.condition_id)

            if not plan:
                log.info("  No arb opportunity this window.")
                continue

            # Execute
            strategy.opportunities_traded += 1
            log.info(f"  *** ARB FOUND ***")
            log.info(f"  Combined: {plan['combined_net']:.4f} | "
                     f"Margin: {plan['margin_net']:.4f} ({plan['margin_net']*100:.2f}%) | "
                     f"VWAP margin: {plan['vwap_margin']:.4f}")
            log.info(f"  Depth: {plan['fillable_depth']:.0f} | "
                     f"Cost: ${plan['total_cost']:.2f} | "
                     f"Expected P&L: ${plan['expected_profit']:.2f}")

            position = execute_arb(executor, api, market, plan)
            positions.append(position)
            capital.deploy(position.total_invested)

            log.info(f"  Result: {position.pairs:.0f} pairs @ {position.combined_avg_cost:.4f}")
            log.info(f"  Invested: ${position.total_invested:.2f} | "
                     f"Expected: ${position.expected_profit:.2f} | "
                     f"Unhedged: {position.excess_shares:.0f}")
            log.info(f"  {capital}")
            log.info(f"  Fill rate: {executor.fill_rate} | API: {api.stats}")

            # Wait for resolution, then free capital
            if market.end_timestamp:
                wait = market.end_timestamp - time.time() + 10
                if wait > 0:
                    log.info(f"  Waiting {wait:.0f}s for resolution...")
                    time.sleep(wait)
            else:
                time.sleep(60)

            # After resolution: one side pays $1, other pays $0
            payout = position.pairs  # $1 per pair guaranteed
            capital.resolve(position.total_invested, payout)
            log.info(f"  Resolved. Payout: ${payout:.2f} | {capital}")

        except KeyboardInterrupt:
            log.info("")
            log.info("=" * 60)
            log.info("Bot stopped. Session summary:")
            log.info(f"  Windows scanned:    {windows_scanned}")
            log.info(f"  Opportunities seen: {strategy.opportunities_seen}")
            log.info(f"  Trades executed:    {len(positions)}")
            log.info(f"  {capital}")
            log.info(f"  Fill rate:          {executor.fill_rate}")
            log.info(f"  API stats:          {api.stats}")
            for p in positions:
                log.info(f"    {p.market_title}: {p.pairs:.0f} pairs "
                         f"@ {p.combined_avg_cost:.4f} -> ${p.expected_profit:.2f}")
            log.info("=" * 60)
            break
        except Exception as e:
            log.error(f"Unexpected error: {e}", exc_info=True)
            time.sleep(10)


# ---------------------------------------------------------------------------
# Trader analysis mode
# ---------------------------------------------------------------------------

def analyze_trader(wallet: str, name: str = "Unknown"):
    api = PolymarketAPI()
    log.info(f"Analyzing: {name} ({wallet[:10]}...)")

    all_trades = []
    offset = 0
    while offset <= 5000:
        batch = api.get_user_activity(wallet, limit=500, offset=offset)
        if not batch:
            break
        btc = [t for t in batch
               if "bitcoin" in t.get("title", "").lower()
               and ("5" in t.get("title", "") or "5m" in t.get("title", "").lower())]
        all_trades.extend(btc)
        offset += 500
        if len(batch) < 500:
            break

    if not all_trades:
        log.info("  No BTC 5-minute trades found.")
        return

    windows: dict[str, list] = defaultdict(list)
    for t in all_trades:
        windows[t.get("conditionId", "?")].append(t)

    log.info(f"  BTC 5m trades: {len(all_trades)}")
    log.info(f"  Windows traded: {len(windows)}")

    combined_costs = []
    total_invested = 0
    total_pairs = 0

    for cid, trades in windows.items():
        title = trades[0].get("title", "Unknown")
        buys_up = [t for t in trades if t.get("outcome", "").lower() == "up" and t.get("side") == "BUY"]
        buys_down = [t for t in trades if t.get("outcome", "").lower() == "down" and t.get("side") == "BUY"]
        sells = [t for t in trades if t.get("side") == "SELL"]

        up_shares = sum(float(t.get("size", 0)) for t in buys_up)
        down_shares = sum(float(t.get("size", 0)) for t in buys_down)
        up_cost = sum(float(t.get("usdcSize", 0)) for t in buys_up)
        down_cost = sum(float(t.get("usdcSize", 0)) for t in buys_down)

        pairs = min(up_shares, down_shares)
        if pairs > 0:
            avg_up = up_cost / up_shares if up_shares else 0
            avg_down = down_cost / down_shares if down_shares else 0
            combined = avg_up + avg_down
            combined_costs.append(combined)
            total_invested += up_cost + down_cost
            total_pairs += pairs
            margin_str = f"+{(1-combined)*100:.2f}%" if combined < 1 else f"-{(combined-1)*100:.2f}%"
            log.info(f"  {title}: {pairs:.0f} pairs @ {combined:.4f} ({margin_str}) sells={len(sells)}")

    if combined_costs:
        avg = sum(combined_costs) / len(combined_costs)
        profitable = sum(1 for c in combined_costs if c < 1.0)
        log.info(f"\n  Avg combined cost: {avg:.4f}")
        log.info(f"  Profitable windows: {profitable}/{len(combined_costs)} "
                 f"({profitable/len(combined_costs)*100:.0f}%)")
        log.info(f"  Total pairs: {total_pairs:.0f}")
        log.info(f"  Total invested: ${total_invested:.2f}")


# ---------------------------------------------------------------------------
# Latency test
# ---------------------------------------------------------------------------

def test_latency():
    log.info("Testing latency to Polymarket CLOB origin (AWS eu-west-2, London)")
    log.info("Note: UK is geoblocked. Use Amsterdam or Zurich VPS.")
    log.info("")

    endpoints = [
        ("CLOB /time", f"{CLOB_BASE}/time"),
        ("Gamma /events", f"{GAMMA_BASE}/events?limit=1"),
        ("Data API", f"{DATA_BASE}/activity?user=0x0000000000000000000000000000000000000000&limit=1"),
    ]

    for name, url in endpoints:
        times = []
        for _ in range(10):
            start = time.perf_counter()
            try:
                requests.get(url, timeout=5)
            except Exception:
                pass
            times.append((time.perf_counter() - start) * 1000)
            time.sleep(0.1)

        avg = sum(times) / len(times)
        p50 = sorted(times)[len(times) // 2]
        mn = min(times)
        log.info(f"  {name}: avg={avg:.1f}ms p50={p50:.1f}ms min={mn:.1f}ms")

    log.info("")
    log.info("Latency guide (to CLOB origin in London):")
    log.info("  <5ms    Amsterdam/Zurich co-lo → competitive")
    log.info("  5-15ms  Nearby EU VPS → viable for wider arbs")
    log.info("  15-50ms US East → will miss tight opportunities")
    log.info("  >50ms   Too slow for this strategy")
    log.info("")
    log.info("Recommended providers:")
    log.info("  QuantVPS Amsterdam    ~$60/mo  (most popular for Polymarket)")
    log.info("  HostHatch Zurich      ~$5-20/mo (community-tested lowest latency)")
    log.info("  AWS eu-west-1 Ireland ~$10-50/mo (self-managed, avoids UK geoblock)")
    log.info("  Vultr Amsterdam       ~$5-24/mo  (budget option)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "run"

    if cmd == "analyze":
        analyze_trader("0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d", "gabagool22")
        print()
        analyze_trader("0xf247584e41117bbbe4cc06e4d2c95741792a5216", "Wee-Playroom")
    elif cmd == "scan":
        api = PolymarketAPI()
        strategy = ArbitrageStrategy(MAX_COMBINED_COST, MIN_PROFIT_MARGIN, DIRECTIONAL_BIAS, FEE_RATE_BPS)
        markets = discover_markets(api)
        log.info(f"Found {len(markets)} BTC 5m markets")
        for m in markets:
            log.info(f"  {m.title} (ends {m.end_date}, fee_bps={m.fee_rate_bps})")
            snapshot = get_snapshot(api, m)
            plan = strategy.evaluate(snapshot)
            if plan:
                log.info(f"  >>> OPPORTUNITY: margin={plan['margin_net']:.4f} "
                         f"vwap={plan['vwap_margin']:.4f} profit=${plan['expected_profit']:.2f}")
    elif cmd == "latency":
        test_latency()
    elif cmd == "run":
        run_bot()
    else:
        print("Usage: python bot.py [run|scan|analyze|latency]")
        print("  run     - Start the trading bot (default)")
        print("  scan    - One-shot scan for current opportunities")
        print("  analyze - Analyze gabagool22 and Wee-Playroom trades")
        print("  latency - Test network latency to Polymarket servers")
