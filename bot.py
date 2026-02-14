"""
Polymarket Crypto Up/Down Arbitrage Bot
========================================

Strategy reverse-engineered from gabagool22 (0x6031b...f96d):
  - Buys BOTH "Up" and "Down" tokens when combined cost < $1.00
  - Uses MAKER orders (0% fee + daily USDC rebates) not taker orders
  - Scales in with 15-30 small orders over ~40 seconds
  - Enters ~2 minutes into each window
  - Holds all positions to expiry (no early exits)
  - Profit = $1.00 - combined_cost per share pair (2-7% per window)

Supports configurable market durations (5m, 15m, 1h) and assets (BTC, ETH, SOL).

INFRASTRUCTURE (verified Feb 2026):
  - CLOB origin server: AWS eu-west-2 (London, UK) behind Cloudflare Anycast
  - UK itself is GEOBLOCKED -- you cannot trade from a London IP
  - Recommended VPS: Amsterdam (5-12ms) or Zurich (community-best)
  - Rate limits: 150/s for /book, 350/s burst for /order
  - Fees: taker pays p*(1-p)*fee_rate_bps/10000; maker pays 0% + earns rebates
  - Price range: 0.01 - 0.99 (API rejects outside)
  - Tick sizes: 0.001 (common), changes dynamically near 0/1
  - Min order: ~5 shares

REALISTIC LIMITATIONS:
  - Arb windows are competitive and close in SECONDS
  - gabagool22 alone does $132M volume -- these are not empty markets
  - Slippage between book read and order fill eats margins
  - Maker orders may not fill; partial fills create directional exposure
  - You compete against co-located bots with <5ms latency
"""

import os
import sys
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from collections import defaultdict, deque

import requests
from dotenv import load_dotenv

load_dotenv()

_log_level = logging.DEBUG if os.getenv("DEBUG", "").lower() in ("1", "true", "yes") else logging.INFO
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log"),
    ],
)
log = logging.getLogger("polyarb")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
CHAIN_ID = 137  # Polygon

# Market selection -- which assets and durations to scan
# Comma-separated, e.g. "btc,eth,sol" and "5,15,60" (minutes)
ENABLED_ASSETS = [a.strip().lower() for a in os.getenv("ENABLED_ASSETS", "btc").split(",")]
MARKET_DURATIONS = [int(d.strip()) for d in os.getenv("MARKET_DURATIONS", "5").split(",")]

# Strategy params
MAX_COMBINED_COST = float(os.getenv("MAX_COMBINED_COST", "0.97"))
MIN_PROFIT_MARGIN = float(os.getenv("MIN_PROFIT_MARGIN", "0.02"))
ORDER_SIZE_USDC = float(os.getenv("ORDER_SIZE_USDC", "50"))
MAX_POSITION_PER_WINDOW = float(os.getenv("MAX_POSITION_PER_WINDOW", "500"))
DIRECTIONAL_BIAS = float(os.getenv("DIRECTIONAL_BIAS", "0.0"))
BANKROLL = float(os.getenv("BANKROLL", "500"))

# Execution params
NUM_SLICES = int(os.getenv("NUM_SLICES", "10"))
SLICE_DELAY_SEC = float(os.getenv("SLICE_DELAY_SEC", "1.5"))
ENTRY_DELAY_SEC = int(os.getenv("ENTRY_DELAY_SEC", "120"))  # gabagool22 enters ~120s in
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "1.0"))
BOOK_POLL_BUDGET = 100  # max /book requests per 10s window
REPRICE_INTERVAL = int(os.getenv("REPRICE_INTERVAL", "3"))

# Order mode: "maker" (0% fee + rebates), "taker" (immediate fill, pays fees)
ORDER_MODE = os.getenv("ORDER_MODE", "maker")
MAKER_OFFSET_TICKS = int(os.getenv("MAKER_OFFSET_TICKS", "1"))  # ticks below ask
MAKER_FILL_TIMEOUT_SEC = float(os.getenv("MAKER_FILL_TIMEOUT_SEC", "5.0"))

# Spending controls
MAX_DAILY_SPEND = float(os.getenv("MAX_DAILY_SPEND", "5000"))
DAILY_LOSS_LIMIT = float(os.getenv("DAILY_LOSS_LIMIT", "50"))

# Fee -- always fetched dynamically per token, this is only the fallback
FEE_RATE_BPS_FALLBACK = int(os.getenv("FEE_RATE_BPS", "0"))


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
    duration_sec: int = 300
    tick_size: float = 0.001
    neg_risk: bool = False
    fee_rate_bps: int = 0
    asset: str = "btc"

    @property
    def end_timestamp(self) -> float:
        try:
            dt = datetime.fromisoformat(self.end_date.replace("Z", "+00:00"))
            return dt.timestamp()
        except (ValueError, AttributeError):
            return 0.0

    @property
    def start_timestamp(self) -> float:
        return self.end_timestamp - self.duration_sec

    @property
    def duration_label(self) -> str:
        m = self.duration_sec // 60
        if m < 60:
            return f"{m}m"
        return f"{m // 60}h"


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
        return sum(level.size for level in self.up_asks)

    @property
    def down_ask_depth(self) -> float:
        return sum(level.size for level in self.down_asks)

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
        """Volume-weighted average price to fill num_shares on ask side."""
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

    def maker_price(self, side: str, tick_size: float, offset_ticks: int = 1) -> float:
        """Price for a maker buy order: best_ask - offset ticks.

        Must be strictly below best_ask to rest on book as maker.
        """
        asks = self.up_asks if side == "up" else self.down_asks
        bids = self.up_bids if side == "up" else self.down_bids
        if not asks:
            return 0.0
        best_ask = asks[0].price
        best_bid = bids[0].price if bids else 0.0
        maker = best_ask - tick_size * offset_ticks
        # Ensure we're at least at best_bid (don't place below the book)
        maker = max(maker, best_bid)
        # Ensure strictly below ask for maker status
        if maker >= best_ask:
            maker = best_ask - tick_size
        return maker if maker >= 0.01 else 0.0


@dataclass
class Position:
    market_title: str
    up_shares: float = 0.0
    down_shares: float = 0.0
    up_cost: float = 0.0
    down_cost: float = 0.0
    orders_placed: int = 0
    orders_filled: int = 0
    maker_fills: int = 0
    taker_fills: int = 0
    unverified_up: float = 0.0  # shares placed but not verified
    unverified_down: float = 0.0

    @property
    def total_invested(self) -> float:
        return self.up_cost + self.down_cost

    @property
    def pairs(self) -> float:
        return min(self.up_shares, self.down_shares)

    @property
    def combined_avg_cost(self) -> float:
        """Average combined cost per pair, computed from per-share averages."""
        if self.pairs == 0:
            return 1.0
        up_avg = self.up_cost / self.up_shares if self.up_shares > 0 else 0
        down_avg = self.down_cost / self.down_shares if self.down_shares > 0 else 0
        return up_avg + down_avg

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
    """Taker fee: p * (1-p) * (fee_rate_bps / 10000). Max ~2.5% at p=0.50 with bps=1000.
    Maker fee is always 0 (makers earn rebates instead)."""
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
    """Round size to 2 decimals (CLOB requirement)."""
    return float(Decimal(str(size)).quantize(Decimal("0.01"), rounding=ROUND_DOWN))


# ---------------------------------------------------------------------------
# Rate limiter
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
        self._fee_cache: dict[str, int] = {}  # token_id -> fee_rate_bps

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
            status = resp.status_code
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

    def get_fee_rate(self, token_id: str) -> int:
        """Fetch live fee_rate_bps from CLOB for a token. Cached per session."""
        if token_id in self._fee_cache:
            return self._fee_cache[token_id]
        result = self._get(f"{CLOB_BASE}/fee-rate", {"token_id": token_id}, self.price_limiter)
        if result and "fee_rate_bps" in result:
            bps = int(result["fee_rate_bps"])
        else:
            bps = FEE_RATE_BPS_FALLBACK
        self._fee_cache[token_id] = bps
        return bps

    def search_crypto_markets(self, assets: list[str], durations: list[int]) -> list[dict]:
        """Search for crypto up/down markets by constructing predictable slugs.

        The Gamma API's generic listing endpoints do not reliably return
        high-frequency recurring markets (BTC 5m, ETH 15m, etc.).
        Instead, we construct the deterministic slug for the current and
        upcoming windows and fetch each directly.

        Slug pattern: {asset}-updown-{duration_tag}-{window_start_unix}
        Duration tags: 5 -> "5m", 15 -> "15m", 60 -> "1h"
        """
        markets = []
        now = time.time()

        for asset in assets:
            for dur_min in durations:
                dur_sec = dur_min * 60
                dur_tag = _DURATION_SLUG_TAGS.get(dur_min, f"{dur_min}m")

                # Round down to current window start, then check current + next few
                current_window = int(now // dur_sec) * dur_sec
                slugs = [f"{asset}-updown-{dur_tag}-{current_window + i * dur_sec}"
                         for i in range(-1, 4)]  # previous, current, +3 upcoming

                for slug in slugs:
                    event_list = self._get(f"{GAMMA_BASE}/events",
                                           {"slug": slug},
                                           self.gamma_limiter, timeout=8)
                    if event_list and isinstance(event_list, list) and len(event_list) > 0:
                        event = event_list[0]
                        if event.get("active") and not event.get("closed"):
                            match = {"asset": asset, "duration_min": dur_min}
                            markets.append({**event, "_match": match})
                            log.debug(f"  [discovery] Found: {event.get('title', slug)}")

        if not markets:
            log.info(f"  [discovery] No markets found via slug lookup "
                     f"(assets={assets}, durations={durations}min). "
                     f"Falling back to generic search...")
            markets = self._search_crypto_markets_generic(assets, durations)

        return markets

    def _search_crypto_markets_generic(self, assets: list[str], durations: list[int]) -> list[dict]:
        """Fallback: scan generic Gamma API listings (may miss recurring markets)."""
        markets = []
        events = self._get(f"{GAMMA_BASE}/events",
                           {"limit": 200, "active": "true", "closed": "false"},
                           self.gamma_limiter, timeout=10)
        if events:
            log.debug(f"  [discovery] Generic /events returned {len(events)} events")
            for event in events:
                title = event.get("title", "")
                ticker = event.get("ticker", "")
                match = classify_market(title, ticker, assets, durations)
                if match:
                    markets.append({**event, "_match": match})

        if markets:
            return markets

        raw = self._get(f"{GAMMA_BASE}/markets",
                        {"limit": 200, "active": "true", "closed": "false"},
                        self.gamma_limiter, timeout=10)
        if raw:
            log.debug(f"  [discovery] Generic /markets returned {len(raw)} markets")
            for m in raw:
                title = m.get("title", "")
                ticker = m.get("groupItemTitle", "")
                match = classify_market(title, ticker, assets, durations)
                if match:
                    markets.append({**m, "_match": match})
        return markets

    def get_market_resolution(self, condition_id: str) -> dict | None:
        """Fetch market resolution data from Gamma API.

        Returns dict with 'resolved', 'winning_outcome', 'payout_up', 'payout_down'
        or None if not yet resolved / fetch failed.
        """
        result = self._get(f"{GAMMA_BASE}/markets",
                           {"condition_id": condition_id},
                           self.gamma_limiter, timeout=8)
        if not result:
            return None
        market_data = result[0] if isinstance(result, list) and result else result
        if not isinstance(market_data, dict):
            return None

        resolved = market_data.get("resolved", False)
        if not resolved:
            return None

        # Outcome prices after resolution: winning side = $1, losing = $0
        outcome_prices = market_data.get("outcomePrices", "")
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except (json.JSONDecodeError, TypeError):
                outcome_prices = []

        outcomes = market_data.get("outcomes", [])
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except (json.JSONDecodeError, TypeError):
                outcomes = []

        payout_up = 0.0
        payout_down = 0.0
        winning = "unknown"
        for i, outcome in enumerate(outcomes):
            price = float(outcome_prices[i]) if i < len(outcome_prices) else 0.0
            if str(outcome).lower() == "up":
                payout_up = price
                if price > 0.5:
                    winning = "up"
            elif str(outcome).lower() == "down":
                payout_down = price
                if price > 0.5:
                    winning = "down"

        return {
            "resolved": True,
            "winning_outcome": winning,
            "payout_up": payout_up,
            "payout_down": payout_down,
        }

    def get_user_activity(self, wallet: str, limit: int = 100, offset: int = 0) -> list[dict]:
        return self._get(f"{DATA_BASE}/activity",
                         {"user": wallet, "limit": limit, "offset": offset},
                         timeout=10) or []

    @property
    def stats(self) -> str:
        return f"requests={self._request_count} errors={self._error_count}"


# ---------------------------------------------------------------------------
# Market classification
# ---------------------------------------------------------------------------

# Slug tags used in Polymarket's deterministic event slugs
# e.g. btc-updown-5m-1771027500
_DURATION_SLUG_TAGS = {5: "5m", 15: "15m", 60: "1h"}

# Duration patterns: maps regex-like patterns to minutes
_DURATION_PATTERNS = {
    5: ["5m", "5-min", "5 min", "five-min", "five min"],
    15: ["15m", "15-min", "15 min", "fifteen-min", "fifteen min"],
    60: ["1h", "1-hour", "1 hour", "one-hour", "one hour", "60m", "60-min", "60 min"],
}

# Asset aliases
_ASSET_ALIASES = {
    "btc": ["btc", "bitcoin"],
    "eth": ["eth", "ethereum", "ether"],
    "sol": ["sol", "solana"],
    "doge": ["doge", "dogecoin"],
    "matic": ["matic", "polygon"],
    "avax": ["avax", "avalanche"],
    "link": ["link", "chainlink"],
}


def _looks_crypto(title: str, ticker: str) -> bool:
    """Quick check if a market title/ticker mentions any known crypto asset."""
    combined = (title + " " + ticker).lower()
    all_aliases = [alias for aliases in _ASSET_ALIASES.values() for alias in aliases]
    return any(alias in combined for alias in all_aliases)


def classify_market(title: str, ticker: str,
                    allowed_assets: list[str],
                    allowed_durations: list[int]) -> dict | None:
    """Classify a market by asset and duration. Returns match info or None.

    Requires BOTH an asset match AND a duration match to avoid false positives
    (e.g. matching a daily BTC market just because it has "up" and "down").
    """
    t = title.lower()
    k = ticker.lower()
    combined = t + " " + k

    # Must be an up/down market
    has_up = "up" in t or "up" in k
    has_down = "down" in t or "down" in k
    if not (has_up and has_down):
        return None

    # Match asset
    matched_asset = None
    for asset in allowed_assets:
        aliases = _ASSET_ALIASES.get(asset, [asset])
        if any(alias in combined for alias in aliases):
            matched_asset = asset
            break
    if not matched_asset:
        return None

    # Match duration -- REQUIRED to prevent matching wrong timeframes
    matched_duration = None
    for dur_min in allowed_durations:
        patterns = _DURATION_PATTERNS.get(dur_min, [f"{dur_min}m"])
        if any(pat in combined for pat in patterns):
            matched_duration = dur_min
            break
    if not matched_duration:
        return None

    return {"asset": matched_asset, "duration_min": matched_duration}


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

    Supports two modes:
    - taker: evaluate using ask prices + taker fees
    - maker: evaluate using maker prices (below ask), 0 fees
    """

    def __init__(self, max_combined: float, min_margin: float,
                 bias: float = 0.0, order_mode: str = "maker"):
        self.max_combined = max_combined
        self.min_margin = min_margin
        self.bias = bias
        self.order_mode = order_mode
        self.opportunities_seen = 0
        self.opportunities_traded = 0

    def evaluate(self, snapshot: OrderBookSnapshot,
                 available_capital: float = ORDER_SIZE_USDC,
                 fee_rate_bps: int = 0,
                 tick_size: float = 0.001) -> dict | None:
        """Evaluate arb opportunity. Returns execution plan or None."""

        up_ask = snapshot.up_best_ask
        down_ask = snapshot.down_best_ask

        if self.order_mode == "maker":
            return self._evaluate_maker(snapshot, available_capital, fee_rate_bps, tick_size)
        else:
            return self._evaluate_taker(snapshot, available_capital, fee_rate_bps, tick_size)

    def _evaluate_taker(self, snapshot: OrderBookSnapshot,
                        available_capital: float,
                        fee_rate_bps: int,
                        tick_size: float) -> dict | None:
        """Taker evaluation: buy at ask, pay fees."""
        up_ask = snapshot.up_best_ask
        down_ask = snapshot.down_best_ask

        up_fee = calculate_fee(up_ask, fee_rate_bps)
        down_fee = calculate_fee(down_ask, fee_rate_bps)
        total_fee = up_fee + down_fee

        combined_raw = up_ask + down_ask
        combined_net = combined_raw + total_fee
        margin_net = 1.0 - combined_net

        log.info(
            f"  [taker] Up={up_ask:.4f} Down={down_ask:.4f} "
            f"Raw={combined_raw:.4f} Fees={total_fee:.4f} "
            f"Net={combined_net:.4f} Margin={margin_net:.4f}"
        )

        if combined_net >= self.max_combined:
            return None
        if margin_net < self.min_margin:
            return None

        self.opportunities_seen += 1

        # Walk the book for realistic depth (within margin tolerance)
        max_fill_price_up = up_ask + margin_net / 2
        max_fill_price_down = down_ask + margin_net / 2
        up_fillable = snapshot.fillable_at_price("up", max_fill_price_up)
        down_fillable = snapshot.fillable_at_price("down", max_fill_price_down)
        max_pairs = min(up_fillable, down_fillable)

        effective_budget = min(available_capital, ORDER_SIZE_USDC, MAX_POSITION_PER_WINDOW)
        budget_pairs = effective_budget / combined_net
        target_pairs = min(max_pairs, budget_pairs)

        if target_pairs < 5:
            log.info(f"  Insufficient depth: {max_pairs:.0f} fillable, need 5+")
            return None

        # VWAP check with correct fee recalculation
        up_vwap = snapshot.vwap("up", target_pairs)
        down_vwap = snapshot.vwap("down", target_pairs)
        vwap_up_fee = calculate_fee(up_vwap, fee_rate_bps)
        vwap_down_fee = calculate_fee(down_vwap, fee_rate_bps)
        vwap_total_fee = vwap_up_fee + vwap_down_fee
        vwap_combined = up_vwap + down_vwap + vwap_total_fee
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

        # Use VWAP prices for realistic cost estimate
        total_cost = (up_shares * (up_vwap + vwap_up_fee)
                      + down_shares * (down_vwap + vwap_down_fee))
        pairs = min(up_shares, down_shares)
        expected_profit = pairs - total_cost

        return {
            "mode": "taker",
            "up_price": up_ask,
            "down_price": down_ask,
            "combined_raw": combined_raw,
            "combined_net": combined_net,
            "margin_raw": 1.0 - combined_raw,
            "margin_net": margin_net,
            "vwap_margin": vwap_margin,
            "fees_per_pair": vwap_total_fee,
            "up_shares": up_shares,
            "down_shares": down_shares,
            "total_cost": total_cost,
            "expected_profit": expected_profit,
            "fillable_depth": max_pairs,
            "fee_rate_bps": fee_rate_bps,
        }

    def _evaluate_maker(self, snapshot: OrderBookSnapshot,
                        available_capital: float,
                        fee_rate_bps: int,
                        tick_size: float) -> dict | None:
        """Maker evaluation: place below ask, 0 fees, earn rebates."""
        up_maker = snapshot.maker_price("up", tick_size, MAKER_OFFSET_TICKS)
        down_maker = snapshot.maker_price("down", tick_size, MAKER_OFFSET_TICKS)

        if up_maker <= 0 or down_maker <= 0:
            return None

        up_maker = round_price(up_maker, tick_size)
        down_maker = round_price(down_maker, tick_size)

        # Maker pays 0 fees
        combined = up_maker + down_maker
        margin = 1.0 - combined

        log.info(
            f"  [maker] Up={up_maker:.4f} Down={down_maker:.4f} "
            f"Combined={combined:.4f} Margin={margin:.4f} (0 fees)"
        )

        if combined >= self.max_combined:
            return None
        if margin < self.min_margin:
            return None

        self.opportunities_seen += 1

        # For maker orders, depth is the ask depth (others may sell into our bids)
        up_ask_depth = snapshot.up_ask_depth
        down_ask_depth = snapshot.down_ask_depth
        max_pairs = min(up_ask_depth, down_ask_depth)

        effective_budget = min(available_capital, ORDER_SIZE_USDC, MAX_POSITION_PER_WINDOW)
        budget_pairs = effective_budget / combined
        target_pairs = min(max_pairs, budget_pairs)

        if target_pairs < 5:
            log.info(f"  Insufficient depth: {max_pairs:.0f} available, need 5+")
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

        total_cost = up_shares * up_maker + down_shares * down_maker
        pairs = min(up_shares, down_shares)
        expected_profit = pairs - total_cost

        # Also compute what taker arb would look like for comparison logging
        taker_combined = snapshot.up_best_ask + snapshot.down_best_ask
        taker_fee = (calculate_fee(snapshot.up_best_ask, fee_rate_bps)
                     + calculate_fee(snapshot.down_best_ask, fee_rate_bps))

        return {
            "mode": "maker",
            "up_price": up_maker,
            "down_price": down_maker,
            "combined_raw": combined,
            "combined_net": combined,  # no fees for maker
            "margin_raw": margin,
            "margin_net": margin,
            "vwap_margin": margin,  # maker orders fill at limit price
            "fees_per_pair": 0.0,
            "up_shares": up_shares,
            "down_shares": down_shares,
            "total_cost": total_cost,
            "expected_profit": expected_profit,
            "fillable_depth": max_pairs,
            "fee_rate_bps": fee_rate_bps,
            "taker_combined_with_fees": taker_combined + taker_fee,
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
        self._maker_count = 0
        self._taker_count = 0
        self._stats_lock = threading.Lock()

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
                        tick_size: float = 0.001, is_maker: bool = False) -> dict | None:
        """Place a limit buy order. Returns order result with orderID if successful."""
        price = round_price(price, tick_size)
        size = round_size(size)
        if size < 5:
            return None

        with self._stats_lock:
            self._order_count += 1
            order_num = self._order_count
        if self.dry_run:
            mode = "MAKER" if is_maker else "TAKER"
            log.info(f"  [DRY-{mode}] BUY {size} @ {price:.4f} (...{token_id[-8:]})")
            return {"status": "dry_run", "price": price, "size": size,
                    "orderID": f"dry_{order_num}"}

        try:
            from py_clob_client.order_builder.constants import BUY
            order = self.client.create_order({
                "token_id": token_id, "price": price, "size": size, "side": BUY,
            })
            result = self.client.post_order(order)
            status = result.get("status", "unknown")
            order_id = result.get("orderID", "")
            log.info(f"  Order #{order_num}: {status} (id={order_id[:12] if order_id else 'none'}...)")
            if status in ("matched", "filled"):
                with self._stats_lock:
                    self._fill_count += 1
                    if is_maker:
                        self._maker_count += 1
                    else:
                        self._taker_count += 1
            elif status == "live":
                log.info(f"  Order {order_id[:12]}... is live (pending fill)")
            else:
                log.warning(f"  Unexpected order status: {result}")
            return result
        except Exception as e:
            log.error(f"  Order failed: {e}")
            return None

    def get_balance(self) -> float | None:
        """Fetch available USDC balance from CLOB. Returns None if unavailable."""
        if self.dry_run or not self.client:
            return None
        try:
            # py-clob-client get_balance_allowance returns
            # {"balance": "...", "allowance": "..."}  (USDC in wei-like units)
            bal_info = self.client.get_balance_allowance()
            if bal_info and "balance" in bal_info:
                # Balance is returned as a string in USDC atomic units (6 decimals)
                raw = float(bal_info["balance"])
                return raw / 1e6
        except Exception as e:
            log.warning(f"  Failed to fetch balance: {e}")
        return None

    def check_order(self, order_id: str) -> dict | None:
        """Check the status of an order. Returns order details or None."""
        if self.dry_run or not order_id or order_id.startswith("dry_"):
            return None
        try:
            return self.client.get_order(order_id)
        except Exception as e:
            log.warning(f"  Failed to check order {order_id[:12]}: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True if cancelled."""
        if self.dry_run or not order_id or order_id.startswith("dry_"):
            return True
        try:
            self.client.cancel(order_id)
            log.info(f"  Cancelled order {order_id[:12]}...")
            return True
        except Exception as e:
            log.warning(f"  Failed to cancel {order_id[:12]}: {e}")
            return False

    def verify_fill(self, order_id: str, timeout: float = MAKER_FILL_TIMEOUT_SEC) -> dict:
        """Wait for fill confirmation. Returns fill info."""
        if self.dry_run or not order_id or order_id.startswith("dry_"):
            return {"filled": True, "size_matched": 0, "status": "dry_run"}

        deadline = time.time() + timeout
        while time.time() < deadline:
            info = self.check_order(order_id)
            if info:
                status = info.get("status", "")
                size_matched = float(info.get("size_matched", 0) or 0)
                if status in ("matched", "filled") or size_matched > 0:
                    return {"filled": True, "size_matched": size_matched, "status": status}
                if status in ("canceled", "cancelled", "expired"):
                    return {"filled": False, "size_matched": 0, "status": status}
            time.sleep(0.5)

        return {"filled": False, "size_matched": 0, "status": "timeout"}

    @property
    def fill_rate(self) -> str:
        if self._order_count == 0:
            return "0/0"
        return (f"{self._fill_count}/{self._order_count} "
                f"({self._fill_count / self._order_count * 100:.0f}%) "
                f"maker={self._maker_count} taker={self._taker_count}")


# ---------------------------------------------------------------------------
# Market discovery
# ---------------------------------------------------------------------------

def parse_market(raw: dict, event: dict | None = None,
                 match_info: dict | None = None) -> Market | None:
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

    duration_min = (match_info or {}).get("duration_min", 5)
    asset = (match_info or {}).get("asset", "btc")

    return Market(
        condition_id=raw.get("conditionId", ""),
        title=raw.get("question", raw.get("title", "Unknown")),
        slug=raw.get("slug", ""),
        up_token_id=tokens[up_idx],
        down_token_id=tokens[down_idx],
        end_date=raw.get("endDate", raw.get("endDateIso", "")),
        event_slug=(event or raw).get("slug", ""),
        duration_sec=duration_min * 60,
        tick_size=float(raw.get("minimumTickSize", 0.001)),
        neg_risk=raw.get("negRisk", False),
        fee_rate_bps=int(raw.get("feeRateBps", 0)),
        asset=asset,
    )


def discover_markets(api: PolymarketAPI, assets: list[str] | None = None,
                     durations: list[int] | None = None) -> list[Market]:
    """Discover active crypto up/down markets for configured assets and durations."""
    assets = assets or ENABLED_ASSETS
    durations = durations or MARKET_DURATIONS
    raw_results = api.search_crypto_markets(assets, durations)
    markets = []
    for item in raw_results:
        match_info = item.get("_match")
        nested = item.get("markets", [])
        if nested:
            for m in nested:
                if not m.get("closed"):
                    parsed = parse_market(m, event=item, match_info=match_info)
                    if parsed:
                        markets.append(parsed)
        else:
            parsed = parse_market(item, match_info=match_info)
            if parsed:
                markets.append(parsed)
    # Deduplicate by condition_id (slug lookups can overlap)
    seen: set[str] = set()
    unique = []
    for m in markets:
        if m.condition_id not in seen:
            seen.add(m.condition_id)
            unique.append(m)
    unique.sort(key=lambda m: m.end_timestamp)
    return unique


def pick_next_market(markets: list[Market], traded: set[str]) -> Market | None:
    now = time.time()
    for m in markets:
        if m.condition_id in traded:
            continue
        if m.end_timestamp and m.end_timestamp < now:
            continue
        # Don't look more than one duration ahead
        if m.start_timestamp and m.start_timestamp > now + m.duration_sec:
            continue
        return m
    return None


# ---------------------------------------------------------------------------
# Dynamic fee fetching
# ---------------------------------------------------------------------------

def fetch_market_fees(api: PolymarketAPI, market: Market) -> int:
    """Fetch the actual fee_rate_bps from the CLOB for a market's tokens.

    Uses the up token; both tokens in a market share the same fee rate.
    Updates the market object in-place.
    """
    bps = api.get_fee_rate(market.up_token_id)
    market.fee_rate_bps = bps
    return bps


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------

def _compute_combined_with_fees(up_price: float, down_price: float,
                                fee_rate_bps: int, is_maker: bool) -> float:
    """Compute combined cost including fees (0 for maker)."""
    if is_maker:
        return up_price + down_price
    up_fee = calculate_fee(up_price, fee_rate_bps)
    down_fee = calculate_fee(down_price, fee_rate_bps)
    return up_price + down_price + up_fee + down_fee


def execute_arb(executor: OrderExecutor, api: PolymarketAPI,
                market: Market, plan: dict) -> Position:
    """
    Scaled entry with periodic re-pricing and fill verification.

    For maker orders: places below ask, verifies fills, cancels unfilled.
    For taker orders: places at ask, assumes fills.
    """
    position = Position(market_title=market.title)
    is_maker = plan["mode"] == "maker"
    fee_rate_bps = plan.get("fee_rate_bps", 0)

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

    log.info(f"  Executing {actual_slices} slices ({plan['mode']}): "
             f"{up_per_slice} Up + {down_per_slice} Down each")

    pending_orders: list[dict] = []  # track for later cancellation

    for i in range(actual_slices):
        # Re-price periodically
        if i > 0 and i % REPRICE_INTERVAL == 0:
            fresh = get_snapshot(api, market)
            if is_maker:
                new_up = fresh.maker_price("up", market.tick_size, MAKER_OFFSET_TICKS)
                new_down = fresh.maker_price("down", market.tick_size, MAKER_OFFSET_TICKS)
                if new_up <= 0 or new_down <= 0:
                    log.warning(f"  Re-price: no valid maker price, stopping at slice {i}")
                    break
                new_up = round_price(new_up, market.tick_size)
                new_down = round_price(new_down, market.tick_size)
            else:
                new_up = fresh.up_best_ask
                new_down = fresh.down_best_ask

            new_combined = _compute_combined_with_fees(new_up, new_down, fee_rate_bps, is_maker)
            if new_combined < MAX_COMBINED_COST:
                current_up_price = new_up
                current_down_price = new_down
                log.info(f"  Re-priced slice {i}: Up={current_up_price:.4f} "
                         f"Down={current_down_price:.4f} Combined={new_combined:.4f}")
            else:
                log.warning(f"  Re-price: combined {new_combined:.4f} >= {MAX_COMBINED_COST}, "
                            f"stopping early at slice {i}")
                break

        # Buy Up + Down concurrently to minimize leg risk
        fut_up = api._pool.submit(
            executor.place_limit_buy, market.up_token_id, current_up_price,
            up_per_slice, market.tick_size, is_maker)
        fut_down = api._pool.submit(
            executor.place_limit_buy, market.down_token_id, current_down_price,
            down_per_slice, market.tick_size, is_maker)

        try:
            up_result = fut_up.result(timeout=10)
        except Exception as e:
            log.error(f"  Up order future failed: {e}")
            up_result = None
        try:
            down_result = fut_down.result(timeout=10)
        except Exception as e:
            log.error(f"  Down order future failed: {e}")
            down_result = None

        # Detect one-legged fill and recover
        if up_result and not down_result:
            up_oid = up_result.get("orderID", "")
            up_status = up_result.get("status", "")
            if is_maker and up_oid and not up_oid.startswith("dry_"):
                executor.cancel_order(up_oid)
                log.warning(f"  Asymmetric fill: Up succeeded, Down failed. "
                            f"Cancelled Up order {up_oid[:12]}...")
            elif up_status in ("matched", "filled"):
                log.warning(f"  Asymmetric taker fill: Up filled but Down failed. "
                            f"Exposed {up_per_slice} Up shares. Stopping slices.")
                position.up_shares += up_per_slice
                position.up_cost += up_per_slice * current_up_price
                position.orders_placed += 1
                position.orders_filled += 1
                position.taker_fills += 1
            break
        if down_result and not up_result:
            down_oid = down_result.get("orderID", "")
            down_status = down_result.get("status", "")
            if is_maker and down_oid and not down_oid.startswith("dry_"):
                executor.cancel_order(down_oid)
                log.warning(f"  Asymmetric fill: Down succeeded, Up failed. "
                            f"Cancelled Down order {down_oid[:12]}...")
            elif down_status in ("matched", "filled"):
                log.warning(f"  Asymmetric taker fill: Down filled but Up failed. "
                            f"Exposed {down_per_slice} Down shares. Stopping slices.")
                position.down_shares += down_per_slice
                position.down_cost += down_per_slice * current_down_price
                position.orders_placed += 1
                position.orders_filled += 1
                position.taker_fills += 1
            break
        if not up_result and not down_result:
            log.warning(f"  Both legs failed on slice {i}, stopping early.")
            break

        # Track orders and update position
        if up_result:
            order_id = up_result.get("orderID", "")
            position.orders_placed += 1
            if is_maker and not executor.dry_run:
                pending_orders.append({"id": order_id, "side": "up",
                                       "size": up_per_slice, "price": current_up_price})
            else:
                # Taker or dry run: assume fill
                position.up_shares += up_per_slice
                position.up_cost += up_per_slice * current_up_price
                if up_result.get("status") not in ("dry_run", None):
                    position.orders_filled += 1
                    position.taker_fills += 1

        if down_result:
            order_id = down_result.get("orderID", "")
            position.orders_placed += 1
            if is_maker and not executor.dry_run:
                pending_orders.append({"id": order_id, "side": "down",
                                       "size": down_per_slice, "price": current_down_price})
            else:
                position.down_shares += down_per_slice
                position.down_cost += down_per_slice * current_down_price
                if down_result.get("status") not in ("dry_run", None):
                    position.orders_filled += 1
                    position.taker_fills += 1

        if i < actual_slices - 1:
            time.sleep(SLICE_DELAY_SEC)

        # Bail if market closing
        if market.end_timestamp and time.time() > market.end_timestamp - 15:
            log.warning(f"  Market closing in <15s, stopping at slice {i + 1}")
            break

    # For maker orders: verify fills and cancel unfilled
    if is_maker and pending_orders and not executor.dry_run:
        log.info(f"  Verifying {len(pending_orders)} maker orders...")
        time.sleep(MAKER_FILL_TIMEOUT_SEC)

        filled_up = 0.0
        filled_down = 0.0
        unfilled_orders: list[dict] = []

        for order in pending_orders:
            fill_info = executor.verify_fill(order["id"], timeout=2.0)
            if fill_info["filled"]:
                size = fill_info.get("size_matched", order["size"])
                if size == 0:
                    size = order["size"]
                if order["side"] == "up":
                    position.up_shares += size
                    position.up_cost += size * order["price"]
                    filled_up += size
                else:
                    position.down_shares += size
                    position.down_cost += size * order["price"]
                    filled_down += size
                position.orders_filled += 1
                position.maker_fills += 1
            else:
                unfilled_orders.append(order)

        # Cancel all unfilled orders
        for order in unfilled_orders:
            executor.cancel_order(order["id"])
            log.info(f"  Unfilled {order['side']} order cancelled "
                     f"({order['size']} @ {order['price']:.4f})")

        # Check for asymmetric fills after maker verification
        excess = abs(filled_up - filled_down)
        if excess > 0 and min(filled_up, filled_down) > 0:
            heavier = "Up" if filled_up > filled_down else "Down"
            log.warning(f"  Asymmetric maker fills: Up={filled_up:.0f} Down={filled_down:.0f} "
                        f"({excess:.0f} unhedged {heavier} shares)")

    # For dry run with maker mode, credit the positions optimistically
    if is_maker and executor.dry_run:
        for order in pending_orders:
            if order["side"] == "up":
                position.up_shares += order["size"]
                position.up_cost += order["size"] * order["price"]
            else:
                position.down_shares += order["size"]
                position.down_cost += order["size"] * order["price"]

    return position


# ---------------------------------------------------------------------------
# Capital tracker with daily limits
# ---------------------------------------------------------------------------

class CapitalTracker:
    """Track deployed vs available capital with daily spending/loss limits."""

    def __init__(self, bankroll: float, daily_spend_limit: float = MAX_DAILY_SPEND,
                 daily_loss_limit: float = DAILY_LOSS_LIMIT):
        self.bankroll = bankroll
        self.deployed = 0.0
        self.realized_pnl = 0.0
        self.daily_spend_limit = daily_spend_limit
        self.daily_loss_limit = daily_loss_limit
        self._daily_spent = 0.0
        self._daily_loss = 0.0
        self._day_start = time.strftime("%Y-%m-%d")

    def _check_day_reset(self):
        today = time.strftime("%Y-%m-%d")
        if today != self._day_start:
            self._daily_spent = 0.0
            self._daily_loss = 0.0
            self._day_start = today

    @property
    def available(self) -> float:
        self._check_day_reset()
        capital_available = self.bankroll + self.realized_pnl - self.deployed
        daily_remaining = self.daily_spend_limit - self._daily_spent
        return min(capital_available, daily_remaining)

    @property
    def daily_loss_exceeded(self) -> bool:
        self._check_day_reset()
        return self._daily_loss >= self.daily_loss_limit

    def deploy(self, amount: float):
        self._check_day_reset()
        self.deployed += amount
        self._daily_spent += amount

    def resolve(self, invested: float, payout: float):
        """Called when a market resolves. Frees capital + records PnL."""
        self.deployed -= invested
        profit = payout - invested
        self.realized_pnl += profit
        if profit < 0:
            self._check_day_reset()
            self._daily_loss += abs(profit)

    def __repr__(self):
        self._check_day_reset()
        return (f"Capital(bankroll=${self.bankroll:.2f} deployed=${self.deployed:.2f} "
                f"available=${self.available:.2f} pnl=${self.realized_pnl:.2f} "
                f"daily_spent=${self._daily_spent:.2f})")


# ---------------------------------------------------------------------------
# Main bot loop
# ---------------------------------------------------------------------------

def run_bot():
    capital = CapitalTracker(BANKROLL)

    assets_str = ",".join(ENABLED_ASSETS)
    durations_str = ",".join(f"{d}m" for d in MARKET_DURATIONS)

    log.info("=" * 60)
    log.info("Polymarket Crypto Up/Down Arbitrage Bot")
    log.info(f"  Assets:             {assets_str}")
    log.info(f"  Durations:          {durations_str}")
    log.info(f"  Order mode:         {ORDER_MODE}")
    log.info(f"  Bankroll:           ${BANKROLL}")
    log.info(f"  Max per window:     ${ORDER_SIZE_USDC}")
    log.info(f"  Daily spend limit:  ${MAX_DAILY_SPEND}")
    log.info(f"  Daily loss limit:   ${DAILY_LOSS_LIMIT}")
    log.info(f"  Max combined cost:  {MAX_COMBINED_COST}")
    log.info(f"  Min profit margin:  {MIN_PROFIT_MARGIN}")
    log.info(f"  Slices:             {NUM_SLICES} (re-price every {REPRICE_INTERVAL})")
    log.info(f"  Entry delay:        {ENTRY_DELAY_SEC}s into window")
    log.info(f"  Directional bias:   {DIRECTIONAL_BIAS}")
    if ORDER_MODE == "maker":
        log.info(f"  Maker offset:       {MAKER_OFFSET_TICKS} tick(s) below ask")
        log.info(f"  Maker fill timeout: {MAKER_FILL_TIMEOUT_SEC}s")
    log.info("=" * 60)
    log.info("")
    log.info("SERVER: AWS eu-west-2 (London) behind Cloudflare")
    log.info("  UK is GEOBLOCKED -- use Amsterdam or Zurich VPS")
    log.info("  Run 'python bot.py latency' to test your connection")
    log.info("")

    api = PolymarketAPI()
    strategy = ArbitrageStrategy(MAX_COMBINED_COST, MIN_PROFIT_MARGIN,
                                 DIRECTIONAL_BIAS, ORDER_MODE)
    executor = OrderExecutor(PRIVATE_KEY)
    executor.initialize()

    traded_markets: set[str] = set()
    positions: list[Position] = []
    windows_scanned = 0
    first_scan = True

    while True:
        try:
            # Check daily loss limit
            if capital.daily_loss_exceeded:
                log.warning(f"  Daily loss limit (${DAILY_LOSS_LIMIT}) reached. Pausing until tomorrow.")
                time.sleep(300)
                continue

            log.info(f"Scanning... {capital}")
            # Force verbose discovery on first scan to aid debugging
            if first_scan:
                _prev_level = log.getEffectiveLevel()
                logging.getLogger().setLevel(logging.DEBUG)
            all_markets = discover_markets(api)
            if first_scan:
                logging.getLogger().setLevel(_prev_level)
                first_scan = False
            log.info(f"  Found {len(all_markets)} active markets "
                     f"({','.join(ENABLED_ASSETS)} / {durations_str})")

            market = pick_next_market(all_markets, traded_markets)
            if not market:
                log.info("  No untraded market available. Waiting 30s...")
                time.sleep(30)
                continue

            log.info(f"  Target: {market.title} [{market.asset.upper()} {market.duration_label}]")

            # Fetch live fee rate from CLOB (not from stale Gamma API data)
            live_fee_bps = fetch_market_fees(api, market)
            log.info(f"  Ends: {market.end_date} | Live fee_bps: {live_fee_bps}")

            # Verify actual USDC balance on exchange
            actual_balance = executor.get_balance()
            if actual_balance is not None:
                if actual_balance < 10:
                    log.warning(f"  Insufficient exchange balance: "
                                f"${actual_balance:.2f} USDC. Waiting...")
                    time.sleep(60)
                    continue
                # Cap available capital to actual balance
                if actual_balance < capital.available:
                    log.info(f"  Exchange balance ${actual_balance:.2f} < "
                             f"tracked ${capital.available:.2f}, using actual")

            # Check capital
            available = min(capital.available, actual_balance) \
                if actual_balance is not None else capital.available
            if available < 10:
                log.warning(f"  Insufficient capital: ${available:.2f} available. Waiting...")
                time.sleep(60)
                continue

            # Wait for optimal entry (~120s into the window, like gabagool22)
            now = time.time()
            if market.start_timestamp:
                # Scale entry delay proportionally to market duration
                # 120s for 5m window, ~240s for 15m, ~480s for 1h
                scaled_delay = ENTRY_DELAY_SEC * (market.duration_sec / 300)
                scaled_delay = min(scaled_delay, market.duration_sec * 0.6)
                target_entry = market.start_timestamp + scaled_delay
                wait_time = target_entry - now
                if 0 < wait_time < market.duration_sec:
                    log.info(f"  Waiting {wait_time:.0f}s for optimal entry timing "
                             f"({scaled_delay:.0f}s into {market.duration_label} window)...")
                    time.sleep(wait_time)

            # Poll for arb opportunity
            windows_scanned += 1
            log.info(f"  Polling order book (window #{windows_scanned})...")
            deadline = market.end_timestamp - 30 if market.end_timestamp else time.time() + 120
            plan = None

            while time.time() < deadline:
                snapshot = get_snapshot(api, market)
                plan = strategy.evaluate(snapshot,
                                         available_capital=available,
                                         fee_rate_bps=live_fee_bps,
                                         tick_size=market.tick_size)
                if plan:
                    break
                time.sleep(POLL_INTERVAL_SEC)

            traded_markets.add(market.condition_id)

            if not plan:
                log.info("  No arb opportunity this window.")
                continue

            # Execute
            strategy.opportunities_traded += 1
            log.info(f"  *** ARB FOUND ({plan['mode'].upper()}) ***")
            log.info(f"  Combined: {plan['combined_net']:.4f} | "
                     f"Margin: {plan['margin_net']:.4f} ({plan['margin_net'] * 100:.2f}%) | "
                     f"VWAP margin: {plan['vwap_margin']:.4f}")
            log.info(f"  Depth: {plan['fillable_depth']:.0f} | "
                     f"Cost: ${plan['total_cost']:.2f} | "
                     f"Expected P&L: ${plan['expected_profit']:.2f}")
            if plan["mode"] == "maker" and "taker_combined_with_fees" in plan:
                log.info(f"  (Taker would be: combined={plan['taker_combined_with_fees']:.4f} "
                         f"with {live_fee_bps}bps fees)")

            position = execute_arb(executor, api, market, plan)
            positions.append(position)
            capital.deploy(position.total_invested)

            log.info(f"  Result: {position.pairs:.0f} pairs @ {position.combined_avg_cost:.4f}")
            log.info(f"  Invested: ${position.total_invested:.2f} | "
                     f"Expected: ${position.expected_profit:.2f} | "
                     f"Unhedged: {position.excess_shares:.0f}")
            log.info(f"  Maker fills: {position.maker_fills} | "
                     f"Taker fills: {position.taker_fills}")
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

            # Query actual resolution -- retry a few times since resolution
            # can lag behind end_timestamp by seconds to minutes
            resolution = None
            for _attempt in range(6):
                resolution = api.get_market_resolution(market.condition_id)
                if resolution and resolution["resolved"]:
                    break
                log.info(f"  Waiting for resolution data (attempt {_attempt + 1}/6)...")
                time.sleep(10)
            if resolution and resolution["resolved"]:
                payout_up = resolution["payout_up"]
                payout_down = resolution["payout_down"]
                payout = (position.up_shares * payout_up
                          + position.down_shares * payout_down)
                log.info(f"  Resolution: {resolution['winning_outcome'].upper()} won | "
                         f"payout_up={payout_up} payout_down={payout_down}")
            else:
                # Fallback: assume paired shares pay $1 (one side wins)
                payout = position.pairs
                log.warning(f"  Resolution data unavailable, estimating payout from pairs")
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
                         f"@ {p.combined_avg_cost:.4f} -> ${p.expected_profit:.2f} "
                         f"(maker={p.maker_fills} taker={p.taker_fills})")
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
        crypto = [t for t in batch
                  if any(asset in t.get("title", "").lower()
                         for asset in ["bitcoin", "btc", "ethereum", "eth", "solana", "sol"])
                  and any(dur in t.get("title", "").lower()
                          for dur in ["5 min", "5-min", "5m", "15 min", "15-min", "15m",
                                      "1 hour", "1-hour", "1h"])]
        all_trades.extend(crypto)
        offset += 500
        if len(batch) < 500:
            break

    if not all_trades:
        log.info("  No crypto up/down trades found.")
        return

    windows: dict[str, list] = defaultdict(list)
    for t in all_trades:
        windows[t.get("conditionId", "?")].append(t)

    log.info(f"  Crypto up/down trades: {len(all_trades)}")
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
            margin_str = f"+{(1 - combined) * 100:.2f}%" if combined < 1 else f"-{(combined - 1) * 100:.2f}%"
            log.info(f"  {title}: {pairs:.0f} pairs @ {combined:.4f} ({margin_str}) sells={len(sells)}")

    if combined_costs:
        avg = sum(combined_costs) / len(combined_costs)
        profitable = sum(1 for c in combined_costs if c < 1.0)
        log.info(f"\n  Avg combined cost: {avg:.4f}")
        log.info(f"  Profitable windows: {profitable}/{len(combined_costs)} "
                 f"({profitable / len(combined_costs) * 100:.0f}%)")
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
        ("CLOB /fee-rate", f"{CLOB_BASE}/fee-rate"),
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
    log.info("  <5ms    Amsterdam/Zurich co-lo -> competitive")
    log.info("  5-15ms  Nearby EU VPS -> viable for wider arbs")
    log.info("  15-50ms US East -> will miss tight opportunities")
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
        strategy = ArbitrageStrategy(MAX_COMBINED_COST, MIN_PROFIT_MARGIN,
                                     DIRECTIONAL_BIAS, ORDER_MODE)
        markets = discover_markets(api)
        log.info(f"Found {len(markets)} markets "
                 f"({','.join(ENABLED_ASSETS)} / "
                 f"{','.join(str(d) + 'm' for d in MARKET_DURATIONS)})")
        for m in markets:
            log.info(f"  {m.title} [{m.asset.upper()} {m.duration_label}] "
                     f"(ends {m.end_date}, fee_bps={m.fee_rate_bps})")
            # Fetch live fees
            live_bps = fetch_market_fees(api, m)
            snapshot = get_snapshot(api, m)
            plan = strategy.evaluate(snapshot, fee_rate_bps=live_bps,
                                     tick_size=m.tick_size)
            if plan:
                log.info(f"  >>> OPPORTUNITY ({plan['mode']}): "
                         f"margin={plan['margin_net']:.4f} "
                         f"vwap={plan['vwap_margin']:.4f} "
                         f"profit=${plan['expected_profit']:.2f}")
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
