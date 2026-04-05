import asyncio
import contextlib
import email.utils
import heapq
import html
import logging
import os
import random
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.error import RetryAfter
from telegram.ext import Application

from vip_database import VipRecord, get_vip_database, reload_vip_database

LOGGER = logging.getLogger(__name__)

# ─── Tuning constants ────────────────────────────────────────────────────────
MAIN_CHAT_ID = '-1003736596502'
TELEGRAM_TOPICS_MAP = {'.com': 5, '.ai': 6, '.dev': 23}
PRIORITY_TLDS = frozenset({".com", ".ai", ".dev"})

MIN_POLL_SECONDS = 1
MIN_RETRY_ATTEMPTS = 1
MIN_RETRY_BASE_SECONDS = 0.2
MIN_BACKOFF_SECONDS = 1.0
MIN_QUOTA_COOLDOWN_SECONDS = 30
MIN_CIRCUIT_BREAKER_SECONDS = 30
DEFAULT_FALLBACK_ASK_PRICE_USD = 10.0
WATCHER_ERROR_RETRY_SECONDS = 5
TARGET_TLDS = {".com", ".ai", ".dev"}
AVAILABLE_BATCH_SIZE = 20
available_domains_batch: list[str] = []
available_domains_batch_lock: asyncio.Lock | None = None

# Spaceship-specific throttle / batch controls
# ─ 2 s intra-batch delay as required; keep default 429-backoff seed here too
SPACESHIP_INTRA_BATCH_DELAY_SECONDS = 2
SPACESHIP_BULK_BATCH_SIZE = 20          # Spaceship /domains/available max batch size


class SpaceshipCircuitOpenError(Exception):
    """Raised when Spaceship API calls are temporarily blocked by circuit breaker."""


@dataclass(frozen=True)
class DomainOpportunity:
    domain: str
    ask_price_usd: float
    source: str
    listing_url: str
    currency: str = "USD"
    availability_status: str = "Available"

    @property
    def tld(self) -> str:
        _, _, ext = self.domain.rpartition(".")
        return f".{ext.lower()}" if ext else ""

    @property
    def sld(self) -> str:
        return self.domain.split(".", 1)[0].lower()

    @property
    def whois_url(self) -> str:
        return f"https://www.whois.com/whois/{self.domain}"


@dataclass(frozen=True)
class ValuationResult:
    estimated_value_usd: float
    method: str
    reason: str
    margin_usd: float
    margin_ratio: float
    is_high_margin: bool
    brandability_score: Optional[float] = None
    root_word_analysis: Optional[Any] = None


@dataclass
class WatcherConfig:
    poll_seconds: int = 30
    eco_poll_seconds: int = 120
    turbo_poll_seconds: int = 8
    turbo_hours_utc: tuple[tuple[int, int], ...] = ((18, 21),)
    request_timeout_seconds: int = 20
    db_path: str = "alerts.db"
    max_domains_per_cycle: int = 200
    max_retry_attempts: int = 4
    retry_base_seconds: float = 1.2
    max_backoff_seconds: float = 45.0
    quota_cooldown_seconds: int = 180
    circuit_breaker_failure_threshold: int = 4
    circuit_breaker_open_seconds: int = 120
    min_margin_usd: float = 20.0
    min_margin_ratio: float = 1.8
    allowed_tlds: set[str] = field(default_factory=lambda: {".com", ".ai", ".dev"})
    high_value_keywords: set[str] = field(
        default_factory=lambda: {
            "ai",
            "crypto",
            "cloud",
            "data",
            "dev",
            "app",
            "bot",
            "pay",
            "trade",
            "labs",
        }
    )
    negative_keywords: set[str] = field(
        default_factory=lambda: {
            "gibberish",
            "unbrandable",
            "unpronounceable",
            "nonsense",
            "meaningless",
            "awkward",
            "spammy",
        }
    )
    min_brandability_score: float = 35.0
    keyword_value_usd: float = 22.0
    # ── Spaceship API credentials ──────────────────────────────────────────────
    # Authentication: Spaceship uses a two-part Key + Secret scheme.
    # Both are passed as individual HTTP headers on every request:
    #   X-Api-Key:    <spaceship_api_key>
    #   X-Api-Secret: <spaceship_api_secret>
    spaceship_api_base_url: str = "https://spaceship.dev/api/v1"
    spaceship_api_key: str = ""
    spaceship_api_secret: str = ""
    proxy_url: str = ""
    human_delay_min_seconds: float = 0.8
    human_delay_max_seconds: float = 2.5
    vip_reload_seconds: int = 3600
    scan_concurrency: int = 50
    general_find_max_length: int = 5
    general_find_tlds: set[str] = field(default_factory=lambda: {".com", ".ai", ".dev"})

    @classmethod
    def from_env(cls) -> "WatcherConfig":
        raw_tlds = os.getenv("ALLOWED_TLDS", ".com,.ai,.dev")
        allowed_tlds = {
            t.strip().lower() if t.strip().startswith(".") else f".{t.strip().lower()}"
            for t in raw_tlds.split(",")
            if t.strip()
        }
        human_delay_min = float(os.getenv("HUMAN_DELAY_MIN_SECONDS", "0.8"))
        human_delay_max = float(os.getenv("HUMAN_DELAY_MAX_SECONDS", "2.5"))
        delay_min = min(human_delay_min, human_delay_max)
        delay_max = max(human_delay_min, human_delay_max)

        raw_high_value_keywords = os.getenv(
            "HIGH_VALUE_KEYWORDS",
            "ai,crypto,cloud,data,dev,app,bot,pay,trade,labs",
        )
        high_value_keywords = {kw.strip().lower() for kw in raw_high_value_keywords.split(",") if kw.strip()}
        raw_negative_keywords = os.getenv(
            "NEGATIVE_KEYWORDS",
            "gibberish,unbrandable,unpronounceable,nonsense,meaningless,awkward,spammy",
        )
        negative_keywords = {kw.strip().lower() for kw in raw_negative_keywords.split(",") if kw.strip()}
        raw_general_find_tlds = os.getenv("GENERAL_FIND_TLDS", ".com,.ai,.dev")
        general_find_tlds = {
            t.strip().lower() if t.strip().startswith(".") else f".{t.strip().lower()}"
            for t in raw_general_find_tlds.split(",")
            if t.strip()
        }

        return cls(
            poll_seconds=int(os.getenv("WATCHER_POLL_SECONDS", "30")),
            eco_poll_seconds=int(os.getenv("ECO_POLL_SECONDS", "120")),
            turbo_poll_seconds=int(os.getenv("TURBO_POLL_SECONDS", "8")),
            turbo_hours_utc=parse_turbo_hours(os.getenv("TURBO_HOURS_UTC", "18-21")),
            request_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
            db_path=os.getenv("ALERT_DB_PATH", "alerts.db"),
            max_domains_per_cycle=int(os.getenv("MAX_DOMAINS_PER_CYCLE", "200")),
            max_retry_attempts=max(MIN_RETRY_ATTEMPTS, int(os.getenv("MAX_RETRY_ATTEMPTS", "4"))),
            retry_base_seconds=max(MIN_RETRY_BASE_SECONDS, float(os.getenv("RETRY_BASE_SECONDS", "1.2"))),
            max_backoff_seconds=max(MIN_BACKOFF_SECONDS, float(os.getenv("MAX_BACKOFF_SECONDS", "45"))),
            quota_cooldown_seconds=max(MIN_QUOTA_COOLDOWN_SECONDS, int(os.getenv("QUOTA_COOLDOWN_SECONDS", "180"))),
            circuit_breaker_failure_threshold=max(2, int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "4"))),
            circuit_breaker_open_seconds=max(
                MIN_CIRCUIT_BREAKER_SECONDS,
                int(os.getenv("CIRCUIT_BREAKER_OPEN_SECONDS", "120")),
            ),
            min_margin_usd=float(os.getenv("ARBITRAGE_MIN_GAP_USD", "20")),
            min_margin_ratio=float(os.getenv("ARBITRAGE_MIN_RATIO", "1.8")),
            allowed_tlds=allowed_tlds or {".com", ".ai", ".dev"},
            high_value_keywords=high_value_keywords
            or {"ai", "crypto", "cloud", "data", "dev", "app", "bot", "pay", "trade", "labs"},
            negative_keywords=negative_keywords
            or {"gibberish", "unbrandable", "unpronounceable", "nonsense", "meaningless", "awkward", "spammy"},
            min_brandability_score=float(os.getenv("MIN_BRANDABILITY_SCORE", "35")),
            keyword_value_usd=float(os.getenv("KEYWORD_VALUE_USD", "22")),
            spaceship_api_base_url=os.getenv("SPACESHIP_API_BASE_URL", "https://spaceship.dev/api/v1").strip() or "https://spaceship.dev/api/v1",
            spaceship_api_key=os.getenv("SPACESHIP_API_KEY", "").strip(),
            spaceship_api_secret=os.getenv("SPACESHIP_API_SECRET", "").strip(),
            proxy_url=os.getenv("PROXY_URL", "").strip(),
            human_delay_min_seconds=delay_min,
            human_delay_max_seconds=delay_max,
            vip_reload_seconds=max(60, int(os.getenv("VIP_RELOAD_SECONDS", "3600"))),
            scan_concurrency=max(1, int(os.getenv("SCAN_CONCURRENCY", "50"))),
            general_find_max_length=max(1, int(os.getenv("GENERAL_FIND_MAX_LENGTH", "5"))),
            general_find_tlds=general_find_tlds or {".com", ".ai", ".dev"},
        )


def parse_turbo_hours(raw_value: str) -> tuple[tuple[int, int], ...]:
    ranges: list[tuple[int, int]] = []
    for part in raw_value.split(","):
        range_text = part.strip()
        if not range_text:
            continue
        if "-" not in range_text:
            LOGGER.warning("Ignoring invalid TURBO_HOURS_UTC range (missing '-'): %s", range_text)
            continue
        start_s, end_s = range_text.split("-", 1)
        try:
            start = int(start_s)
            end = int(end_s)
        except ValueError:
            LOGGER.warning("Ignoring invalid TURBO_HOURS_UTC range (non-integer): %s", range_text)
            continue
        if 0 <= start <= 23 and 0 <= end <= 23:
            if start == end:
                end = (start + 1) % 24
            ranges.append((start, end))
        else:
            LOGGER.warning("Ignoring invalid TURBO_HOURS_UTC range (out of bounds): %s", range_text)
    return tuple(ranges) if ranges else ((18, 21),)


def is_turbo_hour(now_utc: datetime, cfg: WatcherConfig) -> bool:
    hour = now_utc.hour
    for start, end in cfg.turbo_hours_utc:
        if start < end and start <= hour < end:
            return True
        if start > end and (hour >= start or hour < end):
            return True
    return False


def current_poll_seconds(now_utc: datetime, cfg: WatcherConfig) -> int:
    if is_turbo_hour(now_utc, cfg):
        return max(MIN_POLL_SECONDS, cfg.turbo_poll_seconds)
    if cfg.eco_poll_seconds > 0:
        return cfg.eco_poll_seconds
    return cfg.poll_seconds


class AlertStore:
    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sent_alerts (
                chat_id INTEGER NOT NULL,
                domain TEXT NOT NULL,
                source TEXT NOT NULL,
                first_seen_utc TEXT NOT NULL,
                PRIMARY KEY (chat_id, domain)
            )
            """
        )
        self.conn.commit()

    def has_alerted(self, chat_id: int, domain: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sent_alerts WHERE chat_id = ? AND domain = ?",
            (chat_id, domain.lower()),
        ).fetchone()
        return row is not None

    def mark_alerted(self, chat_id: int, domain: str, source: str) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO sent_alerts(chat_id, domain, source, first_seen_utc)
            VALUES (?, ?, ?, ?)
            """,
            (chat_id, domain.lower(), source, datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def validate_required_spaceship_config(cfg: WatcherConfig) -> None:
    """Raise ValueError if mandatory Spaceship API credentials are absent."""
    missing: list[str] = []
    if not cfg.spaceship_api_key:
        missing.append("SPACESHIP_API_KEY")
    if not cfg.spaceship_api_secret:
        missing.append("SPACESHIP_API_SECRET")
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"Missing required Spaceship configuration: {missing_csv}")


def parse_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if re.search(r"[-−]\s*[0-9]", text):
        return None
    match = re.search(r"[0-9]+(?:\.[0-9]+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        parsed = float(match.group(0))
        if parsed < 0:
            return None
        return parsed
    except ValueError:
        return None


def _normalize_price(raw_price: Any) -> Optional[float]:
    """
    Parse and normalise an arbitrary price value to a non-negative USD float.

    Returns None only when the value is unparseable or negative.
    Zero is a valid price (free domain promotions) and is preserved as 0.0.
    """
    parsed = parse_float(raw_price)
    if parsed is None:
        return None
    return round(parsed, 2) if parsed >= 0 else None


def score_with_internal_rules(domain: str, cfg: WatcherConfig) -> tuple[float, float, str]:
    """Return (estimated_value_usd, brandability_score, reason_text) using local heuristics."""
    sld = domain.split(".", 1)[0].lower()
    tld = domain.rpartition(".")[2].lower()
    tld_pref = f".{tld}" if tld else ""

    length = len(sld)
    if length <= 4:
        length_score = 120.0
        brandability = 90.0
    elif length <= 6:
        length_score = 90.0
        brandability = 80.0
    elif length <= 8:
        length_score = 65.0
        brandability = 68.0
    elif length <= 12:
        length_score = 40.0
        brandability = 54.0
    else:
        length_score = 18.0
        brandability = 38.0

    tld_score = {
        ".ai": 65.0,
        ".tech": 62.0,
        ".my": 61.0,
        ".com": 64.0,
        ".app": 58.0,
        ".dev": 60.0,
    }.get(tld_pref, 20.0)

    matched_keywords = [kw for kw in cfg.high_value_keywords if kw in sld]
    keyword_score = min(3, len(matched_keywords)) * cfg.keyword_value_usd

    penalty = 0.0
    if "-" in sld:
        penalty += 14.0
        brandability -= 8.0
    if any(ch.isdigit() for ch in sld):
        penalty += 10.0
        brandability -= 6.0

    estimated = max(5.0, length_score + tld_score + keyword_score - penalty)
    brandability = max(0.0, min(100.0, brandability))
    reason = (
        f"Rule-based score: length={length_score:.1f}, tld={tld_score:.1f}, "
        f"keywords={keyword_score:.1f}, penalty={penalty:.1f}"
    )
    return round(estimated, 2), round(brandability, 2), reason


def priority_score(domain: str, cfg: WatcherConfig, vip_db: dict[str, VipRecord]) -> float:
    sld = domain.split(".", 1)[0].lower()
    tld = f".{domain.rpartition('.')[-1].lower()}" if "." in domain else ""
    score = 0.0

    if sld in vip_db:
        score += 5000.0
    if sld in cfg.high_value_keywords:
        score += 1000.0
    score += sum(250.0 for kw in cfg.high_value_keywords if kw in sld)

    length = len(sld)
    if length <= 3:
        score += 500.0
    elif length <= 5:
        score += 350.0
    elif length <= 7:
        score += 220.0
    elif length <= 10:
        score += 120.0
    else:
        score += 40.0

    if tld in cfg.allowed_tlds:
        score += 80.0
    if "-" in sld:
        score -= 35.0
    if any(ch.isdigit() for ch in sld):
        score -= 20.0

    return score


class SpaceshipClient:
    """
    Async HTTP client for the Spaceship Domain Availability API.

    Authentication (two-part Key + Secret):
    ─────────────────────────────────────────
    Every request carries two custom headers:
        X-Api-Key:    <SPACESHIP_API_KEY>
        X-Api-Secret: <SPACESHIP_API_SECRET>

    Bulk availability endpoint:
        POST  {base_url}/domains/available
        Body: {"domains": ["example.com", ...]}   (max 20 per call)

    HTTP 429 handling (mandatory, non-crashing):
        1.  Read the `Retry-After` response header (seconds or HTTP-date).
        2.  If the header is absent, apply exponential back-off.
        3.  Sleep, then automatically retry the failed batch.
    """

    def __init__(self, session: aiohttp.ClientSession, cfg: WatcherConfig) -> None:
        self.session = session
        self.cfg = cfg
        self._base_url = cfg.spaceship_api_base_url.rstrip("/")
        self._quota_backoff_until_monotonic = 0.0
        self._circuit_failures = 0
        self._circuit_open_until_monotonic = 0.0

    # ── Auth & helpers ────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        """
        Build Spaceship authentication headers.

        Spaceship uses a two-part header scheme:
            X-Api-Key:    your Spaceship API key
            X-Api-Secret: your Spaceship API secret
        Both values are available in your Spaceship account dashboard.
        """
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Api-Key": self.cfg.spaceship_api_key,
            "X-Api-Secret": self.cfg.spaceship_api_secret,
        }

    async def _humanized_delay(self) -> None:
        await asyncio.sleep(
            random.uniform(
                self.cfg.human_delay_min_seconds,
                self.cfg.human_delay_max_seconds,
            )
        )

    # ── Circuit-breaker & quota helpers ──────────────────────────────────────

    def _note_rate_limit(self) -> None:
        loop = asyncio.get_running_loop()
        self._quota_backoff_until_monotonic = max(
            self._quota_backoff_until_monotonic,
            loop.time() + self.cfg.quota_cooldown_seconds,
        )

    def _note_retryable_failure(self) -> None:
        loop = asyncio.get_running_loop()
        self._circuit_failures += 1
        if self._circuit_failures >= self.cfg.circuit_breaker_failure_threshold:
            self._circuit_open_until_monotonic = max(
                self._circuit_open_until_monotonic,
                loop.time() + self.cfg.circuit_breaker_open_seconds,
            )
            self._circuit_failures = 0

    def _note_success(self) -> None:
        self._circuit_failures = 0

    def circuit_open_remaining_seconds(self) -> int:
        loop = asyncio.get_running_loop()
        remaining = self._circuit_open_until_monotonic - loop.time()
        return max(0, int(remaining))

    def quota_backoff_remaining_seconds(self) -> int:
        loop = asyncio.get_running_loop()
        remaining = self._quota_backoff_until_monotonic - loop.time()
        return max(0, int(remaining))

    def _backoff_seconds(self, attempt: int) -> float:
        """Exponential back-off with full jitter, capped at max_backoff_seconds."""
        capped_exponential = min(
            self.cfg.max_backoff_seconds,
            self.cfg.retry_base_seconds * (2 ** (attempt - 1)),
        )
        return random.uniform(0.0, max(capped_exponential, MIN_RETRY_BASE_SECONDS))

    # ── Core request dispatcher ───────────────────────────────────────────────

    async def _request_json_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        json: Optional[Any] = None,
        context_label: str,
    ) -> Optional[Any]:
        """
        Send an HTTP request, retrying on transient errors and rate-limits.

        HTTP 429 – Too Many Requests (mandatory, non-crashing):
        ─────────────────────────────────────────────────────────
        1.  Prefer the `Retry-After` header value (seconds) when present.
        2.  Fall back to exponential back-off when the header is absent.
        3.  Bot NEVER crashes; the failed batch is retried automatically.
        """
        if not url:
            return None

        circuit_wait = self.circuit_open_remaining_seconds()
        if circuit_wait > 0:
            raise SpaceshipCircuitOpenError(
                f"{context_label} blocked by circuit breaker for {circuit_wait}s"
            )

        last_error: Optional[str] = None
        for attempt in range(1, self.cfg.max_retry_attempts + 1):
            await self._humanized_delay()
            try:
                LOGGER.debug(">> Contacting Spaceship API: %s %s", method, url)
                async with self.session.request(
                    method,
                    url,
                    headers=self._headers(),
                    params=params,
                    json=json,
                    proxy=self.cfg.proxy_url or None,
                ) as response:
                    body = await response.text()

                    if response.status == 429:
                        # ── Mandatory 429 handling ──────────────────────────
                        # RFC 7231 allows Retry-After to be either:
                        #   • An integer: number of seconds to wait
                        #   • An HTTP-date string: e.g. "Sat, 05 Apr 2025 12:00:00 GMT"
                        # We handle both formats; fall back to exponential
                        # back-off when the header is absent or unparseable.
                        self._note_rate_limit()
                        self._note_retryable_failure()
                        retry_after_raw = response.headers.get("Retry-After")
                        wait_seconds: float = self._backoff_seconds(attempt)
                        if retry_after_raw:
                            try:
                                # Try seconds (integer/float) first
                                wait_seconds = float(retry_after_raw)
                            except ValueError:
                                # Fall back to RFC 7231 HTTP-date parsing
                                try:
                                    parsed_date = email.utils.parsedate_to_datetime(retry_after_raw)
                                    delta = (parsed_date - datetime.now(timezone.utc)).total_seconds()
                                    wait_seconds = max(0.0, delta)
                                except Exception:
                                    wait_seconds = self._backoff_seconds(attempt)
                        LOGGER.warning(
                            "%s rate-limited (429) on %s; pausing %.2fs before retry (attempt %s/%s)",
                            context_label,
                            url,
                            wait_seconds,
                            attempt,
                            self.cfg.max_retry_attempts,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    if 500 <= response.status < 600:
                        wait_seconds = self._backoff_seconds(attempt)
                        self._note_retryable_failure()
                        LOGGER.warning(
                            "%s upstream status=%s; retrying in %.2fs (attempt %s/%s)",
                            context_label,
                            response.status,
                            wait_seconds,
                            attempt,
                            self.cfg.max_retry_attempts,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    if response.status not in (200, 201):
                        raise RuntimeError(
                            f"{context_label} failed status={response.status} body={body[:300]}"
                        )
                        # Note: Spaceship returns 200 for availability checks.
                        # 201 (Created) is accepted defensively for any future
                        # Spaceship endpoint variants that follow REST conventions.

                    try:
                        payload = await response.json(content_type=None)
                        self._note_success()
                        return payload
                    except Exception as exc:
                        raise RuntimeError(
                            f"{context_label} returned invalid JSON: {exc}"
                        ) from exc

            except aiohttp.ClientError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                wait_seconds = self._backoff_seconds(attempt)
                self._note_retryable_failure()
                LOGGER.info(
                    "%s network error: %s; retrying in %.2fs (attempt %s/%s)",
                    context_label,
                    exc,
                    wait_seconds,
                    attempt,
                    self.cfg.max_retry_attempts,
                )
                await asyncio.sleep(wait_seconds)

        if last_error:
            raise RuntimeError(f"{context_label} failed after retries: {last_error}")
        raise RuntimeError(f"{context_label} failed after retries")

    # ── Domain availability ───────────────────────────────────────────────────

    async def check_domain_availability(self, domain: str) -> Optional["DomainOpportunity"]:
        """
        Check a single domain's availability via the Spaceship API.

        Endpoint: POST {base_url}/domains/available
        Body:     {"domains": ["<domain>"]}
        """
        url = f"{self._base_url}/domains/available"
        payload = await self._request_json_with_retry(
            "POST",
            url,
            json={"domains": [domain]},
            context_label="Spaceship Domain Availability",
        )
        results = _extract_results_list(payload)
        if not results:
            return None
        item = results[0] if isinstance(results[0], dict) else None
        if item is None:
            return None
        return _parse_domain_item(item, domain)

    async def check_domains_availability_bulk(
        self, domains: list[str]
    ) -> tuple[list["DomainOpportunity"], int]:
        """
        Bulk-check up to SPACESHIP_BULK_BATCH_SIZE domains in a single POST.

        Endpoint: POST {base_url}/domains/available
        Body:     {"domains": ["d1.com", "d2.ai", ...]}

        Returns (opportunities, failed_count).
        """
        if not domains:
            return [], 0

        url = f"{self._base_url}/domains/available"
        payload = await self._request_json_with_retry(
            "POST",
            url,
            json={"domains": domains},
            context_label="Spaceship Domain Availability Bulk",
        )

        results = _extract_results_list(payload)
        if results is None:
            raise RuntimeError("Spaceship bulk availability returned unexpected payload format")

        opportunities: list[DomainOpportunity] = []
        failed_count = 0
        normalized_input = {d.strip().lower() for d in domains if isinstance(d, str) and d.strip()}
        seen_domains: set[str] = set()
        status_by_domain: dict[str, str] = {}

        for item in results:
            if not isinstance(item, dict):
                continue
            normalized_domain = _parse_item_domain(item)
            if normalized_domain:
                seen_domains.add(normalized_domain)
            is_available, status_text = _domain_status_from_item(item)
            if normalized_domain:
                status_by_domain[normalized_domain] = status_text
            if not is_available:
                if status_text.strip().lower() in {"unavailable", "tldnotsupported"}:
                    LOGGER.info("Checked %s - Status: %s", normalized_domain or "N/A", status_text)
                continue

            if not normalized_domain or "." not in normalized_domain:
                failed_count += 1
                continue

            opp = _parse_domain_item(item, normalized_domain)
            if opp is not None:
                status_by_domain[normalized_domain] = opp.availability_status or status_text
                opportunities.append(opp)

        if normalized_input:
            missing = normalized_input.difference(seen_domains)
            failed_count += len(missing)
            for missing_domain in missing:
                status_by_domain[missing_domain] = "No API Result"

        if normalized_input:
            for checked_domain in sorted(normalized_input):
                if _is_priority_tld_domain(checked_domain):
                    LOGGER.info(
                        "[INFO] Checked %s - Status: %s",
                        checked_domain,
                        status_by_domain.get(checked_domain, "Unavailable"),
                    )

        return opportunities, failed_count


def _extract_results_list(payload: Any) -> Optional[list]:
    """
    Normalise a Spaceship API response to a plain list of domain-result dicts.

    Spaceship may return either:
      - A top-level JSON array:  [{"domain": ..., "available": ...}, ...]
      - A wrapped object with any of these keys:
          "results"  – primary documented key
          "domains"  – alternate documented key
          "data"     – common REST envelope pattern
          "items"    – common pagination envelope pattern
    Returns None if no recognisable list structure is found.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("results", "domains", "data", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return None


def _parse_domain_item(item: dict, fallback_domain: str) -> Optional["DomainOpportunity"]:
    """
    Convert a single Spaceship domain-check result dict into a DomainOpportunity.

    Price extraction order (first non-None/non-negative value wins):
      1. Flat scalar:              "price": 12.99
      2. Nested "price" dict keys (in order): "listPrice", "yourPrice", "value"
      3. Nested "purchasePrice" dict keys:     "value", "amount", "listPrice"

    If no valid price is found, DEFAULT_FALLBACK_ASK_PRICE_USD is used.
    A price of 0.0 (free domain promotion) is preserved as-is.
    """
    normalized_domain = str(item.get("domain") or fallback_domain).strip().lower()
    if not normalized_domain or "." not in normalized_domain:
        return None

    # Extract price from whichever shape Spaceship sends (see docstring for priority order)
    raw_price: Any = item.get("price")
    if isinstance(raw_price, dict):
        # Nested "price" dict: try "listPrice" → "yourPrice" → "value"
        raw_price = (
            raw_price.get("listPrice")
            if raw_price.get("listPrice") is not None
            else raw_price.get("yourPrice")
            if raw_price.get("yourPrice") is not None
            else raw_price.get("value")
        )

    purchase_price = item.get("purchasePrice")
    if isinstance(purchase_price, dict) and raw_price is None:
        # Nested "purchasePrice" dict: try "value" → "amount" → "listPrice"
        raw_price = (
            purchase_price.get("value")
            if purchase_price.get("value") is not None
            else purchase_price.get("amount")
            if purchase_price.get("amount") is not None
            else purchase_price.get("listPrice")
        )

    ask_price = _normalize_price(raw_price)
    if ask_price is None:
        # Price is missing or unparseable — use the configured fallback
        LOGGER.warning(
            "Spaceship price missing/invalid for %s; using fallback ask price $%.2f",
            normalized_domain,
            DEFAULT_FALLBACK_ASK_PRICE_USD,
        )
        ask_price = DEFAULT_FALLBACK_ASK_PRICE_USD

    status_text = str(item.get("status") or "").strip() or "Available"
    listing_url = f"https://www.spaceship.com/domain-search/?query={normalized_domain}"

    return DomainOpportunity(
        domain=normalized_domain,
        ask_price_usd=ask_price,
        source="Spaceship Availability API",
        listing_url=listing_url,
        currency="USD",
        availability_status=status_text,
    )


def _normalize_tld(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return raw if raw.startswith(".") else f".{raw}"


def _effective_allowed_tlds(cfg: WatcherConfig) -> set[str]:
    configured = {t for t in set(cfg.allowed_tlds).union(PRIORITY_TLDS) if t in TARGET_TLDS}
    return configured or set(TARGET_TLDS)


def _is_priority_tld_domain(domain: str) -> bool:
    clean = str(domain or "").strip().lower()
    if "." not in clean:
        return False
    return f".{clean.rpartition('.')[-1]}" in PRIORITY_TLDS


def _parse_item_domain(item: dict[str, Any]) -> str:
    for key in ("domain", "domainName", "name", "fqdn"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    sld = str(item.get("sld") or item.get("label") or "").strip().lower()
    tld = _normalize_tld(str(item.get("tld") or item.get("zone") or "").strip().lower())
    if sld and tld:
        return f"{sld}{tld}"
    return ""


def _domain_status_from_item(item: dict[str, Any]) -> tuple[bool, str]:
    available = item.get("available")
    if isinstance(available, bool):
        return available, "Available" if available else "Unavailable"

    is_available = item.get("isAvailable")
    if isinstance(is_available, bool):
        return is_available, "Available" if is_available else "Unavailable"

    is_registered = item.get("isRegistered")
    if isinstance(is_registered, bool):
        return (not is_registered), "Unavailable" if is_registered else "Available"

    registered = item.get("registered")
    if isinstance(registered, bool):
        return (not registered), "Unavailable" if registered else "Available"

    result = str(item.get("result") or "").strip().lower()
    if result:
        if result in {"available", "free", "open"}:
            return True, "Available"
        if result in {"unavailable", "registered", "taken", "reserved", "blocked"}:
            LOGGER.info("Checked %s - Status: Unavailable", _parse_item_domain(item) or "N/A")
            return False, "Unavailable"
        if result == "tldnotsupported":
            LOGGER.info("Checked %s - Status: Tldnotsupported", _parse_item_domain(item) or "N/A")
            return False, "Tldnotsupported"
        return False, result.title()

    availability = str(item.get("availability") or "").strip().lower()
    if availability:
        if availability in {"available", "free", "open"}:
            return True, "Available"
        if availability in {"unavailable", "registered", "taken", "reserved", "blocked"}:
            LOGGER.info("Checked %s - Status: Unavailable", _parse_item_domain(item) or "N/A")
            return False, "Unavailable"
        if availability == "tldnotsupported":
            LOGGER.info("Checked %s - Status: Tldnotsupported", _parse_item_domain(item) or "N/A")
            return False, "Tldnotsupported"
        return False, availability.title()

    status = str(item.get("status") or "").strip().lower()
    if status:
        if status in {"available", "free", "open"}:
            return True, "Available"
        if status in {"unavailable", "registered", "taken", "reserved", "blocked"}:
            LOGGER.info("Checked %s - Status: Unavailable", _parse_item_domain(item) or "N/A")
            return False, "Unavailable"
        if status == "tldnotsupported":
            LOGGER.info("Checked %s - Status: Tldnotsupported", _parse_item_domain(item) or "N/A")
            return False, "Tldnotsupported"
        return False, status.title()

    return False, "Unavailable"


def format_available_domains_batch_summary(domains: list[str]) -> str:
    safe_domains = [html.escape(d) for d in domains[:AVAILABLE_BATCH_SIZE]]
    lines = "\n".join(f"{idx}. <code>{domain}</code>" for idx, domain in enumerate(safe_domains, start=1))
    count = len(safe_domains)
    return (
        f"📦 <b>VIP Available Domains Batch ({count})</b>\n"
        f"تم رصد {count} دومين متاح:\n"
        f"{lines}"
    )


async def send_batch_summary_notification(
    app: Application,
    domains: list[str],
) -> bool:
    payload: dict[str, Any] = {
        "chat_id": int(MAIN_CHAT_ID),
        "text": format_available_domains_batch_summary(domains),
        "parse_mode": ParseMode.HTML,
        "disable_web_page_preview": True,
    }
    while True:
        try:
            await app.bot.send_message(**payload)
            await asyncio.sleep(1)
            return True
        except RetryAfter as e:
            await asyncio.sleep(float(getattr(e, "retry_after", 1) or 1))
        except Exception:
            LOGGER.exception("Batch summary Telegram send failed chat_id=%s", payload["chat_id"])
            return False


async def evaluate_opportunity(
    opportunity: DomainOpportunity,
    cfg: WatcherConfig,
) -> ValuationResult:
    estimated, brandability_score, reason = score_with_internal_rules(opportunity.domain, cfg)
    margin_usd = estimated - opportunity.ask_price_usd
    ratio = estimated / opportunity.ask_price_usd if opportunity.ask_price_usd > 0 else 0.0
    is_high_margin = margin_usd >= cfg.min_margin_usd and ratio >= cfg.min_margin_ratio

    if brandability_score < cfg.min_brandability_score:
        is_high_margin = False
        reason = f"{reason}; rejected for precision (brandability {brandability_score:.2f}<{cfg.min_brandability_score:.2f})"

    return ValuationResult(
        estimated_value_usd=estimated,
        method="internal_rules",
        reason=reason,
        margin_usd=round(margin_usd, 2),
        margin_ratio=round(ratio, 2),
        is_high_margin=is_high_margin,
        brandability_score=brandability_score,
        root_word_analysis=None,
    )


def format_alert(opportunity: DomainOpportunity, valuation: ValuationResult) -> str:
    domain = html.escape(opportunity.domain)
    method = html.escape(valuation.method)
    source = html.escape(opportunity.source)
    ask = html.escape(f"${opportunity.ask_price_usd:.2f} {opportunity.currency}")
    estimate = html.escape(f"${valuation.estimated_value_usd:.2f} USD")
    gap = html.escape(f"${valuation.margin_usd:.2f}")
    ratio = html.escape(f"x{valuation.margin_ratio:.2f}")
    brandability = (
        html.escape(f"{valuation.brandability_score:.2f}")
        if valuation.brandability_score is not None
        else "N/A"
    )

    buy_url = html.escape(f"https://www.spaceship.com/domain-search/?query={opportunity.domain}")
    return (
        "🔥 <b>High-Margin Domain Deal</b>\n"
        f"🌐 <b>Domain:</b> <code>{domain}</code>\n"
        f"🏪 <b>Source:</b> {source}\n"
        f"💵 <b>Asking Price:</b> {ask}\n"
        f"🧠 <b>Estimated Value:</b> {estimate}\n"
        f"🎯 <b>Brandability:</b> {brandability}\n"
        f"📈 <b>Gap:</b> {gap} ({ratio})\n"
        f"⚙️ <b>Valuation Method:</b> <code>{method}</code>\n"
        f"🔗 <b>Listing:</b> {html.escape(opportunity.listing_url)}\n"
        f"🛒 <b>Buy:</b> <a href=\"{buy_url}\">Open in Spaceship</a>"
    )


async def send_telegram_notification(
    app: Application,
    domain_name: str,
    text: str,
    *,
    parse_mode: str = "HTML",
    reply_markup: InlineKeyboardMarkup | None = None,
    disable_web_page_preview: bool = True,
) -> None:
    clean_domain = (domain_name or "").strip().lower().rstrip(".")
    _, _, ext = clean_domain.rpartition(".")
    tld = f".{ext}" if ext else ""

    base_payload: dict[str, Any] = {
        "chat_id": int(MAIN_CHAT_ID),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
    }
    if reply_markup is not None:
        base_payload["reply_markup"] = reply_markup

    mapped_thread_id = TELEGRAM_TOPICS_MAP.get(tld)
    if mapped_thread_id is not None:
        topic_payload = dict(base_payload)
        topic_payload["message_thread_id"] = mapped_thread_id
        while True:
            try:
                await app.bot.send_message(**topic_payload)
                print(f"[SUCCESS] Sent to Topic {mapped_thread_id}")
                await asyncio.sleep(1)
                return
            except RetryAfter as e:
                await asyncio.sleep(float(getattr(e, "retry_after", 1) or 1))
            except Exception:
                break

    while True:
        try:
            await app.bot.send_message(**base_payload)
            print("[FALLBACK] Thread failed, sent to General Group.")
            await asyncio.sleep(1)
            return
        except RetryAfter as e:
            await asyncio.sleep(float(getattr(e, "retry_after", 1) or 1))
        except Exception:
            LOGGER.exception(
                "Telegram fallback send failed chat_id=%s tld=%s domain=%s",
                base_payload["chat_id"],
                tld or "N/A",
                domain_name,
            )
            raise


async def emit_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
    valuation: ValuationResult,
) -> None:
    buy_url = f"https://www.spaceship.com/domain-search/?query={opportunity.domain}"
    rows = [[InlineKeyboardButton("🛒 Buy / Register on Spaceship", url=buy_url)]]
    if opportunity.listing_url.rstrip("/") != buy_url.rstrip("/"):
        rows.append([InlineKeyboardButton("🔗 Open Listing", url=opportunity.listing_url)])
    rows.append([InlineKeyboardButton("📊 Whois", url=opportunity.whois_url)])
    keyboard = InlineKeyboardMarkup(rows)
    await send_telegram_notification(
        app=app,
        domain_name=opportunity.domain,
        text=format_alert(opportunity, valuation),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def is_general_find_candidate(opportunity: DomainOpportunity, cfg: WatcherConfig) -> bool:
    sld = opportunity.sld.lower()
    if not sld:
        return False
    if opportunity.tld.lower() not in cfg.general_find_tlds:
        return False
    if len(sld) > cfg.general_find_max_length:
        return False
    if "-" in sld or any(ch.isdigit() for ch in sld):
        return False
    return True


def format_general_find_alert(opportunity: DomainOpportunity) -> str:
    domain = html.escape(opportunity.domain)
    source = html.escape(opportunity.source)
    ask = html.escape(f"${opportunity.ask_price_usd:.2f} {opportunity.currency}")
    buy_url = html.escape(f"https://www.spaceship.com/domain-search/?query={opportunity.domain}")
    return (
        "🟡 <b>[General Find]</b>\n"
        "Net strategy candidate (non-VIP quality hit)\n"
        f"🌐 <b>Domain:</b> <code>{domain}</code>\n"
        f"🏪 <b>Source:</b> {source}\n"
        f"💵 <b>Asking Price:</b> {ask}\n"
        f"🔗 <b>Listing:</b> {html.escape(opportunity.listing_url)}\n"
        f"🛒 <b>Buy:</b> <a href=\"{buy_url}\">Open in Spaceship</a>"
    )


async def emit_general_find_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
) -> None:
    buy_url = f"https://www.spaceship.com/domain-search/?query={opportunity.domain}"
    rows = [[InlineKeyboardButton("🛒 Review on Spaceship", url=buy_url)]]
    rows.append([InlineKeyboardButton("📊 Whois", url=opportunity.whois_url)])
    keyboard = InlineKeyboardMarkup(rows)
    await send_telegram_notification(
        app=app,
        domain_name=opportunity.domain,
        text=format_general_find_alert(opportunity),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def format_vip_alert(opportunity: DomainOpportunity, vip: VipRecord) -> str:
    domain = html.escape(f"{opportunity.sld}.{opportunity.tld.lstrip('.')}")
    status = html.escape(opportunity.availability_status or "N/A")
    sector = html.escape(vip.sector or "N/A")
    rating = html.escape(vip.rating or "N/A")
    meaning_en = html.escape(vip.meaning_en or "N/A")
    meaning_ar = html.escape(vip.meaning_ar or "N/A")
    buy_url = html.escape(f"https://www.spaceship.com/domain-search/?query={opportunity.domain}")
    return (
        "🚨 <b>VIP DOMAIN MATCH SPOTTED!</b> 🚨\n"
        f"🌍 <b>Domain:</b> <code>{domain}</code>\n"
        f"🟢 <b>Status:</b> {status}\n"
        f"🏢 <b>Sector:</b> {sector}\n"
        f"⭐ <b>Rating:</b> {rating}\n"
        f"🇬🇧 <b>EN Meaning:</b> {meaning_en}\n"
        f"🇦🇪 <b>AR Meaning:</b> {meaning_ar}\n"
        f"🛒 <b>Buy:</b> <a href=\"{buy_url}\">Open in Spaceship</a>"
    )


async def emit_vip_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
    vip: VipRecord,
) -> None:
    buy_url = f"https://www.spaceship.com/domain-search/?query={opportunity.domain}"
    rows = [[InlineKeyboardButton("🔗 BUY NOW / SNIPE", url=buy_url)]]
    keyboard = InlineKeyboardMarkup(rows)
    await send_telegram_notification(
        app=app,
        domain_name=opportunity.domain,
        text=format_vip_alert(opportunity, vip),
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def _parse_chat_id_keys(chat_filters: Any) -> list[int]:
    if not isinstance(chat_filters, dict):
        return []
    ids: list[int] = []
    for key in chat_filters.keys():
        try:
            ids.append(int(key))
        except (TypeError, ValueError):
            continue
    return ids


def _chat_filter_matches(
    opportunity: DomainOpportunity,
    valuation: ValuationResult,
    filters: dict[str, Any],
) -> bool:
    selected_tlds = filters.get("tlds")
    if isinstance(selected_tlds, list) and selected_tlds:
        selected = {str(t).lower() for t in selected_tlds if isinstance(t, str)}
        if opportunity.tld not in selected:
            return False

    max_price = parse_float(filters.get("max_price"))
    if max_price is not None and opportunity.ask_price_usd > max_price:
        return False

    min_appraisal = parse_float(filters.get("min_appraisal"))
    if min_appraisal is not None and valuation.estimated_value_usd < min_appraisal:
        return False

    max_length_raw = filters.get("max_length")
    if max_length_raw is not None:
        try:
            max_length = int(max_length_raw)
        except (TypeError, ValueError):
            max_length = None
        if max_length is not None and len(opportunity.sld) > max_length:
            return False

    keywords = filters.get("keywords")
    if isinstance(keywords, list) and keywords:
        sld = opportunity.sld.lower()
        normalized = [str(keyword).lower() for keyword in keywords if isinstance(keyword, str)]
        if normalized and not any(keyword in sld for keyword in normalized):
            return False

    return True


def build_candidate_domains(vip_db: dict[str, VipRecord], cfg: WatcherConfig) -> list[str]:
    """Build candidate domains from VIP roots and high-value keyword permutations across allowed TLDs."""
    domains: set[str] = set()
    effective_tlds = _effective_allowed_tlds(cfg)
    for root in vip_db.keys():
        normalized_root = root.strip().lower()
        if not normalized_root:
            continue
        for tld in effective_tlds:
            domains.add(f"{normalized_root}{tld}")
    for keyword in cfg.high_value_keywords:
        for tld in effective_tlds:
            domains.add(f"{keyword}{tld}")
            domains.add(f"get{keyword}{tld}")
            domains.add(f"my{keyword}{tld}")
    return sorted(domains)


def select_circular_batch(items: list[str], cursor: int, batch_size: int) -> tuple[list[str], int]:
    """Return a circular batch and next cursor for stable round-robin scanning."""
    if not items or batch_size <= 0:
        return [], 0
    start = cursor % len(items)
    batch = [items[(start + idx) % len(items)] for idx in range(min(batch_size, len(items)))]
    next_cursor = (start + len(batch)) % len(items)
    return batch, next_cursor


async def watch_events(app: Application, chat_id: int) -> None:
    cfg = WatcherConfig.from_env()
    validate_required_spaceship_config(cfg)

    LOGGER.info(
        "Starting Spaceship watcher (default_poll=%ss, eco=%ss, turbo=%ss, turbo_hours=%s, api_base=%s, proxy=%s, delay=%.2f-%.2fs)",
        cfg.poll_seconds,
        cfg.eco_poll_seconds,
        cfg.turbo_poll_seconds,
        cfg.turbo_hours_utc,
        cfg.spaceship_api_base_url,
        bool(cfg.proxy_url),
        cfg.human_delay_min_seconds,
        cfg.human_delay_max_seconds,
    )

    while True:
        if bool(app.bot_data.get("watcher_paused", False)):
            await asyncio.sleep(1)
            continue
        now_utc = datetime.now(timezone.utc)
        in_turbo = is_turbo_hour(now_utc, cfg)
        poll_seconds = current_poll_seconds(now_utc, cfg)
        try:
            summary = await fetch_spaceship_domains(app, chat_id)
        except asyncio.CancelledError:
            LOGGER.info("Spaceship watcher cancelled.")
            raise
        except Exception as exc:
            LOGGER.exception("Spaceship watcher cycle failed: %s", exc)
            await asyncio.sleep(WATCHER_ERROR_RETRY_SECONDS)
            continue

        next_wait = max(
            poll_seconds,
            int(summary.get("quota_wait_seconds", 0)),
            int(summary.get("breaker_wait_seconds", 0)),
        )
        LOGGER.info(
            "Cycle complete mode=%s checked=%s opportunities=%s next_poll=%ss quota_wait=%ss breaker_wait=%ss",
            "turbo" if in_turbo else "eco",
            int(summary.get("domains_checked", 0)),
            int(summary.get("opportunities", 0)),
            poll_seconds,
            int(summary.get("quota_wait_seconds", 0)),
            int(summary.get("breaker_wait_seconds", 0)),
        )
        resume_event = app.bot_data.get("watcher_resume_event")
        if not isinstance(resume_event, asyncio.Event):
            resume_event = asyncio.Event()
            app.bot_data["watcher_resume_event"] = resume_event
        try:
            await asyncio.wait_for(resume_event.wait(), timeout=next_wait)
            resume_event.clear()
        except TimeoutError:
            pass


async def fetch_spaceship_domains(app: Application, chat_id: int) -> dict[str, int]:
    """
    Run one full scan cycle against the Spaceship API.

    Batching & anti-ban controls:
    ──────────────────────────────
    • Domains are chunked into batches of SPACESHIP_BULK_BATCH_SIZE (50).
    • An asyncio.sleep(SPACESHIP_INTRA_BATCH_DELAY_SECONDS) pause is inserted
      between every consecutive batch to simulate natural traffic patterns.
    • HTTP 429 responses are handled non-crashingly inside SpaceshipClient via
      Retry-After / exponential back-off (see SpaceshipClient._request_json_with_retry).
    """
    cfg = WatcherConfig.from_env()
    global available_domains_batch, available_domains_batch_lock
    if available_domains_batch_lock is None:
        available_domains_batch_lock = asyncio.Lock()
    validate_required_spaceship_config(cfg)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)
    app.bot_data.setdefault("scan_cycle_counter", 0)
    app.bot_data.setdefault(
        "latest_scan_summary",
        {"domains_checked": 0, "vip_matches": 0, "general_finds": 0},
    )

    store = AlertStore(cfg.db_path)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            client = SpaceshipClient(session, cfg)
            vip_folder = Path(__file__).with_name("vip_data")
            active_vip_db = get_vip_database(vip_folder)
            candidate_domains = build_candidate_domains(active_vip_db, cfg)
            if not candidate_domains:
                summary = {
                    "domains_checked": 0,
                    "vip_matches": 0,
                    "general_finds": 0,
                    "opportunities": 0,
                    "api_blocked_failed": 0,
                    "quota_wait_seconds": 0,
                    "breaker_wait_seconds": 0,
                }
                app.bot_data["scan_cycle_counter"] = int(app.bot_data.get("scan_cycle_counter", 0)) + 1
                app.bot_data["latest_scan_summary"] = summary
                return summary

            limit = min(cfg.max_domains_per_cycle, len(candidate_domains))
            domain_cursor = int(app.bot_data.get("domain_cursor", 0))
            selected_domains, next_cursor = select_circular_batch(
                candidate_domains,
                domain_cursor,
                limit,
            )
            app.bot_data["domain_cursor"] = next_cursor
            LOGGER.info("Fetching from Spaceship API: domains=%s", len(selected_domains))
            com_count = sum(1 for d in selected_domains if d.endswith(".com"))
            ai_count = sum(1 for d in selected_domains if d.endswith(".ai"))
            LOGGER.info(
                "Priority coverage this cycle: .com=%s .ai=%s (total=%s)",
                com_count,
                ai_count,
                len(selected_domains),
            )
            opportunities: list[DomainOpportunity] = []
            api_blocked_failed = 0
            for idx in range(0, len(selected_domains), SPACESHIP_BULK_BATCH_SIZE):
                batch = selected_domains[idx : idx + SPACESHIP_BULK_BATCH_SIZE]
                try:
                    batch_opps, batch_failed = await client.check_domains_availability_bulk(batch)
                    opportunities.extend(batch_opps)
                    api_blocked_failed += int(batch_failed)
                except SpaceshipCircuitOpenError as exc:
                    LOGGER.info("%s", exc)
                    api_blocked_failed += len(batch)
                except Exception as exc:
                    LOGGER.warning(
                        "Availability bulk check failed for batch_start=%s size=%s: %s",
                        idx,
                        len(batch),
                        exc,
                    )
                    api_blocked_failed += len(batch)
                # Intra-batch delay: simulate natural traffic; required anti-ban measure
                if idx + SPACESHIP_BULK_BATCH_SIZE < len(selected_domains):
                    await asyncio.sleep(SPACESHIP_INTRA_BATCH_DELAY_SECONDS)
            LOGGER.info("Fetched %s domains from Spaceship (available=%s)", len(selected_domains), len(opportunities))

            chat_filters = app.bot_data.get("chat_filters", {})
            target_chat_ids = {chat_id}
            target_chat_ids.update(_parse_chat_id_keys(chat_filters))

            priority_heap: list[tuple[float, str, DomainOpportunity]] = []
            for opportunity in opportunities:
                heapq.heappush(
                    priority_heap,
                    (
                        -priority_score(opportunity.domain, cfg, active_vip_db),
                        opportunity.domain,
                        opportunity,
                    ),
                )

            candidates: list[DomainOpportunity] = []
            while priority_heap:
                _, _, opportunity = heapq.heappop(priority_heap)
                candidates.append(opportunity)

            vip_candidates: list[DomainOpportunity] = []
            non_vip_candidates: list[DomainOpportunity] = []
            for item in candidates:
                root_word = item.sld.lower()
                tld = item.tld.lower()
                vip_match = tld in cfg.allowed_tlds and root_word in active_vip_db
                if vip_match:
                    vip_candidates.append(item)
                else:
                    non_vip_candidates.append(item)

            vip_match_count = 0
            general_match_count = 0

            for opportunity in vip_candidates:
                try:
                    vip_record = active_vip_db.get(opportunity.sld)
                    if vip_record is None:
                        continue
                    vip_match_count += 1
                    for target_chat_id in target_chat_ids:
                        if store.has_alerted(target_chat_id, opportunity.domain):
                            continue
                        try:
                            domain_name = opportunity.domain
                            vip_payload: dict[str, Any] = {
                                "chat_id": int(MAIN_CHAT_ID),
                                "text": format_vip_alert(opportunity, vip_record),
                                "parse_mode": "HTML",
                                "disable_web_page_preview": True,
                            }
                            while True:
                                try:
                                    await app.bot.send_message(**vip_payload)
                                    break
                                except RetryAfter as e:
                                    await asyncio.sleep(e.retry_after)

                            store.mark_alerted(target_chat_id, opportunity.domain, opportunity.source)
                            async with available_domains_batch_lock:
                                available_domains_batch.append(domain_name)
                                domains_to_send = (
                                    available_domains_batch[:AVAILABLE_BATCH_SIZE]
                                    if len(available_domains_batch) >= AVAILABLE_BATCH_SIZE
                                    else []
                                )
                            if domains_to_send:
                                batch_payload: dict[str, Any] = {
                                    "chat_id": int(MAIN_CHAT_ID),
                                    "text": format_available_domains_batch_summary(domains_to_send),
                                    "parse_mode": ParseMode.HTML,
                                    "disable_web_page_preview": True,
                                }
                                batch_sent = False
                                while True:
                                    try:
                                        await app.bot.send_message(**batch_payload)
                                        batch_sent = True
                                        break
                                    except RetryAfter as e:
                                        await asyncio.sleep(e.retry_after)
                                    except Exception:
                                        LOGGER.exception(
                                            "Batch summary Telegram send failed chat_id=%s",
                                            batch_payload["chat_id"],
                                        )
                                        break
                                if batch_sent:
                                    async with available_domains_batch_lock:
                                        available_domains_batch.clear()
                            LOGGER.info(
                                "VIP Telegram send success chat_id=%s domain=%s",
                                target_chat_id,
                                opportunity.domain,
                            )
                        except Exception as send_exc:
                            LOGGER.exception(
                                "VIP Telegram send failed chat_id=%s domain=%s error=%s",
                                target_chat_id,
                                opportunity.domain,
                                send_exc,
                            )
                    LOGGER.info("VIP alert sent domain=%s status=%s", opportunity.domain, opportunity.availability_status)
                except Exception as exc:
                    LOGGER.exception("Failed to send VIP Telegram alert for %s: %s", opportunity.domain, exc)

            for opportunity in non_vip_candidates:
                if not is_general_find_candidate(opportunity, cfg):
                    continue
                try:
                    general_match_count += 1
                    for target_chat_id in target_chat_ids:
                        if store.has_alerted(target_chat_id, opportunity.domain):
                            continue
                        try:
                            await send_telegram_notification(
                                app=app,
                                domain_name=opportunity.domain,
                                text=format_general_find_alert(opportunity),
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )
                            store.mark_alerted(target_chat_id, opportunity.domain, opportunity.source)
                            LOGGER.info(
                                "General Telegram send success chat_id=%s domain=%s",
                                target_chat_id,
                                opportunity.domain,
                            )
                        except Exception as send_exc:
                            LOGGER.exception(
                                "General Telegram send failed chat_id=%s domain=%s error=%s",
                                target_chat_id,
                                opportunity.domain,
                                send_exc,
                            )
                    LOGGER.info(
                        "General find alert sent domain=%s ask=$%.2f tld=%s len=%s",
                        opportunity.domain,
                        opportunity.ask_price_usd,
                        opportunity.tld,
                        len(opportunity.sld),
                    )
                except Exception as exc:
                    LOGGER.exception("Failed to send General Find alert for %s: %s", opportunity.domain, exc)

            summary = {
                "domains_checked": len(selected_domains),
                "vip_matches": vip_match_count,
                "general_finds": general_match_count,
                "opportunities": len(opportunities),
                "api_blocked_failed": api_blocked_failed,
                "quota_wait_seconds": client.quota_backoff_remaining_seconds(),
                "breaker_wait_seconds": client.circuit_open_remaining_seconds(),
            }
            app.bot_data["scan_cycle_counter"] = int(app.bot_data.get("scan_cycle_counter", 0)) + 1
            app.bot_data["latest_scan_summary"] = summary
            return summary
    finally:
        store.close()
