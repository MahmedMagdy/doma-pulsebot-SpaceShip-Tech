import asyncio
import csv
import html
import logging
import os
import random
import re
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
from telegram import InlineKeyboardMarkup
from telegram.error import RetryAfter
from telegram.ext import Application

LOGGER = logging.getLogger(__name__)

# ─── Tuning constants ────────────────────────────────────────────────────────
MAIN_CHAT_ID = -1003736596502
# Strict .tech mode: all available alerts are routed to the fixed topic below.
TELEGRAM_TOPIC_ID = 20991
PRIORITY_TLDS = frozenset({".tech"})

MIN_POLL_SECONDS = 1
MIN_QUOTA_COOLDOWN_SECONDS = 30
MIN_CIRCUIT_BREAKER_SECONDS = 30
DEFAULT_FALLBACK_ASK_PRICE_USD = 10.0
WATCHER_ERROR_RETRY_SECONDS = 5
TARGET_TLDS = {".tech"}
DEFAULT_TECH_CATEGORY = "General Tech"
DEFAULT_MARKET_LOGIC = "High-Value Keyword"
PROCESSED_STATUS_AVAILABLE = "Available"
PROCESSED_STATUS_TAKEN = "Taken"
PROCESSED_STATUS_ERROR = "Error"
PROCESSED_STATUS_ALLOWED = {
    PROCESSED_STATUS_AVAILABLE,
    PROCESSED_STATUS_TAKEN,
    PROCESSED_STATUS_ERROR,
}
MAX_SUITABLE_PRICE_USD = 50.00
PREMIUM_PRICE_PATHS: tuple[tuple[str, ...], ...] = (
    ("pricing", "premium", "register"),
    ("pricing", "premium", "registerPrice"),
    ("pricing", "premium", "price"),
    ("pricing", "premium", "amount"),
    ("premiumPrice",),
    ("premium", "register"),
    ("premium", "registerPrice"),
    ("premium", "price"),
    ("premium", "amount"),
)
STANDARD_PRICE_PATHS: tuple[tuple[str, ...], ...] = (
    ("pricing", "standard", "register"),
    ("pricing", "standard", "registerPrice"),
    ("pricing", "standard", "price"),
    ("pricing", "standard", "amount"),
    ("pricing", "register"),
    ("pricing", "registerPrice"),
    ("price",),
    ("registerPrice",),
)

# Spaceship-specific throttle / batch controls
# Throttle math: target 95% of allowed request rate to stay safely below provider limits.
SPACESHIP_REQUESTS_PER_MINUTE_LIMIT = 30
SPACESHIP_TARGET_REQUESTS_PER_MINUTE = SPACESHIP_REQUESTS_PER_MINUTE_LIMIT * 0.95
SPACESHIP_INTRA_BATCH_DELAY_SECONDS = round(60 / SPACESHIP_TARGET_REQUESTS_PER_MINUTE, 3)  # 2.105s
SPACESHIP_BULK_BATCH_SIZE = 20  # Spaceship /domains/available max batch size
SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS = 3
SPACESHIP_API_MAX_ATTEMPTS = 4
SPACESHIP_STUBBORN_RETRY_ATTEMPTS = 6
SPACESHIP_STUBBORN_MAX_BACKOFF_SECONDS = 32
TELEGRAM_GROUP_MESSAGES_PER_MINUTE_LIMIT = 20
TELEGRAM_TARGET_MESSAGES_PER_MINUTE = TELEGRAM_GROUP_MESSAGES_PER_MINUTE_LIMIT * 0.95
TELEGRAM_MIN_MESSAGE_INTERVAL_SECONDS = round(60 / TELEGRAM_TARGET_MESSAGES_PER_MINUTE, 3)  # 3.158s
DEFAULT_STATUS_EMOJI = "🟡"
PRICE_VERIFICATION_FAILED_TEXT = "Verification Failed (Check Manually!)"
PROCESSED_CSV_LOCK = threading.Lock()
TELEGRAM_SEND_LOCK = asyncio.Lock()
TELEGRAM_NEXT_ALLOWED_SEND_MONOTONIC = 0.0


class SpaceshipCircuitOpenError(Exception):
    """Raised when Spaceship API calls are temporarily blocked by circuit breaker."""


@dataclass(frozen=True)
class DomainOpportunity:
    """Represents one available domain result; ask_price_usd=None means price verification failed."""
    domain: str
    ask_price_usd: Optional[float]
    domain_price: str
    is_suitable: bool
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


@dataclass
class WatcherConfig:
    poll_seconds: int = 30
    eco_poll_seconds: int = 120
    turbo_poll_seconds: int = 8
    turbo_hours_utc: tuple[tuple[int, int], ...] = ((18, 21),)
    request_timeout_seconds: int = 20
    db_path: str = "alerts.db"
    max_domains_per_cycle: int = 200
    quota_cooldown_seconds: int = 180
    circuit_breaker_failure_threshold: int = 4
    circuit_breaker_open_seconds: int = 120
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

    @classmethod
    def from_env(cls) -> "WatcherConfig":
        human_delay_min = float(os.getenv("HUMAN_DELAY_MIN_SECONDS", "0.8"))
        human_delay_max = float(os.getenv("HUMAN_DELAY_MAX_SECONDS", "2.5"))
        delay_min = min(human_delay_min, human_delay_max)
        delay_max = max(human_delay_min, human_delay_max)

        raw_high_value_keywords = os.getenv(
            "HIGH_VALUE_KEYWORDS",
            "ai,crypto,cloud,data,dev,app,bot,pay,trade,labs",
        )
        high_value_keywords = {kw.strip().lower() for kw in raw_high_value_keywords.split(",") if kw.strip()}
        return cls(
            poll_seconds=int(os.getenv("WATCHER_POLL_SECONDS", "30")),
            eco_poll_seconds=int(os.getenv("ECO_POLL_SECONDS", "120")),
            turbo_poll_seconds=int(os.getenv("TURBO_POLL_SECONDS", "8")),
            turbo_hours_utc=parse_turbo_hours(os.getenv("TURBO_HOURS_UTC", "18-21")),
            request_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
            db_path=os.getenv("ALERT_DB_PATH", "alerts.db"),
            max_domains_per_cycle=int(os.getenv("MAX_DOMAINS_PER_CYCLE", "200")),
            quota_cooldown_seconds=max(MIN_QUOTA_COOLDOWN_SECONDS, int(os.getenv("QUOTA_COOLDOWN_SECONDS", "180"))),
            circuit_breaker_failure_threshold=max(2, int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "4"))),
            circuit_breaker_open_seconds=max(
                MIN_CIRCUIT_BREAKER_SECONDS,
                int(os.getenv("CIRCUIT_BREAKER_OPEN_SECONDS", "120")),
            ),
            allowed_tlds=set(TARGET_TLDS),
            high_value_keywords=high_value_keywords
            or {"ai", "crypto", "cloud", "data", "dev", "app", "bot", "pay", "trade", "labs"},
            spaceship_api_base_url=os.getenv("SPACESHIP_API_BASE_URL", "https://spaceship.dev/api/v1").strip() or "https://spaceship.dev/api/v1",
            spaceship_api_key=os.getenv("SPACESHIP_API_KEY", "").strip(),
            spaceship_api_secret=os.getenv("SPACESHIP_API_SECRET", "").strip(),
            proxy_url=os.getenv("PROXY_URL", "").strip(),
            human_delay_min_seconds=delay_min,
            human_delay_max_seconds=delay_max,
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


def _coerce_non_negative_price(value: Any) -> Optional[float]:
    parsed = parse_float(value)
    if parsed is None or parsed < 0:
        return None
    return round(parsed, 2)


# REPLACE HERE: Strict .tech domain validator module
def _sanitize_strict_tech_domain(raw_domain: Any) -> str:
    """
    Strictly validate and normalize a domain with exactly one .tech extension.

    Rules:
    - no whitespace
    - exactly one trailing .tech extension
    - keyword contains only [a-z0-9-]
    - keyword cannot start/end with '-'
    """
    clean_domain = str(raw_domain or "").strip().lower()
    if not clean_domain:
        return ""
    if any(ch.isspace() for ch in clean_domain):
        return ""
    if not clean_domain.endswith(".tech"):
        return ""
    if clean_domain.count(".tech") != 1:
        return ""
    keyword = clean_domain[:-5]
    if not keyword or "." in keyword:
        return ""
    if not re.fullmatch(r"[a-z0-9-]+", keyword):
        return ""
    if keyword.startswith("-") or keyword.endswith("-"):
        return ""
    return f"{keyword}.tech"


def _read_dict_path(node: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = node
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _is_premium_domain_item(item: dict[str, Any]) -> bool:
    for flag_key in ("isPremium", "premium"):
        flag_value = item.get(flag_key)
        if isinstance(flag_value, bool) and flag_value:
            return True
        if isinstance(flag_value, str) and flag_value.strip().lower() == "true":
            return True
    tier_value = item.get("tier")
    if isinstance(tier_value, str) and tier_value.strip().lower() == "premium":
        return True
    return False


def _find_domain_object_for_query(payload: Any, domain_name: str) -> Optional[dict[str, Any]]:
    normalized_query = _sanitize_strict_tech_domain(domain_name) or str(domain_name or "").strip().lower()
    if not normalized_query:
        return None

    candidate_items: list[dict[str, Any]] = []
    if isinstance(payload, list):
        candidate_items = [entry for entry in payload if isinstance(entry, dict)]
    elif isinstance(payload, dict):
        wrapped_results = _extract_results_list(payload)
        if wrapped_results is not None:
            candidate_items = [entry for entry in wrapped_results if isinstance(entry, dict)]
        else:
            candidate_items = [payload]

    for item in candidate_items:
        item_domain = _sanitize_strict_tech_domain(_parse_item_domain(item)) or _parse_item_domain(item)
        if item_domain and item_domain.lower() == normalized_query:
            return item
    return None


def _extract_price_from_paths(item: dict[str, Any], paths: tuple[tuple[str, ...], ...]) -> Optional[float]:
    """Read the first valid path in declared order; path order is the deterministic precedence."""
    for path in paths:
        parsed = _coerce_non_negative_price(_read_dict_path(item, path))
        if parsed is not None:
            return parsed
    return None


# REPLACE HERE: Deterministic multi-layer Spaceship price extractor
def extract_spaceship_price(payload: Any, domain_name: str) -> Optional[float]:
    """
    Deterministic extractor:
    1) match exact domain object,
    2) resolve premium status,
    3) read only status-specific price paths,
    4) cast to float or return None.
    """
    item = _find_domain_object_for_query(payload, domain_name)
    if item is None:
        return None

    if _is_premium_domain_item(item):
        return _extract_price_from_paths(item, PREMIUM_PRICE_PATHS)
    return _extract_price_from_paths(item, STANDARD_PRICE_PATHS)


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

    HTTP/transient handling:
        - Every low-level HTTP request gets four attempts with fixed 3-second delay.
        - Batch checks wrap this with a stubborn 6-attempt exponential retry loop.
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
        Send an HTTP request with exactly four attempts on transient failures.
        """
        if not url:
            return None

        circuit_wait = self.circuit_open_remaining_seconds()
        if circuit_wait > 0:
            raise SpaceshipCircuitOpenError(
                f"{context_label} blocked by circuit breaker for {circuit_wait}s"
            )

        last_error: Optional[str] = None
        for attempt in range(1, SPACESHIP_API_MAX_ATTEMPTS + 1):
            is_last_attempt = attempt == SPACESHIP_API_MAX_ATTEMPTS
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
                        self._note_rate_limit()
                        self._note_retryable_failure()
                        if is_last_attempt:
                            raise RuntimeError(f"{context_label} failed status=429 body={body[:300]}")
                        LOGGER.warning(
                            "%s rate-limited (429) on %s; pausing %.2fs before retry (attempt %s/%s)",
                            context_label,
                            url,
                            SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS,
                            attempt,
                            SPACESHIP_API_MAX_ATTEMPTS,
                        )
                        await asyncio.sleep(SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS)
                        continue

                    if 500 <= response.status < 600:
                        self._note_retryable_failure()
                        if is_last_attempt:
                            raise RuntimeError(
                                f"{context_label} failed status={response.status} body={body[:300]}"
                            )
                        LOGGER.warning(
                            "%s upstream status=%s; retrying in %.2fs (attempt %s/%s)",
                            context_label,
                            response.status,
                            SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS,
                            attempt,
                            SPACESHIP_API_MAX_ATTEMPTS,
                        )
                        await asyncio.sleep(SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS)
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

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self._note_retryable_failure()
                if is_last_attempt:
                    break
                LOGGER.info(
                    "%s network error: %s; retrying in %.2fs (attempt %s/%s)",
                    context_label,
                    exc,
                    SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS,
                    attempt,
                    SPACESHIP_API_MAX_ATTEMPTS,
                )
                await asyncio.sleep(SPACESHIP_API_SINGLE_RETRY_DELAY_SECONDS)

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

    Domain and pricing are accepted when:
      - Domain is a strict .tech format with exactly one ".tech" extension.
      - Price is extracted deterministically via extract_spaceship_price.
    """
    fallback_sanitized = _sanitize_strict_tech_domain(fallback_domain)
    item_sanitized = _sanitize_strict_tech_domain(_parse_item_domain(item))
    normalized_domain = item_sanitized or fallback_sanitized
    if not normalized_domain:
        return None
    if item_sanitized and fallback_sanitized and item_sanitized != fallback_sanitized:
        LOGGER.warning(
            "Dropping mismatched domain payload item=%s fallback=%s",
            item_sanitized,
            fallback_sanitized,
        )
        return None

    verified_price = extract_spaceship_price(item, normalized_domain)
    ask_price = verified_price
    if verified_price is None:
        domain_price = PRICE_VERIFICATION_FAILED_TEXT
    else:
        domain_price = f"{verified_price:.2f}"

    status_text = str(item.get("status") or "").strip() or "Available"
    sanitized_domain = normalized_domain
    buy_link = f"https://www.spaceship.com/domain-search/?query={sanitized_domain}"

    return DomainOpportunity(
        domain=normalized_domain,
        ask_price_usd=ask_price,
        domain_price=domain_price,
        is_suitable=(ask_price is not None and ask_price <= MAX_SUITABLE_PRICE_USD),
        source="Spaceship Availability API",
        listing_url=buy_link,
        currency="USD",
        availability_status=status_text,
    )


def _normalize_tld(value: str) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return raw if raw.startswith(".") else f".{raw}"


def _effective_allowed_tlds() -> set[str]:
    return set(TARGET_TLDS)


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


def log_to_processed_csv(base_keyword: str, full_domain: str, status: str) -> None:
    """
    Persist per-domain processing result to processed_domains.csv.

    - Opens file in append mode.
    - Auto-creates with header when absent.
    - Status is constrained to: Available, Taken, Error.
    """
    normalized_status = status if status in PROCESSED_STATUS_ALLOWED else PROCESSED_STATUS_ERROR
    output_path = Path(__file__).with_name("processed_domains.csv")

    with PROCESSED_CSV_LOCK:
        try:
            file_is_empty = not output_path.exists() or output_path.stat().st_size == 0
        except FileNotFoundError:
            file_is_empty = True
        with output_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if file_is_empty:
                writer.writerow(["Keyword", "Full_Domain", "Status"])
            writer.writerow(
                [
                    str(base_keyword or "").strip(),
                    str(full_domain or "").strip().lower(),
                    normalized_status,
                ]
            )


def _base_keyword_from_domain(full_domain: str) -> str:
    clean_domain = str(full_domain or "").strip().lower()
    if clean_domain.endswith(".tech"):
        return clean_domain.removesuffix(".tech")
    return clean_domain.split(".", 1)[0] if "." in clean_domain else clean_domain


async def check_domains_with_single_retry(
    client: "SpaceshipClient",
    domains: list[str],
) -> tuple[list["DomainOpportunity"], dict[str, str]]:
    """
    Check one batch with a stubborn 6-attempt wrapper retry policy.

    Returns:
      (available_opportunities, status_by_domain)
    where status_by_domain values are strictly one of:
      Available, Taken, Error.
    """
    normalized_domains = []
    for raw_domain in domains:
        sanitized_domain = _sanitize_strict_tech_domain(raw_domain)
        if not sanitized_domain:
            LOGGER.warning("Skipping invalid domain before API call: %s", raw_domain)
            continue
        normalized_domains.append(sanitized_domain)
    if not normalized_domains:
        return [], {}

    async def _run_once() -> tuple[list["DomainOpportunity"], dict[str, str]]:
        opportunities, failed_count = await client.check_domains_availability_bulk(normalized_domains)
        if failed_count:
            LOGGER.info(
                "Bulk availability returned %s unresolved results in batch_size=%s",
                failed_count,
                len(normalized_domains),
            )
        available_domains = {op.domain.strip().lower() for op in opportunities}
        status_map = {
            domain: (
                PROCESSED_STATUS_AVAILABLE
                if domain in available_domains
                else PROCESSED_STATUS_TAKEN
            )
            for domain in normalized_domains
        }
        return opportunities, status_map

    for attempt in range(1, SPACESHIP_STUBBORN_RETRY_ATTEMPTS + 1):
        try:
            return await _run_once()
        except Exception as error:
            if attempt >= SPACESHIP_STUBBORN_RETRY_ATTEMPTS:
                LOGGER.error(
                    "Domain check failed after %s stubborn attempts; batch_size=%s: %s",
                    SPACESHIP_STUBBORN_RETRY_ATTEMPTS,
                    len(normalized_domains),
                    error,
                )
                break
            backoff_seconds = min(2 ** attempt, SPACESHIP_STUBBORN_MAX_BACKOFF_SECONDS)
            LOGGER.warning(
                "Stubborn retry for batch_size=%s after attempt %s/%s; waiting %ss: %s",
                len(normalized_domains),
                attempt,
                SPACESHIP_STUBBORN_RETRY_ATTEMPTS,
                backoff_seconds,
                error,
            )
            await asyncio.sleep(backoff_seconds)
    return [], {domain: PROCESSED_STATUS_ERROR for domain in normalized_domains}


def format_available_alert(
    sanitized_domain: str,
    final_verified_price: float,
    category: str,
    market_logic: str,
    buy_link: str,
) -> str:
    clean_domain = html.escape(str(sanitized_domain or "").strip().lower())
    clean_price = f"{final_verified_price:.2f}"
    clean_category = html.escape(str(category or "").strip()) or DEFAULT_TECH_CATEGORY
    clean_market_logic = html.escape(str(market_logic or "").strip()) or DEFAULT_MARKET_LOGIC
    clean_link = html.escape(str(buy_link or "").strip(), quote=True)
    return (
        f"🟢 <b>Domain:</b> {clean_domain}\n"
        f"📂 <b>Niche:</b> {clean_category}\n"
        f"💡 <b>Market Logic:</b> {clean_market_logic}\n"
        f"💰 <b>Price:</b> ${clean_price}\n"
        f"🛒 <b>Buy:</b> <a href=\"{clean_link}\">Open in Spaceship</a>"
    )


def format_verification_failed_alert(
    sanitized_domain: str,
    buy_link: str,
) -> str:
    clean_domain = html.escape(str(sanitized_domain or "").strip().lower())
    clean_link = html.escape(str(buy_link or "").strip(), quote=True)
    return (
        f"🟡 <b>Domain:</b> {clean_domain} | "
        f"💰 <b>Price:</b> ⚠️ {PRICE_VERIFICATION_FAILED_TEXT} | "
        f"🛒 <b>Buy:</b> <a href=\"{clean_link}\">Spaceship Search URL</a>"
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
    payload: dict[str, Any] = {
        "chat_id": int(MAIN_CHAT_ID),
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": disable_web_page_preview,
        "message_thread_id": TELEGRAM_TOPIC_ID,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    async def _respect_telegram_group_rate_limit() -> None:
        global TELEGRAM_NEXT_ALLOWED_SEND_MONOTONIC
        loop = asyncio.get_running_loop()
        async with TELEGRAM_SEND_LOCK:
            now = loop.time()
            wait_seconds = TELEGRAM_NEXT_ALLOWED_SEND_MONOTONIC - now
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
                now = loop.time()
            TELEGRAM_NEXT_ALLOWED_SEND_MONOTONIC = max(
                TELEGRAM_NEXT_ALLOWED_SEND_MONOTONIC,
                now,
            ) + TELEGRAM_MIN_MESSAGE_INTERVAL_SECONDS

    while True:
        try:
            await _respect_telegram_group_rate_limit()
            await app.bot.send_message(**payload)
            LOGGER.info(
                "✅ VERIFIED: Telegram message sent for %s to topic=%s",
                domain_name,
                TELEGRAM_TOPIC_ID,
            )
            return
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except Exception as e:
            LOGGER.error("❌ FAILED to send %s to Telegram topic %s: %s", domain_name, TELEGRAM_TOPIC_ID, e)
            raise


def build_candidate_domains() -> tuple[list[str], dict[str, dict[str, str]]]:
    """Build candidate `.tech` domains and metadata from tech_targets.csv."""
    domains: set[str] = set()
    metadata_by_domain: dict[str, dict[str, str]] = {}
    csv_path = Path(__file__).with_name("tech_targets.csv")
    if not csv_path.exists():
        LOGGER.warning("Tech targets CSV not found: %s", csv_path)
        return [], {}

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=",")
            source_columns = reader.fieldnames or []
            normalized_columns = {str(name or "").strip().lower(): name for name in source_columns}
            domain_column = normalized_columns.get("domain")
            keyword_column = normalized_columns.get("keyword")
            category_column = (
                normalized_columns.get("niche")
                or normalized_columns.get("category")
                or normalized_columns.get("niche/category")
            )
            market_logic_column = normalized_columns.get("market logic")
            if not domain_column or not category_column or not market_logic_column:
                LOGGER.warning(
                    "tech_targets.csv is missing required columns. expected=%s found=%s",
                    ["Domain", "Niche/Category", "Market Logic"],
                    source_columns,
                )
                return [], {}

            for row in reader:
                raw_domain = str(row.get(domain_column) or "").strip()
                raw_keyword = str(row.get(keyword_column) or "").strip().lower() if keyword_column else ""
                if not raw_domain:
                    if re.fullmatch(r"[a-z0-9]([a-z0-9-]*[a-z0-9])?", raw_keyword or ""):
                        raw_domain = f"{raw_keyword}.tech"
                    else:
                        continue
                sanitized_domain = _sanitize_strict_tech_domain(raw_domain)
                if not sanitized_domain:
                    continue
                category = str(row.get(category_column) or "").strip() or DEFAULT_TECH_CATEGORY
                logic = str(row.get(market_logic_column) or "").strip() or DEFAULT_MARKET_LOGIC
                domains.add(sanitized_domain)
                metadata_by_domain[sanitized_domain] = {"category": category, "logic": logic}
    except OSError as exc:
        LOGGER.warning("Unable to access tech targets CSV %s: %s", csv_path, exc)
        return [], {}
    except csv.Error as exc:
        LOGGER.warning("Malformed tech targets CSV %s: %s", csv_path, exc)
        return [], {}

    return sorted(domains), metadata_by_domain


def select_circular_batch(items: list[str], cursor: int, batch_size: int) -> tuple[list[str], int]:
    """Return a circular batch and next cursor for stable round-robin scanning."""
    if not items or batch_size <= 0:
        return [], 0
    start = cursor % len(items)
    batch = [items[(start + idx) % len(items)] for idx in range(min(batch_size, len(items)))]
    next_cursor = (start + len(batch)) % len(items)
    return batch, next_cursor


async def watch_events(app: Application) -> None:
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
            summary = await fetch_spaceship_domains(app)
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


async def fetch_spaceship_domains(app: Application) -> dict[str, int]:
    """
    Run one full scan cycle against the Spaceship API.

    Batching & anti-ban controls:
    ──────────────────────────────
    • Domains are chunked into batches of SPACESHIP_BULK_BATCH_SIZE (20).
    • An asyncio.sleep(SPACESHIP_INTRA_BATCH_DELAY_SECONDS) pause is inserted
      between every consecutive batch to simulate natural traffic patterns.
    • Every low-level request gets up to 4 attempts with 3-second retry delay.
    • Every batch check is wrapped by a 6-attempt stubborn retry loop (2s, 4s, 8s...).
    """
    cfg = WatcherConfig.from_env()
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
            candidate_domains, domain_metadata = build_candidate_domains()
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
            tech_count = sum(1 for d in selected_domains if d.endswith(".tech"))
            LOGGER.info("Priority coverage this cycle: .tech=%s (total=%s)", tech_count, len(selected_domains))
            opportunities: list[DomainOpportunity] = []
            api_blocked_failed = 0
            for idx in range(0, len(selected_domains), SPACESHIP_BULK_BATCH_SIZE):
                batch = selected_domains[idx : idx + SPACESHIP_BULK_BATCH_SIZE]
                batch_opps, batch_statuses = await check_domains_with_single_retry(client, batch)
                opportunities.extend(batch_opps)
                for checked_domain in batch:
                    clean_domain = str(checked_domain or "").strip().lower()
                    if not clean_domain:
                        continue
                    base_keyword = _base_keyword_from_domain(clean_domain)
                    status = batch_statuses.get(clean_domain, PROCESSED_STATUS_ERROR)
                    log_to_processed_csv(base_keyword, clean_domain, status)
                    if status == PROCESSED_STATUS_ERROR:
                        api_blocked_failed += 1
                # Intra-batch delay: simulate natural traffic; required anti-ban measure
                if idx + SPACESHIP_BULK_BATCH_SIZE < len(selected_domains):
                    await asyncio.sleep(SPACESHIP_INTRA_BATCH_DELAY_SECONDS)
            LOGGER.info("Fetched %s domains from Spaceship (available=%s)", len(selected_domains), len(opportunities))

            vip_match_count = 0
            general_match_count = 0
            fixed_chat_id = MAIN_CHAT_ID
            # REPLACE HERE: Zero-attrition Force-Catch routing matrix (Case A/B/C)
            for opportunity in opportunities:
                try:
                    if opportunity.availability_status.strip().lower() != "available":
                        continue
                    if store.has_alerted(fixed_chat_id, opportunity.domain):
                        continue
                    sanitized_domain = _sanitize_strict_tech_domain(opportunity.domain)
                    if not sanitized_domain:
                        continue
                    metadata = domain_metadata.get(sanitized_domain)
                    is_metadata_backed = metadata is not None
                    category = str((metadata or {}).get("category") or DEFAULT_TECH_CATEGORY)
                    market_logic = str((metadata or {}).get("logic") or DEFAULT_MARKET_LOGIC)
                    final_verified_price = opportunity.ask_price_usd
                    if final_verified_price is None:
                        buy_link = f"https://www.spaceship.com/domain-search/?query={sanitized_domain}"
                        await send_telegram_notification(
                            app=app,
                            domain_name=opportunity.domain,
                            text=format_verification_failed_alert(
                                sanitized_domain=sanitized_domain,
                                buy_link=buy_link,
                            ),
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                        )
                        store.mark_alerted(fixed_chat_id, opportunity.domain, opportunity.source)
                        if is_metadata_backed:
                            vip_match_count += 1
                        else:
                            general_match_count += 1
                        LOGGER.info("Telegram send success domain=%s case=verification_failed", opportunity.domain)
                        continue
                    if final_verified_price > MAX_SUITABLE_PRICE_USD:
                        LOGGER.info(
                            "Dropping domain=%s reason=over_budget price=%.2f",
                            opportunity.domain,
                            final_verified_price,
                        )
                        continue
                    buy_link = f"https://www.spaceship.com/domain-search/?query={sanitized_domain}"
                    await send_telegram_notification(
                        app=app,
                        domain_name=opportunity.domain,
                        text=format_available_alert(
                            sanitized_domain=sanitized_domain,
                            final_verified_price=final_verified_price,
                            category=category,
                            market_logic=market_logic,
                            buy_link=buy_link,
                        ),
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                    )
                    store.mark_alerted(fixed_chat_id, opportunity.domain, opportunity.source)
                    if is_metadata_backed:
                        vip_match_count += 1
                    else:
                        general_match_count += 1
                    LOGGER.info("Telegram send success domain=%s", opportunity.domain)
                except Exception as send_exc:
                    LOGGER.exception("Telegram send failed domain=%s error=%s", opportunity.domain, send_exc)

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
