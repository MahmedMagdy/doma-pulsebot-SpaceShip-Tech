import asyncio
import contextlib
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
from telegram.ext import Application

from vip_database import VipRecord, get_vip_database, reload_vip_database

LOGGER = logging.getLogger(__name__)
MIN_POLL_SECONDS = 1
MIN_RETRY_ATTEMPTS = 1
MIN_RETRY_BASE_SECONDS = 0.2
MIN_BACKOFF_SECONDS = 1.0
MIN_QUOTA_COOLDOWN_SECONDS = 30
MIN_CIRCUIT_BREAKER_SECONDS = 30
GODADDY_PRICE_MICROS_THRESHOLD = 100000
DEFAULT_FALLBACK_ASK_PRICE_USD = 10.0


class GoDaddyCircuitOpenError(Exception):
    """Raised when GoDaddy API calls are temporarily blocked by circuit breaker."""


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
    allowed_tlds: set[str] = field(default_factory=lambda: {".ae", ".tech", ".my", ".com", ".app"})
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
    godaddy_api_base_url: str = "https://api.godaddy.com"
    godaddy_api_key: str = ""
    godaddy_api_secret: str = ""
    godaddy_check_type: str = "FAST"
    proxy_url: str = ""
    human_delay_min_seconds: float = 0.8
    human_delay_max_seconds: float = 2.5
    vip_reload_seconds: int = 3600
    scan_concurrency: int = 50
    general_find_max_length: int = 5
    general_find_tlds: set[str] = field(default_factory=lambda: {".com", ".ae"})

    @classmethod
    def from_env(cls) -> "WatcherConfig":
        raw_tlds = os.getenv("ALLOWED_TLDS", ".ae,.tech,.my,.com,.app")
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
        raw_general_find_tlds = os.getenv("GENERAL_FIND_TLDS", ".com,.ae")
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
            allowed_tlds=allowed_tlds or {".ae", ".tech", ".my", ".com", ".app"},
            high_value_keywords=high_value_keywords
            or {"ai", "crypto", "cloud", "data", "dev", "app", "bot", "pay", "trade", "labs"},
            negative_keywords=negative_keywords
            or {"gibberish", "unbrandable", "unpronounceable", "nonsense", "meaningless", "awkward", "spammy"},
            min_brandability_score=float(os.getenv("MIN_BRANDABILITY_SCORE", "35")),
            keyword_value_usd=float(os.getenv("KEYWORD_VALUE_USD", "22")),
            godaddy_api_base_url=os.getenv("GODADDY_API_BASE_URL", "https://api.godaddy.com").strip() or "https://api.godaddy.com",
            godaddy_api_key=os.getenv("GODADDY_API_KEY", "").strip(),
            godaddy_api_secret=os.getenv("GODADDY_API_SECRET", "").strip(),
            godaddy_check_type=os.getenv("GODADDY_CHECK_TYPE", "FAST").strip() or "FAST",
            proxy_url=os.getenv("PROXY_URL", "").strip(),
            human_delay_min_seconds=delay_min,
            human_delay_max_seconds=delay_max,
            vip_reload_seconds=max(60, int(os.getenv("VIP_RELOAD_SECONDS", "3600"))),
            scan_concurrency=max(1, int(os.getenv("SCAN_CONCURRENCY", "50"))),
            general_find_max_length=max(1, int(os.getenv("GENERAL_FIND_MAX_LENGTH", "5"))),
            general_find_tlds=general_find_tlds or {".com", ".ae"},
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


def validate_required_godaddy_config(cfg: WatcherConfig) -> None:
    missing: list[str] = []
    if not cfg.godaddy_api_key:
        missing.append("GODADDY_API_KEY")
    if not cfg.godaddy_api_secret:
        missing.append("GODADDY_API_SECRET")
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"Missing required GoDaddy configuration: {missing_csv}")


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


def _normalize_godaddy_price(raw_price: Any) -> Optional[float]:
    parsed = parse_float(raw_price)
    if parsed is None:
        return None
    # GoDaddy availability API may return large numeric prices in micros.
    # This normalizes all values at/above threshold to USD by dividing by 1,000,000.
    if parsed >= GODADDY_PRICE_MICROS_THRESHOLD:
        return round(parsed / 1_000_000.0, 2)
    return round(parsed, 2)


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
        ".ae": 65.0,
        ".tech": 62.0,
        ".my": 61.0,
        ".com": 64.0,
        ".app": 58.0,
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


class GoDaddyClient:
    def __init__(self, session: aiohttp.ClientSession, cfg: WatcherConfig) -> None:
        self.session = session
        self.cfg = cfg
        self._base_url = cfg.godaddy_api_base_url.rstrip("/")
        self._quota_backoff_until_monotonic = 0.0
        self._circuit_failures = 0
        self._circuit_open_until_monotonic = 0.0

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"sso-key {self.cfg.godaddy_api_key}:{self.cfg.godaddy_api_secret}",
        }

    async def _humanized_delay(self) -> None:
        await asyncio.sleep(
            random.uniform(
                self.cfg.human_delay_min_seconds,
                self.cfg.human_delay_max_seconds,
            )
        )

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
        capped_exponential = min(
            self.cfg.max_backoff_seconds,
            self.cfg.retry_base_seconds * (2 ** (attempt - 1)),
        )
        return random.uniform(0.0, max(capped_exponential, MIN_RETRY_BASE_SECONDS))

    async def _request_json_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        context_label: str,
    ) -> Optional[Any]:
        if not url:
            return None

        circuit_wait = self.circuit_open_remaining_seconds()
        if circuit_wait > 0:
            raise GoDaddyCircuitOpenError(
                f"{context_label} blocked by circuit breaker for {circuit_wait}s"
            )

        last_error: Optional[str] = None
        for attempt in range(1, self.cfg.max_retry_attempts + 1):
            await self._humanized_delay()
            try:
                async with self.session.request(
                    method,
                    url,
                    headers=self._headers(),
                    params=params,
                    proxy=self.cfg.proxy_url or None,
                ) as response:
                    body = await response.text()
                    if response.status == 429:
                        self._note_rate_limit()
                        self._note_retryable_failure()
                        wait_seconds = self._backoff_seconds(attempt)
                        LOGGER.warning(
                            "%s rate-limited (429) on %s; retrying in %.2fs",
                            context_label,
                            url,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue
                    if 500 <= response.status < 600:
                        self._note_retryable_failure()
                        wait_seconds = self._backoff_seconds(attempt)
                        LOGGER.warning(
                            "%s upstream status=%s; retrying in %.2fs",
                            context_label,
                            response.status,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue
                    if response.status != 200:
                        raise RuntimeError(
                            f"{context_label} failed status={response.status} body={body[:300]}"
                        )
                    try:
                        payload = await response.json(content_type=None)
                        self._note_success()
                        return payload
                    except Exception as exc:
                        raise RuntimeError(f"{context_label} returned invalid JSON: {exc}") from exc
            except aiohttp.ClientError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                self._note_retryable_failure()
                wait_seconds = self._backoff_seconds(attempt)
                LOGGER.info("%s network error: %s; retrying in %.2fs", context_label, exc, wait_seconds)
                await asyncio.sleep(wait_seconds)

        if last_error:
            raise RuntimeError(f"{context_label} failed after retries: {last_error}")
        raise RuntimeError(f"{context_label} failed after retries")

    async def check_domain_availability(self, domain: str) -> Optional[DomainOpportunity]:
        url = f"{self._base_url}/v1/domains/available"
        payload = await self._request_json_with_retry(
            "GET",
            url,
            params={"domain": domain, "checkType": self.cfg.godaddy_check_type},
            context_label="GoDaddy Domain Availability",
        )
        if not isinstance(payload, dict):
            return None

        available = payload.get("available")
        if isinstance(available, bool) and not available:
            return None

        normalized_domain = str(payload.get("domain") or domain).strip().lower()
        if not normalized_domain or "." not in normalized_domain:
            return None

        ask_price = _normalize_godaddy_price(payload.get("price"))
        if ask_price is None or ask_price <= 0:
            LOGGER.warning(
                "GoDaddy price missing/invalid for %s; using fallback ask price $%.2f",
                normalized_domain,
                DEFAULT_FALLBACK_ASK_PRICE_USD,
            )
            ask_price = DEFAULT_FALLBACK_ASK_PRICE_USD

        status_text = str(payload.get("status") or "").strip() or "Available"

        return DomainOpportunity(
            domain=normalized_domain,
            ask_price_usd=ask_price,
            source="GoDaddy Availability API",
            listing_url=f"https://www.godaddy.com/domainsearch/find?domainToCheck={normalized_domain}",
            currency="USD",
            availability_status=status_text,
        )


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

    return (
        "🔥 <b>High-Margin Domain Deal</b>\n"
        f"🌐 <b>Domain:</b> <code>{domain}</code>\n"
        f"🏪 <b>Source:</b> {source}\n"
        f"💵 <b>Asking Price:</b> {ask}\n"
        f"🧠 <b>Estimated Value:</b> {estimate}\n"
        f"🎯 <b>Brandability:</b> {brandability}\n"
        f"📈 <b>Gap:</b> {gap} ({ratio})\n"
        f"⚙️ <b>Valuation Method:</b> <code>{method}</code>\n"
        f"🔗 <b>Listing:</b> {html.escape(opportunity.listing_url)}"
    )


async def emit_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
    valuation: ValuationResult,
) -> None:
    buy_url = f"https://www.godaddy.com/domainsearch/find?domainToCheck={opportunity.domain}"
    rows = [[InlineKeyboardButton("🛒 Buy / Register on GoDaddy", url=buy_url)]]
    if opportunity.listing_url.rstrip("/") != buy_url.rstrip("/"):
        rows.append([InlineKeyboardButton("🔗 Open Listing", url=opportunity.listing_url)])
    rows.append([InlineKeyboardButton("📊 Whois", url=opportunity.whois_url)])
    keyboard = InlineKeyboardMarkup(rows)
    await app.bot.send_message(
        chat_id=chat_id,
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
    return (
        "🟡 <b>[General Find]</b>\n"
        "Net strategy candidate (non-VIP quality hit)\n"
        f"🌐 <b>Domain:</b> <code>{domain}</code>\n"
        f"🏪 <b>Source:</b> {source}\n"
        f"💵 <b>Asking Price:</b> {ask}\n"
        f"🔗 <b>Listing:</b> {html.escape(opportunity.listing_url)}"
    )


async def emit_general_find_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
) -> None:
    buy_url = f"https://www.godaddy.com/domainsearch/find?domainToCheck={opportunity.domain}"
    rows = [[InlineKeyboardButton("🛒 Review on GoDaddy", url=buy_url)]]
    rows.append([InlineKeyboardButton("📊 Whois", url=opportunity.whois_url)])
    keyboard = InlineKeyboardMarkup(rows)
    await app.bot.send_message(
        chat_id=chat_id,
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
    return (
        "🚨 <b>VIP DOMAIN MATCH SPOTTED!</b> 🚨\n"
        f"🌍 <b>Domain:</b> <code>{domain}</code>\n"
        f"🟢 <b>Status:</b> {status}\n"
        f"🏢 <b>Sector:</b> {sector}\n"
        f"⭐ <b>Rating:</b> {rating}\n"
        f"🇬🇧 <b>EN Meaning:</b> {meaning_en}\n"
        f"🇦🇪 <b>AR Meaning:</b> {meaning_ar}"
    )


async def emit_vip_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
    vip: VipRecord,
) -> None:
    buy_url = f"https://www.godaddy.com/domainsearch/find?domainToCheck={opportunity.domain}"
    rows = [[InlineKeyboardButton("🔗 BUY NOW / SNIPE", url=buy_url)]]
    keyboard = InlineKeyboardMarkup(rows)
    await app.bot.send_message(
        chat_id=chat_id,
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
    for root in vip_db.keys():
        normalized_root = root.strip().lower()
        if not normalized_root:
            continue
        for tld in cfg.allowed_tlds:
            domains.add(f"{normalized_root}{tld}")
    for keyword in cfg.high_value_keywords:
        for tld in cfg.allowed_tlds:
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
    validate_required_godaddy_config(cfg)
    store = AlertStore(cfg.db_path)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)

    LOGGER.info(
        "Starting GoDaddy watcher (default_poll=%ss, eco=%ss, turbo=%ss, turbo_hours=%s, api_base=%s, proxy=%s, delay=%.2f-%.2fs)",
        cfg.poll_seconds,
        cfg.eco_poll_seconds,
        cfg.turbo_poll_seconds,
        cfg.turbo_hours_utc,
        cfg.godaddy_api_base_url,
        bool(cfg.proxy_url),
        cfg.human_delay_min_seconds,
        cfg.human_delay_max_seconds,
    )

    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = GoDaddyClient(session, cfg)
        scan_semaphore = asyncio.Semaphore(cfg.scan_concurrency)
        vip_folder = Path(__file__).with_name("vip_data")
        current_vip_db = get_vip_database(vip_folder)
        vip_snapshot_lock = asyncio.Lock()
        domain_cursor = 0

        async def vip_reload_loop() -> None:
            nonlocal current_vip_db
            while True:
                try:
                    refreshed = reload_vip_database(vip_folder)
                    async with vip_snapshot_lock:
                        current_vip_db = refreshed
                    LOGGER.info("VIP DB reloaded entries=%s", len(refreshed))
                except Exception as exc:
                    LOGGER.warning("VIP DB reload failed: %s", exc)
                await asyncio.sleep(cfg.vip_reload_seconds)

        vip_reload_task = asyncio.create_task(vip_reload_loop())

        try:
            while True:
                force_scan_event = app.bot_data.get("force_scan_event")
                if not isinstance(force_scan_event, asyncio.Event):
                    force_scan_event = asyncio.Event()
                    app.bot_data["force_scan_event"] = force_scan_event

                force_scan_run = False
                if bool(app.bot_data.get("watcher_paused", False)):
                    LOGGER.info("Watcher paused; waiting for /resume or /force_scan trigger.")
                    await force_scan_event.wait()
                    force_scan_event.clear()
                    force_scan_run = True
                    LOGGER.info("Pause override trigger received; running immediate cycle.")

                now_utc = datetime.now(timezone.utc)
                in_turbo = is_turbo_hour(now_utc, cfg)
                poll_seconds = current_poll_seconds(now_utc, cfg)

                async with vip_snapshot_lock:
                    active_vip_db = dict(current_vip_db)

                candidate_domains = build_candidate_domains(active_vip_db, cfg)
                if not candidate_domains:
                    LOGGER.info("No candidate domains built this cycle.")
                    await asyncio.sleep(poll_seconds)
                    continue

                limit = min(cfg.max_domains_per_cycle, len(candidate_domains))
                selected_domains, domain_cursor = select_circular_batch(
                    candidate_domains,
                    domain_cursor,
                    limit,
                )

                async def check_with_guard(domain: str) -> Optional[DomainOpportunity]:
                    async with scan_semaphore:
                        try:
                            return await client.check_domain_availability(domain)
                        except GoDaddyCircuitOpenError as exc:
                            LOGGER.info("%s", exc)
                            return None
                        except Exception as exc:
                            LOGGER.warning("Availability check failed for %s: %s", domain, exc)
                            return None

                checks = await asyncio.gather(*(check_with_guard(domain) for domain in selected_domains))
                opportunities = [op for op in checks if op is not None]

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

                for opportunity in vip_candidates:
                    try:
                        vip_record = active_vip_db.get(opportunity.sld)
                        if vip_record is None:
                            continue
                        for target_chat_id in target_chat_ids:
                            if store.has_alerted(target_chat_id, opportunity.domain):
                                continue
                            await emit_vip_alert(app, target_chat_id, opportunity, vip_record)
                            store.mark_alerted(target_chat_id, opportunity.domain, opportunity.source)
                        LOGGER.info("VIP alert sent domain=%s status=%s", opportunity.domain, opportunity.availability_status)
                    except Exception as exc:
                        LOGGER.exception("Failed to send VIP Telegram alert for %s: %s", opportunity.domain, exc)

                for opportunity in non_vip_candidates:
                    if not is_general_find_candidate(opportunity, cfg):
                        continue
                    try:
                        for target_chat_id in target_chat_ids:
                            if store.has_alerted(target_chat_id, opportunity.domain):
                                continue
                            await emit_general_find_alert(app, target_chat_id, opportunity)
                            store.mark_alerted(target_chat_id, opportunity.domain, opportunity.source)
                        LOGGER.info(
                            "General find alert sent domain=%s ask=$%.2f tld=%s len=%s",
                            opportunity.domain,
                            opportunity.ask_price_usd,
                            opportunity.tld,
                            len(opportunity.sld),
                        )
                    except Exception as exc:
                        LOGGER.exception("Failed to send General Find alert for %s: %s", opportunity.domain, exc)

                quota_wait = client.quota_backoff_remaining_seconds()
                breaker_wait = client.circuit_open_remaining_seconds()
                next_wait = max(poll_seconds, quota_wait, breaker_wait)
                LOGGER.info(
                    "Cycle complete mode=%s checked=%s available=%s next_poll=%ss quota_wait=%ss breaker_wait=%ss",
                    "turbo" if in_turbo else "eco",
                    len(selected_domains),
                    len(opportunities),
                    poll_seconds,
                    quota_wait,
                    breaker_wait,
                )
                if force_scan_run:
                    continue
                if bool(app.bot_data.get("watcher_paused", False)):
                    LOGGER.info("Watcher set to paused after cycle; waiting for trigger.")
                    continue

                try:
                    await asyncio.wait_for(force_scan_event.wait(), timeout=next_wait)
                    force_scan_event.clear()
                    LOGGER.info("Force scan command received; running next cycle immediately.")
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            LOGGER.info("GoDaddy watcher cancelled.")
            raise
        finally:
            vip_reload_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await vip_reload_task
            store.close()
