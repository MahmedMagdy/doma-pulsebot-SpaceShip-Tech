import asyncio
import logging
import os
import random
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application

LOGGER = logging.getLogger(__name__)
SPECIAL_CHARS = r"_*[]()~`>#+-=|{}.!"
MIN_POLL_SECONDS = 1
MIN_RETRY_ATTEMPTS = 1
MIN_RETRY_BASE_SECONDS = 0.2
MIN_BACKOFF_SECONDS = 1.0
MIN_QUOTA_COOLDOWN_SECONDS = 30
JITTER_MIN_SECONDS = 0.15
JITTER_MAX_SECONDS = 0.85
APPRAISAL_FALLBACK_TOKENS = (
    "out of credits",
    "rate limit exceeded",
    "quota exhausted",
    "credits exhausted",
    "insufficient credits",
)


class AppraisalUnavailableError(Exception):
    """Raised when Atom AI appraisal cannot be used and fallback is required."""


@dataclass(frozen=True)
class DomainOpportunity:
    domain: str
    ask_price_usd: float
    source: str
    listing_url: str
    currency: str = "USD"

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
    min_margin_usd: float = 20.0
    min_margin_ratio: float = 1.8
    allowed_tlds: set[str] = field(default_factory=lambda: {".dev", ".app", ".cloud"})
    keyword_value_usd: float = 22.0
    atom_partnership_url: str = ""
    atom_partnership_api_key: str = ""
    atom_appraisal_url: str = ""
    atom_appraisal_api_key: str = ""
    proxy_url: str = ""
    human_delay_min_seconds: float = 0.8
    human_delay_max_seconds: float = 2.5
    seo_api_url: str = ""
    seo_api_key: str = ""
    search_volume_api_url: str = ""
    search_volume_api_key: str = ""
    namebio_api_url: str = ""
    namebio_api_key: str = ""
    seo_bonus_points: float = 12.0
    search_volume_bonus_points: float = 10.0
    historical_sales_bonus_points: float = 15.0
    high_value_keywords: tuple[str, ...] = (
        "ai",
        "app",
        "api",
        "agent",
        "bot",
        "cloud",
        "compute",
        "crypto",
        "data",
        "dev",
        "labs",
        "ml",
        "saas",
        "tech",
        "web",
    )

    @classmethod
    def from_env(cls) -> "WatcherConfig":
        raw_tlds = os.getenv("ALLOWED_TLDS", ".dev,.app,.cloud")
        allowed_tlds = {
            t.strip().lower() if t.strip().startswith(".") else f".{t.strip().lower()}"
            for t in raw_tlds.split(",")
            if t.strip()
        }
        human_delay_min = float(os.getenv("HUMAN_DELAY_MIN_SECONDS", "0.8"))
        human_delay_max = float(os.getenv("HUMAN_DELAY_MAX_SECONDS", "2.5"))
        delay_min = min(human_delay_min, human_delay_max)
        delay_max = max(human_delay_min, human_delay_max)
        partnership_urls_raw = os.getenv("ATOM_PARTNERSHIP_API_URLS", "").strip()
        partnership_urls = tuple(
            part.strip()
            for part in partnership_urls_raw.split(",")
            if part.strip()
        )
        if not partnership_urls:
            single_url = os.getenv("ATOM_PARTNERSHIP_API_URL", "").strip()
            partnership_urls = (single_url,) if single_url else tuple()
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
            min_margin_usd=float(os.getenv("ARBITRAGE_MIN_GAP_USD", "20")),
            min_margin_ratio=float(os.getenv("ARBITRAGE_MIN_RATIO", "1.8")),
            allowed_tlds=allowed_tlds or {".dev", ".app", ".cloud"},
            keyword_value_usd=float(os.getenv("KEYWORD_VALUE_USD", "22")),
            atom_partnership_url=partnership_urls[0] if partnership_urls else "",
            atom_partnership_api_key=os.getenv("ATOM_PARTNERSHIP_API_KEY", "").strip(),
            atom_appraisal_url=os.getenv("ATOM_APPRAISAL_API_URL", "").strip(),
            atom_appraisal_api_key=os.getenv("ATOM_APPRAISAL_API_KEY", "").strip(),
            proxy_url=os.getenv("PROXY_URL", "").strip(),
            human_delay_min_seconds=delay_min,
            human_delay_max_seconds=delay_max,
            seo_api_url=os.getenv("SEO_API_URL", "").strip(),
            seo_api_key=os.getenv("SEO_API_KEY", "").strip(),
            search_volume_api_url=os.getenv("SEARCH_VOL_API_URL", "").strip(),
            search_volume_api_key=os.getenv("SEARCH_VOL_API_KEY", "").strip(),
            namebio_api_url=os.getenv("NAMEBIO_API_URL", "").strip(),
            namebio_api_key=os.getenv("NAMEBIO_API_KEY", "").strip(),
            seo_bonus_points=float(os.getenv("SEO_BONUS_POINTS", "12")),
            search_volume_bonus_points=float(os.getenv("SEARCH_VOL_BONUS_POINTS", "10")),
            historical_sales_bonus_points=float(os.getenv("NAMEBIO_BONUS_POINTS", "15")),
        )


def parse_turbo_hours(raw_value: str) -> tuple[tuple[int, int], ...]:
    ranges: list[tuple[int, int]] = []
    for part in raw_value.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" not in token:
            continue
        start_s, end_s = token.split("-", 1)
        try:
            start = int(start_s)
            end = int(end_s)
        except ValueError:
            continue
        if 0 <= start <= 23 and 0 <= end <= 23 and start != end:
            ranges.append((start, end))
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
                domain TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                first_seen_utc TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def has_alerted(self, domain: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM sent_alerts WHERE domain = ?",
            (domain.lower(),),
        ).fetchone()
        return row is not None

    def mark_alerted(self, domain: str, source: str) -> None:
        self.conn.execute(
            """
            INSERT OR IGNORE INTO sent_alerts(domain, source, first_seen_utc)
            VALUES (?, ?, ?)
            """,
            (domain.lower(), source, datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def escape_md_v2(value: str) -> str:
    escaped = value
    for char in SPECIAL_CHARS:
        escaped = escaped.replace(char, f"\\{char}")
    return escaped


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


def extract_error_message(data: dict[str, Any]) -> Optional[str]:
    """Return the first meaningful API error text from common response keys."""
    return (
        str(
            data.get("message")
            or data.get("error")
            or data.get("detail")
            or data.get("error_description")
            or data.get("error_message")
            or data.get("description")
            or ""
        ).strip()
        or None
    )


def extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "domains", "results", "listings", "opportunities", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, dict)]
    return [payload]


def score_with_internal_rules(domain: str, cfg: WatcherConfig) -> tuple[float, str]:
    sld = domain.split(".", 1)[0].lower()
    tld = domain.rpartition(".")[2].lower()
    tld_pref = f".{tld}" if tld else ""

    length = len(sld)
    if length <= 4:
        length_score = 120.0
    elif length <= 6:
        length_score = 90.0
    elif length <= 8:
        length_score = 65.0
    elif length <= 12:
        length_score = 40.0
    else:
        length_score = 18.0

    tld_score = {
        ".dev": 65.0,
        ".app": 58.0,
        ".cloud": 52.0,
    }.get(tld_pref, 20.0)

    matched_keywords = [kw for kw in cfg.high_value_keywords if kw in sld]
    keyword_score = min(3, len(matched_keywords)) * cfg.keyword_value_usd

    penalty = 0.0
    if "-" in sld:
        penalty += 14.0
    if any(ch.isdigit() for ch in sld):
        penalty += 10.0

    total = max(5.0, length_score + tld_score + keyword_score - penalty)
    reason = (
        f"Rule-based score: length={length_score:.1f}, tld={tld_score:.1f}, "
        f"keywords={keyword_score:.1f}, penalty={penalty:.1f}"
    )
    return round(total, 2), reason


class AtomClient:
    def __init__(self, session: aiohttp.ClientSession, cfg: WatcherConfig) -> None:
        self.session = session
        self.cfg = cfg
        self._partnership_urls = self._normalize_urls(
            os.getenv("ATOM_PARTNERSHIP_API_URLS", ""),
            cfg.atom_partnership_url,
        )
        self._round_robin_index = 0
        self._quota_backoff_until_monotonic = 0.0

    def _headers(self, api_key: str) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["X-API-Key"] = api_key
        return headers

    async def _humanized_delay(self) -> None:
        await asyncio.sleep(
            random.uniform(
                self.cfg.human_delay_min_seconds,
                self.cfg.human_delay_max_seconds,
            )
        )

    @staticmethod
    def _normalize_urls(raw_urls: str, fallback_url: str) -> tuple[str, ...]:
        urls = tuple(url.strip() for url in raw_urls.split(",") if url.strip())
        if urls:
            return urls
        return (fallback_url,) if fallback_url else tuple()

    def _note_rate_limit(self) -> None:
        loop = asyncio.get_running_loop()
        self._quota_backoff_until_monotonic = max(
            self._quota_backoff_until_monotonic,
            loop.time() + self.cfg.quota_cooldown_seconds,
        )

    def quota_backoff_remaining_seconds(self) -> int:
        loop = asyncio.get_running_loop()
        remaining = self._quota_backoff_until_monotonic - loop.time()
        return max(0, int(remaining))

    def _backoff_seconds(self, attempt: int) -> float:
        exponential = self.cfg.retry_base_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(JITTER_MIN_SECONDS, JITTER_MAX_SECONDS)
        return min(self.cfg.max_backoff_seconds, exponential + jitter)

    async def _request_json_with_retry(
        self,
        method: str,
        urls: tuple[str, ...],
        *,
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        json_payload: Optional[dict[str, Any]] = None,
        context_label: str,
        suppress_on_4xx: bool = False,
    ) -> Optional[Any]:
        if not urls:
            return None

        last_error: Optional[str] = None
        for attempt in range(1, self.cfg.max_retry_attempts + 1):
            url_index = (self._round_robin_index + (attempt - 1)) % len(urls)
            url = urls[url_index]
            await self._humanized_delay()
            try:
                async with self.session.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_payload,
                    proxy=self.cfg.proxy_url or None,
                ) as response:
                    body = await response.text()
                    if response.status == 429:
                        self._note_rate_limit()
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
                        wait_seconds = self._backoff_seconds(attempt)
                        LOGGER.warning(
                            "%s upstream error status=%s on %s; retrying in %.2fs",
                            context_label,
                            response.status,
                            url,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue
                    if response.status != 200:
                        if suppress_on_4xx and 400 <= response.status < 500:
                            LOGGER.debug(
                                "%s skipped status=%s on %s body=%s",
                                context_label,
                                response.status,
                                url,
                                body[:240],
                            )
                            return None
                        raise RuntimeError(
                            f"{context_label} failed status={response.status} body={body[:300]}"
                        )
                    self._round_robin_index = (url_index + 1) % len(urls)
                    try:
                        return await response.json(content_type=None)
                    except Exception as exc:
                        raise RuntimeError(f"{context_label} returned invalid JSON: {exc}") from exc
            except aiohttp.ClientError as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                wait_seconds = self._backoff_seconds(attempt)
                LOGGER.warning(
                    "%s network error on %s: %s; retrying in %.2fs",
                    context_label,
                    url,
                    exc,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)

        if last_error:
            raise RuntimeError(f"{context_label} failed after retries: {last_error}")
        raise RuntimeError(f"{context_label} failed after retries")

    async def fetch_partnership_domains(self) -> list[DomainOpportunity]:
        if not self._partnership_urls:
            LOGGER.warning("ATOM_PARTNERSHIP_API_URL(S) is not set; no domains fetched.")
            return []

        payload = await self._request_json_with_retry(
            "GET",
            self._partnership_urls,
            headers=self._headers(self.cfg.atom_partnership_api_key),
            context_label="Partnership API",
        )
        if payload is None:
            return []

        rows = extract_rows(payload)
        opportunities: list[DomainOpportunity] = []
        for row in rows:
            domain = str(
                row.get("domain")
                or row.get("name")
                or row.get("domain_name")
                or ""
            ).strip().lower()
            if not domain or "." not in domain:
                continue

            ask_price = (
                parse_float(row.get("asking_price_usd"))
                or parse_float(row.get("askingPriceUsd"))
                or parse_float(row.get("asking_price"))
                or parse_float(row.get("ask"))
                or parse_float(row.get("price_usd"))
                or parse_float(row.get("price"))
            )
            if ask_price is None or ask_price <= 0:
                continue

            listing_url = str(
                row.get("listing_url")
                or row.get("buy_url")
                or row.get("url")
                or row.get("link")
                or f"https://www.atom.com/domains/{domain}"
            ).strip()
            source = str(row.get("source") or "Atom Partnership").strip() or "Atom Partnership"
            currency = str(row.get("currency") or "USD").strip() or "USD"

            opportunities.append(
                DomainOpportunity(
                    domain=domain,
                    ask_price_usd=float(ask_price),
                    source=source,
                    listing_url=listing_url,
                    currency=currency,
                )
            )

        if len(opportunities) > self.cfg.max_domains_per_cycle:
            opportunities = opportunities[: self.cfg.max_domains_per_cycle]

        return opportunities

    async def appraise_with_atom_ai(self, domain: str) -> float:
        if not self.cfg.atom_appraisal_url:
            raise AppraisalUnavailableError("ATOM_APPRAISAL_API_URL is not set")

        payload = {"domain": domain}
        for attempt in range(1, self.cfg.max_retry_attempts + 1):
            await self._humanized_delay()
            try:
                async with self.session.post(
                    self.cfg.atom_appraisal_url,
                    headers=self._headers(self.cfg.atom_appraisal_api_key),
                    json=payload,
                    proxy=self.cfg.proxy_url or None,
                ) as response:
                    body_text = await response.text()
                    lowered = body_text.lower()

                    if response.status == 429:
                        self._note_rate_limit()
                        wait_seconds = self._backoff_seconds(attempt)
                        LOGGER.warning(
                            "Appraisal API rate-limited (429) for %s; retrying in %.2fs",
                            domain,
                            wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        continue

                    if response.status != 200:
                        if any(token in lowered for token in APPRAISAL_FALLBACK_TOKENS) or response.status in {402, 403}:
                            raise AppraisalUnavailableError(
                                f"AI appraisal unavailable (status={response.status}): {body_text[:240]}"
                            )
                        if 500 <= response.status < 600:
                            wait_seconds = self._backoff_seconds(attempt)
                            LOGGER.warning(
                                "Appraisal API error status=%s for %s; retrying in %.2fs",
                                response.status,
                                domain,
                                wait_seconds,
                            )
                            await asyncio.sleep(wait_seconds)
                            continue
                        raise AppraisalUnavailableError(
                            f"AI appraisal error status={response.status}: {body_text[:240]}"
                        )

                    try:
                        data = await response.json(content_type=None)
                        break
                    except Exception as exc:
                        raise AppraisalUnavailableError(
                            f"AI appraisal returned invalid JSON: {exc}"
                        ) from exc
            except aiohttp.ClientError as exc:
                wait_seconds = self._backoff_seconds(attempt)
                LOGGER.warning(
                    "Appraisal API network error for %s: %s; retrying in %.2fs",
                    domain,
                    exc,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
        else:
            raise AppraisalUnavailableError("AI appraisal retries exhausted")

        value = None
        if isinstance(data, dict):
            value = (
                parse_float(data.get("appraised_value_usd"))
                or parse_float(data.get("appraisal_usd"))
                or parse_float(data.get("estimated_value_usd"))
                or parse_float(data.get("estimatedValueUsd"))
                or parse_float(data.get("value_usd"))
                or parse_float(data.get("value"))
                or parse_float(data.get("estimate"))
            )
            if value is None:
                details = extract_error_message(data)
                if details:
                    raise AppraisalUnavailableError(f"AI appraisal did not provide value: {details}")

        if value is None or value <= 0:
            raise AppraisalUnavailableError("AI appraisal did not provide a valid estimated value")

        return float(value)

    async def seo_backlinks_bonus(self, domain: str) -> tuple[float, str]:
        if not self.cfg.seo_api_key:
            LOGGER.debug("Skipping SEO / Backlinks Check - No API Key")
            return 0.0, "skipped_no_key"
        if not self.cfg.seo_api_url:
            LOGGER.debug("Skipping SEO / Backlinks Check - No API URL")
            return 0.0, "skipped_no_url"

        try:
            data = await self._request_json_with_retry(
                "GET",
                (self.cfg.seo_api_url,),
                headers=self._headers(self.cfg.seo_api_key),
                params={"domain": domain},
                context_label="SEO API",
                suppress_on_4xx=True,
            )
        except Exception as exc:
            LOGGER.debug("Skipping SEO / Backlinks Check - %s", exc)
            return 0.0, "request_failed"
        if data is None:
            return 0.0, "skipped_4xx"
        if not isinstance(data, dict):
            return 0.0, "invalid_payload"

        seo_score = parse_float(data.get("authority"))
        if seo_score is None:
            seo_score = parse_float(data.get("backlinks"))
        if seo_score is not None and seo_score > 0:
            return self.cfg.seo_bonus_points, "seo_signal_detected"
        return 0.0, "no_signal"

    async def search_volume_bonus(self, domain: str) -> tuple[float, str]:
        if not self.cfg.search_volume_api_key:
            LOGGER.debug("Skipping Search Volume Check - No API Key")
            return 0.0, "skipped_no_key"
        if not self.cfg.search_volume_api_url:
            LOGGER.debug("Skipping Search Volume Check - No API URL")
            return 0.0, "skipped_no_url"

        keyword = domain.split(".", 1)[0]
        try:
            data = await self._request_json_with_retry(
                "GET",
                (self.cfg.search_volume_api_url,),
                headers=self._headers(self.cfg.search_volume_api_key),
                params={"keyword": keyword},
                context_label="Search Volume API",
                suppress_on_4xx=True,
            )
        except Exception as exc:
            LOGGER.debug("Skipping Search Volume Check - %s", exc)
            return 0.0, "request_failed"
        if data is None:
            return 0.0, "skipped_4xx"
        if not isinstance(data, dict):
            return 0.0, "invalid_payload"

        if "search_volume" in data:
            volume = parse_float(data.get("search_volume"))
        else:
            volume = parse_float(data.get("volume"))
        if volume is not None and volume > 0:
            return self.cfg.search_volume_bonus_points, "volume_signal_detected"
        return 0.0, "no_signal"

    async def historical_sales_bonus(self, domain: str) -> tuple[float, str]:
        if not self.cfg.namebio_api_key:
            LOGGER.debug("Skipping Historical Sales Check - No API Key")
            return 0.0, "skipped_no_key"
        if not self.cfg.namebio_api_url:
            LOGGER.debug("Skipping Historical Sales Check - No API URL")
            return 0.0, "skipped_no_url"

        keyword = domain.split(".", 1)[0]
        try:
            data = await self._request_json_with_retry(
                "GET",
                (self.cfg.namebio_api_url,),
                headers=self._headers(self.cfg.namebio_api_key),
                params={"keyword": keyword},
                context_label="Historical Sales API",
                suppress_on_4xx=True,
            )
        except Exception as exc:
            LOGGER.debug("Skipping Historical Sales Check - %s", exc)
            return 0.0, "request_failed"
        if data is None:
            return 0.0, "skipped_4xx"

        if not isinstance(data, dict):
            return 0.0, "invalid_payload"
        if "sales_count" in data:
            sales_count = parse_float(data.get("sales_count"))
        else:
            sales_count = parse_float(data.get("count"))
        has_sales = bool(sales_count is not None and sales_count > 0)
        if not has_sales and isinstance(data.get("sales"), list):
            has_sales = len(data["sales"]) > 0
        if has_sales:
            return self.cfg.historical_sales_bonus_points, "historical_sales_detected"
        return 0.0, "no_signal"


async def evaluate_opportunity(
    client: AtomClient,
    opportunity: DomainOpportunity,
    cfg: WatcherConfig,
) -> ValuationResult:
    try:
        ai_value = await client.appraise_with_atom_ai(opportunity.domain)
        method = "atom_ai"
        reason = "Atom Appraisal API"
        estimated = ai_value
        LOGGER.info("Valuation method=AI domain=%s estimated=$%.2f", opportunity.domain, estimated)
    except AppraisalUnavailableError as exc:
        estimated, rule_reason = score_with_internal_rules(opportunity.domain, cfg)
        method = "rule_based_fallback"
        reason = f"{rule_reason}; fallback_reason={exc}"
        LOGGER.warning(
            "Valuation method=FALLBACK domain=%s estimated=$%.2f reason=%s",
            opportunity.domain,
            estimated,
            exc,
        )

    bonus_total = 0.0
    bonus_parts: list[str] = []
    for check_name, bonus_fn in (
        ("seo", client.seo_backlinks_bonus),
        ("search_volume", client.search_volume_bonus),
        ("historical_sales", client.historical_sales_bonus),
    ):
        try:
            bonus, detail = await bonus_fn(opportunity.domain)
        except Exception as exc:
            LOGGER.debug("Skipping %s bonus check due to runtime error: %s", check_name, exc)
            continue
        if bonus > 0:
            bonus_total += bonus
            bonus_parts.append(f"{check_name}=+{bonus:.2f}({detail})")

    if bonus_total > 0:
        estimated += bonus_total
        reason = f"{reason}; bonus_total=+{bonus_total:.2f}; {', '.join(bonus_parts)}"
        LOGGER.info(
            "Applied optional bonuses domain=%s total_bonus=$%.2f details=%s",
            opportunity.domain,
            bonus_total,
            "; ".join(bonus_parts),
        )

    margin_usd = estimated - opportunity.ask_price_usd
    ratio = estimated / opportunity.ask_price_usd if opportunity.ask_price_usd > 0 else 0.0
    is_high_margin = margin_usd >= cfg.min_margin_usd and ratio >= cfg.min_margin_ratio

    return ValuationResult(
        estimated_value_usd=round(estimated, 2),
        method=method,
        reason=reason,
        margin_usd=round(margin_usd, 2),
        margin_ratio=round(ratio, 2),
        is_high_margin=is_high_margin,
    )


def format_alert(opportunity: DomainOpportunity, valuation: ValuationResult) -> str:
    domain = escape_md_v2(opportunity.domain)
    method = escape_md_v2(valuation.method)
    source = escape_md_v2(opportunity.source)
    listing_url = escape_md_v2(opportunity.listing_url)
    ask = escape_md_v2(f"${opportunity.ask_price_usd:.2f} {opportunity.currency}")
    estimate = escape_md_v2(f"${valuation.estimated_value_usd:.2f} USD")
    gap = escape_md_v2(f"${valuation.margin_usd:.2f}")
    ratio = escape_md_v2(f"x{valuation.margin_ratio:.2f}")

    return (
        "🔥 *High\\-Margin Domain Deal*\n"
        f"🌐 *Domain:* `{domain}`\n"
        f"🏪 *Source:* {source}\n"
        f"💵 *Asking Price:* {ask}\n"
        f"🧠 *Estimated Value:* {estimate}\n"
        f"📈 *Gap:* {gap} \\({ratio}\\)\n"
        f"⚙️ *Valuation Method:* `{method}`\n"
        f"🔗 *Listing:* {listing_url}"
    )


async def emit_alert(
    app: Application,
    chat_id: int,
    opportunity: DomainOpportunity,
    valuation: ValuationResult,
) -> None:
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🛒 Open Listing", url=opportunity.listing_url)],
            [InlineKeyboardButton("📊 Whois", url=opportunity.whois_url)],
        ]
    )
    await app.bot.send_message(
        chat_id=chat_id,
        text=format_alert(opportunity, valuation),
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def watch_events(app: Application, chat_id: int) -> None:
    cfg = WatcherConfig.from_env()
    store = AlertStore(cfg.db_path)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)

    LOGGER.info(
        "Starting Atom watcher (default_poll=%ss, eco=%ss, turbo=%ss, turbo_hours=%s, partnership=%s, appraisal=%s, proxy=%s, delay=%.2f-%.2fs)",
        cfg.poll_seconds,
        cfg.eco_poll_seconds,
        cfg.turbo_poll_seconds,
        cfg.turbo_hours_utc,
        bool(cfg.atom_partnership_url),
        bool(cfg.atom_appraisal_url),
        bool(cfg.proxy_url),
        cfg.human_delay_min_seconds,
        cfg.human_delay_max_seconds,
    )

    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = AtomClient(session, cfg)

        try:
            while True:
                now_utc = datetime.now(timezone.utc)
                in_turbo = is_turbo_hour(now_utc, cfg)
                poll_seconds = current_poll_seconds(now_utc, cfg)
                try:
                    opportunities = await client.fetch_partnership_domains()
                except Exception as exc:
                    LOGGER.exception("Failed to fetch partnership domains: %s", exc)
                    backoff_wait = max(poll_seconds, client.quota_backoff_remaining_seconds())
                    await asyncio.sleep(backoff_wait)
                    continue

                if not opportunities:
                    LOGGER.info("No partnership opportunities found this cycle.")

                for opportunity in opportunities:
                    if opportunity.tld not in cfg.allowed_tlds:
                        continue
                    if store.has_alerted(opportunity.domain):
                        continue

                    try:
                        valuation = await evaluate_opportunity(client, opportunity, cfg)
                    except Exception as exc:
                        LOGGER.exception("Failed to evaluate %s: %s", opportunity.domain, exc)
                        continue

                    if not valuation.is_high_margin:
                        continue

                    try:
                        await emit_alert(app, chat_id, opportunity, valuation)
                        store.mark_alerted(opportunity.domain, opportunity.source)
                        LOGGER.info(
                            "Alert sent domain=%s ask=$%.2f estimate=$%.2f method=%s",
                            opportunity.domain,
                            opportunity.ask_price_usd,
                            valuation.estimated_value_usd,
                            valuation.method,
                        )
                    except Exception as exc:
                        LOGGER.exception("Failed to send Telegram alert for %s: %s", opportunity.domain, exc)

                quota_wait = client.quota_backoff_remaining_seconds()
                next_wait = max(poll_seconds, quota_wait)
                LOGGER.info(
                    "Cycle complete mode=%s opportunities=%s next_poll=%ss quota_wait=%ss",
                    "turbo" if in_turbo else "eco",
                    len(opportunities),
                    poll_seconds,
                    quota_wait,
                )
                await asyncio.sleep(next_wait)
        except asyncio.CancelledError:
            LOGGER.info("Atom watcher cancelled.")
            raise
        finally:
            store.close()
