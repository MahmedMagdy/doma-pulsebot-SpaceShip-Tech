import asyncio
import heapq
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
MIN_CIRCUIT_BREAKER_SECONDS = 30
JITTER_MIN_SECONDS = 0.15
JITTER_MAX_SECONDS = 0.85


class AppraisalUnavailableError(Exception):
    """Raised when Atom appraisal API is unavailable."""


class AtomCircuitOpenError(Exception):
    """Raised when Atom API calls are temporarily blocked by the circuit breaker."""


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
    circuit_breaker_failure_threshold: int = 4
    circuit_breaker_open_seconds: int = 120
    min_margin_usd: float = 20.0
    min_margin_ratio: float = 1.8
    allowed_tlds: set[str] = field(default_factory=lambda: {".dev", ".app", ".cloud"})
    keyword_value_usd: float = 22.0
    atom_partnership_url: str = ""
    atom_api_key: str = ""
    atom_user_id: str = ""
    atom_appraisal_url: str = ""
    atom_appraisal_key: str = ""
    atom_trademark_url: str = ""
    atom_trademark_key: str = ""
    proxy_url: str = ""
    human_delay_min_seconds: float = 0.8
    human_delay_max_seconds: float = 2.5

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
        partnership_url = os.getenv("ATOM_PARTNERSHIP_API_URL", "").strip()
        appraisal_url = os.getenv("ATOM_APPRAISAL_API_URL", "").strip()
        trademark_url = os.getenv("ATOM_TRADEMARK_API_URL", "").strip()
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
            circuit_breaker_failure_threshold=max(
                2,
                int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "4")),
            ),
            circuit_breaker_open_seconds=max(
                MIN_CIRCUIT_BREAKER_SECONDS,
                int(os.getenv("CIRCUIT_BREAKER_OPEN_SECONDS", "120")),
            ),
            min_margin_usd=float(os.getenv("ARBITRAGE_MIN_GAP_USD", "20")),
            min_margin_ratio=float(os.getenv("ARBITRAGE_MIN_RATIO", "1.8")),
            allowed_tlds=allowed_tlds or {".dev", ".app", ".cloud"},
            keyword_value_usd=float(os.getenv("KEYWORD_VALUE_USD", "22")),
            atom_partnership_url=partnership_url,
            atom_api_key=os.getenv("ATOM_API_KEY", "").strip(),
            atom_user_id=os.getenv("ATOM_USER_ID", "").strip(),
            atom_appraisal_url=appraisal_url,
            atom_appraisal_key=os.getenv("ATOM_APPRAISAL_KEY", "").strip(),
            atom_trademark_url=trademark_url,
            atom_trademark_key=os.getenv("ATOM_TRADEMARK_KEY", "").strip(),
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
            # same-hour token like "18-18" is treated as a one-hour window [18:00, 19:00)
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


def validate_required_atom_config(cfg: WatcherConfig) -> None:
    missing: list[str] = []
    if not cfg.atom_partnership_url:
        missing.append("ATOM_PARTNERSHIP_API_URL")
    if not cfg.atom_api_key:
        missing.append("ATOM_API_KEY")
    if not cfg.atom_user_id:
        missing.append("ATOM_USER_ID")
    if missing:
        missing_csv = ", ".join(missing)
        raise ValueError(f"Missing required Atom configuration: {missing_csv}")


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


def is_quota_exhaustion_error(exc: Exception) -> bool:
    message = str(exc)
    return "status=429" in message or "status=403" in message


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


def priority_score(opportunity: DomainOpportunity, cfg: WatcherConfig) -> float:
    """Return a heuristic score so highest-value candidates are processed first."""
    sld = opportunity.sld
    score = 0.0

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

    if opportunity.tld in cfg.allowed_tlds:
        score += 80.0
    if "-" in sld:
        score -= 35.0
    if any(ch.isdigit() for ch in sld):
        score -= 20.0

    return score


class AtomClient:
    def __init__(self, session: aiohttp.ClientSession, cfg: WatcherConfig) -> None:
        self.session = session
        self.cfg = cfg
        self._partnership_url = cfg.atom_partnership_url
        self._quota_backoff_until_monotonic = 0.0
        self._logged_trademark_config_warning = False

    def _headers(self, api_key: str) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["X-API-Key"] = api_key
        if self.cfg.atom_user_id:
            headers["X-User-Id"] = self.cfg.atom_user_id
        return headers

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
        headers: Optional[dict[str, str]] = None,
        params: Optional[dict[str, Any]] = None,
        json_payload: Optional[dict[str, Any]] = None,
        context_label: str,
        suppress_on_4xx: bool = False,
    ) -> Optional[Any]:
        if not url:
            return None
        circuit_wait = self.circuit_open_remaining_seconds()
        if circuit_wait > 0:
            raise AtomCircuitOpenError(
                f"{context_label} blocked by circuit breaker for {circuit_wait}s"
            )

        last_error: Optional[str] = None
        for attempt in range(1, self.cfg.max_retry_attempts + 1):
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
                        # 429 indicates temporary throttling; track cooldown then retry with backoff.
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

    async def fetch_partnership_domains(self) -> list[DomainOpportunity]:
        if not self._partnership_url:
            LOGGER.warning("ATOM_PARTNERSHIP_API_URL is not set; no domains fetched.")
            return []

        try:
            payload = await self._request_json_with_retry(
                "GET",
                self._partnership_urls,
                headers=self._headers(self.cfg.atom_partnership_api_key),
                context_label="Partnership API",
            )
        except AtomCircuitOpenError as exc:
            LOGGER.info("%s", exc)
            return []
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

        return opportunities

    async def appraise_with_atom_ai(self, domain: str) -> float:
        if not self.cfg.atom_appraisal_url:
            raise AppraisalUnavailableError("ATOM_APPRAISAL_API_URL is not set")
        if self.circuit_open_remaining_seconds() > 0:
            raise AppraisalUnavailableError(
                f"AI appraisal temporarily paused by circuit breaker ({self.circuit_open_remaining_seconds()}s)"
            )

        payload = {"domain": domain}
        data: Optional[Any] = None
        for attempt in range(1, self.cfg.max_retry_attempts + 1):
            await self._humanized_delay()
            try:
                async with self.session.post(
                    self.cfg.atom_appraisal_url,
                    headers=self._headers(self.cfg.atom_appraisal_key),
                    json=payload,
                    proxy=self.cfg.proxy_url or None,
                ) as response:
                    body_text = await response.text()

                    if response.status == 429:
                        self._note_rate_limit()
                        self._note_retryable_failure()
                        wait_seconds = self._backoff_seconds(attempt)
                        LOGGER.info("Appraisal API rate-limited (429); retrying in %.2fs", wait_seconds)
                        await asyncio.sleep(wait_seconds)
                        continue

                    if response.status != 200:
                        if 500 <= response.status < 600:
                            self._note_retryable_failure()
                            wait_seconds = self._backoff_seconds(attempt)
                            LOGGER.info(
                                "Appraisal API status=%s; retrying in %.2fs",
                                response.status,
                                wait_seconds,
                            )
                            await asyncio.sleep(wait_seconds)
                            continue
                        raise AppraisalUnavailableError(
                            f"AI appraisal error status={response.status}: {body_text[:240]}"
                        )

                    try:
                        # JSON parsed successfully; stop retrying and continue valuation flow.
                        data = await response.json(content_type=None)
                        self._note_success()
                        break
                    except Exception as exc:
                        raise AppraisalUnavailableError(
                            f"AI appraisal returned invalid JSON: {exc}"
                        ) from exc
            except aiohttp.ClientError as exc:
                self._note_retryable_failure()
                wait_seconds = self._backoff_seconds(attempt)
                LOGGER.info("Appraisal API network error: %s; retrying in %.2fs", exc, wait_seconds)
                await asyncio.sleep(wait_seconds)
        else:
            raise AppraisalUnavailableError(
                f"AI appraisal retries exhausted for {domain} after {self.cfg.max_retry_attempts} attempts"
            )

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

    async def passes_trademark_filter(self, domain: str) -> bool:
        if not self.cfg.atom_trademark_url:
            if not self._logged_trademark_config_warning:
                LOGGER.warning("Trademark API URL not configured - bypassing trademark filter")
                self._logged_trademark_config_warning = True
            return True

        payload = {"domain": domain}
        try:
            data = await self._request_json_with_retry(
                "POST",
                self.cfg.atom_trademark_url,
                headers=self._headers(self.cfg.atom_trademark_key),
                json_payload=payload,
                context_label="Trademark API",
            )
        except Exception as exc:
            if is_quota_exhaustion_error(exc):
                LOGGER.warning("Trademark quota exhausted - bypassing filter for %s", domain)
            else:
                LOGGER.warning("Trademark filter failed for %s - bypassing filter: %s", domain, exc)
            return True

        if data is None:
            return True

        if isinstance(data, dict):
            blocked = data.get("blocked")
            if isinstance(blocked, bool):
                return not blocked

            is_clear = data.get("is_clear")
            if isinstance(is_clear, bool):
                return is_clear

            conflict = data.get("has_conflict")
            if isinstance(conflict, bool):
                return not conflict

            status_text = str(
                data.get("status")
                or data.get("result")
                or data.get("decision")
                or data.get("trademark_status")
                or ""
            ).strip().lower()
            if status_text in {"clear", "approved", "pass", "ok", "safe"}:
                return True
            if status_text in {"blocked", "deny", "denied", "fail", "conflict", "infringing"}:
                return False

        return True

async def evaluate_opportunity(
    client: AtomClient,
    opportunity: DomainOpportunity,
    cfg: WatcherConfig,
) -> ValuationResult:
    method = "atom_ai"
    reason = "Atom Appraisal API"
    try:
        ai_value = await client.appraise_with_atom_ai(opportunity.domain)
        estimated = ai_value
        LOGGER.info("Valuation method=AI domain=%s estimated=$%.2f", opportunity.domain, estimated)
    except AppraisalUnavailableError as exc:
        if is_quota_exhaustion_error(exc):
            LOGGER.warning("Appraisal quota exhausted - bypassing filter for %s", opportunity.domain)
        else:
            LOGGER.warning("Appraisal failed for %s - bypassing filter: %s", opportunity.domain, exc)
        estimated = opportunity.ask_price_usd
        method = "bypass_no_appraisal"
        reason = "Appraisal unavailable; bypassed for fault tolerance"
    except Exception as exc:
        LOGGER.warning("Appraisal unexpected error for %s - bypassing filter: %s", opportunity.domain, exc)
        estimated = opportunity.ask_price_usd
        method = "bypass_no_appraisal"
        reason = "Appraisal unavailable; bypassed for fault tolerance"

    margin_usd = estimated - opportunity.ask_price_usd
    ratio = estimated / opportunity.ask_price_usd if opportunity.ask_price_usd > 0 else 0.0
    is_high_margin = method == "bypass_no_appraisal" or (
        margin_usd >= cfg.min_margin_usd and ratio >= cfg.min_margin_ratio
    )

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
    validate_required_atom_config(cfg)
    store = AlertStore(cfg.db_path)
    timeout = aiohttp.ClientTimeout(total=cfg.request_timeout_seconds)

    LOGGER.info(
        "Starting Atom watcher (default_poll=%ss, eco=%ss, turbo=%ss, turbo_hours=%s, partnership=%s, appraisal=%s, trademark=%s, user_id=%s, proxy=%s, delay=%.2f-%.2fs)",
        cfg.poll_seconds,
        cfg.eco_poll_seconds,
        cfg.turbo_poll_seconds,
        cfg.turbo_hours_utc,
        bool(cfg.atom_partnership_url),
        bool(cfg.atom_appraisal_url),
        bool(cfg.atom_trademark_url),
        bool(cfg.atom_user_id),
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
                    wait_seconds = max(poll_seconds, client.quota_backoff_remaining_seconds())
                    await asyncio.sleep(wait_seconds)
                    continue

                if not opportunities:
                    LOGGER.info("No partnership opportunities found this cycle.")

                priority_heap: list[tuple[float, str, DomainOpportunity]] = []
                for opportunity in opportunities:
                    if opportunity.tld not in cfg.allowed_tlds:
                        continue
                    if store.has_alerted(opportunity.domain):
                        continue
                    heapq.heappush(
                        priority_heap,
                        (
                            -priority_score(opportunity, cfg),
                            opportunity.domain,
                            opportunity,
                        ),
                    )

                evaluations_left = min(len(priority_heap), cfg.max_domains_per_cycle)
                while priority_heap and evaluations_left > 0:
                    _, _, opportunity = heapq.heappop(priority_heap)
                    evaluations_left -= 1

                    try:
                        valuation = await evaluate_opportunity(client, opportunity, cfg)
                    except Exception as exc:
                        LOGGER.exception("Failed to evaluate %s: %s", opportunity.domain, exc)
                        continue

                    if not valuation.is_high_margin:
                        continue

                    try:
                        passes_trademark = await client.passes_trademark_filter(opportunity.domain)
                    except Exception as exc:
                        LOGGER.warning(
                            "Trademark filter error for %s - bypassing filter: %s",
                            opportunity.domain,
                            exc,
                        )
                        passes_trademark = True
                    if not passes_trademark:
                        LOGGER.info("Trademark filter blocked domain=%s", opportunity.domain)
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
                breaker_wait = client.circuit_open_remaining_seconds()
                next_wait = max(poll_seconds, quota_wait, breaker_wait)
                LOGGER.info(
                    "Cycle complete mode=%s opportunities=%s next_poll=%ss quota_wait=%ss breaker_wait=%ss",
                    "turbo" if in_turbo else "eco",
                    len(opportunities),
                    poll_seconds,
                    quota_wait,
                    breaker_wait,
                )
                await asyncio.sleep(next_wait)
        except asyncio.CancelledError:
            LOGGER.info("Atom watcher cancelled.")
            raise
        finally:
            store.close()
