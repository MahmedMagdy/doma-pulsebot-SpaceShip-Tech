import asyncio
from functools import partial
import logging
import os
import random
import re
import sqlite3
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable, List, Optional, Sequence

import aiohttp
from bs4 import BeautifulSoup
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application
from aftermarket_watcher import AftermarketSource

LOGGER = logging.getLogger(__name__)
SPECIAL_CHARS = r"_*[]()~`>#+-=|{}.!"
GODADDY_MICRO_PRICE_THRESHOLD = 1000.0
GODADDY_MICRO_DIVISOR = 1_000_000.0


class RetryableAPIError(Exception):
    def __init__(self, message: str, retry_after_seconds: Optional[float] = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class EndpointNotFoundError(RuntimeError):
    pass


def parse_retry_after_seconds(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(text)
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt - now).total_seconds())
    except (TypeError, ValueError):
        return None


async def with_exponential_backoff(
    op,
    *,
    op_name: str,
    base_delay_seconds: float,
    cap_delay_seconds: float,
    max_retries: int,
):
    attempt = 0
    while True:
        try:
            return await op()
        except (
            RetryableAPIError,
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:
            if attempt >= max_retries:
                raise
            retry_after = (
                exc.retry_after_seconds
                if isinstance(exc, RetryableAPIError)
                else None
            )
            exponential = min(cap_delay_seconds, base_delay_seconds * (2 ** (attempt + 1)))
            jitter = random.uniform(0, base_delay_seconds)
            delay = max(exponential + jitter, retry_after or 0.0)
            LOGGER.warning(
                "%s failed (%s). Retrying in %.2fs (attempt %s/%s).",
                op_name,
                exc,
                delay,
                attempt + 1,
                max_retries,
            )
            await asyncio.sleep(delay)
            attempt += 1


def escape_md_v2(value: str) -> str:
    escaped = value
    for char in SPECIAL_CHARS:
        escaped = escaped.replace(char, f"\\{char}")
    return escaped


@dataclass(frozen=True)
class DomainListing:
    domain: str
    source: str
    buy_url: str
    price_usd: Optional[float] = None
    currency: str = "USD"
    is_drop: bool = False
    is_expired: bool = False
    is_aftermarket: bool = False
    keyword_match: Optional[str] = None

    @property
    def whois_url(self) -> str:
        return f"https://www.whois.com/whois/{self.domain}"

    @property
    def tld(self) -> str:
        _, _, ext = self.domain.rpartition(".")
        return f".{ext.lower()}" if ext else ""

    @property
    def sld(self) -> str:
        return self.domain.split(".", 1)[0].lower()


@dataclass(frozen=True)
class ArbitrageEvaluation:
    intrinsic_value_usd: float
    arbitrage_gap_usd: float
    arbitrage_ratio: float
    premium_keyword: Optional[str]
    hot_deal_reason: Optional[str]


@dataclass
class WatcherConfig:
    poll_seconds: int = 30
    allowed_tlds: set[str] = field(default_factory=lambda: {".dev", ".app", ".cloud", ".my"})
    max_sld_len: int = 14
    keywords: set[str] = field(
        default_factory=lambda: {
            "ai",
            "app",
            "bot",
            "cloud",
            "code",
            "data",
            "dev",
            "labs",
            "ml",
            "saas",
            "tech",
            "web",
        }
    )
    standard_reg_max: float = 15.0
    discount_trigger_pct: float = 50.0
    tld_value_weights: dict[str, float] = field(
        default_factory=lambda: {
            ".dev": 1.35,
            ".app": 1.25,
            ".cloud": 1.2,
            ".my": 1.15,
            ".com": 1.0,
        }
    )
    keyword_value_usd: float = 9.0
    max_short_name_bonus_usd: float = 12.0
    base_intrinsic_value_usd: float = 8.0
    arbitrage_min_gap_usd: float = 20.0
    arbitrage_min_ratio: float = 1.8
    db_path: str = "alerts.db"
    candidates_per_cycle: int = 20
    per_source_concurrency: int = 10
    batch_check_size: int = 50
    max_retries: int = 4
    backoff_base_seconds: float = 0.5
    backoff_cap_seconds: float = 8.0
    go_use_ote: bool = False
    namecheap_use_sandbox: bool = False
    namecom_base_url: str = "https://api.name.com"
    expired_domains_url: str = ""
    allow_unofficial_scraping: bool = False
    http_timeout_seconds: int = 20
    check_cache_ttl_seconds: int = 3600
    eco_mode_enabled: bool = True
    eco_poll_seconds: int = 120
    turbo_poll_seconds: int = 30
    turbo_hours_utc: set[int] = field(default_factory=lambda: {12, 13, 14, 15, 16, 17, 18, 19, 20, 21})
    eco_dead_hours_utc: set[int] = field(default_factory=lambda: {0, 1, 2, 3, 4, 5, 6})
    aftermarket_enabled: bool = True
    aftermarket_max_listings_per_source: int = 75
    namecheap_marketplace_url: str = "https://www.namecheap.com/domains/marketplace/"
    dynadot_aftermarket_url: str = "https://www.dynadot.com/market/"
    godaddy_closeouts_url: str = "https://www.godaddy.com/domains/aftermarket"
    dynadot_api_key: str = ""
    dynadot_base_url: str = "https://api.dynadot.com/api3.json"
    supplemental_words: list[str] = field(
        default_factory=lambda: [
            "byte",
            "build",
            "chip",
            "drive",
            "forge",
            "logic",
            "model",
            "stack",
            "sync",
            "token",
        ]
    )

    @classmethod
    def from_env(cls) -> "WatcherConfig":
        def parse_hours(raw: str, default: set[int]) -> set[int]:
            values: set[int] = set()
            for token in raw.split(","):
                token = token.strip()
                if not token:
                    continue
                if "-" in token:
                    left, _, right = token.partition("-")
                    try:
                        start = int(left)
                        end = int(right)
                    except ValueError:
                        continue
                    if start <= end:
                        for h in range(start, end + 1):
                            if 0 <= h <= 23:
                                values.add(h)
                    else:
                        for h in list(range(start, 24)) + list(range(0, end + 1)):
                            if 0 <= h <= 23:
                                values.add(h)
                    continue
                try:
                    hour = int(token)
                except ValueError:
                    continue
                if 0 <= hour <= 23:
                    values.add(hour)
            return values or set(default)

        def parse_tld_weights(raw: str, default: dict[str, float]) -> dict[str, float]:
            if not raw.strip():
                return dict(default)
            parsed: dict[str, float] = {}
            for pair in raw.split(","):
                left, sep, right = pair.partition(":")
                if not sep:
                    continue
                tld = left.strip().lower()
                if not tld:
                    continue
                if not tld.startswith("."):
                    tld = f".{tld}"
                try:
                    parsed[tld] = max(0.1, float(right.strip()))
                except ValueError:
                    continue
            return parsed or dict(default)

        keywords = {
            k.strip().lower()
            for k in os.getenv("DOMAIN_KEYWORDS", "").split(",")
            if k.strip()
        }
        tlds = {
            t.strip().lower() if t.strip().startswith(".") else f".{t.strip().lower()}"
            for t in os.getenv("ALLOWED_TLDS", ".dev,.app,.cloud,.my").split(",")
            if t.strip()
        }
        default_weights = {
            ".dev": 1.35,
            ".app": 1.25,
            ".cloud": 1.2,
            ".my": 1.15,
            ".com": 1.0,
        }
        legacy_go_candidates = os.getenv("GODADDY_CANDIDATES_PER_CYCLE", "").strip()
        if legacy_go_candidates and not os.getenv("CANDIDATES_PER_CYCLE", "").strip():
            LOGGER.warning(
                "GODADDY_CANDIDATES_PER_CYCLE is deprecated; use CANDIDATES_PER_CYCLE."
            )
        return cls(
            poll_seconds=int(os.getenv("WATCHER_POLL_SECONDS", "30")),
            allowed_tlds=tlds or {".dev", ".app", ".cloud", ".my"},
            max_sld_len=int(os.getenv("MAX_SLD_LENGTH", "14")),
            keywords=keywords
            or {
                "ai",
                "app",
                "bot",
                "cloud",
                "code",
                "data",
                "dev",
                "labs",
                "ml",
                "saas",
                "tech",
                "web",
            },
            standard_reg_max=float(os.getenv("STANDARD_REG_MAX_USD", "15")),
            discount_trigger_pct=float(os.getenv("DISCOUNT_TRIGGER_PERCENT", "50")),
            tld_value_weights=parse_tld_weights(
                os.getenv("TLD_VALUE_WEIGHTS", ""),
                default_weights,
            ),
            keyword_value_usd=float(os.getenv("KEYWORD_VALUE_USD", "9")),
            max_short_name_bonus_usd=float(os.getenv("MAX_SHORT_NAME_BONUS_USD", "12")),
            base_intrinsic_value_usd=float(os.getenv("BASE_INTRINSIC_VALUE_USD", "8")),
            arbitrage_min_gap_usd=float(os.getenv("ARBITRAGE_MIN_GAP_USD", "20")),
            arbitrage_min_ratio=float(os.getenv("ARBITRAGE_MIN_RATIO", "1.8")),
            db_path=os.getenv("ALERT_DB_PATH", "alerts.db"),
            candidates_per_cycle=int(
                os.getenv(
                    "CANDIDATES_PER_CYCLE",
                    legacy_go_candidates or "20",
                )
            ),
            per_source_concurrency=int(os.getenv("PER_SOURCE_CONCURRENCY", "10")),
            batch_check_size=int(os.getenv("BATCH_CHECK_SIZE", "50")),
            max_retries=int(os.getenv("API_MAX_RETRIES", "4")),
            backoff_base_seconds=float(os.getenv("BACKOFF_BASE_SECONDS", "0.5")),
            backoff_cap_seconds=float(os.getenv("BACKOFF_CAP_SECONDS", "8.0")),
            go_use_ote=os.getenv("GODADDY_USE_OTE", "false").lower() == "true",
            namecheap_use_sandbox=os.getenv("NAMECHEAP_USE_SANDBOX", "false").lower() == "true",
            namecom_base_url=os.getenv("NAMECOM_BASE_URL", "").strip() or "https://api.name.com",
            expired_domains_url=os.getenv("EXPIRED_DOMAINS_URL", "").strip(),
            allow_unofficial_scraping=os.getenv("ALLOW_UNOFFICIAL_SCRAPING", "false").lower() == "true",
            http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
            check_cache_ttl_seconds=int(os.getenv("CHECK_CACHE_TTL_SECONDS", "3600")),
            eco_mode_enabled=os.getenv("ECO_MODE_ENABLED", "true").lower() == "true",
            eco_poll_seconds=int(os.getenv("ECO_POLL_SECONDS", "120")),
            turbo_poll_seconds=int(os.getenv("TURBO_POLL_SECONDS", "30")),
            turbo_hours_utc=parse_hours(
                os.getenv("TURBO_HOURS_UTC", "12-21"),
                {12, 13, 14, 15, 16, 17, 18, 19, 20, 21},
            ),
            eco_dead_hours_utc=parse_hours(
                os.getenv("ECO_DEAD_HOURS_UTC", "0-6"),
                {0, 1, 2, 3, 4, 5, 6},
            ),
            aftermarket_enabled=os.getenv("AFTERMARKET_ENABLED", "true").lower() == "true",
            aftermarket_max_listings_per_source=int(
                os.getenv("AFTERMARKET_MAX_LISTINGS_PER_SOURCE", "75")
            ),
            namecheap_marketplace_url=os.getenv(
                "NAMECHEAP_MARKETPLACE_URL",
                "https://www.namecheap.com/domains/marketplace/",
            ).strip(),
            dynadot_aftermarket_url=os.getenv(
                "DYNADOT_AFTERMARKET_URL",
                "https://www.dynadot.com/market/",
            ).strip(),
            godaddy_closeouts_url=os.getenv(
                "GODADDY_CLOSEOUTS_URL",
                "https://www.godaddy.com/domains/aftermarket",
            ).strip(),
            dynadot_api_key=os.getenv("DYNADOT_API_KEY", "").strip(),
            dynadot_base_url=os.getenv("DYNADOT_BASE_URL", "https://api.dynadot.com/api3.json").strip(),
            supplemental_words=[
                w.strip().lower()
                for w in os.getenv(
                    "SUPPLEMENTAL_WORDS",
                    "byte,build,chip,drive,forge,logic,model,stack,sync,token",
                ).split(",")
                if w.strip()
            ],
        )


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


class DomainCheckCache:
    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = max(60, ttl_seconds)
        self._cache: dict[str, datetime] = {}

    def should_check(self, domain: str) -> bool:
        key = domain.lower()
        now = datetime.now(timezone.utc)
        expires_at = self._cache.get(key)
        if expires_at and expires_at > now:
            return False
        if expires_at and expires_at <= now:
            self._cache.pop(key, None)
        return True

    def mark_checked(self, domain: str) -> None:
        self._cache[domain.lower()] = datetime.now(timezone.utc) + timedelta(seconds=self.ttl_seconds)

    def prune(self) -> None:
        now = datetime.now(timezone.utc)
        expired = [k for k, v in self._cache.items() if v <= now]
        for key in expired:
            self._cache.pop(key, None)


class GoDaddyAvailabilitySource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        api_secret: str,
        use_ote: bool,
        max_retries: int,
        base_delay_seconds: float,
        cap_delay_seconds: float,
        per_source_concurrency: int,
    ) -> None:
        self.session = session
        self.api_key = api_key
        self.api_secret = api_secret
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.cap_delay_seconds = cap_delay_seconds
        self.per_source_concurrency = per_source_concurrency
        self._semaphore = asyncio.Semaphore(self.per_source_concurrency)
        self.base_url = (
            "https://api.ote-godaddy.com"
            if use_ote
            else "https://api.godaddy.com"
        )

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        headers = {"Authorization": f"sso-key {self.api_key}:{self.api_secret}"}

        async def guarded(domain: str) -> Optional[DomainListing]:
            async with self._semaphore:
                return await self._check_domain(domain, headers)

        tasks = [guarded(domain) for domain in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        listings: list[DomainListing] = []
        for result in results:
            if isinstance(result, DomainListing):
                listings.append(result)
            elif isinstance(result, Exception):
                LOGGER.warning("GoDaddy lookup failed: %s", result)
        return listings

    async def _check_domain(
        self, domain: str, headers: dict[str, str]
    ) -> Optional[DomainListing]:
        url = f"{self.base_url}/v1/domains/available"
        async def op() -> Optional[DomainListing]:
            async with self.session.get(
                url,
                headers=headers,
                params={"domain": domain, "checkType": "FAST"},
            ) as response:
                if response.status == 429:
                    raise RetryableAPIError(
                        "GoDaddy rate limited",
                        parse_retry_after_seconds(response.headers.get("Retry-After")),
                    )
                if response.status >= 500:
                    raise RetryableAPIError(f"GoDaddy server error: {response.status}")
                if response.status != 200:
                    body = await response.text()
                    raise RuntimeError(
                        f"GoDaddy API status={response.status} domain={domain} body={body[:300]}"
                    )
                payload = await response.json()
                if not payload.get("available"):
                    return None

                raw_price = payload.get("price")
                price_usd = None
                if isinstance(raw_price, (int, float)):
                    price_usd = float(raw_price)
                    # GoDaddy returns integer price in micros for many products.
                    # Example: 12990000 => 12.99 USD.
                    if price_usd > GODADDY_MICRO_PRICE_THRESHOLD:
                        price_usd = round(price_usd / GODADDY_MICRO_DIVISOR, 2)
                return DomainListing(
                    domain=domain.lower(),
                    source="GoDaddy",
                    buy_url=f"https://www.godaddy.com/domainsearch/find?domainToCheck={domain}",
                    price_usd=price_usd,
                    currency=payload.get("currency", "USD"),
                    is_drop=True,
                )

        return await with_exponential_backoff(
            op,
            op_name=f"GoDaddy check {domain}",
            base_delay_seconds=self.base_delay_seconds,
            cap_delay_seconds=self.cap_delay_seconds,
            max_retries=self.max_retries,
        )


class NamecheapAvailabilitySource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_user: str,
        api_key: str,
        username: str,
        client_ip: str,
        use_sandbox: bool,
        batch_check_size: int,
        max_retries: int,
        base_delay_seconds: float,
        cap_delay_seconds: float,
    ) -> None:
        self.session = session
        self.api_user = api_user
        self.api_key = api_key
        self.username = username
        self.client_ip = client_ip
        self.batch_check_size = batch_check_size
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.cap_delay_seconds = cap_delay_seconds
        self.base_url = (
            "https://api.sandbox.namecheap.com/xml.response"
            if use_sandbox
            else "https://api.namecheap.com/xml.response"
        )

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        if not candidates:
            return []
        deduped = list(dict.fromkeys(candidates))
        chunks = [
            deduped[idx : idx + self.batch_check_size]
            for idx in range(0, len(deduped), self.batch_check_size)
        ]
        tasks = [self._check_batch(batch) for batch in chunks if batch]
        if not tasks:
            return []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        listings: list[DomainListing] = []
        for result in results:
            if isinstance(result, list):
                listings.extend(result)
            elif isinstance(result, Exception):
                LOGGER.warning("Namecheap lookup failed: %s", result)
        return listings

    async def _check_batch(self, domains: Sequence[str]) -> list[DomainListing]:
        params = {
            "ApiUser": self.api_user,
            "ApiKey": self.api_key,
            "UserName": self.username,
            "ClientIp": self.client_ip,
            "Command": "namecheap.domains.check",
            "DomainList": ",".join(domains),
        }

        async def op() -> list[DomainListing]:
            async with self.session.get(self.base_url, params=params) as response:
                if response.status == 429:
                    raise RetryableAPIError(
                        "Namecheap rate limited",
                        parse_retry_after_seconds(response.headers.get("Retry-After")),
                    )
                if response.status >= 500:
                    raise RetryableAPIError(f"Namecheap server error: {response.status}")
                if response.status != 200:
                    body = await response.text()
                    raise RuntimeError(
                        f"Namecheap API status={response.status} body={body[:300]}"
                    )

                xml_text = await response.text()
                root = ET.fromstring(xml_text)
                listings: list[DomainListing] = []
                for node in root.iter():
                    if not node.tag.endswith("DomainCheckResult"):
                        continue
                    domain = (node.attrib.get("Domain") or "").strip().lower()
                    if not domain:
                        continue
                    if node.attrib.get("Available", "false").lower() != "true":
                        continue

                    price_usd = None
                    for key in (
                        "PremiumRegistrationPrice",
                        "RegistrationPrice",
                        "Price",
                    ):
                        raw = node.attrib.get(key)
                        if raw is None:
                            continue
                        try:
                            price_usd = float(raw)
                            break
                        except ValueError:
                            continue

                    listings.append(
                        DomainListing(
                            domain=domain,
                            source="Namecheap",
                            buy_url=f"https://www.namecheap.com/domains/registration/results/?domain={domain}",
                            price_usd=price_usd,
                            currency="USD",
                            is_drop=True,
                        )
                    )
                return listings

        return await with_exponential_backoff(
            op,
            op_name=f"Namecheap batch check ({len(domains)} domains)",
            base_delay_seconds=self.base_delay_seconds,
            cap_delay_seconds=self.cap_delay_seconds,
            max_retries=self.max_retries,
        )


class NameComAvailabilitySource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        username: str,
        token: str,
        base_url: str,
        batch_check_size: int,
        max_retries: int,
        base_delay_seconds: float,
        cap_delay_seconds: float,
    ) -> None:
        self.session = session
        self.auth = aiohttp.BasicAuth(login=username, password=token)
        self.base_url = base_url.rstrip("/")
        self.batch_check_size = batch_check_size
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.cap_delay_seconds = cap_delay_seconds

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        if not candidates:
            return []
        deduped = list(dict.fromkeys(candidates))
        chunks = [
            deduped[idx : idx + self.batch_check_size]
            for idx in range(0, len(deduped), self.batch_check_size)
        ]
        tasks = [self._check_batch(batch) for batch in chunks if batch]
        if not tasks:
            return []
        results = await asyncio.gather(*tasks, return_exceptions=True)
        listings: list[DomainListing] = []
        for result in results:
            if isinstance(result, list):
                listings.extend(result)
            elif isinstance(result, Exception):
                LOGGER.warning("Name.com lookup failed: %s", result)
        return listings

    async def _check_batch(self, domains: Sequence[str]) -> list[DomainListing]:
        endpoints = (
            "/v4/domains:checkAvailability",
            "/v4/domains/checkAvailability",
        )
        payload = {"domainNames": list(domains)}
        last_error: Optional[Exception] = None

        async def do_request(endpoint: str) -> list[DomainListing]:
            async with self.session.post(
                f"{self.base_url}{endpoint}",
                auth=self.auth,
                json=payload,
            ) as response:
                if response.status == 404:
                    raise EndpointNotFoundError(endpoint)
                if response.status == 429:
                    raise RetryableAPIError(
                        "Name.com rate limited",
                        parse_retry_after_seconds(response.headers.get("Retry-After")),
                    )
                if response.status >= 500:
                    raise RetryableAPIError(f"Name.com server error: {response.status}")
                if response.status in (401, 403):
                    body = await response.text()
                    raise RuntimeError(
                        f"Name.com authentication failed status={response.status} body={body[:300]}"
                    )
                if response.status != 200:
                    body = await response.text()
                    raise RuntimeError(
                        f"Name.com API status={response.status} body={body[:300]}"
                    )

                payload_json = await response.json()
                raw_rows = payload_json.get("results")
                if not isinstance(raw_rows, list):
                    raw_rows = [payload_json]
                listings: list[DomainListing] = []
                for row in raw_rows:
                    if not isinstance(row, dict):
                        continue
                    domain = (
                        row.get("domainName")
                        or row.get("domain")
                        or row.get("hostname")
                        or ""
                    ).strip().lower()
                    if not domain:
                        continue
                    available = row.get("purchasable")
                    if available is None:
                        available = row.get("available")
                    if not bool(available):
                        continue
                    price_usd = None
                    for key in ("purchasePrice", "price", "premiumPrice"):
                        raw = row.get(key)
                        if raw is None:
                            continue
                        try:
                            price_usd = float(raw)
                            break
                        except (TypeError, ValueError):
                            continue

                    listings.append(
                        DomainListing(
                            domain=domain,
                            source="Name.com",
                            buy_url=f"https://www.name.com/domain/search/{domain}",
                            price_usd=price_usd,
                            currency="USD",
                            is_drop=True,
                        )
                    )
                return listings

        for endpoint in endpoints:
            try:
                return await with_exponential_backoff(
                    partial(do_request, endpoint),
                    op_name=f"Name.com batch check ({len(domains)} domains)",
                    base_delay_seconds=self.base_delay_seconds,
                    cap_delay_seconds=self.cap_delay_seconds,
                    max_retries=self.max_retries,
                )
            except EndpointNotFoundError as exc:
                last_error = exc
                continue
            except (
                RetryableAPIError,
                aiohttp.ClientError,
                asyncio.TimeoutError,
                RuntimeError,
                ValueError,
                TypeError,
            ) as exc:
                last_error = exc
                break

        raise RuntimeError(
            "Name.com API endpoint unavailable: "
            f"{type(last_error).__name__}: {last_error}"
        )


class DynadotAvailabilitySource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        base_url: str,
        batch_check_size: int,
        max_retries: int,
        base_delay_seconds: float,
        cap_delay_seconds: float,
    ) -> None:
        self.session = session
        self.api_key = api_key
        self.base_url = base_url
        self.batch_check_size = batch_check_size
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.cap_delay_seconds = cap_delay_seconds

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        if not candidates:
            return []
        deduped = list(dict.fromkeys(candidates))
        chunks = [
            deduped[idx : idx + self.batch_check_size]
            for idx in range(0, len(deduped), self.batch_check_size)
        ]
        tasks = [self._check_batch(batch) for batch in chunks if batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        listings: list[DomainListing] = []
        for result in results:
            if isinstance(result, list):
                listings.extend(result)
            elif isinstance(result, Exception):
                LOGGER.warning("Dynadot lookup failed: %s", result)
        return listings

    async def _check_batch(self, domains: Sequence[str]) -> list[DomainListing]:
        params = {
            "key": self.api_key,
            "command": "search",
            "domain": ",".join(domains),
            "response": "json",
        }

        async def op() -> list[DomainListing]:
            async with self.session.get(self.base_url, params=params) as response:
                if response.status == 429:
                    raise RetryableAPIError(
                        "Dynadot rate limited",
                        parse_retry_after_seconds(response.headers.get("Retry-After")),
                    )
                if response.status >= 500:
                    raise RetryableAPIError(f"Dynadot server error: {response.status}")
                if response.status != 200:
                    body = await response.text()
                    raise RuntimeError(
                        f"Dynadot API status={response.status} body={body[:300]}"
                    )

                payload = await response.json(content_type=None)
                rows = payload.get("SearchResponse", {}).get("SearchResults", [])
                if not isinstance(rows, list):
                    rows = []
                listings: list[DomainListing] = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    domain = (row.get("DomainName") or row.get("domain") or "").strip().lower()
                    if not domain:
                        continue
                    available = row.get("Available")
                    if isinstance(available, str):
                        available = available.lower() in {"true", "yes", "1"}
                    if available is False:
                        continue
                    price_usd = None
                    for key in ("Price", "RegistrationPrice", "price"):
                        raw = row.get(key)
                        if raw is None:
                            continue
                        try:
                            price_usd = float(raw)
                            break
                        except (TypeError, ValueError):
                            continue
                    listings.append(
                        DomainListing(
                            domain=domain,
                            source="Dynadot",
                            buy_url=f"https://www.dynadot.com/domain/search.html?domain={domain}",
                            price_usd=price_usd,
                            currency="USD",
                            is_drop=True,
                        )
                    )
                return listings

        return await with_exponential_backoff(
            op,
            op_name=f"Dynadot batch check ({len(domains)} domains)",
            base_delay_seconds=self.base_delay_seconds,
            cap_delay_seconds=self.cap_delay_seconds,
            max_retries=self.max_retries,
        )


class ExpiredDomainsScraperSource:
    PRICE_RE = re.compile(r"\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")

    def __init__(
        self,
        session: aiohttp.ClientSession,
        page_url: str,
        allowed_tlds: set[str],
    ) -> None:
        self.session = session
        self.page_url = page_url
        tld_pattern = "|".join(sorted(tld.lstrip(".") for tld in allowed_tlds))
        self.domain_re = re.compile(rf"^[a-z0-9-]+\.(?:{tld_pattern})$", re.IGNORECASE)

    async def fetch(self) -> list[DomainListing]:
        async with self.session.get(self.page_url) as response:
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(
                    f"ExpiredDomains scrape failed status={response.status} body={body[:300]}"
                )
            html = await response.text()
        return self._parse_html(html)

    def _parse_html(self, html: str) -> list[DomainListing]:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("tr")
        listings: list[DomainListing] = []
        for row in rows:
            text_cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if not text_cells:
                continue
            domain = next((c for c in text_cells if self.domain_re.match(c)), None)
            if not domain:
                continue
            joined = " ".join(text_cells)
            match = self.PRICE_RE.search(joined)
            price = float(match.group(1)) if match else None
            listings.append(
                DomainListing(
                    domain=domain.lower(),
                    source="ExpiredDomains",
                    buy_url=self.page_url,
                    price_usd=price,
                    currency="USD",
                    is_expired=True,
                )
            )
        return listings


def keyword_hit(sld: str, keywords: Iterable[str]) -> Optional[str]:
    for kw in keywords:
        if kw and kw in sld:
            return kw
    return None


def is_allowed_listing(listing: DomainListing, cfg: WatcherConfig) -> bool:
    if listing.tld not in cfg.allowed_tlds:
        return False
    if len(listing.sld) > cfg.max_sld_len:
        return False
    return True


def evaluate_domain(listing: DomainListing, cfg: WatcherConfig) -> ArbitrageEvaluation:
    premium_kw = keyword_hit(listing.sld, cfg.keywords)
    tld_weight = cfg.tld_value_weights.get(listing.tld, 0.95)
    intrinsic = cfg.base_intrinsic_value_usd * tld_weight
    if premium_kw:
        intrinsic += cfg.keyword_value_usd
    shortness = max(0, cfg.max_sld_len - len(listing.sld))
    intrinsic += min(cfg.max_short_name_bonus_usd, shortness * 1.1)
    if listing.is_aftermarket:
        intrinsic *= 1.1

    if listing.price_usd is None or listing.price_usd <= 0:
        return ArbitrageEvaluation(
            intrinsic_value_usd=round(intrinsic, 2),
            arbitrage_gap_usd=0.0,
            arbitrage_ratio=0.0,
            premium_keyword=premium_kw,
            hot_deal_reason=None,
        )

    gap = intrinsic - listing.price_usd
    ratio = intrinsic / listing.price_usd
    discount_floor_price = cfg.standard_reg_max * (1 - cfg.discount_trigger_pct / 100)
    is_hot = gap >= cfg.arbitrage_min_gap_usd and ratio >= cfg.arbitrage_min_ratio
    if not is_hot and premium_kw and listing.price_usd <= discount_floor_price:
        is_hot = True
    reason = None
    if is_hot:
        reason = (
            f"Arbitrage gap ${gap:.2f} "
            f"(intrinsic ${intrinsic:.2f} vs ask ${listing.price_usd:.2f}, x{ratio:.2f})"
        )
    return ArbitrageEvaluation(
        intrinsic_value_usd=round(intrinsic, 2),
        arbitrage_gap_usd=round(gap, 2),
        arbitrage_ratio=round(ratio, 2),
        premium_keyword=premium_kw,
        hot_deal_reason=reason,
    )


def format_alert(listing: DomainListing, hot_reason: Optional[str]) -> str:
    domain = escape_md_v2(listing.domain)
    source = escape_md_v2(listing.source)
    tld = escape_md_v2(listing.tld)
    if listing.price_usd is None:
        price = "N/A"
    else:
        price = escape_md_v2(f"${listing.price_usd:.2f} {listing.currency}")

    headline = "🔥 *HOT DEAL*" if hot_reason else "🚨 *Domain Opportunity*"
    deal_line = (
        f"\n⚡ {escape_md_v2(hot_reason)}" if hot_reason else ""
    )

    tags = []
    if listing.is_drop:
        tags.append("drop")
    if listing.is_expired:
        tags.append("expired")
    if listing.is_aftermarket:
        tags.append("aftermarket")
    tag_text = escape_md_v2(", ".join(tags) if tags else "market")

    return (
        f"{headline}\n"
        f"🌐 *Domain:* `{domain}`\n"
        f"🧩 *TLD:* `{tld}`\n"
        f"💰 *Price:* {price}\n"
        f"🏷 *Type:* {tag_text}\n"
        f"🏪 *Source:* {source}"
        f"{deal_line}"
    )


def build_candidates(cfg: WatcherConfig) -> list[str]:
    base_words = sorted(cfg.keywords) + cfg.supplemental_words
    short_words = [w for w in base_words if len(w) <= cfg.max_sld_len]
    if not short_words:
        short_words = ["ai", "dev", "app", "bot", "code"]
    allowed_tlds = tuple(cfg.allowed_tlds)

    domains = set()
    for _ in range(cfg.candidates_per_cycle):
        word = random.choice(short_words)
        tld = random.choice(allowed_tlds)
        domains.add(f"{word}{tld}")
    return list(domains)


async def emit_listing_alert(
    app: Application,
    chat_id: int,
    listing: DomainListing,
    evaluation: ArbitrageEvaluation,
) -> None:
    message = format_alert(listing, evaluation.hot_deal_reason)
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(f"🛒 Buy on {listing.source}", url=listing.buy_url)],
            [InlineKeyboardButton("📊 Check Whois", url=listing.whois_url)],
        ]
    )
    await app.bot.send_message(
        chat_id=chat_id,
        text=message,
        parse_mode=ParseMode.MARKDOWN_V2,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def watch_events(app: Application, chat_id: int) -> None:
    cfg = WatcherConfig.from_env()
    store = AlertStore(cfg.db_path)
    check_cache = DomainCheckCache(cfg.check_cache_ttl_seconds)
    timeout = aiohttp.ClientTimeout(total=cfg.http_timeout_seconds)

    go_key = os.getenv("GODADDY_API_KEY", "").strip()
    go_secret = os.getenv("GODADDY_API_SECRET", "").strip()
    have_godaddy = bool(go_key and go_secret)

    namecheap_api_user = os.getenv("NAMECHEAP_API_USER", "").strip()
    namecheap_api_key = os.getenv("NAMECHEAP_API_KEY", "").strip()
    namecheap_username = os.getenv("NAMECHEAP_USERNAME", "").strip()
    namecheap_client_ip = os.getenv("NAMECHEAP_CLIENT_IP", "").strip()
    have_namecheap = bool(
        namecheap_api_user
        and namecheap_api_key
        and namecheap_username
        and namecheap_client_ip
    )

    namecom_username = os.getenv("NAMECOM_USERNAME", "").strip()
    namecom_token = os.getenv("NAMECOM_TOKEN", "").strip()
    have_namecom = bool(namecom_username and namecom_token)
    have_dynadot = bool(cfg.dynadot_api_key)

    def get_poll_seconds() -> int:
        if not cfg.eco_mode_enabled:
            return cfg.poll_seconds
        utc_hour = datetime.now(timezone.utc).hour
        if utc_hour in cfg.turbo_hours_utc:
            return max(5, cfg.turbo_poll_seconds)
        if utc_hour in cfg.eco_dead_hours_utc:
            return max(cfg.poll_seconds, cfg.eco_poll_seconds)
        return cfg.poll_seconds

    LOGGER.info("Starting domain watcher with tlds=%s, max_sld_len=%s", cfg.allowed_tlds, cfg.max_sld_len)
    if not have_godaddy and not have_namecheap and not have_namecom and not have_dynadot:
        LOGGER.warning(
            "No official registrar API configured. Set GoDaddy, Namecheap, Name.com, and/or Dynadot credentials."
        )
    if cfg.expired_domains_url and not cfg.allow_unofficial_scraping:
        LOGGER.warning(
            "EXPIRED_DOMAINS_URL is set but unofficial scraping is disabled; skipping scraper source."
        )

    async with aiohttp.ClientSession(timeout=timeout) as session:
        go_source = (
            GoDaddyAvailabilitySource(
                session,
                go_key,
                go_secret,
                cfg.go_use_ote,
                cfg.max_retries,
                cfg.backoff_base_seconds,
                cfg.backoff_cap_seconds,
                cfg.per_source_concurrency,
            )
            if have_godaddy
            else None
        )
        namecheap_source = (
            NamecheapAvailabilitySource(
                session,
                namecheap_api_user,
                namecheap_api_key,
                namecheap_username,
                namecheap_client_ip,
                cfg.namecheap_use_sandbox,
                cfg.batch_check_size,
                cfg.max_retries,
                cfg.backoff_base_seconds,
                cfg.backoff_cap_seconds,
            )
            if have_namecheap
            else None
        )
        namecom_source = (
            NameComAvailabilitySource(
                session,
                namecom_username,
                namecom_token,
                cfg.namecom_base_url,
                cfg.batch_check_size,
                cfg.max_retries,
                cfg.backoff_base_seconds,
                cfg.backoff_cap_seconds,
            )
            if have_namecom
            else None
        )
        dynadot_source = (
            DynadotAvailabilitySource(
                session,
                cfg.dynadot_api_key,
                cfg.dynadot_base_url,
                cfg.batch_check_size,
                cfg.max_retries,
                cfg.backoff_base_seconds,
                cfg.backoff_cap_seconds,
            )
            if have_dynadot
            else None
        )
        exp_source = (
            ExpiredDomainsScraperSource(session, cfg.expired_domains_url, cfg.allowed_tlds)
            if cfg.expired_domains_url and cfg.allow_unofficial_scraping
            else None
        )
        aftermarket_sources: list[AftermarketSource] = []
        if cfg.aftermarket_enabled:
            if cfg.namecheap_marketplace_url:
                aftermarket_sources.append(
                    AftermarketSource(
                        session,
                        "Namecheap Marketplace",
                        cfg.namecheap_marketplace_url,
                        cfg.allowed_tlds,
                        cfg.aftermarket_max_listings_per_source,
                    )
                )
            if cfg.dynadot_aftermarket_url:
                aftermarket_sources.append(
                    AftermarketSource(
                        session,
                        "Dynadot Aftermarket",
                        cfg.dynadot_aftermarket_url,
                        cfg.allowed_tlds,
                        cfg.aftermarket_max_listings_per_source,
                    )
                )
            if cfg.godaddy_closeouts_url:
                aftermarket_sources.append(
                    AftermarketSource(
                        session,
                        "GoDaddy Closeouts",
                        cfg.godaddy_closeouts_url,
                        cfg.allowed_tlds,
                        cfg.aftermarket_max_listings_per_source,
                    )
                )

        try:
            cycle = 0
            while True:
                listings: list[DomainListing] = []
                tasks: list[asyncio.Task] = []
                check_cache.prune()

                provider_sources: list[tuple[str, object]] = []
                if go_source:
                    provider_sources.append(("GoDaddy", go_source))
                if namecheap_source:
                    provider_sources.append(("Namecheap", namecheap_source))
                if namecom_source:
                    provider_sources.append(("Name.com", namecom_source))
                if dynadot_source:
                    provider_sources.append(("Dynadot", dynadot_source))

                if provider_sources:
                    candidates = [d for d in build_candidates(cfg) if check_cache.should_check(d)]
                    if candidates:
                        offset = cycle % len(provider_sources)
                        rotated = provider_sources[offset:] + provider_sources[:offset]
                        shards: dict[str, list[str]] = {name: [] for name, _ in rotated}
                        for idx, domain in enumerate(candidates):
                            provider_name, _ = rotated[idx % len(rotated)]
                            shards[provider_name].append(domain)
                            check_cache.mark_checked(domain)
                        source_map = {name: src for name, src in rotated}
                        for provider_name, provider_candidates in shards.items():
                            if provider_candidates:
                                tasks.append(
                                    asyncio.create_task(source_map[provider_name].fetch(provider_candidates))
                                )
                if exp_source:
                    tasks.append(asyncio.create_task(exp_source.fetch()))
                for source in aftermarket_sources:
                    tasks.append(asyncio.create_task(source.fetch()))

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, list):
                            if result and isinstance(result[0], dict):
                                normalized: list[DomainListing] = []
                                for row in result:
                                    try:
                                        domain = str(row.get("domain", "")).strip().lower()
                                        source = str(row.get("source", "Aftermarket")).strip() or "Aftermarket"
                                        buy_url = str(row.get("buy_url", "")).strip() or "https://www.google.com/search?q=domain+marketplace"
                                        raw_price = row.get("price_usd")
                                        price_usd = float(raw_price) if raw_price is not None else None
                                        currency = str(row.get("currency", "USD")).strip() or "USD"
                                        normalized.append(
                                            DomainListing(
                                                domain=domain,
                                                source=source,
                                                buy_url=buy_url,
                                                price_usd=price_usd,
                                                currency=currency,
                                                is_aftermarket=bool(row.get("is_aftermarket", False)),
                                            )
                                        )
                                    except (TypeError, ValueError, AttributeError):
                                        continue
                                listings.extend(normalized)
                            else:
                                listings.extend(result)
                        elif isinstance(result, Exception):
                            LOGGER.warning("Watcher source error: %s", result)

                for listing in listings:
                    if not is_allowed_listing(listing, cfg):
                        continue
                    evaluation = evaluate_domain(listing, cfg)
                    if evaluation.premium_keyword:
                        listing = replace(listing, keyword_match=evaluation.premium_keyword)
                    if not listing.keyword_match and not evaluation.hot_deal_reason:
                        continue

                    if store.has_alerted(listing.domain):
                        continue

                    await emit_listing_alert(app, chat_id, listing, evaluation)
                    store.mark_alerted(listing.domain, listing.source)

                cycle += 1
                await asyncio.sleep(get_poll_seconds())
        except asyncio.CancelledError:
            LOGGER.info("Domain watcher cancelled.")
            raise
        finally:
            store.close()
