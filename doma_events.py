import asyncio
from functools import partial
import logging
import os
import random
import re
import sqlite3
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Iterable, Optional, Sequence

import aiohttp
from bs4 import BeautifulSoup
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application

LOGGER = logging.getLogger(__name__)
SPECIAL_CHARS = r"_*[]()~`>#+-=|{}.!"
GODADDY_MICRO_PRICE_THRESHOLD = 1000.0
GODADDY_MICRO_DIVISOR = 1_000_000.0
MIN_ULTRA_DISCOUNT_PRICE = 0.5
ULTRA_DISCOUNT_MULTIPLIER = 0.3
ULTRA_DISCOUNT_SCORE = 20
DEEP_DISCOUNT_SCORE = 12
REGULAR_DISCOUNT_SCORE = 6
KEYWORD_SCORE_MAX = 50
KEYWORD_BASE_SCORE = 20
KEYWORD_LEN_MULTIPLIER = 3
KEYWORD_EDGE_BONUS = 5
KEYWORD_COMPONENT_CAP = 45
LENGTH_COMPONENT_MAX = 30
OPTIMAL_SLD_LENGTH = 6
SHORT_LENGTH_UPPER_BOUND = 12
LONG_NAME_BASE = 8
SHORT_NAME_PENALTY = 4
LONG_NAME_PENALTY = 2
DROP_SIGNAL_SCORE = 5
EXPIRED_SIGNAL_SCORE = 5


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


def dedupe_domains(domains: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(domains))


def apply_negative_cache_updates(
    cache: "TTLDomainCache", requested_domains: Sequence[str], available_domains: set[str]
) -> None:
    requested = {d.lower() for d in requested_domains}
    for domain in requested - available_domains:
        cache.put(domain)
    for domain in available_domains:
        cache.delete(domain)


@dataclass(frozen=True)
class DomainListing:
    domain: str
    source: str
    buy_url: str
    price_usd: Optional[float] = None
    currency: str = "USD"
    is_drop: bool = False
    is_expired: bool = False
    keyword_match: Optional[str] = None
    confidence_score: Optional[int] = None

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


@dataclass
class WatcherConfig:
    poll_seconds: int = 30
    eco_poll_seconds: int = 30
    turbo_poll_seconds: int = 12
    turbo_hours_utc: set[int] = field(default_factory=lambda: set(range(13, 23)))
    allowed_tlds: set[str] = field(default_factory=lambda: {".app", ".dev", ".com"})
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
    negative_cache_ttl_seconds: int = 300
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
        poll_seconds = int(os.getenv("WATCHER_POLL_SECONDS", "30"))
        eco_poll_seconds = int(os.getenv("ECO_POLL_SECONDS", str(poll_seconds)))
        turbo_poll_seconds = int(
            os.getenv(
                "TURBO_POLL_SECONDS",
                str(max(5, eco_poll_seconds // 2)),
            )
        )
        turbo_hours_raw = os.getenv("TURBO_HOURS_UTC", "13,14,15,16,17,18,19,20,21,22")
        turbo_hours_utc: set[int] = set()
        for hour in turbo_hours_raw.split(","):
            text = hour.strip()
            if not text.isdigit():
                if text:
                    LOGGER.warning("Ignoring invalid TURBO_HOURS_UTC value: %s", text)
                continue
            parsed_hour = int(text)
            if 0 <= parsed_hour <= 23:
                turbo_hours_utc.add(parsed_hour)
            else:
                LOGGER.warning("Ignoring out-of-range TURBO_HOURS_UTC hour: %s", text)
        keywords = {
            k.strip().lower()
            for k in os.getenv("DOMAIN_KEYWORDS", "").split(",")
            if k.strip()
        }
        tlds = {
            t.strip().lower() if t.strip().startswith(".") else f".{t.strip().lower()}"
            for t in os.getenv("ALLOWED_TLDS", ".app,.dev,.com").split(",")
            if t.strip()
        }
        legacy_go_candidates = os.getenv("GODADDY_CANDIDATES_PER_CYCLE", "").strip()
        if legacy_go_candidates and not os.getenv("CANDIDATES_PER_CYCLE", "").strip():
            LOGGER.warning(
                "GODADDY_CANDIDATES_PER_CYCLE is deprecated; use CANDIDATES_PER_CYCLE."
            )
        return cls(
            poll_seconds=poll_seconds,
            eco_poll_seconds=max(5, eco_poll_seconds),
            turbo_poll_seconds=max(3, turbo_poll_seconds),
            turbo_hours_utc=turbo_hours_utc or set(range(13, 23)),
            allowed_tlds=tlds or {".app", ".dev", ".com"},
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
            negative_cache_ttl_seconds=int(os.getenv("NEGATIVE_CACHE_TTL_SECONDS", "300")),
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


class TTLDomainCache:
    def __init__(self, ttl_seconds: int) -> None:
        self.ttl_seconds = max(1, ttl_seconds)
        self._expiry: dict[str, float] = {}

    def contains(self, domain: str) -> bool:
        key = domain.lower()
        now = time.monotonic()
        exp = self._expiry.get(key)
        if exp is None:
            return False
        if exp <= now:
            self._expiry.pop(key, None)
            return False
        return True

    def put(self, domain: str) -> None:
        self._expiry[domain.lower()] = time.monotonic() + self.ttl_seconds

    def delete(self, domain: str) -> None:
        self._expiry.pop(domain.lower(), None)


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
        negative_cache_ttl_seconds: int,
    ) -> None:
        self.session = session
        self.api_key = api_key
        self.api_secret = api_secret
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.cap_delay_seconds = cap_delay_seconds
        self.per_source_concurrency = per_source_concurrency
        self._semaphore = asyncio.Semaphore(self.per_source_concurrency)
        self._negative_cache = TTLDomainCache(ttl_seconds=negative_cache_ttl_seconds)
        self.base_url = (
            "https://api.ote-godaddy.com"
            if use_ote
            else "https://api.godaddy.com"
        )

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        headers = {"Authorization": f"sso-key {self.api_key}:{self.api_secret}"}
        deduped = dedupe_domains(candidates)
        eligible = [domain for domain in deduped if not self._negative_cache.contains(domain)]
        if not eligible:
            return []

        async def guarded(domain: str) -> Optional[DomainListing]:
            async with self._semaphore:
                return await self._check_domain(domain, headers)

        tasks = [guarded(domain) for domain in eligible]
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
                    self._negative_cache.put(domain)
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

        listing = await with_exponential_backoff(
            op,
            op_name=f"GoDaddy check {domain}",
            base_delay_seconds=self.base_delay_seconds,
            cap_delay_seconds=self.cap_delay_seconds,
            max_retries=self.max_retries,
        )
        if listing is None:
            self._negative_cache.put(domain)
        else:
            self._negative_cache.delete(domain)
        return listing


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
        negative_cache_ttl_seconds: int,
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
        self._negative_cache = TTLDomainCache(ttl_seconds=negative_cache_ttl_seconds)
        self.base_url = (
            "https://api.sandbox.namecheap.com/xml.response"
            if use_sandbox
            else "https://api.namecheap.com/xml.response"
        )

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        if not candidates:
            return []
        deduped = dedupe_domains(candidates)
        eligible = [domain for domain in deduped if not self._negative_cache.contains(domain)]
        if not eligible:
            return []
        chunks = [
            eligible[idx : idx + self.batch_check_size]
            for idx in range(0, len(eligible), self.batch_check_size)
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
                available_domains: set[str] = set()
                for node in root.iter():
                    if not node.tag.endswith("DomainCheckResult"):
                        continue
                    domain = (node.attrib.get("Domain") or "").strip().lower()
                    if not domain:
                        continue
                    if node.attrib.get("Available", "false").lower() != "true":
                        continue
                    available_domains.add(domain)

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
                apply_negative_cache_updates(self._negative_cache, domains, available_domains)
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
        negative_cache_ttl_seconds: int,
    ) -> None:
        self.session = session
        self.auth = aiohttp.BasicAuth(login=username, password=token)
        self.base_url = base_url.rstrip("/")
        self.batch_check_size = batch_check_size
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.cap_delay_seconds = cap_delay_seconds
        self._negative_cache = TTLDomainCache(ttl_seconds=negative_cache_ttl_seconds)

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        if not candidates:
            return []
        deduped = dedupe_domains(candidates)
        eligible = [domain for domain in deduped if not self._negative_cache.contains(domain)]
        if not eligible:
            return []
        chunks = [
            eligible[idx : idx + self.batch_check_size]
            for idx in range(0, len(eligible), self.batch_check_size)
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
                available_domains: set[str] = set()
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
                    available_domains.add(domain)
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
                apply_negative_cache_updates(self._negative_cache, domains, available_domains)
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


def build_hot_deal_reason(listing: DomainListing, cfg: WatcherConfig) -> Optional[str]:
    if listing.price_usd is None:
        return None
    premium_kw = keyword_hit(listing.sld, cfg.keywords)
    if premium_kw and listing.price_usd <= cfg.standard_reg_max:
        return f"Premium keyword '{premium_kw}' at near-reg fee"
    if listing.price_usd <= cfg.standard_reg_max * (1 - cfg.discount_trigger_pct / 100):
        return "Massive discount detected"
    return None


def compute_confidence_score(
    listing: DomainListing, cfg: WatcherConfig, keyword: Optional[str] = None
) -> int:
    score = 0

    keyword = keyword or listing.keyword_match or keyword_hit(listing.sld, cfg.keywords)
    if keyword:
        kw_strength = min(KEYWORD_COMPONENT_CAP, KEYWORD_BASE_SCORE + len(keyword) * KEYWORD_LEN_MULTIPLIER)
        if listing.sld.startswith(keyword) or listing.sld.endswith(keyword):
            kw_strength += KEYWORD_EDGE_BONUS
        score += min(KEYWORD_SCORE_MAX, kw_strength)

    sld_len = len(listing.sld)
    if sld_len <= SHORT_LENGTH_UPPER_BOUND:
        length_score = max(0, LENGTH_COMPONENT_MAX - abs(sld_len - OPTIMAL_SLD_LENGTH) * SHORT_NAME_PENALTY)
    else:
        length_score = max(0, LONG_NAME_BASE - (sld_len - SHORT_LENGTH_UPPER_BOUND) * LONG_NAME_PENALTY)
    score += min(LENGTH_COMPONENT_MAX, length_score)

    if listing.price_usd is not None and listing.price_usd >= 0:
        # Keep a fixed low floor and a dynamic baseline to avoid over-scoring tiny absolute deltas
        # when standard registrar pricing is already very low.
        if listing.price_usd <= max(MIN_ULTRA_DISCOUNT_PRICE, cfg.standard_reg_max * ULTRA_DISCOUNT_MULTIPLIER):
            score += ULTRA_DISCOUNT_SCORE
        elif listing.price_usd <= cfg.standard_reg_max * (1 - cfg.discount_trigger_pct / 100):
            score += DEEP_DISCOUNT_SCORE
        elif listing.price_usd <= cfg.standard_reg_max:
            score += REGULAR_DISCOUNT_SCORE

    if listing.is_drop:
        score += DROP_SIGNAL_SCORE
    if listing.is_expired:
        score += EXPIRED_SIGNAL_SCORE

    return max(0, min(100, int(score)))


def get_poll_interval_seconds(cfg: WatcherConfig, now_utc: Optional[datetime] = None) -> int:
    now = now_utc or datetime.now(timezone.utc)
    hour = now.hour
    if hour in cfg.turbo_hours_utc:
        return max(3, cfg.turbo_poll_seconds)
    return max(5, cfg.eco_poll_seconds)


def format_alert(listing: DomainListing, hot_reason: Optional[str]) -> str:
    domain = escape_md_v2(listing.domain)
    source = escape_md_v2(listing.source)
    tld = escape_md_v2(listing.tld)
    if listing.price_usd is None:
        price = "N/A"
    else:
        price = escape_md_v2(f"${listing.price_usd:.2f} {listing.currency}")
    confidence = escape_md_v2(
        f"{listing.confidence_score if listing.confidence_score is not None else 0}/100"
    )

    headline = "🔥 *HOT DEAL*" if hot_reason else "🚨 *Domain Opportunity*"
    deal_line = (
        f"\n⚡ {escape_md_v2(hot_reason)}" if hot_reason else ""
    )

    tags = []
    if listing.is_drop:
        tags.append("drop")
    if listing.is_expired:
        tags.append("expired")
    tag_text = escape_md_v2(", ".join(tags) if tags else "market")

    return (
        f"{headline}\n"
        f"🌐 *Domain:* `{domain}`\n"
        f"🧩 *TLD:* `{tld}`\n"
        f"💰 *Price:* {price}\n"
        f"🎯 *Confidence:* `{confidence}`\n"
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


async def emit_listing_alert(app: Application, chat_id: int, listing: DomainListing, cfg: WatcherConfig) -> None:
    hot_reason = build_hot_deal_reason(listing, cfg)
    message = format_alert(listing, hot_reason)
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

    LOGGER.info("Starting domain watcher with tlds=%s, max_sld_len=%s", cfg.allowed_tlds, cfg.max_sld_len)
    if not have_godaddy and not have_namecheap and not have_namecom:
        LOGGER.warning(
            "No official registrar API configured. Set GoDaddy, Namecheap, and/or Name.com credentials."
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
                cfg.negative_cache_ttl_seconds,
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
                cfg.negative_cache_ttl_seconds,
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
                cfg.negative_cache_ttl_seconds,
            )
            if have_namecom
            else None
        )
        exp_source = (
            ExpiredDomainsScraperSource(session, cfg.expired_domains_url, cfg.allowed_tlds)
            if cfg.expired_domains_url and cfg.allow_unofficial_scraping
            else None
        )

        try:
            while True:
                listings: list[DomainListing] = []
                tasks: list[asyncio.Task] = []
                candidates_for_cycle = build_candidates(cfg)

                if go_source:
                    tasks.append(asyncio.create_task(go_source.fetch(list(candidates_for_cycle))))
                if namecheap_source:
                    tasks.append(asyncio.create_task(namecheap_source.fetch(list(candidates_for_cycle))))
                if namecom_source:
                    tasks.append(asyncio.create_task(namecom_source.fetch(list(candidates_for_cycle))))
                if exp_source:
                    tasks.append(asyncio.create_task(exp_source.fetch()))

                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, list):
                            listings.extend(result)
                        elif isinstance(result, Exception):
                            LOGGER.warning("Watcher source error: %s", result)

                for listing in listings:
                    if not is_allowed_listing(listing, cfg):
                        continue

                    kw = keyword_hit(listing.sld, cfg.keywords)
                    if not kw:
                        continue
                    score = compute_confidence_score(
                        replace(listing, keyword_match=kw),
                        cfg,
                        keyword=kw,
                    )
                    listing = replace(listing, keyword_match=kw, confidence_score=score)

                    if store.has_alerted(listing.domain):
                        continue

                    await emit_listing_alert(app, chat_id, listing, cfg)
                    store.mark_alerted(listing.domain, listing.source)

                poll_seconds = get_poll_interval_seconds(cfg)
                LOGGER.debug("Watcher sleeping for %ss", poll_seconds)
                await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            LOGGER.info("Domain watcher cancelled.")
            raise
        finally:
            store.close()
