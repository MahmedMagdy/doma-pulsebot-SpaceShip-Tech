import asyncio
import logging
import os
import random
import re
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence

import aiohttp
from bs4 import BeautifulSoup
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application

LOGGER = logging.getLogger(__name__)
SPECIAL_CHARS = r"_*[]()~`>#+-=|{}.!"
GODADDY_MICRO_PRICE_THRESHOLD = 1000.0
GODADDY_MICRO_DIVISOR = 1_000_000.0


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


@dataclass
class WatcherConfig:
    poll_seconds: int = 30
    allowed_tlds: set[str] = field(default_factory=lambda: {".app", ".dev", ".com"})
    max_sld_len: int = 5
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
    go_candidates_per_cycle: int = 20
    go_use_ote: bool = False
    expired_domains_url: str = ""
    http_timeout_seconds: int = 20
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
        return cls(
            poll_seconds=int(os.getenv("WATCHER_POLL_SECONDS", "30")),
            allowed_tlds=tlds or {".app", ".dev", ".com"},
            max_sld_len=int(os.getenv("MAX_SLD_LENGTH", "5")),
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
            go_candidates_per_cycle=int(os.getenv("GODADDY_CANDIDATES_PER_CYCLE", "20")),
            go_use_ote=os.getenv("GODADDY_USE_OTE", "false").lower() == "true",
            expired_domains_url=os.getenv("EXPIRED_DOMAINS_URL", "").strip(),
            http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
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


class GoDaddyAvailabilitySource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        api_secret: str,
        use_ote: bool,
    ) -> None:
        self.session = session
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = (
            "https://api.ote-godaddy.com"
            if use_ote
            else "https://api.godaddy.com"
        )

    async def fetch(self, candidates: Sequence[str]) -> list[DomainListing]:
        headers = {"Authorization": f"sso-key {self.api_key}:{self.api_secret}"}
        tasks = [self._check_domain(domain, headers) for domain in candidates]
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
        async with self.session.get(
            url,
            headers=headers,
            params={"domain": domain, "checkType": "FAST"},
        ) as response:
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
    for _ in range(cfg.go_candidates_per_cycle):
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

    LOGGER.info("Starting domain watcher with tlds=%s, max_sld_len=%s", cfg.allowed_tlds, cfg.max_sld_len)
    if not have_godaddy and not cfg.expired_domains_url:
        LOGGER.warning("No real data source configured. Set GoDaddy API keys and/or EXPIRED_DOMAINS_URL.")

    async with aiohttp.ClientSession(timeout=timeout) as session:
        go_source = (
            GoDaddyAvailabilitySource(session, go_key, go_secret, cfg.go_use_ote)
            if have_godaddy
            else None
        )
        exp_source = (
            ExpiredDomainsScraperSource(session, cfg.expired_domains_url, cfg.allowed_tlds)
            if cfg.expired_domains_url
            else None
        )

        try:
            while True:
                listings: list[DomainListing] = []
                tasks: list[asyncio.Task] = []

                if go_source:
                    candidates = build_candidates(cfg)
                    tasks.append(asyncio.create_task(go_source.fetch(candidates)))
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
                    listing = replace(listing, keyword_match=kw)

                    if store.has_alerted(listing.domain):
                        continue

                    await emit_listing_alert(app, chat_id, listing, cfg)
                    store.mark_alerted(listing.domain, listing.source)

                await asyncio.sleep(cfg.poll_seconds)
        except asyncio.CancelledError:
            LOGGER.info("Domain watcher cancelled.")
            raise
        finally:
            store.close()
