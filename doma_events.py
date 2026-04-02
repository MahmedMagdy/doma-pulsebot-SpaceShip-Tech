import asyncio
import logging
import os
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


class AppraisalUnavailableError(RuntimeError):
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
    request_timeout_seconds: int = 20
    db_path: str = "alerts.db"
    max_domains_per_cycle: int = 200
    min_margin_usd: float = 20.0
    min_margin_ratio: float = 1.8
    allowed_tlds: set[str] = field(default_factory=lambda: {".dev", ".app", ".cloud"})
    keyword_value_usd: float = 22.0
    atom_partnership_url: str = ""
    atom_partnership_api_key: str = ""
    atom_appraisal_url: str = ""
    atom_appraisal_api_key: str = ""
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
        return cls(
            poll_seconds=int(os.getenv("WATCHER_POLL_SECONDS", "30")),
            request_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "20")),
            db_path=os.getenv("ALERT_DB_PATH", "alerts.db"),
            max_domains_per_cycle=int(os.getenv("MAX_DOMAINS_PER_CYCLE", "200")),
            min_margin_usd=float(os.getenv("ARBITRAGE_MIN_GAP_USD", "20")),
            min_margin_ratio=float(os.getenv("ARBITRAGE_MIN_RATIO", "1.8")),
            allowed_tlds=allowed_tlds or {".dev", ".app", ".cloud"},
            keyword_value_usd=float(os.getenv("KEYWORD_VALUE_USD", "22")),
            atom_partnership_url=os.getenv("ATOM_PARTNERSHIP_API_URL", "").strip(),
            atom_partnership_api_key=os.getenv("ATOM_PARTNERSHIP_API_KEY", "").strip(),
            atom_appraisal_url=os.getenv("ATOM_APPRAISAL_API_URL", "").strip(),
            atom_appraisal_api_key=os.getenv("ATOM_APPRAISAL_API_KEY", "").strip(),
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
    match = re.search(r"-?[0-9]+(?:\.[0-9]+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


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

    def _headers(self, api_key: str) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["X-API-Key"] = api_key
        return headers

    async def fetch_partnership_domains(self) -> list[DomainOpportunity]:
        if not self.cfg.atom_partnership_url:
            LOGGER.warning("ATOM_PARTNERSHIP_API_URL is not set; no domains fetched.")
            return []

        async with self.session.get(
            self.cfg.atom_partnership_url,
            headers=self._headers(self.cfg.atom_partnership_api_key),
        ) as response:
            body = await response.text()
            if response.status != 200:
                raise RuntimeError(
                    f"Partnership API error status={response.status} body={body[:300]}"
                )
            try:
                payload = await response.json(content_type=None)
            except Exception as exc:
                raise RuntimeError(f"Partnership API returned invalid JSON: {exc}") from exc

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
        async with self.session.post(
            self.cfg.atom_appraisal_url,
            headers=self._headers(self.cfg.atom_appraisal_api_key),
            json=payload,
        ) as response:
            body_text = await response.text()
            lowered = body_text.lower()

            if response.status != 200:
                if any(
                    token in lowered
                    for token in (
                        "out of credits",
                        "rate limit exceeded",
                        "quota exhausted",
                        "quota",
                        "credits",
                        "rate limit",
                    )
                ) or response.status in {402, 403, 429}:
                    raise AppraisalUnavailableError(
                        f"AI appraisal unavailable (status={response.status}): {body_text[:240]}"
                    )
                raise AppraisalUnavailableError(
                    f"AI appraisal error status={response.status}: {body_text[:240]}"
                )

            try:
                data = await response.json(content_type=None)
            except Exception as exc:
                raise AppraisalUnavailableError(f"AI appraisal returned invalid JSON: {exc}") from exc

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
                details = data.get("message") or data.get("error") or data.get("detail")
                if details:
                    raise AppraisalUnavailableError(f"AI appraisal did not provide value: {details}")

        if value is None or value <= 0:
            raise AppraisalUnavailableError("AI appraisal did not provide a valid estimated value")

        return float(value)


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
        method = "fallback_manual"
        reason = f"{rule_reason}; fallback_reason={exc}"
        LOGGER.warning(
            "Valuation method=FALLBACK domain=%s estimated=$%.2f reason=%s",
            opportunity.domain,
            estimated,
            exc,
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
        "🔥 *High\-Margin Domain Deal*\n"
        f"🌐 *Domain:* `{domain}`\n"
        f"🏪 *Source:* {source}\n"
        f"💵 *Asking Price:* {ask}\n"
        f"🧠 *Estimated Value:* {estimate}\n"
        f"📈 *Gap:* {gap} \({ratio}\)\n"
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
        "Starting Atom watcher (poll=%ss, partnership=%s, appraisal=%s)",
        cfg.poll_seconds,
        bool(cfg.atom_partnership_url),
        bool(cfg.atom_appraisal_url),
    )

    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = AtomClient(session, cfg)

        try:
            while True:
                try:
                    opportunities = await client.fetch_partnership_domains()
                except Exception as exc:
                    LOGGER.exception("Failed to fetch partnership domains: %s", exc)
                    await asyncio.sleep(cfg.poll_seconds)
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

                await asyncio.sleep(cfg.poll_seconds)
        except asyncio.CancelledError:
            LOGGER.info("Atom watcher cancelled.")
            raise
        finally:
            store.close()
