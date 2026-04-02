import json
import re
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup

MARKETPLACE_PRICE_RE = re.compile(r"(?:\$|usd\s*)?([0-9]+(?:\.[0-9]{1,2})?)", re.IGNORECASE)


class AftermarketSource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        source_name: str,
        page_url: str,
        allowed_tlds: set[str],
        max_listings: int,
    ) -> None:
        self.session = session
        self.source_name = source_name
        self.page_url = page_url
        self.max_listings = max(10, max_listings)
        tld_pattern = "|".join(sorted(tld.lstrip(".") for tld in allowed_tlds))
        self.domain_re = re.compile(rf"\b([a-z0-9-]+\.(?:{tld_pattern}))\b", re.IGNORECASE)

    async def fetch(self) -> list[dict]:
        async with self.session.get(self.page_url) as response:
            if response.status != 200:
                return []
            text = await response.text()
            content_type = (response.headers.get("Content-Type") or "").lower()

        if "json" in content_type:
            try:
                payload = json.loads(text)
            except (json.JSONDecodeError, TypeError, ValueError):
                payload = None
            if payload is not None:
                return self._parse_json(payload)
        return self._parse_html(text)

    def _parse_json(self, payload) -> list[dict]:
        candidate_rows: list[dict] = []
        if isinstance(payload, list):
            candidate_rows.extend(row for row in payload if isinstance(row, dict))
        elif isinstance(payload, dict):
            for key in ("items", "results", "listings", "domains", "data"):
                value = payload.get(key)
                if isinstance(value, list):
                    candidate_rows.extend(row for row in value if isinstance(row, dict))

        listings: list[dict] = []
        for row in candidate_rows[: self.max_listings]:
            domain = (
                row.get("domain")
                or row.get("domainName")
                or row.get("name")
                or ""
            )
            domain = str(domain).strip().lower()
            if not self.domain_re.search(domain):
                continue
            listings.append(
                {
                    "domain": domain,
                    "source": self.source_name,
                    "buy_url": str(row.get("url") or row.get("link") or self.page_url).strip(),
                    "price_usd": self._parse_price(
                        row.get("price")
                        or row.get("buyNowPrice")
                        or row.get("amount")
                        or row.get("currentBid")
                    ),
                    "currency": "USD",
                    "is_aftermarket": True,
                }
            )
        return listings

    def _parse_html(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        listings: list[dict] = []
        seen: set[str] = set()
        for row in soup.select("tr, li, article, .listing, .result, .domain"):
            text = row.get_text(" ", strip=True)
            match = self.domain_re.search(text)
            if not match:
                continue
            domain = match.group(1).lower()
            if domain in seen:
                continue
            seen.add(domain)
            href = ""
            anchor = row.find("a", href=True)
            if anchor:
                href = str(anchor["href"]).strip()
            listings.append(
                {
                    "domain": domain,
                    "source": self.source_name,
                    "buy_url": href or self.page_url,
                    "price_usd": self._parse_price(text),
                    "currency": "USD",
                    "is_aftermarket": True,
                }
            )
            if len(listings) >= self.max_listings:
                break
        return listings

    @staticmethod
    def _parse_price(value) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value)
        match = MARKETPLACE_PRICE_RE.search(text)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None
