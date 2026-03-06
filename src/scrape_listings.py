"""HTML listing/search page parsers for supported property sites."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse


Record = Dict[str, Any]


_SCRIPT_LD_JSON_RE = re.compile(
    r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)


def _clean_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = unescape(value)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _to_number(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    text = str(value)
    text = text.replace(",", "")
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group(0))


def _to_int(value: Any) -> Optional[int]:
    number = _to_number(value)
    if number is None:
        return None
    return int(number)


def _stable_listing_id(*parts: Any) -> str:
    joined = "|".join(str(part).strip().lower() for part in parts if part not in (None, ""))
    digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()
    return f"lst_{digest[:16]}"


@dataclass(frozen=True)
class SiteAdapter:
    site_name: str

    def matches_url(self, url: str) -> bool:
        parsed = urlparse(url)
        return self.site_name in parsed.netloc.lower()

    def can_parse_html(self, html: str) -> bool:
        return False

    def parse(self, url: str, html: str) -> List[Record]:
        raise NotImplementedError


class OnthehouseAdapter(SiteAdapter):
    def __init__(self) -> None:
        super().__init__(site_name="onthehouse")

    def can_parse_html(self, html: str) -> bool:
        lower = html.lower()
        return "onthehouse" in lower or "realestatelisting" in lower

    def parse(self, url: str, html: str) -> List[Record]:
        records: List[Record] = []

        for payload in _SCRIPT_LD_JSON_RE.findall(html):
            try:
                data = json.loads(_clean_text(payload))
            except json.JSONDecodeError:
                continue
            records.extend(self._records_from_ld_json(url, data))

        if records:
            return _dedupe_by_id(records)

        card_re = re.compile(
            r"<a[^>]+href=[\"'](?P<href>/property/[^\"']+)[\"'][^>]*>(?P<body>.*?)</a>",
            re.IGNORECASE | re.DOTALL,
        )
        for match in card_re.finditer(html):
            href = match.group("href")
            body = _clean_text(match.group("body"))
            listing_url = urljoin(url, href)
            price_match = re.search(r"\$[\d,]+", body)
            beds_match = re.search(r"(\d+)\s*bed", body, re.IGNORECASE)
            baths_match = re.search(r"(\d+)\s*bath", body, re.IGNORECASE)
            price = _to_int(price_match.group(0)) if price_match else None
            bedrooms = _to_int(beds_match.group(1)) if beds_match else None
            bathrooms = _to_int(baths_match.group(1)) if baths_match else None
            snippet = body[:240]
            records.append(
                {
                    "listing_id": _stable_listing_id(self.site_name, listing_url, snippet, price, bedrooms),
                    "url": listing_url,
                    "address": None,
                    "rent": price,
                    "price": price,
                    "bedrooms": bedrooms,
                    "bathrooms": bathrooms,
                    "size_sqft": None,
                    "listed_date": None,
                    "source_site": self.site_name,
                    "raw_snippet": snippet,
                }
            )

        return _dedupe_by_id(records)

    def _records_from_ld_json(self, source_url: str, payload: Any) -> List[Record]:
        objects: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            objects.append(payload)
            graph = payload.get("@graph")
            if isinstance(graph, list):
                objects.extend(obj for obj in graph if isinstance(obj, dict))
        elif isinstance(payload, list):
            objects.extend(obj for obj in payload if isinstance(obj, dict))

        records: List[Record] = []
        for obj in objects:
            type_name = str(obj.get("@type", "")).lower()
            if "realestatelisting" not in type_name and "residence" not in type_name:
                continue

            listing_url = obj.get("url") or source_url
            listing_url = urljoin(source_url, str(listing_url))
            address = _address_to_text(obj.get("address"))
            offers = obj.get("offers") if isinstance(obj.get("offers"), dict) else {}
            price = _to_int(offers.get("price") or obj.get("price"))
            bedrooms = _to_int(obj.get("numberOfBedrooms") or obj.get("bedrooms"))
            bathrooms = _to_number(obj.get("numberOfBathroomsTotal") or obj.get("bathrooms"))
            size = _to_number((obj.get("floorSize") or {}).get("value") if isinstance(obj.get("floorSize"), dict) else obj.get("floorSize"))
            listed_date = obj.get("datePosted")

            snippet_parts = [obj.get("name"), address, offers.get("priceCurrency"), offers.get("price")]
            snippet = _clean_text(" ".join(str(part) for part in snippet_parts if part not in (None, "")))[:240]

            records.append(
                {
                    "listing_id": _stable_listing_id(self.site_name, listing_url, address, price, bedrooms),
                    "url": listing_url,
                    "address": address or None,
                    "rent": price,
                    "price": price,
                    "bedrooms": bedrooms,
                    "bathrooms": bathrooms,
                    "size_sqft": size,
                    "listed_date": listed_date,
                    "source_site": self.site_name,
                    "raw_snippet": snippet,
                }
            )
        return records


class RealestateAdapter(SiteAdapter):
    def __init__(self) -> None:
        super().__init__(site_name="realestate")

    def can_parse_html(self, html: str) -> bool:
        return "realestate.com.au" in html.lower()

    def parse(self, url: str, html: str) -> List[Record]:
        return []


class DomainAdapter(SiteAdapter):
    def __init__(self) -> None:
        super().__init__(site_name="domain")

    def can_parse_html(self, html: str) -> bool:
        return "domain.com.au" in html.lower()

    def parse(self, url: str, html: str) -> List[Record]:
        return []


def _address_to_text(address: Any) -> str:
    if isinstance(address, str):
        return _clean_text(address)
    if isinstance(address, dict):
        fields = [
            address.get("streetAddress"),
            address.get("addressLocality"),
            address.get("addressRegion"),
            address.get("postalCode"),
        ]
        return _clean_text(", ".join(str(part) for part in fields if part not in (None, "")))
    return ""


def _dedupe_by_id(records: List[Record]) -> List[Record]:
    seen = set()
    output: List[Record] = []
    for row in records:
        lid = row.get("listing_id")
        if not lid or lid in seen:
            continue
        seen.add(lid)
        output.append(row)
    return output


ADAPTERS: List[SiteAdapter] = [
    OnthehouseAdapter(),
    RealestateAdapter(),
    DomainAdapter(),
]


def parse_listing_page(url: str, html: str) -> List[Record]:
    """Parse a listing/search HTML page into normalized records."""
    for adapter in ADAPTERS:
        if adapter.matches_url(url) or adapter.can_parse_html(html):
            return adapter.parse(url, html)
    return []
