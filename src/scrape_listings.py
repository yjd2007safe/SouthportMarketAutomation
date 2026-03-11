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

_NEXT_DATA_RE = re.compile(
    r"<script[^>]*id=[\"']__NEXT_DATA__[\"'][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)

_JSON_ASSIGNMENT_RE = re.compile(
    r"(?:window\.__INITIAL_STATE__|window\.__NEXT_DATA__)\s*=\s*(\{.*?\})\s*;",
    re.IGNORECASE | re.DOTALL,
)

_SCRIPT_TAG_RE = re.compile(r"<script[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)

_RESIDENTIAL_CARD_RE = re.compile(
    r'<article[^>]+data-testid=["\']ResidentialCard["\'][^>]*>(.*?)</article>',
    re.IGNORECASE | re.DOTALL,
)

_DOMAIN_LISTING_RE = re.compile(
    r'<li[^>]+data-testid=["\']listing-\d+["\'][^>]*>(.*?)</li>',
    re.IGNORECASE | re.DOTALL,
)

_CHALLENGE_MARKERS = {
    "kasada": (
        "kasada",
        "kpsdk",
        "_kpsdk_",
        "x-kpsdk",
    ),
    "incapsula": (
        "incapsula",
        "_incap_",
        "imperva",
        "incident id",
    ),
    "captcha": (
        "captcha",
        "g-recaptcha",
        "hcaptcha",
        "cf-chl-captcha",
        "are you human",
        "robot check",
    ),
}


def _extract_json_object_after_marker(text: str, marker: str) -> List[Any]:
    start = 0
    values: List[Any] = []
    while True:
        idx = text.find(marker, start)
        if idx < 0:
            break
        brace_idx = text.find("{", idx + len(marker))
        if brace_idx < 0:
            break

        depth = 0
        in_string = False
        escaped = False
        end_idx = -1
        for pos in range(brace_idx, len(text)):
            ch = text[pos]
            if in_string:
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    end_idx = pos + 1
                    break

        if end_idx > 0:
            try:
                values.append(json.loads(text[brace_idx:end_idx]))
            except json.JSONDecodeError:
                pass
            start = end_idx
        else:
            break

    return values


def _extract_json_states(html: str) -> List[Any]:
    payloads: List[Any] = []

    for block in _NEXT_DATA_RE.findall(html):
        try:
            payloads.append(json.loads(_clean_text(block)))
        except json.JSONDecodeError:
            continue

    for block in _JSON_ASSIGNMENT_RE.findall(html):
        try:
            payloads.append(json.loads(_clean_text(block)))
        except json.JSONDecodeError:
            continue

    for marker in (
        "window.__INITIAL_STATE__ =",
        "window.__NEXT_DATA__ =",
        "window.__NUXT__ =",
        "window.__APOLLO_STATE__ =",
    ):
        payloads.extend(_extract_json_object_after_marker(html, marker))

    for script_body in _SCRIPT_TAG_RE.findall(html):
        if "window.__INITIAL_STATE__" not in script_body and "window.__NUXT__" not in script_body:
            continue
        payloads.extend(_extract_json_object_after_marker(script_body, "="))

    return payloads


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


def _normalize_property_category(value: Any) -> Optional[str]:
    text = _clean_text(str(value or "")).lower()
    if not text:
        return None
    if any(token in text for token in ("townhouse", "town home", "town-home", "town house")):
        return "townhouse"
    if any(token in text for token in ("apartment", "unit", "flat")):
        return "apartment"
    if any(token in text for token in ("detached", "house", "single family", "single-family")):
        return "detached_house"
    return None


def _extract_area(value: Any, fallback_unit: str = "sqm") -> tuple[Optional[float], Optional[str]]:
    if value in (None, ""):
        return None, None
    unit = None
    number = None
    if isinstance(value, dict):
        number = _to_number(value.get("value") or value.get("size") or value.get("area"))
        raw_unit = value.get("unitCode") or value.get("unitText") or value.get("unit")
        unit = _clean_text(str(raw_unit or "")).lower() or None
    else:
        text = _clean_text(str(value)).lower()
        number = _to_number(text)
        if "sqft" in text or "ft²" in text or "ft2" in text:
            unit = "sqft"
        elif "sqm" in text or "m²" in text or "m2" in text:
            unit = "sqm"
    if number is None:
        return None, None
    return number, unit or fallback_unit


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

        for data in _extract_json_states(html):
            records.extend(self._records_from_next_data(url, data))

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
            bathrooms = _to_number(baths_match.group(1)) if baths_match else None
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
            records.append(self._record_from_obj(source_url, obj))
        return [row for row in records if row]

    def _records_from_next_data(self, source_url: str, payload: Any) -> List[Record]:
        records: List[Record] = []

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                looks_like_listing = any(
                    key in value
                    for key in (
                        "listingUrl",
                        "propertyUrl",
                        "address",
                        "price",
                        "bedrooms",
                        "bathrooms",
                        "weeklyPrice",
                        "displayAddress",
                    )
                )
                if looks_like_listing and any(k in value for k in ("listingUrl", "propertyUrl", "url")):
                    record = self._record_from_obj(source_url, value)
                    if record:
                        records.append(record)
                for nested in value.values():
                    walk(nested)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(payload)
        return records

    def _record_from_obj(self, source_url: str, obj: Dict[str, Any]) -> Record:
        listing_url = (
            obj.get("url")
            or obj.get("listingUrl")
            or obj.get("propertyUrl")
            or obj.get("canonicalUrl")
            or obj.get("href")
            or source_url
        )
        listing_url = urljoin(source_url, str(listing_url))

        address = _address_to_text(obj.get("address") or obj.get("displayAddress") or obj.get("fullAddress"))
        offers = obj.get("offers") if isinstance(obj.get("offers"), dict) else {}

        price = _to_int(
            offers.get("price")
            or obj.get("price")
            or obj.get("displayPrice")
            or obj.get("weeklyPrice")
            or obj.get("rent")
            or obj.get("weeklyRent")
        )
        bedrooms = _to_int(obj.get("numberOfBedrooms") or obj.get("bedrooms") or obj.get("beds"))
        bathrooms = _to_number(obj.get("numberOfBathroomsTotal") or obj.get("bathrooms") or obj.get("baths"))

        floor_size = obj.get("floorSize")
        building_area, building_area_unit = _extract_area(
            floor_size or obj.get("buildingArea") or obj.get("internalArea") or obj.get("size_sqft")
        )
        land_area, land_area_unit = _extract_area(obj.get("landSize") or obj.get("landArea"))
        size = building_area or land_area

        property_category = _normalize_property_category(
            obj.get("propertyType") or obj.get("propertyCategory") or obj.get("dwellingType")
        )

        listed_date = obj.get("datePosted") or obj.get("listedDate") or obj.get("dateListed")

        snippet_parts = [
            obj.get("name"),
            address,
            offers.get("priceCurrency"),
            offers.get("price"),
            obj.get("displayPrice"),
        ]
        snippet = _clean_text(" ".join(str(part) for part in snippet_parts if part not in (None, "")))[:240]

        return {
            "listing_id": _stable_listing_id(self.site_name, listing_url, address, price, bedrooms),
            "url": listing_url,
            "address": address or None,
            "rent": price,
            "price": price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "size_sqft": size,
            "property_category": property_category,
            "land_area": land_area,
            "land_area_unit": land_area_unit,
            "building_area": building_area,
            "building_area_unit": building_area_unit,
            "listed_date": listed_date,
            "source_site": self.site_name,
            "raw_snippet": snippet,
        }


class RealestateAdapter(SiteAdapter):
    def __init__(self) -> None:
        super().__init__(site_name="realestate")

    def can_parse_html(self, html: str) -> bool:
        return "realestate.com.au" in html.lower()

    def parse(self, url: str, html: str) -> List[Record]:
        records: List[Record] = []
        for payload in _SCRIPT_LD_JSON_RE.findall(html):
            try:
                data = json.loads(_clean_text(payload))
            except json.JSONDecodeError:
                continue
            records.extend(self._records_from_state(url, data))

        for data in _extract_json_states(html):
            records.extend(self._records_from_state(url, data))

        if not records:
            records.extend(self._records_from_html_cards(url, html))

        return _dedupe_by_id(records)

    def _records_from_state(self, source_url: str, payload: Any) -> List[Record]:
        records: List[Record] = []

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                listing_url = (
                    value.get("canonicalUrl")
                    or value.get("prettyDetailsUrl")
                    or value.get("detailsUrl")
                    or value.get("listingUrl")
                    or value.get("url")
                )
                is_candidate = bool(listing_url) and any(
                    key in value
                    for key in (
                        "address",
                        "displayAddress",
                        "price",
                        "priceText",
                        "beds",
                        "bedrooms",
                        "baths",
                        "bathrooms",
                    )
                )
                if is_candidate:
                    record = self._record_from_obj(source_url, value)
                    if record:
                        records.append(record)
                for nested in value.values():
                    walk(nested)
            elif isinstance(value, list):
                for item in value:
                    walk(item)

        walk(payload)
        return records

    def _record_from_obj(self, source_url: str, obj: Dict[str, Any]) -> Record:
        listing_url = (
            obj.get("canonicalUrl")
            or obj.get("prettyDetailsUrl")
            or obj.get("detailsUrl")
            or obj.get("listingUrl")
            or obj.get("url")
            or source_url
        )
        listing_url = urljoin(source_url, str(listing_url))

        address = _address_to_text(
            obj.get("address")
            or obj.get("displayAddress")
            or obj.get("streetAddress")
            or obj.get("fullAddress")
        )

        price = _to_int(
            obj.get("price")
            or obj.get("priceText")
            or obj.get("displayPrice")
            or obj.get("rentalPrice")
            or obj.get("weeklyPrice")
        )
        bedrooms = _to_int(obj.get("bedrooms") or obj.get("beds"))
        bathrooms = _to_number(obj.get("bathrooms") or obj.get("baths"))
        building_area, building_area_unit = _extract_area(
            obj.get("buildingArea") or obj.get("internalArea") or obj.get("size") or obj.get("size_sqft")
        )
        land_area, land_area_unit = _extract_area(obj.get("landSize") or obj.get("landArea"))
        size = building_area or land_area
        property_category = _normalize_property_category(
            obj.get("propertyType") or obj.get("propertyCategory") or obj.get("dwellingType")
        )
        listed_date = obj.get("dateListed") or obj.get("listingDate")

        snippet = _clean_text(
            " ".join(
                str(part)
                for part in (
                    obj.get("headline"),
                    address,
                    obj.get("priceText") or obj.get("displayPrice"),
                )
                if part not in (None, "")
            )
        )[:240]

        return self._build_record(
            source_url=source_url,
            listing_url=listing_url,
            address=address,
            price=price,
            bedrooms=bedrooms,
            bathrooms=bathrooms,
            size=size,
            property_category=property_category,
            land_area=land_area,
            land_area_unit=land_area_unit,
            building_area=building_area,
            building_area_unit=building_area_unit,
            listed_date=listed_date,
            snippet=snippet,
        )

    def _records_from_html_cards(self, source_url: str, html: str) -> List[Record]:
        records: List[Record] = []
        for card_html in _RESIDENTIAL_CARD_RE.findall(html):
            address_match = re.search(
                r'<h2[^>]*class=["\'][^"\']*residential-card__address-heading[^"\']*["\'][^>]*>.*?<a[^>]+href=["\']([^"\']+)["\'][^>]*>\s*<span[^>]*>(.*?)</span>',
                card_html,
                re.IGNORECASE | re.DOTALL,
            )
            if not address_match:
                continue
            listing_url = urljoin(source_url, _clean_text(address_match.group(1)))
            address = _clean_text(_strip_tags(address_match.group(2)))

            price_match = re.search(r'<span[^>]*class=["\'][^"\']*property-price[^"\']*["\'][^>]*>(.*?)</span>', card_html, re.IGNORECASE | re.DOTALL)
            price_text = _clean_text(_strip_tags(price_match.group(1))) if price_match else ""
            price = _to_int(price_text)

            primary_match = re.search(r'<ul[^>]*residential-card__primary[^>]*aria-label=["\']([^"\']+)["\']', card_html, re.IGNORECASE | re.DOTALL)
            primary_label = _clean_text(primary_match.group(1)) if primary_match else ""
            property_category = _normalize_property_category(primary_label.split(' with ', 1)[0] if primary_label else "")
            bedrooms = _to_int(_first_match(primary_label, r'(\d+)\s+bedrooms?'))
            bathrooms = _to_number(_first_match(primary_label, r'(\d+(?:\.\d+)?)\s+bathrooms?'))
            car_spaces = _to_int(_first_match(primary_label, r'(\d+)\s+car\s+spaces?'))
            area_value, area_unit = (None, None)
            if re.search(r'(sqm|m²|sq m|square metres?|square meters?)', primary_label, re.IGNORECASE):
                area_value, area_unit = _extract_area(primary_label)

            snippet = _clean_text(f"{address} {price_text} {primary_label}")[:240]
            record = self._build_record(
                source_url=source_url,
                listing_url=listing_url,
                address=address,
                price=price,
                bedrooms=bedrooms,
                bathrooms=bathrooms,
                size=area_value,
                property_category=property_category,
                land_area=area_value,
                land_area_unit=area_unit,
                building_area=None,
                building_area_unit=None,
                listed_date=None,
                snippet=snippet,
            )
            if car_spaces is not None:
                record["car_spaces"] = car_spaces
            records.append(record)
        return records

    def _build_record(
        self,
        *,
        source_url: str,
        listing_url: str,
        address: str,
        price: Any,
        bedrooms: Any,
        bathrooms: Any,
        size: Any,
        property_category: Any,
        land_area: Any,
        land_area_unit: Any,
        building_area: Any,
        building_area_unit: Any,
        listed_date: Any,
        snippet: str,
    ) -> Record:
        return {
            "listing_id": _stable_listing_id(self.site_name, listing_url, address, price, bedrooms),
            "url": listing_url,
            "address": address or None,
            "rent": price,
            "price": price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "size_sqft": size,
            "property_category": property_category,
            "land_area": land_area,
            "land_area_unit": land_area_unit,
            "building_area": building_area,
            "building_area_unit": building_area_unit,
            "listed_date": listed_date,
            "source_site": self.site_name,
            "raw_snippet": snippet,
        }


class DomainAdapter(SiteAdapter):
    def __init__(self) -> None:
        super().__init__(site_name="domain")

    def can_parse_html(self, html: str) -> bool:
        return "domain.com.au" in html.lower()

    def parse(self, url: str, html: str) -> List[Record]:
        records: List[Record] = []
        for card_html in _DOMAIN_LISTING_RE.findall(html):
            url_match = re.search(r'<a[^>]+href=["\'](https://www\.domain\.com\.au/[^"\']+)["\'][^>]*class=["\'][^"\']*address', card_html, re.IGNORECASE | re.DOTALL)
            if not url_match:
                continue
            listing_url = _clean_text(url_match.group(1))

            line1_match = re.search(r'data-testid=["\']address-line1["\'][^>]*>(.*?)</span>', card_html, re.IGNORECASE | re.DOTALL)
            line2_match = re.search(r'data-testid=["\']address-line2["\'][^>]*>(.*?)</span>', card_html, re.IGNORECASE | re.DOTALL)
            address = _clean_text(', '.join(filter(None, [
                _clean_text(_strip_tags(line1_match.group(1))) if line1_match else '',
                _clean_text(_strip_tags(line2_match.group(1))) if line2_match else '',
            ]))).replace(', ,', ',')

            price_match = re.search(r'data-testid=["\']listing-card-price["\'][^>]*>(.*?)</p>', card_html, re.IGNORECASE | re.DOTALL)
            price_text = _clean_text(_strip_tags(price_match.group(1))) if price_match else ''
            price = _to_int(price_text)

            features = re.findall(r'data-testid=["\']property-features-text-container["\'][^>]*>(\d+(?:\.\d+)?)\s*<span[^>]*data-testid=["\']property-features-text["\'][^>]*>([^<]+)</span>', card_html, re.IGNORECASE | re.DOTALL)
            bedrooms = bathrooms = car_spaces = None
            for value, label in features:
                ll = _clean_text(label).lower()
                if ll.startswith('bed'):
                    bedrooms = _to_int(value)
                elif ll.startswith('bath'):
                    bathrooms = _to_number(value)
                elif ll.startswith('park') or ll.startswith('car'):
                    car_spaces = _to_int(value)

            property_category_match = re.search(r'data-testid=["\']listing-card-title["\'][^>]*>(.*?)</', card_html, re.IGNORECASE | re.DOTALL)
            property_category = _normalize_property_category(_clean_text(_strip_tags(property_category_match.group(1))) if property_category_match else '')
            snippet = _clean_text(f'{address} {price_text}')[:240]
            record = {
                'listing_id': _stable_listing_id(self.site_name, listing_url, address, price, bedrooms),
                'url': listing_url,
                'address': address or None,
                'rent': price,
                'price': price,
                'bedrooms': bedrooms,
                'bathrooms': bathrooms,
                'size_sqft': None,
                'property_category': property_category,
                'land_area': None,
                'land_area_unit': None,
                'building_area': None,
                'building_area_unit': None,
                'listed_date': None,
                'source_site': self.site_name,
                'raw_snippet': snippet,
            }
            if car_spaces is not None:
                record['car_spaces'] = car_spaces
            records.append(record)
        return _dedupe_by_id(records)


def _strip_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value or "")


def _first_match(text: str, pattern: str) -> Optional[str]:
    match = re.search(pattern, text or "", re.IGNORECASE)
    return match.group(1) if match else None


def _address_to_text(address: Any) -> str:
    if isinstance(address, str):
        return _clean_text(address)
    if isinstance(address, dict):
        fields = [
            address.get("streetAddress") or address.get("line1"),
            address.get("addressLocality") or address.get("suburb"),
            address.get("addressRegion") or address.get("state"),
            address.get("postalCode") or address.get("postcode"),
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


def detect_challenge_page(html: str) -> Optional[str]:
    """Detect common anti-bot challenge signatures in raw HTML."""
    lowered = html.lower()
    for provider, markers in _CHALLENGE_MARKERS.items():
        if any(marker in lowered for marker in markers):
            return provider
    return None


def parse_listing_page(url: str, html: str) -> List[Record]:
    """Parse a listing/search HTML page into normalized records."""
    for adapter in ADAPTERS:
        if adapter.matches_url(url):
            return adapter.parse(url, html)
    for adapter in ADAPTERS:
        if adapter.can_parse_html(html):
            return adapter.parse(url, html)
    return []
