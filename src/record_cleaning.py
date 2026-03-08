"""Global record normalization and dedup helpers."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, Iterable, List
from urllib.parse import urlsplit, urlunsplit

Record = Dict[str, Any]


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip().lower().replace(",", "")
    m = re.search(r"\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _detect_area_unit(value: Any, default: str = "sqm") -> str | None:
    text = _clean_text(value).lower()
    if not text:
        return None
    if any(tok in text for tok in ("sqft", "ft²", "ft2", "square feet")):
        return "sqft"
    if any(tok in text for tok in ("sqm", "m²", "m2", "square metre", "square meter")):
        return "sqm"
    return default


def _normalize_property_category(value: Any) -> str | None:
    text = _clean_text(value).lower()
    if not text:
        return None
    if any(tok in text for tok in ("townhouse", "town home", "town-house", "town house")):
        return "townhouse"
    if any(tok in text for tok in ("apartment", "unit", "flat")):
        return "apartment"
    if any(tok in text for tok in ("detached", "house", "single family", "single-family")):
        return "detached_house"
    return None


def canonical_url(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    parsed = urlsplit(text)
    scheme = (parsed.scheme or "https").lower()
    host = (parsed.netloc or "").lower()
    path = parsed.path.rstrip("/")
    return urlunsplit((scheme, host, path, "", ""))


def canonical_address(value: Any) -> str:
    text = " ".join(_clean_text(value).lower().replace(",", " ").split())
    return text


def stable_url_or_address_hash(url: str, address: str) -> str:
    payload = json.dumps({"url": url, "address": address}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _listing_id_value(record: Record) -> str:
    for key in ("listing_id", "id"):
        value = _clean_text(record.get(key))
        if value:
            return value.lower()
    return ""


def stable_global_key(record: Record) -> str:
    listing_id = _listing_id_value(record)
    if listing_id and not listing_id.startswith("lst_"):
        return f"id:{listing_id}"

    canonical = canonical_url(record.get("url") or record.get("listing_url"))
    address = canonical_address(record.get("address"))
    if canonical or address:
        return f"ua:{stable_url_or_address_hash(canonical, address)}"

    if listing_id:
        return f"id:{listing_id}"

    stable = json.dumps(record, sort_keys=True, separators=(",", ":"))
    return f"row:{hashlib.sha256(stable.encode('utf-8')).hexdigest()[:24]}"


def _apply_property_field_fallbacks(normalized: Record) -> None:
    category = _normalize_property_category(
        normalized.get("property_category")
        or normalized.get("property_type")
        or normalized.get("dwelling_type")
        or normalized.get("type")
    )
    if category:
        normalized["property_category"] = category

    if normalized.get("building_area") in (None, ""):
        building_area = _to_float(normalized.get("building_size") or normalized.get("internal_area") or normalized.get("floor_area"))
        if building_area is not None:
            normalized["building_area"] = building_area
    if normalized.get("building_area_unit") in (None, ""):
        unit = _detect_area_unit(normalized.get("building_size") or normalized.get("floor_area"))
        if unit:
            normalized["building_area_unit"] = unit

    if normalized.get("land_area") in (None, ""):
        land_area = _to_float(normalized.get("land_size") or normalized.get("lot_size"))
        if land_area is not None:
            normalized["land_area"] = land_area
    if normalized.get("land_area_unit") in (None, ""):
        unit = _detect_area_unit(normalized.get("land_size") or normalized.get("lot_size"))
        if unit:
            normalized["land_area_unit"] = unit


def normalize_record(record: Record, *, source_url: str = "", source_site: str = "") -> Record:
    normalized = dict(record)

    url = canonical_url(normalized.get("url") or normalized.get("listing_url") or source_url)
    if url:
        normalized["url"] = url

    site = _clean_text(normalized.get("source_site") or normalized.get("site") or source_site)
    if not site and url:
        site = (urlsplit(url).hostname or "").lower()
    if site:
        normalized["source_site"] = site.lower()

    src_url = canonical_url(normalized.get("source_url") or source_url)
    if src_url:
        normalized["source_url"] = src_url

    _apply_property_field_fallbacks(normalized)

    global_key = stable_global_key(normalized)
    normalized["global_key"] = global_key
    return normalized


def normalize_and_dedupe_records(
    records: Iterable[Record], *, source_url: str = "", source_site: str = ""
) -> List[Record]:
    deduped: Dict[str, Record] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        normalized = normalize_record(record, source_url=source_url, source_site=source_site)
        deduped[normalized["global_key"]] = normalized
    return list(deduped.values())
