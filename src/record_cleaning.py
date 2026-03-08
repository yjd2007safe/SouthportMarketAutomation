"""Global record normalization and dedup helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlsplit, urlunsplit

Record = Dict[str, Any]


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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
