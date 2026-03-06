"""Source discovery utilities for AU property/listing ingestion."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse

ALLOWED_SCHEMES = {"http", "https"}
INGESTABLE_CATEGORIES = {"listing", "search"}

SEED_SOURCES: List[Dict[str, object]] = [
    {
        "site": "realestate.com.au",
        "category": "search",
        "url_template": "https://www.realestate.com.au/rent/in-southport,+qld+4215/list-1",
        "confidence": 0.98,
        "notes": "Primary AU listing portal with suburb-level rental and sale search pages.",
    },
    {
        "site": "domain.com.au",
        "category": "search",
        "url_template": "https://www.domain.com.au/rent/southport-qld-4215/",
        "confidence": 0.97,
        "notes": "Major AU property marketplace with structured suburb filters.",
    },
    {
        "site": "onthehouse.com.au",
        "category": "listing",
        "url_template": "https://www.onthehouse.com.au/for-rent/qld/gold-coast/southport",
        "confidence": 0.82,
        "notes": "AU property portal with suburb listing pages and historical context.",
    },
    {
        "site": "allhomes.com.au",
        "category": "search",
        "url_template": "https://www.allhomes.com.au/rent/southport-gold-coast-qld",
        "confidence": 0.79,
        "notes": "National listing portal with suburb search landing pages.",
    },
    {
        "site": "sqmresearch.com.au",
        "category": "market-data",
        "url_template": "https://sqmresearch.com.au/asking-property-prices.php?postcode=4215&t=1",
        "confidence": 0.74,
        "notes": "Credible AU market metrics source; useful enrichment but not listing-first.",
    },
    {
        "site": "qld.gov.au",
        "category": "government",
        "url_template": "https://www.qld.gov.au/housing/renting",
        "confidence": 0.67,
        "notes": "Official tenancy guidance and policy context.",
    },
]

EXPANSION_SOURCES: List[Dict[str, object]] = [
    {
        "site": "homely.com.au",
        "category": "search",
        "url_template": "https://www.homely.com.au/for-rent/southport-gold-coast-qld",
        "confidence": 0.71,
        "notes": "Secondary AU listing portal for optional source diversification.",
    },
    {
        "site": "propertyvalue.com.au",
        "category": "market-data",
        "url_template": "https://www.propertyvalue.com.au/suburb/southport-4215-qld",
        "confidence": 0.6,
        "notes": "Property valuation context site; generally non-ingestable.",
    },
]


def normalize_area_name(area: str) -> str:
    """Return lowercase hyphenated area label."""
    compact = " ".join(area.strip().split())
    return compact.lower().replace(",", "").replace(" ", "-")


def _render_url(template: str, area: str) -> str:
    slug = normalize_area_name(area)
    return template.replace("southport", slug)


def validate_source_entry(source: Dict[str, object]) -> bool:
    """Validate minimal source shape and URL properties."""
    required = {"url", "site", "category", "confidence", "notes"}
    if not required.issubset(set(source)):
        return False

    parsed = urlparse(str(source["url"]))
    if parsed.scheme not in ALLOWED_SCHEMES:
        return False
    if not parsed.netloc:
        return False

    confidence = source.get("confidence")
    if not isinstance(confidence, (int, float)):
        return False
    return 0 <= float(confidence) <= 1


def is_ingestable_source(source: Dict[str, object]) -> bool:
    """Return True when source looks suitable for listing/search ingestion."""
    if not validate_source_entry(source):
        return False
    if str(source.get("category", "")).lower() not in INGESTABLE_CATEGORIES:
        return False

    url = str(source["url"]).lower()
    parsed = urlparse(url)
    if parsed.path.endswith((".csv", ".json")):
        return True

    ingestable_tokens = ("rent", "sale", "listing", "for-rent", "for-sale", "property")
    if any(token in url for token in ingestable_tokens):
        return True

    return str(source.get("category", "")).lower() in INGESTABLE_CATEGORIES


def discover_sources(area: str = "Southport", *, include_expansion: bool = False) -> List[Dict[str, object]]:
    """Build deterministic source candidates for an area."""
    rows = list(SEED_SOURCES)
    if include_expansion:
        rows.extend(EXPANSION_SOURCES)

    discovered: List[Dict[str, object]] = []
    for item in rows:
        entry = {
            "url": _render_url(str(item["url_template"]), area),
            "site": item["site"],
            "category": item["category"],
            "confidence": float(item["confidence"]),
            "notes": item["notes"],
        }
        if validate_source_entry(entry):
            discovered.append(entry)

    discovered.sort(key=lambda row: (-float(row["confidence"]), str(row["site"])))
    return discovered


def filter_ingestable_sources(sources: Iterable[Dict[str, object]]) -> List[Dict[str, object]]:
    """Filter to valid listing/search pages."""
    return [source for source in sources if is_ingestable_source(source)]


def dump_yaml_like(data: List[Dict[str, object]]) -> str:
    """Dump a minimal YAML representation without third-party deps."""
    lines: List[str] = []
    for row in data:
        lines.append("-")
        for key in ("url", "site", "category", "confidence", "notes"):
            value = row.get(key, "")
            if isinstance(value, str):
                escaped = value.replace('"', '\\"')
                lines.append(f"  {key}: \"{escaped}\"")
            else:
                lines.append(f"  {key}: {value}")
    return "\n".join(lines) + "\n"


def write_sources(sources: List[Dict[str, object]], output_path: Path) -> Path:
    """Persist discovered sources as JSON or YAML by extension."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ext = output_path.suffix.lower()
    if ext in {".yaml", ".yml"}:
        output_path.write_text(dump_yaml_like(sources), encoding="utf-8")
    else:
        output_path.write_text(json.dumps(sources, indent=2), encoding="utf-8")
    return output_path


def load_sources_file(path: Path) -> List[Dict[str, object]]:
    """Load a source list from json, yaml-like, or newline URL text."""
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []

    if path.suffix.lower() == ".json":
        payload = json.loads(raw)
        if isinstance(payload, dict):
            payload = payload.get("sources", [])
        return [dict(item) for item in payload]

    if path.suffix.lower() in {".yaml", ".yml"}:
        rows: List[Dict[str, object]] = []
        current: Dict[str, object] = {}
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped == "-":
                if current:
                    rows.append(current)
                current = {}
                continue
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            value = value.strip().strip('"').strip("'")
            if key.strip() == "confidence":
                try:
                    current[key.strip()] = float(value)
                except ValueError:
                    current[key.strip()] = value
            else:
                current[key.strip()] = value
        if current:
            rows.append(current)
        return rows

    return [
        {
            "url": line.strip(),
            "site": urlparse(line.strip()).netloc,
            "category": "search",
            "confidence": 0.5,
            "notes": "Loaded from plain-text source list.",
        }
        for line in raw.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Discover AU property sources for an area")
    parser.add_argument("--area", default="Southport", help="Target suburb or area name")
    parser.add_argument("--output", required=True, help="Output .json/.yaml file")
    parser.add_argument(
        "--include-expansion",
        action="store_true",
        help="Include optional expanded source candidates",
    )
    parser.add_argument(
        "--ingestable-only",
        action="store_true",
        help="Only include validated listing/search pages",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    sources = discover_sources(args.area, include_expansion=args.include_expansion)
    if args.ingestable_only:
        sources = filter_ingestable_sources(sources)

    path = write_sources(sources, Path(args.output))
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
