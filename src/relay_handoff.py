"""Human relay handoff utilities for blocked daily pipeline sources."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

DEFAULT_REQUIRED_KEYS: Tuple[str, ...] = (
    "listing_id",
    "rent",
    "snapshot_date",
)
DEFAULT_MIN_RECORDS = 1
SCHEMA_VERSION = "1.0"


class RelayPayloadValidationError(ValueError):
    """Raised when a manual relay payload is malformed."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_pending_handoff(
    *,
    source_url: str,
    run_date: str,
    reason: str,
    handoff_dir: Path,
    required_keys: Sequence[str] = DEFAULT_REQUIRED_KEYS,
    min_records: int = DEFAULT_MIN_RECORDS,
) -> Path:
    handoff_dir.mkdir(parents=True, exist_ok=True)
    handoff_id = f"relay_{run_date}_{uuid.uuid4().hex[:8]}"
    payload_name = f"relay_payload_{handoff_id}.json"
    handoff = {
        "schema_version": SCHEMA_VERSION,
        "handoff_id": handoff_id,
        "status": "pending",
        "created_at": _now_iso(),
        "run_date": run_date,
        "source_url": source_url,
        "reason": reason,
        "required_schema": {
            "required_keys": list(required_keys),
            "min_records": int(min_records),
        },
        "expected_payload_path": str(handoff_dir / payload_name),
        "operator_instructions": "Capture listing JSON payload with required keys and write to expected_payload_path.",
    }
    path = handoff_dir / f"pending_relay_{handoff_id}.json"
    path.write_text(json.dumps(handoff, indent=2), encoding="utf-8")
    return path


def load_handoff(path: Path) -> Dict[str, Any]:
    handoff = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(handoff, dict):
        raise RelayPayloadValidationError("Handoff must be a JSON object")
    for key in ("handoff_id", "run_date", "source_url", "required_schema", "expected_payload_path"):
        if key not in handoff:
            raise RelayPayloadValidationError(f"Missing handoff key: {key}")
    return handoff


def _load_payload(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RelayPayloadValidationError("Relay payload must be a JSON object")
    return payload


def validate_payload_against_handoff(handoff: Dict[str, Any], payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    required_payload_keys = ("handoff_id", "source_url", "run_date", "listings")
    for key in required_payload_keys:
        if key not in payload:
            raise RelayPayloadValidationError(f"Missing payload key: {key}")

    if payload["handoff_id"] != handoff["handoff_id"]:
        raise RelayPayloadValidationError("Payload handoff_id does not match handoff artifact")
    if str(payload["source_url"]) != str(handoff["source_url"]):
        raise RelayPayloadValidationError("Payload source_url does not match handoff artifact")
    if str(payload["run_date"]) != str(handoff["run_date"]):
        raise RelayPayloadValidationError("Payload run_date does not match handoff artifact")

    listings = payload["listings"]
    if not isinstance(listings, list):
        raise RelayPayloadValidationError("Payload listings must be an array")

    schema = handoff.get("required_schema", {})
    required_keys = list(schema.get("required_keys") or DEFAULT_REQUIRED_KEYS)
    min_records = int(schema.get("min_records") or DEFAULT_MIN_RECORDS)

    if len(listings) < min_records:
        raise RelayPayloadValidationError(
            f"Payload listings count {len(listings)} is below required minimum {min_records}"
        )

    for index, row in enumerate(listings):
        if not isinstance(row, dict):
            raise RelayPayloadValidationError(f"Listing at index {index} must be an object")
        missing = [key for key in required_keys if row.get(key) in (None, "")]
        if missing:
            raise RelayPayloadValidationError(
                f"Listing at index {index} missing required keys: {', '.join(missing)}"
            )

    return listings


def validate_payload_file(handoff_path: Path, payload_path: Path) -> List[Dict[str, Any]]:
    handoff = load_handoff(handoff_path)
    payload = _load_payload(payload_path)
    return validate_payload_against_handoff(handoff, payload)


def materialize_normalized_from_payload(
    *, handoff_path: Path, payload_path: Path, normalized_dir: Path
) -> Path:
    handoff = load_handoff(handoff_path)
    payload = _load_payload(payload_path)
    listings = validate_payload_against_handoff(handoff, payload)

    normalized_dir.mkdir(parents=True, exist_ok=True)
    output_path = normalized_dir / f"normalized_{handoff['handoff_id']}.json"
    output_path.write_text(json.dumps(listings, indent=2), encoding="utf-8")
    return output_path


def mark_handoff_status(handoff_path: Path, *, status: str, note: str = "") -> Path:
    handoff = load_handoff(handoff_path)
    handoff["status"] = status
    handoff["updated_at"] = _now_iso()
    if note:
        handoff["status_note"] = note
    handoff_path.write_text(json.dumps(handoff, indent=2), encoding="utf-8")
    return handoff_path


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Relay handoff helper")
    sub = parser.add_subparsers(dest="command", required=True)

    create_cmd = sub.add_parser("create")
    create_cmd.add_argument("--source-url", required=True)
    create_cmd.add_argument("--run-date", required=True)
    create_cmd.add_argument("--reason", required=True)
    create_cmd.add_argument("--handoff-dir", default="data/handoffs")

    validate_cmd = sub.add_parser("validate")
    validate_cmd.add_argument("--handoff", required=True)
    validate_cmd.add_argument("--payload", required=True)

    materialize_cmd = sub.add_parser("materialize")
    materialize_cmd.add_argument("--handoff", required=True)
    materialize_cmd.add_argument("--payload", required=True)
    materialize_cmd.add_argument("--normalized-dir", default="data/normalized")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.command == "create":
        path = create_pending_handoff(
            source_url=args.source_url,
            run_date=args.run_date,
            reason=args.reason,
            handoff_dir=Path(args.handoff_dir),
        )
        print(path)
        return 0

    if args.command == "validate":
        listings = validate_payload_file(Path(args.handoff), Path(args.payload))
        print(f"valid records={len(listings)}")
        return 0

    if args.command == "materialize":
        path = materialize_normalized_from_payload(
            handoff_path=Path(args.handoff),
            payload_path=Path(args.payload),
            normalized_dir=Path(args.normalized_dir),
        )
        print(path)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
