import json
from pathlib import Path
import subprocess
import threading
import time

import relay_handoff


def test_create_pending_handoff_artifact(tmp_path):
    handoff = relay_handoff.create_pending_handoff(
        source_url="https://example.com/listings",
        run_date="2025-03-05",
        reason="challenge:kasada",
        handoff_dir=tmp_path,
    )

    payload = json.loads(handoff.read_text(encoding="utf-8"))
    assert handoff.name.startswith("pending_relay_")
    assert payload["status"] == "pending"
    assert payload["source_url"] == "https://example.com/listings"
    assert payload["required_schema"]["required_keys"] == ["listing_id", "rent", "snapshot_date"]
    assert payload["required_schema"]["min_records"] == 1


def test_validate_payload_rejects_missing_required_keys(tmp_path):
    handoff = relay_handoff.create_pending_handoff(
        source_url="https://example.com/listings",
        run_date="2025-03-05",
        reason="blocked",
        handoff_dir=tmp_path,
    )
    meta = json.loads(handoff.read_text(encoding="utf-8"))
    payload = Path(meta["expected_payload_path"])
    payload.write_text(
        json.dumps(
            {
                "handoff_id": meta["handoff_id"],
                "source_url": meta["source_url"],
                "run_date": meta["run_date"],
                "listings": [{"listing_id": "x1", "snapshot_date": "2025-03-05"}],
            }
        ),
        encoding="utf-8",
    )

    try:
        relay_handoff.validate_payload_file(handoff, payload)
        assert False, "Expected RelayPayloadValidationError"
    except relay_handoff.RelayPayloadValidationError as exc:
        assert "missing required keys" in str(exc).lower()


def test_resume_from_relay_script_runs_pipeline(tmp_path):
    handoff = relay_handoff.create_pending_handoff(
        source_url="https://example.com/listings",
        run_date="2025-03-05",
        reason="blocked",
        handoff_dir=tmp_path / "handoffs",
    )
    meta = json.loads(handoff.read_text(encoding="utf-8"))
    payload = Path(meta["expected_payload_path"])
    payload.parent.mkdir(parents=True, exist_ok=True)
    payload.write_text(
        json.dumps(
            {
                "handoff_id": meta["handoff_id"],
                "source_url": meta["source_url"],
                "run_date": meta["run_date"],
                "listings": [
                    {
                        "listing_id": "lst_1",
                        "rent": 2100,
                        "snapshot_date": "2025-03-05",
                        "first_seen": "2025-03-01",
                        "last_seen": "2025-03-05",
                        "bedrooms": 2,
                        "size_sqft": 700,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    reports_dir = tmp_path / "reports"
    normalized_dir = tmp_path / "normalized"
    script = Path(__file__).resolve().parents[1] / "scripts" / "resume_from_relay.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--handoff",
            str(handoff),
            "--payload",
            str(payload),
            "--normalized-dir",
            str(normalized_dir),
            "--reports-dir",
            str(reports_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (reports_dir / "market_analysis.json").exists()
    assert not (reports_dir / "market_report.json").exists()
    updated = json.loads(handoff.read_text(encoding="utf-8"))
    assert updated["status"] == "completed"
