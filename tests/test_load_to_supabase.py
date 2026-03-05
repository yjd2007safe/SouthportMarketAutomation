import json
from pathlib import Path

import pytest

import load_to_supabase


def test_load_supabase_config_requires_both_values():
    with pytest.raises(RuntimeError, match="SUPABASE_URL"):
        load_to_supabase.load_supabase_config({"SUPABASE_KEY": "k"})

    with pytest.raises(RuntimeError, match="SUPABASE_KEY"):
        load_to_supabase.load_supabase_config({"SUPABASE_URL": "https://example.supabase.co"})


def test_prepare_rows_are_deterministic():
    rows = [{"id": "abc", "rent": 2000, "bedrooms": 2, "size_sqft": 820}]
    raw = load_to_supabase.prepare_raw_rows(rows, "2025-03-05", "daily")
    clean = load_to_supabase.prepare_clean_rows(rows, "2025-03-05", "daily")

    assert raw == [
        {
            "snapshot_date": "2025-03-05",
            "source": "daily",
            "listing_key": "abc",
            "payload": '{"bedrooms":2,"id":"abc","rent":2000,"size_sqft":820}',
        }
    ]
    assert clean == [
        {
            "snapshot_date": "2025-03-05",
            "source": "daily",
            "listing_key": "abc",
            "payload": '{"bedrooms":2,"id":"abc","rent":2000,"size_sqft":820}',
            "rent": 2000,
            "bedrooms": 2,
            "size_sqft": 820,
        }
    ]


def test_upsert_mapping_uses_expected_endpoint_and_conflict_keys(tmp_path):
    normalized = tmp_path / "normalized.json"
    normalized.write_text(json.dumps([{"id": "x1", "rent": 1800}]), encoding="utf-8")

    summary = tmp_path / "analysis.json"
    summary.write_text(
        json.dumps(
            {
                "record_count": 1,
                "price_level_distribution": {"missing_price": 0},
                "listing_age_proxy": {"sample_size": 1},
            }
        ),
        encoding="utf-8",
    )

    raw = tmp_path / "raw.csv"
    raw.write_text("id,rent\nx1,1800\n", encoding="utf-8")

    captured = []

    def fake_request(**kwargs):
        captured.append(kwargs)

    load_to_supabase.run_load(
        normalized_input=Path(normalized),
        summary_json=Path(summary),
        raw_input=Path(raw),
        snapshot_date="2025-03-05",
        source="daily",
        env={"SUPABASE_URL": "https://example.supabase.co", "SUPABASE_KEY": "test-key"},
        request_fn=fake_request,
    )

    assert len(captured) == 3
    assert captured[0]["url"].endswith("/rest/v1/clean_listings_snapshot?on_conflict=snapshot_date%2Csource%2Clisting_key")
    assert captured[1]["url"].endswith("/rest/v1/raw_listings?on_conflict=snapshot_date%2Csource%2Clisting_key")
    assert captured[2]["url"].endswith("/rest/v1/daily_market_summary?on_conflict=snapshot_date%2Csource%2Cmetric")
    assert all(call["headers"]["apikey"] == "test-key" for call in captured)
