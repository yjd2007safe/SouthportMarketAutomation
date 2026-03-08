import json
import analyze


def _sample_records():
    return [
        {
            "rent": "1400",
            "snapshot_date": "2025-01-15",
            "first_seen": "2025-01-01",
            "last_seen": "2025-01-21",
            "bedrooms": "1",
            "size_sqft": "450",
        },
        {
            "price": "2100",
            "observed_at": "2025-01-20T10:00:00Z",
            "listed_date": "2025-01-10",
            "last_seen": "2025-01-22",
            "beds": "2",
            "sqft": "780",
        },
        {
            "monthly_rent": "3200",
            "snapshot_date": "2025-02-03",
            "first_seen": "2025-01-01",
            "last_seen": "2025-02-10",
            "bedroom_count": "3",
            "area_sqft": "1150",
        },
        {
            "list_price": "4000",
            "snapshot_date": "2025-02-15",
            "first_seen": "2025-02-01",
            "last_seen": "2025-02-20",
            "bedrooms": "4",
            "size_sqft": "1400",
        },
        {
            "rent": "",
            "snapshot_date": "2025-02-25",
            "bedrooms": "0",
            "size_sqft": "390",
        },
    ]


def test_analyze_records_computes_five_dimensions_with_missing_handling():
    stats = analyze.analyze_records(_sample_records())

    assert stats["record_count"] == 5
    assert stats["price_level_distribution"]["bins"] == {
        "budget": 1,
        "mid": 1,
        "premium": 1,
        "luxury": 1,
    }
    assert stats["price_level_distribution"]["missing_price"] == 1

    assert stats["rent_trend"] == [
        {"month": "2025-01", "average_rent": 1750.0, "sample_size": 2},
        {"month": "2025-02", "average_rent": 3600.0, "sample_size": 2},
    ]
    assert stats["listing_volume_trend"] == [
        {"month": "2025-01", "listing_count": 2},
        {"month": "2025-02", "listing_count": 3},
    ]

    assert stats["listing_age_proxy"] == {
        "sample_size": 4,
        "average_days": 22.75,
        "median_days": 19.5,
    }

    segments = {row["segment"]: row for row in stats["bedroom_size_mix"]}
    assert segments["1_bed|compact"]["median_rent"] == 1400.0
    assert segments["2_bed|standard"]["median_rent"] == 2100.0
    assert segments["studio|compact"]["median_rent"] is None


def test_run_analysis_writes_json_csv_and_markdown_reports(tmp_path):
    input_path = tmp_path / "normalized.json"
    input_path.write_text(json.dumps(_sample_records()), encoding="utf-8")

    reports_dir = tmp_path / "reports"
    stats = analyze.run_analysis(input_path, reports_dir, "daily")

    assert stats["record_count"] == 5

    expected = {
        "daily.json",
        "daily_price_distribution.csv",
        "daily_rent_trend.csv",
        "daily_volume_trend.csv",
        "daily_bedroom_size_mix.csv",
        "daily_summary.md",
    }
    assert expected == {path.name for path in reports_dir.iterdir()}

    summary = (reports_dir / "daily_summary.md").read_text(encoding="utf-8")
    assert "Southport Market Analysis Summary" in summary
    assert "Records analyzed: **5**" in summary


def test_load_records_supports_csv(tmp_path):
    csv_path = tmp_path / "normalized.csv"
    csv_path.write_text(
        "rent,snapshot_date,bedrooms,size_sqft\n"
        "1500,2025-01-01,1,500\n",
        encoding="utf-8",
    )

    rows = analyze.load_records(csv_path)
    assert len(rows) == 1
    assert rows[0]["rent"] == "1500"
    assert rows[0]["snapshot_date"] == "2025-01-01"
    assert rows[0]["global_key"]


def test_parse_args_reads_cli_flags():
    args = analyze.parse_args(
        ["--input", "data/normalized.json", "--reports-dir", "reports", "--prefix", "run1"]
    )
    assert args.input == "data/normalized.json"
    assert args.reports_dir == "reports"
    assert args.prefix == "run1"
