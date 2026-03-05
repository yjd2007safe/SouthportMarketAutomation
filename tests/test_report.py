import json

import report


def _analysis_payload():
    return {
        "record_count": 5,
        "price_level_distribution": {
            "bins": {"budget": 1, "mid": 2, "premium": 1, "luxury": 0},
            "missing_price": 1,
        },
        "rent_trend": [
            {"month": "2025-01", "average_rent": 1800.0, "sample_size": 3},
            {"month": "2025-02", "average_rent": 2200.0, "sample_size": 1},
        ],
        "listing_volume_trend": [
            {"month": "2025-01", "listing_count": 4},
            {"month": "2025-02", "listing_count": 1},
        ],
        "listing_age_proxy": {
            "sample_size": 3,
            "average_days": 14.5,
            "median_days": 12.0,
        },
        "bedroom_size_mix": [
            {"segment": "1_bed|compact", "count": 2, "median_rent": 1700.0},
            {"segment": "2_bed|standard", "count": 1, "median_rent": 2300.0},
        ],
    }


def test_run_report_writes_markdown_csv_and_json(tmp_path):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()
    (reports_dir / "market_analysis.json").write_text(
        json.dumps(_analysis_payload()), encoding="utf-8"
    )

    outputs = report.run_report(reports_dir, "market_analysis", "market_report")

    assert set(outputs.keys()) == {"json", "csv", "markdown"}
    assert outputs["json"].exists()
    assert outputs["csv"].exists()
    assert outputs["markdown"].exists()

    markdown = outputs["markdown"].read_text(encoding="utf-8")
    assert "# Southport Market Report" in markdown
    assert "## Price Level Distribution" in markdown
    assert "## Rent Trend" in markdown
    assert "## Listing Volume Trend" in markdown
    assert "## Listing Age Proxy" in markdown
    assert "## Bedroom/Size Mix" in markdown

    csv_text = outputs["csv"].read_text(encoding="utf-8")
    assert "dimension,section,metric,value" in csv_text
    assert "price_level_distribution,Price Level Distribution,bin:budget,1" in csv_text


def test_load_analysis_stats_gracefully_handles_missing_or_partial_input(tmp_path):
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    missing_stats = report.load_analysis_stats(reports_dir, "nope")
    assert missing_stats["record_count"] == 0
    assert missing_stats["rent_trend"] == []

    partial = {"record_count": 3, "listing_age_proxy": {"sample_size": 2}}
    (reports_dir / "partial.json").write_text(json.dumps(partial), encoding="utf-8")

    loaded = report.load_analysis_stats(reports_dir, "partial")
    assert loaded["record_count"] == 3
    assert loaded["rent_trend"] == []
    assert loaded["listing_age_proxy"] == {"sample_size": 2}


def test_parse_args_reads_cli_flags():
    args = report.parse_args(
        [
            "--reports-dir",
            "reports",
            "--analysis-prefix",
            "daily_analysis",
            "--output-prefix",
            "daily_report",
        ]
    )
    assert args.reports_dir == "reports"
    assert args.analysis_prefix == "daily_analysis"
    assert args.output_prefix == "daily_report"
