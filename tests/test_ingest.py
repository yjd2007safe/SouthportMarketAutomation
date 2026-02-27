from datetime import datetime, timezone
from pathlib import Path

import ingest


def test_parse_args_requires_source_and_reads_flags():
    args = ingest.parse_args(["--source", "listings.csv", "--output-dir", "out", "--filename", "daily"])
    assert args.source == "listings.csv"
    assert args.output_dir == "out"
    assert args.filename == "daily"


def test_resolve_source_url():
    source_type, normalized = ingest.resolve_source("https://example.com/feed.json")
    assert source_type == "url"
    assert normalized == "https://example.com/feed.json"


def test_resolve_source_file_is_absolute(tmp_path):
    file_path = tmp_path / "input.csv"
    file_path.write_text("id,price\n1,1200\n", encoding="utf-8")

    source_type, normalized = ingest.resolve_source(str(file_path))
    assert source_type == "file"
    assert normalized == str(file_path.resolve())


def test_create_output_path_creates_dir_and_uses_source_stem(tmp_path):
    output_dir = tmp_path / "nested" / "raw"
    fixed = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    out_path = ingest.create_output_path(output_dir, "listings.csv", timestamp=fixed)

    assert output_dir.exists()
    assert out_path.name == "listings_20250102T030405Z.json"


def test_create_output_path_uses_filename_override(tmp_path):
    fixed = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    out_path = ingest.create_output_path(tmp_path, "https://example.com/feed.json", filename="custom", timestamp=fixed)
    assert out_path.name == "custom_20250102T030405Z.json"
