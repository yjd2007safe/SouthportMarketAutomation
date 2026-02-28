import json

import normalize


def test_parse_price_value_and_contract_date():
    assert normalize.parse_price_value("$1,250,000") == 1250000
    assert normalize.parse_price_value("Price on request") is None
    assert normalize.parse_contract_date("14/01/2025") == "2025-01-14"
    assert normalize.parse_contract_date("2025-01-14T12:00:00Z") == "2025-01-14"


def test_deduplicate_prefers_address_date_price_and_falls_back_to_url_hash():
    raw = [
        {
            "source": "agent_a",
            "url": "https://example.com/listing/1?utm=x",
            "address": "1 Main St",
            "contract_date": "2025-01-01",
            "price": "$500,000",
        },
        {
            "source": "agent_b",
            "url": "https://example.com/listing/1?utm=y",
            "address": "1 Main St",
            "contract_date": "2025-01-01",
            "price": "$500,000",
        },
        {
            "source": "agent_c",
            "url": "https://example.com/listing/2",
            "address": "",
            "contract_date": "",
            "price": "",
        },
        {
            "source": "agent_d",
            "url": "https://example.com/listing/2",
            "address": "",
            "contract_date": "",
            "price": "",
        },
    ]

    deduped, summary = normalize.normalize_and_dedup(raw)

    assert summary == {"input": 4, "normalized": 4, "deduped": 2, "dropped": 0}
    assert len(deduped) == 2


def test_run_creates_expected_output_paths_and_files(tmp_path):
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    input_path = raw_dir / "2025-01-14.jsonl"
    input_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "source": "site",
                        "url": "https://example.com/1",
                        "address": "10 Queen St",
                        "contract_date": "2025-01-14",
                        "price": "$900,000",
                    }
                )
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    clean_dir = tmp_path / "clean"
    summary = normalize.run([
        "--input",
        str(input_path),
        "--date",
        "2025-01-14",
        "--clean-dir",
        str(clean_dir),
    ])

    assert summary == {"input": 1, "normalized": 1, "deduped": 1, "dropped": 0}
    assert (clean_dir / "2025-01-14.jsonl").exists()
    assert (clean_dir / "2025-01-14.csv").exists()
