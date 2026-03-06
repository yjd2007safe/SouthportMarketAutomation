from pathlib import Path

import discover_sources


def test_discover_sources_includes_seed_domains_for_southport():
    sources = discover_sources.discover_sources("Southport")
    urls = [item["url"] for item in sources]

    assert any("realestate.com.au" in url for url in urls)
    assert any("domain.com.au" in url for url in urls)


def test_filter_ingestable_sources_excludes_non_listing_categories():
    sources = [
        {
            "url": "https://www.realestate.com.au/rent/in-southport,+qld+4215/list-1",
            "site": "realestate.com.au",
            "category": "search",
            "confidence": 0.9,
            "notes": "listing",
        },
        {
            "url": "https://www.qld.gov.au/housing/renting",
            "site": "qld.gov.au",
            "category": "government",
            "confidence": 0.8,
            "notes": "policy",
        },
    ]

    ingestable = discover_sources.filter_ingestable_sources(sources)

    assert len(ingestable) == 1
    assert ingestable[0]["site"] == "realestate.com.au"


def test_load_sources_file_yaml_and_validate(tmp_path):
    source_file = tmp_path / "sources.yaml"
    source_file.write_text(
        "\n".join(
            [
                "-",
                '  url: "https://www.domain.com.au/rent/southport-qld-4215/"',
                '  site: "domain.com.au"',
                '  category: "search"',
                "  confidence: 0.9",
                '  notes: "seed"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    loaded = discover_sources.load_sources_file(source_file)

    assert len(loaded) == 1
    assert discover_sources.validate_source_entry(loaded[0])
    assert loaded[0]["site"] == "domain.com.au"


def test_write_sources_json_round_trip(tmp_path):
    output = tmp_path / "sources.json"
    sources = discover_sources.filter_ingestable_sources(
        discover_sources.discover_sources("Southport")
    )

    discover_sources.write_sources(sources, output)
    loaded = discover_sources.load_sources_file(output)

    assert loaded == sources


def test_normalize_area_name_is_deterministic():
    assert discover_sources.normalize_area_name(" Southport, QLD 4215 ") == "southport-qld-4215"
