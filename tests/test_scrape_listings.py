from pathlib import Path

import scrape_listings


def test_parse_onthehouse_ldjson_fixture_into_structured_records():
    html = Path("tests/fixtures_onthehouse_search.html").read_text(encoding="utf-8")

    rows = scrape_listings.parse_listing_page("https://www.onthehouse.com.au/rent/southport-qld-4215", html)

    assert len(rows) == 2
    first = rows[0]
    assert first["listing_id"].startswith("lst_")
    assert first["url"].startswith("https://www.onthehouse.com.au/property/")
    assert first["address"] == "12 Smith St, Southport, QLD, 4215"
    assert first["rent"] == 650
    assert first["price"] == 650
    assert first["bedrooms"] == 2
    assert first["bathrooms"] == 1.0
    assert first["size_sqft"] == 710.0
    assert first["listed_date"] == "2025-03-01"
    assert first["source_site"] == "onthehouse"
    assert "Southport" in first["raw_snippet"]


def test_parse_onthehouse_fallback_card_extracts_partial_record():
    html = """
    <html><body>
      <a href='/property/qld/southport-4215/2-foo-st-3'>
        2 Foo St Southport $700 3 bed 2 bath with parking
      </a>
    </body></html>
    """

    rows = scrape_listings.parse_listing_page("https://www.onthehouse.com.au/rent/southport", html)

    assert len(rows) == 1
    assert rows[0]["url"] == "https://www.onthehouse.com.au/property/qld/southport-4215/2-foo-st-3"
    assert rows[0]["rent"] == 700
    assert rows[0]["bedrooms"] == 3
    assert rows[0]["bathrooms"] == 2


def test_parse_unknown_site_returns_empty_records():
    rows = scrape_listings.parse_listing_page("https://example.com/search", "<html><body>no listings</body></html>")
    assert rows == []
