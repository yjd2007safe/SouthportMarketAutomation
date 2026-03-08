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




def test_parse_onthehouse_for_sale_route_uses_existing_parser():
    html = Path("tests/fixtures_onthehouse_search.html").read_text(encoding="utf-8")

    rows = scrape_listings.parse_listing_page("https://www.onthehouse.com.au/for-sale/qld/gold-coast/southport", html)

    assert len(rows) == 2
    assert rows[0]["source_site"] == "onthehouse"
    assert rows[0]["price"] == 650
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


def test_parse_onthehouse_next_data_fixture_into_structured_records():
    html = Path("tests/fixtures_onthehouse_next_data.html").read_text(encoding="utf-8")

    rows = scrape_listings.parse_listing_page("https://www.onthehouse.com.au/rent/southport-qld-4215", html)

    assert len(rows) == 2
    assert rows[0]["listing_id"].startswith("lst_")
    assert rows[0]["url"].startswith("https://www.onthehouse.com.au/property/")
    assert rows[0]["rent"] == 780
    assert rows[0]["bedrooms"] == 3
    assert rows[0]["bathrooms"] == 2.0


def test_parse_onthehouse_initial_state_fixture_extracts_records():
    html = Path("tests/fixtures_onthehouse_initial_state.html").read_text(encoding="utf-8")

    rows = scrape_listings.parse_listing_page("https://www.onthehouse.com.au/for-rent/qld/gold-coast/southport", html)

    assert len(rows) == 1
    assert rows[0]["url"].startswith("https://www.onthehouse.com.au/property/")
    assert rows[0]["rent"] == 920
    assert rows[0]["bedrooms"] == 4


def test_parse_realestate_initial_state_fixture_extracts_records():
    html = Path("tests/fixtures_realestate_initial_state.html").read_text(encoding="utf-8")

    rows = scrape_listings.parse_listing_page("https://www.realestate.com.au/rent/in-southport,+qld+4215/list-1", html)

    assert len(rows) == 2
    assert rows[0]["source_site"] == "realestate"
    assert rows[0]["url"].startswith("https://www.realestate.com.au/property-")
    assert rows[0]["rent"] == 850


def test_detect_challenge_page_kasada_fixture():
    html = Path("tests/fixtures_kasada_blockpage.html").read_text(encoding="utf-8")
    assert scrape_listings.detect_challenge_page(html) == "kasada"


def test_detect_challenge_page_incapsula_fixture():
    html = Path("tests/fixtures_incapsula_blockpage.html").read_text(encoding="utf-8")
    assert scrape_listings.detect_challenge_page(html) == "incapsula"


def test_detect_challenge_page_captcha_fixture():
    html = Path("tests/fixtures_captcha_blockpage.html").read_text(encoding="utf-8")
    assert scrape_listings.detect_challenge_page(html) == "captcha"
