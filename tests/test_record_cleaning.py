import record_cleaning


def test_stable_global_key_prefers_reliable_listing_id():
    key = record_cleaning.stable_global_key({"listing_id": "ABC-123", "url": "https://example.com/a"})
    assert key == "id:abc-123"


def test_stable_global_key_falls_back_for_generated_listing_ids():
    key = record_cleaning.stable_global_key(
        {"listing_id": "lst_abcdef1234", "url": "https://example.com/p/1?x=1", "address": "1 Main St"}
    )
    assert key.startswith("ua:")


def test_normalize_and_dedupe_records_sets_provenance():
    rows = [{"url": "https://EXAMPLE.com/p/1?x=1", "address": "1 Main St"}]
    out = record_cleaning.normalize_and_dedupe_records(rows, source_url="https://site.test/search", source_site="site.test")
    assert len(out) == 1
    assert out[0]["source_url"] == "https://site.test/search"
    assert out[0]["source_site"] == "site.test"
    assert out[0]["url"] == "https://example.com/p/1"
    assert out[0]["global_key"].startswith("ua:")
