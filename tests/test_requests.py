import pytest

import requests


def test_validate_url_scheme_allows_https():
    requests.validate_url_scheme("https://example.com/a")


def test_validate_url_scheme_rejects_file_scheme():
    with pytest.raises(ValueError, match="Unsupported URL scheme"):
        requests.validate_url_scheme("file:///etc/passwd")


def test_fetch_text_happy_path_with_mock_opener():
    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"status": "ok"}'

    seen = {}

    def fake_opener(url, timeout):
        seen["url"] = url
        seen["timeout"] = timeout
        return DummyResponse()

    body = requests.fetch_text("https://example.com/data.json", timeout=3, opener=fake_opener)

    assert body == '{"status": "ok"}'
    assert seen == {"url": "https://example.com/data.json", "timeout": 3}
