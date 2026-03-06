from urllib.error import HTTPError, URLError

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


def test_fetch_text_retries_429_then_succeeds():
    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"ok"

    calls = {"count": 0}
    sleeps = []

    def fake_opener(url, timeout):
        calls["count"] += 1
        if calls["count"] < 3:
            raise HTTPError(url=url, code=429, msg="Too Many Requests", hdrs=None, fp=None)
        return DummyResponse()

    body = requests.fetch_text(
        "https://www.realestate.com.au/rent",
        opener=fake_opener,
        sleep_fn=lambda seconds: sleeps.append(seconds),
        random_fn=lambda: 0.0,
        backoff_base=0.1,
        max_retries=3,
    )

    assert body == "ok"
    assert calls["count"] == 3
    assert sleeps == [0.1, 0.2]


def test_fetch_text_raises_blocked_source_after_retries():
    def always_429(url, timeout):
        raise HTTPError(url=url, code=429, msg="Too Many Requests", hdrs=None, fp=None)

    with pytest.raises(requests.BlockedSourceError, match="Blocked source"):
        requests.fetch_text(
            "https://www.realestate.com.au/rent",
            opener=always_429,
            sleep_fn=lambda _: None,
            random_fn=lambda: 0.0,
            backoff_base=0.01,
            max_retries=2,
        )


def test_fetch_text_retries_network_error_then_succeeds():
    class DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b"ok"

    calls = {"count": 0}

    def flaky_opener(url, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise URLError("connection reset")
        return DummyResponse()

    body = requests.fetch_text(
        "https://example.com/data.json",
        opener=flaky_opener,
        sleep_fn=lambda _: None,
        random_fn=lambda: 0.0,
        backoff_base=0.01,
        max_retries=2,
    )

    assert body == "ok"
