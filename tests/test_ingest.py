import json
from pathlib import Path

import ingest


class DummyResponse:
    def __init__(self, status_code=200, text="ok", should_raise=False):
        self.status_code = status_code
        self.text = text
        self._should_raise = should_raise

    def raise_for_status(self):
        if self._should_raise:
            raise ingest.requests.HTTPError("bad response")


class FlakySession:
    def __init__(self):
        self.calls = 0

    def get(self, url, timeout):
        self.calls += 1
        if self.calls == 1:
            raise ingest.requests.Timeout("timed out")
        return DummyResponse(status_code=200, text="hello world")


def test_load_sources_supports_source_and_url(tmp_path: Path):
    src = tmp_path / "sources.txt"
    src.write_text("# comment\nnews,https://example.com\nhttps://another.example\n", encoding="utf-8")

    sources = ingest.load_sources(src)

    assert len(sources) == 2
    assert sources[0].source == "news"
    assert sources[0].url == "https://example.com"
    assert sources[1].source == "https://another.example"


def test_fetch_url_retries_then_succeeds(monkeypatch):
    session = FlakySession()
    monkeypatch.setattr(ingest.time, "sleep", lambda _: None)

    status, text, error = ingest.fetch_url(session, "https://example.com", timeout=0.1, retries=1)

    assert status == 200
    assert text == "hello world"
    assert error is None
    assert session.calls == 2


def test_write_jsonl_creates_output(tmp_path: Path):
    output = tmp_path / "raw" / "out.jsonl"
    records = [{"source": "a", "url": "u", "fetched_at": "now", "text_snippet": "x", "status": 200, "error": None}]

    ingest.write_jsonl(records, output)

    lines = output.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["source"] == "a"


def test_make_record_truncates_snippet():
    source = ingest.Source(source="x", url="https://x")

    record = ingest.make_record(source, 200, "abcdef", None, max_snippet_chars=3)

    assert record["text_snippet"] == "abc"
    assert record["status"] == 200
