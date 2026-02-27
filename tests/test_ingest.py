import datetime as dt
import tempfile
import unittest
from pathlib import Path

from src.ingest import output_path, parse_args, resolve_sources


class IngestTests(unittest.TestCase):
    def test_parse_args_accepts_multiple_sources(self):
        args = parse_args(["--source", "https://a.example", "--source", "https://b.example"])
        self.assertEqual(args.sources, ["https://a.example", "https://b.example"])

    def test_output_path_creates_directory_and_daily_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = output_path(f"{tmpdir}/nested/raw", run_date=dt.date(2026, 1, 2))
            self.assertTrue(Path(tmpdir, "nested", "raw").exists())
            self.assertEqual(target.name, "2026-01-02.jsonl")

    def test_resolve_sources_falls_back_to_defaults(self):
        sources = resolve_sources(None)
        self.assertTrue(len(sources) >= 1)


if __name__ == "__main__":
    unittest.main()
