import functools
from pathlib import Path
import subprocess


def test_run_daily_end_to_end_with_source_csv(tmp_path):
    source = tmp_path / "listings.csv"
    source.write_text(
        "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
        "1500,2025-03-01,2025-02-20,2025-03-05,1,500\n",
        encoding="utf-8",
    )

    raw_dir = tmp_path / "raw"
    normalized_dir = tmp_path / "normalized"
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--source",
            str(source),
            "--date",
            "2025-03-05",
            "--raw-dir",
            str(raw_dir),
            "--normalized-dir",
            str(normalized_dir),
            "--reports-dir",
            str(reports_dir),
            "--log-dir",
            str(log_dir),
            "--analysis-prefix",
            "daily_analysis",
            "--report-prefix",
            "daily_report",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert any(raw_dir.iterdir())
    assert any(normalized_dir.iterdir())
    assert (reports_dir / "daily_analysis.json").exists()
    assert (reports_dir / "daily_report.json").exists()
    assert (log_dir / "run_2025-03-05.log").exists()


def test_run_daily_requires_source_or_normalized_input(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        ["bash", str(script), "--reports-dir", str(tmp_path / "reports")],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "provide --source, --source-list, or --normalized-input" in result.stderr


def test_run_daily_with_supabase_requires_env(tmp_path):
    normalized = tmp_path / "normalized.json"
    normalized.write_text("[]", encoding="utf-8")

    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--normalized-input",
            str(normalized),
            "--date",
            "2025-03-05",
            "--reports-dir",
            str(reports_dir),
            "--log-dir",
            str(log_dir),
            "--with-supabase",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "SUPABASE_URL" in result.stdout or "SUPABASE_URL" in result.stderr


def test_run_daily_with_source_list_json(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    source_dir = tmp_path / "http_sources"
    source_dir.mkdir()
    (source_dir / "a.csv").write_text(
        "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
        "1900,2025-03-01,2025-02-15,2025-03-05,2,700\n",
        encoding="utf-8",
    )

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source_dir))
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {
                        "url": f"http://127.0.0.1:{port}/a.csv",
                        "site": "local-http",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "ingestable",
                    },
                    {
                        "url": "https://www.qld.gov.au/housing/renting",
                        "site": "qld.gov.au",
                        "category": "government",
                        "confidence": 0.8,
                        "notes": "non-ingestable",
                    },
                ]
            ),
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--raw-dir",
                str(raw_dir),
                "--normalized-dir",
                str(normalized_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(log_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert any(raw_dir.iterdir())
    assert any(normalized_dir.iterdir())
    assert (reports_dir / "market_analysis.json").exists()
    assert (reports_dir / "market_report.json").exists()


def test_run_daily_with_source_list_html_parses_to_structured_json(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    source_dir = tmp_path / "http_sources"
    source_dir.mkdir()
    fixture = Path(__file__).resolve().parent / "fixtures_onthehouse_search.html"
    (source_dir / "search").write_text(fixture.read_text(encoding="utf-8"), encoding="utf-8")

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source_dir))
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {
                        "url": f"http://127.0.0.1:{port}/search",
                        "site": "onthehouse.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "ingestable",
                    }
                ]
            ),
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--raw-dir",
                str(raw_dir),
                "--normalized-dir",
                str(normalized_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(log_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    normalized_files = list(normalized_dir.glob("*.json"))
    assert normalized_files
    payload = json.loads(normalized_files[0].read_text(encoding="utf-8"))
    assert isinstance(payload, list)
    assert payload
    assert payload[0]["listing_id"].startswith("lst_")
    assert payload[0]["source_site"] == "onthehouse"


def test_run_daily_source_list_partial_success_with_blocked_source(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/blocked":
                self.send_response(429)
                self.end_headers()
                self.wfile.write(b"rate limited")
            elif self.path == "/ok.csv":
                body = (
                    "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
                    "2100,2025-03-01,2025-02-15,2025-03-05,2,780\n"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            return

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {
                        "url": f"http://127.0.0.1:{port}/blocked",
                        "site": "realestate.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "blocked",
                    },
                    {
                        "url": f"http://127.0.0.1:{port}/ok.csv",
                        "site": "local-http",
                        "category": "search",
                        "confidence": 0.92,
                        "notes": "ok",
                    },
                ]
            ),
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--raw-dir",
                str(raw_dir),
                "--normalized-dir",
                str(normalized_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(log_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert "status=blocked" in result.stdout
    assert "status=ok" in result.stdout
    assert "backend=http" in result.stdout
    assert "attempts=" in result.stdout
    assert "outcome=ok" in result.stdout
    assert (reports_dir / "market_analysis.json").exists()


def test_run_daily_marks_parse_failed_when_html_has_zero_records(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/empty":
                body = b"<html><body><h1>realestate.com.au</h1><p>no listings</p></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            return

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {
                        "url": f"http://127.0.0.1:{port}/empty",
                        "site": "realestate.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "empty html",
                    }
                ]
            ),
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--raw-dir",
                str(raw_dir),
                "--normalized-dir",
                str(normalized_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(log_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode != 0
    assert "status=parse_failed" in result.stdout
    assert "parse_failed=1" in result.stdout


def test_run_daily_source_list_continues_on_challenge_blocked_html(tmp_path):
    import functools
    import http.server
    import json
    import socketserver
    import threading

    source_dir = tmp_path / "http_sources"
    source_dir.mkdir()
    blocked_html = Path(__file__).resolve().parent / "fixtures_kasada_blockpage.html"
    (source_dir / "blocked").write_text(blocked_html.read_text(encoding="utf-8"), encoding="utf-8")
    (source_dir / "ok.csv").write_text(
        "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
        "2200,2025-03-01,2025-02-15,2025-03-05,2,760\n",
        encoding="utf-8",
    )

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source_dir))
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {
                        "url": f"http://127.0.0.1:{port}/blocked",
                        "site": "realestate.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "kasada blocked",
                    },
                    {
                        "url": f"http://127.0.0.1:{port}/ok.csv",
                        "site": "local-http",
                        "category": "search",
                        "confidence": 0.90,
                        "notes": "ingestable",
                    },
                ]
            ),
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--raw-dir",
                str(raw_dir),
                "--normalized-dir",
                str(normalized_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(log_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert "status=blocked" in result.stdout
    assert "challenge:kasada" in result.stdout
    assert "challenge_blocked=1" in result.stdout
    assert "status=ok" in result.stdout
    assert (reports_dir / "market_analysis.json").exists()


def test_run_daily_source_list_continues_on_parse_failed_when_one_source_succeeds(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/empty":
                body = b"<html><body><h1>realestate.com.au</h1><p>no listings</p></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/ok.csv":
                body = (
                    "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
                    "1950,2025-03-01,2025-02-20,2025-03-05,2,700\n"
                ).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/csv")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            return

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {
                        "url": f"http://127.0.0.1:{port}/empty",
                        "site": "realestate.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "parse failed",
                    },
                    {
                        "url": f"http://127.0.0.1:{port}/ok.csv",
                        "site": "local-http",
                        "category": "search",
                        "confidence": 0.90,
                        "notes": "ok",
                    },
                ]
            ),
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--raw-dir",
                str(raw_dir),
                "--normalized-dir",
                str(normalized_dir),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(log_dir),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert "status=parse_failed" in result.stdout
    assert "parse_failed=1" in result.stdout
    assert "status=ok" in result.stdout
    assert (reports_dir / "market_analysis.json").exists()
