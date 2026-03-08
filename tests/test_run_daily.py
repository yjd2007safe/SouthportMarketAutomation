import functools
from pathlib import Path
import subprocess
import time


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
            "--no-supabase",
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
    assert not (reports_dir / "daily_report.json").exists()
    assert (log_dir / "run_2025-03-05.log").exists()


def test_run_daily_relay_mode_falls_back_to_http_when_relay_fails(tmp_path):
    import http.server
    import os
    import socketserver
    import threading

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/ok.csv":
                body = (
                    "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
                    "2000,2025-03-01,2025-02-20,2025-03-05,2,740\n"
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

        bridge = tmp_path / "relay_bridge.py"
        bridge.write_text(
            "import sys\n"
            "print('relay failure', file=sys.stderr)\n"
            "raise SystemExit(1)\n",
            encoding="utf-8",
        )

        raw_dir = tmp_path / "raw"
        normalized_dir = tmp_path / "normalized"
        reports_dir = tmp_path / "reports"
        log_dir = tmp_path / "logs"

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        env = os.environ.copy()
        env["SMA_RELAY_BRIDGE_SCRIPT"] = str(bridge)
        env["SMA_FETCH_RELAY_DOMAINS"] = "127.0.0.1"

        result = subprocess.run(
            [
                "bash",
                str(script),
                "--no-supabase",
                "--source",
                f"http://127.0.0.1:{port}/ok.csv",
                "--date",
                "2025-03-05",
                "--fetch-mode",
                "relay",
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
            env=env,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    assert "fetch_mode=relay" in result.stdout
    assert "backend_used=http" in result.stdout
    assert (reports_dir / "market_analysis.json").exists()


def test_run_daily_rejects_invalid_fetch_mode(tmp_path):
    source = tmp_path / "listings.csv"
    source.write_text(
        "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
        "1500,2025-03-01,2025-02-20,2025-03-05,1,500\n",
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        ["bash", str(script), "--no-supabase", "--source", str(source), "--fetch-mode", "bad"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "invalid --fetch-mode" in result.stderr


def test_run_daily_requires_source_or_normalized_input(tmp_path):
    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        ["bash", str(script), "--no-supabase", "--reports-dir", str(tmp_path / "reports")],
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
                "--no-supabase",
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
    assert not (reports_dir / "market_report.json").exists()


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
                "--no-supabase",
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
                        "url": f"http://127.0.0.1:{port}/ok.csv",
                        "site": "local-http",
                        "category": "search",
                        "confidence": 0.92,
                        "notes": "ok",
                    },
                    {
                        "url": f"http://127.0.0.1:{port}/blocked",
                        "site": "realestate.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "blocked",
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
                "--no-supabase",
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
    assert "backend_used=http" in result.stdout
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
                "--no-supabase",
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
                        "url": f"http://127.0.0.1:{port}/ok.csv",
                        "site": "local-http",
                        "category": "search",
                        "confidence": 0.90,
                        "notes": "ingestable",
                    },
                    {
                        "url": f"http://127.0.0.1:{port}/blocked",
                        "site": "realestate.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "kasada blocked",
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
                "--no-supabase",
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
                "--no-supabase",
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


def test_run_daily_rejects_invalid_stability_profile(tmp_path):
    source = tmp_path / "listings.csv"
    source.write_text(
        "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
        "1500,2025-03-01,2025-02-20,2025-03-05,1,500\n",
        encoding="utf-8",
    )

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        ["bash", str(script), "--no-supabase", "--source", str(source), "--stability-profile", "turbo"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "invalid --stability-profile" in result.stderr


def test_run_daily_creates_relay_handoff_and_times_out_without_payload(tmp_path):
    import functools
    import json
    import http.server
    import socketserver
    import threading

    source_dir = tmp_path / "src"
    source_dir.mkdir()
    blocked_html = Path(__file__).resolve().parent / "fixtures_kasada_blockpage.html"
    (source_dir / "blocked").write_text(blocked_html.read_text(encoding="utf-8"), encoding="utf-8")

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
                        "notes": "priority blocked",
                    }
                ]
            ),
            encoding="utf-8",
        )

        handoff_dir = tmp_path / "handoffs"
        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--no-supabase",
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--handoff-dir",
                str(handoff_dir),
                "--relay-timeout-seconds",
                "1",
                "--relay-poll-seconds",
                "1",
                "--raw-dir",
                str(tmp_path / "raw"),
                "--normalized-dir",
                str(tmp_path / "normalized"),
                "--reports-dir",
                str(tmp_path / "reports"),
                "--log-dir",
                str(tmp_path / "logs"),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode != 0
    artifacts = list(handoff_dir.glob("pending_relay_*.json"))
    assert artifacts
    payload = json.loads(artifacts[0].read_text(encoding="utf-8"))
    assert payload["status"] == "timed_out"
    assert "relay_handoff" in result.stdout


def test_run_daily_resumes_with_manual_relay_payload(tmp_path):
    import functools
    import json
    import http.server
    import socketserver
    import threading

    source_dir = tmp_path / "src"
    source_dir.mkdir()
    blocked_html = Path(__file__).resolve().parent / "fixtures_kasada_blockpage.html"
    (source_dir / "blocked").write_text(blocked_html.read_text(encoding="utf-8"), encoding="utf-8")

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source_dir))

    def operator_write_payload(handoff_dir: Path):
        deadline = time.time() + 10
        while time.time() < deadline:
            candidates = list(handoff_dir.glob("pending_relay_*.json"))
            if candidates:
                handoff = json.loads(candidates[0].read_text(encoding="utf-8"))
                payload_path = Path(handoff["expected_payload_path"])
                payload_path.parent.mkdir(parents=True, exist_ok=True)
                payload_path.write_text(
                    json.dumps(
                        {
                            "handoff_id": handoff["handoff_id"],
                            "source_url": handoff["source_url"],
                            "run_date": handoff["run_date"],
                            "listings": [
                                {
                                    "listing_id": "lst_relay_1",
                                    "rent": 2300,
                                    "snapshot_date": "2025-03-05",
                                    "first_seen": "2025-03-01",
                                    "last_seen": "2025-03-05",
                                    "bedrooms": 2,
                                    "size_sqft": 720,
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                return
            time.sleep(0.1)

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
                        "notes": "priority blocked",
                    }
                ]
            ),
            encoding="utf-8",
        )

        handoff_dir = tmp_path / "handoffs"
        operator = threading.Thread(target=operator_write_payload, args=(handoff_dir,), daemon=True)
        operator.start()

        script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
        reports_dir = tmp_path / "reports"
        result = subprocess.run(
            [
                "bash",
                str(script),
                "--no-supabase",
                "--source-list",
                str(source_list),
                "--date",
                "2025-03-05",
                "--handoff-dir",
                str(handoff_dir),
                "--relay-timeout-seconds",
                "8",
                "--relay-poll-seconds",
                "1",
                "--raw-dir",
                str(tmp_path / "raw"),
                "--normalized-dir",
                str(tmp_path / "normalized"),
                "--reports-dir",
                str(reports_dir),
                "--log-dir",
                str(tmp_path / "logs"),
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        operator.join(timeout=2)
        httpd.shutdown()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr
    artifacts = list(handoff_dir.glob("pending_relay_*.json"))
    assert artifacts
    payload = json.loads(artifacts[0].read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert "relay_handoff" in result.stdout
    assert "resumed" in result.stdout
    assert (reports_dir / "market_analysis.json").exists()


def test_run_daily_source_list_passes_navigation_profile_metadata(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    source_dir = tmp_path / "http_sources"
    source_dir.mkdir()
    (source_dir / "a.csv").write_text(
        "rent,snapshot_date,first_seen,last_seen,bedrooms,size_sqft\n"
        "2100,2025-03-01,2025-02-20,2025-03-05,2,740\n",
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
                        "site": "onthehouse.com.au",
                        "category": "search",
                        "confidence": 0.95,
                        "notes": "ingestable",
                        "navigation_profile": "onthehouse_sale_southport",
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
                "--no-supabase",
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
    assert "nav_profile=onthehouse_sale_southport" in result.stdout

def test_run_daily_source_list_global_dedup_and_provenance(tmp_path):
    import http.server
    import json
    import socketserver
    import threading

    source_dir = tmp_path / "http_sources"
    source_dir.mkdir()
    row = "listing_id,address,url,rent,snapshot_date,bedrooms,size_sqft\n"
    listing = "dup-001,1 Main St,https://example.com/property/1,2100,2025-03-05,2,700\n"
    (source_dir / "a.csv").write_text(row + listing, encoding="utf-8")
    (source_dir / "b.csv").write_text(row + listing, encoding="utf-8")

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source_dir))
    with socketserver.TCPServer(("127.0.0.1", 0), handler) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        port = httpd.server_address[1]

        source_list = tmp_path / "sources.json"
        source_list.write_text(
            json.dumps(
                [
                    {"url": f"http://127.0.0.1:{port}/a.csv", "site": "alpha", "category": "search", "confidence": 0.9, "notes": "a"},
                    {"url": f"http://127.0.0.1:{port}/b.csv", "site": "beta", "category": "search", "confidence": 0.9, "notes": "b"},
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
                "--no-supabase",
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
    combined = normalized_dir / "normalized_2025-03-05_combined.json"
    assert combined.exists()
    payload = json.loads(combined.read_text(encoding="utf-8"))
    assert len(payload) == 1
    assert payload[0]["global_key"].startswith("id:")
    assert payload[0]["source_url"].startswith("http://127.0.0.1:")
    assert payload[0]["source_site"] == "127.0.0.1"

    analysis = json.loads((reports_dir / "market_analysis.json").read_text(encoding="utf-8"))
    assert analysis["record_count"] == 1



def test_run_daily_skips_report_on_non_saturday_non_month_start(tmp_path):
    normalized = tmp_path / "normalized.json"
    normalized.write_text("[]", encoding="utf-8")
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--no-supabase",
            "--normalized-input",
            str(normalized),
            "--date",
            "2025-03-05",
            "--reports-dir",
            str(reports_dir),
            "--log-dir",
            str(log_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "[stage:report] skipped" in result.stdout




def test_run_daily_generates_weekly_on_saturday(tmp_path):
    normalized = tmp_path / "normalized.json"
    normalized.write_text("[]", encoding="utf-8")
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--no-supabase",
            "--normalized-input",
            str(normalized),
            "--date",
            "2025-03-08",
            "--reports-dir",
            str(reports_dir),
            "--log-dir",
            str(log_dir),
            "--report-local-output-mode",
            "persist",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "mode=weekly" in result.stdout
    assert "mode=monthly" not in result.stdout
    assert (reports_dir / "market_report_weekly_exec.json").exists()
    assert (reports_dir / "market_report_weekly_detailed.json").exists()
    assert not (reports_dir / "market_report_monthly_exec.json").exists()


def test_run_daily_generates_weekly_and_monthly_when_both_conditions(tmp_path):
    normalized = tmp_path / "normalized.json"
    normalized.write_text("[]", encoding="utf-8")
    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--no-supabase",
            "--normalized-input",
            str(normalized),
            "--date",
            "2025-02-01",
            "--reports-dir",
            str(reports_dir),
            "--log-dir",
            str(log_dir),
            "--report-local-output-mode",
            "persist",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "mode=weekly" in result.stdout
    assert "mode=monthly" in result.stdout
    assert (reports_dir / "market_report_weekly_exec.json").exists()
    assert (reports_dir / "market_report_weekly_detailed.json").exists()
    assert (reports_dir / "market_report_monthly_exec.json").exists()
    assert (reports_dir / "market_report_monthly_detailed.json").exists()

def test_run_daily_saturday_generates_weekly_exec_and_detailed_reports(tmp_path):
    normalized = tmp_path / "normalized.json"
    normalized.write_text(
        """[
  {"global_key":"id:a","status":"sold","sold_date":"2025-03-03","property_category":"house","price":900000}
]""",
        encoding="utf-8",
    )

    reports_dir = tmp_path / "reports"
    log_dir = tmp_path / "logs"

    script = Path(__file__).resolve().parents[1] / "scripts" / "run_daily.sh"
    result = subprocess.run(
        [
            "bash",
            str(script),
            "--no-supabase",
            "--normalized-input",
            str(normalized),
            "--date",
            "2025-03-08",
            "--reports-dir",
            str(reports_dir),
            "--log-dir",
            str(log_dir),
            "--report-local-output-mode",
            "persist",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (reports_dir / "market_report_weekly_exec.json").exists()
    assert (reports_dir / "market_report_weekly_detailed.json").exists()
    assert not (reports_dir / "market_report_monthly_exec.json").exists()
