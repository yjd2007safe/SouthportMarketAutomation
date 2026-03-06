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
    import functools
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
