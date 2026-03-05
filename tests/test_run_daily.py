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
    assert "provide --source or --normalized-input" in result.stderr
