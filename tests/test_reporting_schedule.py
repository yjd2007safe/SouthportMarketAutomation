from datetime import datetime, timedelta, timezone

import reporting_schedule
from reporting_schedule import (
    _build_asia_shanghai_timezone,
    determine_report_modes,
    previous_month_window_for_run,
    weekly_window_for_run,
)


def test_weekly_window_saturday_covers_previous_sunday_to_saturday():
    window = weekly_window_for_run("2025-03-08")
    assert window.period_start.isoformat() == "2025-03-02"
    assert window.period_end.isoformat() == "2025-03-08"


def test_previous_month_window_first_day():
    window = previous_month_window_for_run("2025-03-01")
    assert window.period_start.isoformat() == "2025-02-01"
    assert window.period_end.isoformat() == "2025-02-28"


def test_determine_modes_supports_both():
    assert determine_report_modes("2025-02-01") == ["weekly", "monthly"]
    assert determine_report_modes("2025-03-05") == []


def test_determine_modes_saturday_only():
    assert determine_report_modes("2025-03-08") == ["weekly"]


def test_timezone_builder_falls_back_when_zoneinfo_unavailable():
    tz = _build_asia_shanghai_timezone(zoneinfo_factory=None)
    now = datetime(2025, 3, 1, 12, 0, tzinfo=timezone.utc).astimezone(tz)
    assert now.utcoffset() == timedelta(hours=8)


def test_parse_run_date_uses_timezone_conversion_for_aware_datetime():
    aware = datetime(2025, 3, 7, 20, 30, tzinfo=timezone.utc)
    assert reporting_schedule.parse_run_date(aware).isoformat() == "2025-03-08"


def test_determine_modes_wraps_parse_errors():
    try:
        determine_report_modes("2025/03/08")
    except ValueError as exc:
        assert "Unable to evaluate report schedule" in str(exc)
    else:
        raise AssertionError("Expected ValueError")
