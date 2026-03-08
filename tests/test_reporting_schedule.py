from reporting_schedule import (
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
