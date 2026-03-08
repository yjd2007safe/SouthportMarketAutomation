"""Scheduling/date window utilities for sales report generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from typing import Callable, Optional

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - exercised via injected fallback in tests
    ZoneInfo = None  # type: ignore[assignment]


def _build_asia_shanghai_timezone(
    zoneinfo_factory: Optional[Callable[[str], tzinfo]] = ZoneInfo,
) -> tzinfo:
    """Return an Asia/Shanghai timezone object with a Python 3.8-safe fallback."""

    if zoneinfo_factory is not None:
        try:
            return zoneinfo_factory("Asia/Shanghai")
        except Exception:
            pass
    # Asia/Shanghai has no DST and is fixed UTC+08.
    return timezone(timedelta(hours=8), name="Asia/Shanghai")


ASIA_SHANGHAI = _build_asia_shanghai_timezone()


@dataclass(frozen=True)
class ReportWindow:
    period_start: date
    period_end: date


def parse_run_date(run_date: str | date | datetime) -> date:
    if isinstance(run_date, datetime):
        if run_date.tzinfo is None:
            return run_date.date()
        return run_date.astimezone(ASIA_SHANGHAI).date()
    if isinstance(run_date, date):
        return run_date
    return datetime.strptime(run_date, "%Y-%m-%d").date()


def weekly_window_for_run(run_date: str | date | datetime) -> ReportWindow:
    day = parse_run_date(run_date)
    days_since_sunday = (day.weekday() + 1) % 7
    period_start = day - timedelta(days=days_since_sunday)
    period_end = period_start + timedelta(days=6)
    return ReportWindow(period_start=period_start, period_end=period_end)


def previous_month_window_for_run(run_date: str | date | datetime) -> ReportWindow:
    day = parse_run_date(run_date)
    first_of_month = day.replace(day=1)
    period_end = first_of_month - timedelta(days=1)
    period_start = period_end.replace(day=1)
    return ReportWindow(period_start=period_start, period_end=period_end)


def should_generate_weekly(run_date: str | date | datetime) -> bool:
    return parse_run_date(run_date).weekday() == 5  # Saturday


def should_generate_monthly(run_date: str | date | datetime) -> bool:
    return parse_run_date(run_date).day == 1


def determine_report_modes(run_date: str | date | datetime) -> list[str]:
    try:
        parsed_run_date = parse_run_date(run_date)
    except Exception as exc:
        raise ValueError(f"Unable to evaluate report schedule for run_date={run_date!r}") from exc

    modes: list[str] = []
    if parsed_run_date.weekday() == 5:
        modes.append("weekly")
    if parsed_run_date.day == 1:
        modes.append("monthly")
    return modes
