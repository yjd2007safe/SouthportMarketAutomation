"""Scheduling/date window utilities for sales report generation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class ReportWindow:
    period_start: date
    period_end: date


def parse_run_date(run_date: str | date | datetime) -> date:
    if isinstance(run_date, datetime):
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
    modes: list[str] = []
    if should_generate_weekly(run_date):
        modes.append("weekly")
    if should_generate_monthly(run_date):
        modes.append("monthly")
    return modes
