from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, Sequence

from feishu_leave_sync.models import LeaveSegment


WEEKLY_REPORT_TIME = time(hour=9, minute=0)
MAX_WEEKLY_REPORT_SEGMENTS = 20
WEEKDAY_LABELS = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
FULL_DAY_START_TIME = time(hour=10, minute=0)
FULL_DAY_END_TIME = time(hour=19, minute=0)


@dataclass(frozen=True)
class WeeklyReportWindow:
    week_start: datetime
    week_end: datetime

    @property
    def period_key(self) -> str:
        return self.week_start.date().isoformat()

    @property
    def display_range(self) -> str:
        week_end_date = (self.week_end - timedelta(days=1)).date()
        return f"{self.week_start:%m/%d} - {week_end_date:%m/%d}"


@dataclass(frozen=True)
class WeeklyLeaveReport:
    card: dict
    segment_count: int
    distinct_user_count: int
    overlapping_today_count: int
    omitted_segment_count: int
    period_key: str


@dataclass(frozen=True)
class DisplayLeaveEntry:
    start_at: datetime
    end_at: datetime
    is_full_day: bool


def get_weekly_report_window(now: datetime) -> WeeklyReportWindow:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    week_start_date = now.date() - timedelta(days=now.weekday())
    week_start = datetime.combine(week_start_date, time.min, tzinfo=now.tzinfo)
    return WeeklyReportWindow(
        week_start=week_start,
        week_end=week_start + timedelta(days=7),
    )


def get_next_weekly_report_run_at(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    week_start = now.date() - timedelta(days=now.weekday())
    candidate = datetime.combine(week_start, WEEKLY_REPORT_TIME, tzinfo=now.tzinfo)
    if candidate > now:
        return candidate
    return candidate + timedelta(days=7)


def is_weekly_report_due(now: datetime) -> bool:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    if now.weekday() != 0:
        return False
    return now.timetz().replace(tzinfo=None) >= WEEKLY_REPORT_TIME


def select_weekly_report_segments(
    segments: Iterable[LeaveSegment],
    now: datetime,
) -> list[LeaveSegment]:
    window = get_weekly_report_window(now)
    filtered = [
        segment
        for segment in segments
        if segment.end_at > now
        and segment.start_at < window.week_end
        and segment.end_at > window.week_start
    ]
    return sorted(filtered, key=lambda item: (item.start_at, item.end_at, item.user_id, item.instance_code))


def build_weekly_leave_report_card(
    segments: Sequence[LeaveSegment],
    now: datetime,
) -> WeeklyLeaveReport:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    window = get_weekly_report_window(now)
    visible_segments = list(segments[:MAX_WEEKLY_REPORT_SEGMENTS])
    omitted_segment_count = max(len(segments) - len(visible_segments), 0)
    distinct_user_count = len({segment.user_id for segment in segments})
    overlapping_today_count = sum(
        1
        for segment in segments
        if _segment_overlaps_date(segment, now.date())
    )

    header_template = "green" if not segments else "orange"
    elements: list[dict] = []

    if visible_segments:
        for user_id, entries in _group_user_display_entries(visible_segments).items():
            lines = [f"<at id={user_id}></at>"]
            lines.extend(_format_display_entry(entry) for entry in entries)
            elements.append(_markdown_element("\n".join(lines), margin="10px 0px 0px 0px"))
    else:
        elements.append(
            _markdown_element(
                "本周暂无已审批生效的请假日程，当前无需做额外排班调整。",
                margin="8px 0px 0px 0px",
            )
        )

    if omitted_segment_count:
        elements.append(
            _markdown_element(
                f"_其余 {omitted_segment_count} 条请假已省略，避免卡片过长。_",
                margin="10px 0px 0px 0px",
            )
        )

    card = {
        "schema": "2.0",
        "config": {
            "update_multi": True,
            "style": {
                "text_size": {
                    "normal_v2": {
                        "default": "normal",
                        "pc": "normal",
                        "mobile": "heading",
                    }
                }
            },
        },
        "header": {
            "title": {
                "tag": "plain_text",
                "content": "本周请假预报",
            },
            "subtitle": {
                "tag": "plain_text",
                "content": f"{window.display_range} · 周一 09:00 推送",
            },
            "template": header_template,
            "padding": "12px 12px 12px 12px",
        },
        "body": {
            "direction": "vertical",
            "padding": "12px 12px 12px 12px",
            "elements": elements,
        },
    }
    return WeeklyLeaveReport(
        card=card,
        segment_count=len(segments),
        distinct_user_count=distinct_user_count,
        overlapping_today_count=overlapping_today_count,
        omitted_segment_count=omitted_segment_count,
        period_key=window.period_key,
    )


def _group_user_display_entries(segments: Sequence[LeaveSegment]) -> "OrderedDict[str, list[DisplayLeaveEntry]]":
    grouped: "OrderedDict[str, list[LeaveSegment]]" = OrderedDict()
    for segment in segments:
        grouped.setdefault(segment.user_id, []).append(segment)
    return OrderedDict(
        (user_id, _merge_user_segments(user_segments))
        for user_id, user_segments in grouped.items()
    )


def _merge_user_segments(segments: Sequence[LeaveSegment]) -> list[DisplayLeaveEntry]:
    merged: list[DisplayLeaveEntry] = []
    for segment in sorted(segments, key=lambda item: (item.start_at, item.end_at, item.instance_code)):
        is_full_day = _is_full_day_segment(segment)
        current_entry = DisplayLeaveEntry(
            start_at=segment.start_at,
            end_at=segment.end_at,
            is_full_day=is_full_day,
        )
        if not merged:
            merged.append(current_entry)
            continue

        previous_entry = merged[-1]
        if (
            previous_entry.is_full_day
            and current_entry.is_full_day
            and current_entry.start_at.date() == previous_entry.end_at.date() + timedelta(days=1)
        ):
            merged[-1] = DisplayLeaveEntry(
                start_at=previous_entry.start_at,
                end_at=current_entry.end_at,
                is_full_day=True,
            )
            continue

        merged.append(current_entry)
    return merged


def _format_display_entry(entry: DisplayLeaveEntry) -> str:
    if entry.is_full_day:
        if entry.start_at.date() == entry.end_at.date():
            return f"{_format_day_label(entry.start_at)}整天请假"
        return f"{_format_day_label(entry.start_at)}-{_format_day_label(entry.end_at)}整天请假"
    if entry.start_at.date() == entry.end_at.date():
        return f"{_format_day_label(entry.start_at)} {entry.start_at:%H:%M}-{entry.end_at:%H:%M} 请假"
    return (
        f"{_format_day_label(entry.start_at)} {entry.start_at:%H:%M}"
        f" - {_format_day_label(entry.end_at)} {entry.end_at:%H:%M} 请假"
    )


def _format_day_label(value: datetime) -> str:
    return f"{value:%m/%d}（{WEEKDAY_LABELS[value.weekday()]}）"


def _is_full_day_segment(segment: LeaveSegment) -> bool:
    return (
        segment.start_at.timetz().replace(tzinfo=None) == FULL_DAY_START_TIME
        and segment.end_at.timetz().replace(tzinfo=None) == FULL_DAY_END_TIME
        and segment.end_at.date() >= segment.start_at.date()
    )


def _segment_overlaps_date(segment: LeaveSegment, target_date: date) -> bool:
    day_start = datetime.combine(target_date, time.min, tzinfo=segment.start_at.tzinfo)
    day_end = day_start + timedelta(days=1)
    return segment.start_at < day_end and segment.end_at > day_start


def _markdown_element(content: str, *, margin: str = "0px 0px 0px 0px") -> dict:
    return {
        "tag": "markdown",
        "content": content,
        "text_align": "left",
        "text_size": "normal_v2",
        "margin": margin,
    }
