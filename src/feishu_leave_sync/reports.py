from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, Sequence

from feishu_leave_sync.models import LeaveSegment


WEEKLY_REPORT_TIME = time(hour=9, minute=0)
MAX_WEEKLY_REPORT_SEGMENTS = 20
WEEKDAY_LABELS = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


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
    summary_markdown = "\n".join(
        (
            "**统计概览**",
            f"- 已审批生效请假：**{len(segments)} 条**",
            f"- 涉及员工：**{distinct_user_count} 人**",
            f"- 今日在途/待开始：**{overlapping_today_count} 条**",
        )
    )

    elements = [_markdown_element(summary_markdown)]

    if visible_segments:
        elements.append(
            _markdown_element(
                "以下为本周已通过审批、且在推送时仍未结束的请假安排。",
                margin="8px 0px 8px 0px",
            )
        )
        for heading, lines in _group_segment_lines(visible_segments).items():
            elements.append(
                _markdown_element(
                    "\n".join((f"**{heading}**", *lines)),
                    margin="10px 0px 0px 0px",
                )
            )
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

    elements.append(
        _markdown_element(
            "\n".join(
                (
                    f"生成时间：`{now:%Y-%m-%d %H:%M}`",
                    f"统计范围：`{window.week_start:%Y-%m-%d}` 至 `{(window.week_end - timedelta(days=1)):%Y-%m-%d}`",
                    "数据来源：飞书审批已通过记录与本地实时同步状态。",
                )
            ),
            margin="12px 0px 0px 0px",
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


def _group_segment_lines(segments: Sequence[LeaveSegment]) -> "OrderedDict[str, list[str]]":
    grouped: "OrderedDict[str, list[str]]" = OrderedDict()
    for segment in segments:
        heading = f"{segment.start_at:%m/%d} {WEEKDAY_LABELS[segment.start_at.weekday()]}"
        grouped.setdefault(heading, []).append(_format_segment_line(segment))
    return grouped


def _format_segment_line(segment: LeaveSegment) -> str:
    mention = f"<at id={segment.user_id}></at>"
    if segment.start_at.date() == segment.end_at.date():
        return f"- `{segment.start_at:%H:%M} - {segment.end_at:%H:%M}` {mention}"
    return (
        "- "
        f"`{segment.start_at:%m/%d %H:%M} → {segment.end_at:%m/%d %H:%M}` "
        f"{mention}"
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
