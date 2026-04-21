from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from feishu_leave_sync.models import LeaveSegment
from feishu_leave_sync.reports import (
    build_weekly_leave_report_card,
    get_next_weekly_report_run_at,
    select_weekly_report_segments,
)


def _segment(
    instance_code: str,
    *,
    user_id: str,
    start_at: str,
    end_at: str,
) -> LeaveSegment:
    return LeaveSegment(
        instance_code=instance_code,
        user_id=user_id,
        start_at=datetime.fromisoformat(start_at),
        end_at=datetime.fromisoformat(end_at),
        timezone_name="Asia/Shanghai",
        source="test",
    )


def test_get_next_weekly_report_run_at_uses_same_monday_before_nine() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    monday_morning = datetime(2026, 4, 20, 8, 30, tzinfo=tz)

    assert get_next_weekly_report_run_at(monday_morning) == datetime(2026, 4, 20, 9, 0, tzinfo=tz)


def test_get_next_weekly_report_run_at_rolls_forward_after_window() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    monday_after_nine = datetime(2026, 4, 20, 9, 1, tzinfo=tz)

    assert get_next_weekly_report_run_at(monday_after_nine) == datetime(2026, 4, 27, 9, 0, tzinfo=tz)


def test_select_weekly_report_segments_only_keeps_current_week_future_items() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    now = datetime(2026, 4, 20, 9, 0, tzinfo=tz)
    segments = [
        _segment(
            "current-week",
            user_id="user_1",
            start_at="2026-04-20T10:00:00+08:00",
            end_at="2026-04-20T18:00:00+08:00",
        ),
        _segment(
            "already-ended",
            user_id="user_2",
            start_at="2026-04-20T06:00:00+08:00",
            end_at="2026-04-20T08:00:00+08:00",
        ),
        _segment(
            "next-week",
            user_id="user_3",
            start_at="2026-04-27T10:00:00+08:00",
            end_at="2026-04-27T18:00:00+08:00",
        ),
    ]

    selected = select_weekly_report_segments(segments, now)

    assert [segment.instance_code for segment in selected] == ["current-week"]


def test_build_weekly_leave_report_card_contains_mentions_and_summary() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    now = datetime(2026, 4, 20, 9, 0, tzinfo=tz)
    segments = [
        _segment(
            "instance-1",
            user_id="user_1",
            start_at="2026-04-20T10:00:00+08:00",
            end_at="2026-04-20T18:00:00+08:00",
        ),
        _segment(
            "instance-2",
            user_id="user_2",
            start_at="2026-04-21T14:00:00+08:00",
            end_at="2026-04-21T14:30:00+08:00",
        ),
    ]

    report = build_weekly_leave_report_card(segments, now)
    elements = report.card["body"]["elements"]
    combined_content = "\n".join(element["content"] for element in elements)

    assert report.segment_count == 2
    assert report.distinct_user_count == 2
    assert report.period_key == "2026-04-20"
    assert "<at id=user_1></at>" in combined_content
    assert "<at id=user_2></at>" in combined_content
    assert "已审批生效请假：**2 条**" in combined_content
