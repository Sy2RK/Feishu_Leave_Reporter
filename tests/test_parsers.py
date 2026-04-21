from __future__ import annotations

from zoneinfo import ZoneInfo

import pytest

from feishu_leave_sync.parsers import (
    build_segment_from_instance_detail,
    build_segments_from_event,
    parse_leave_range,
)


def test_parse_leave_range_with_non_json_string() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    raw = "[[2024-09-05 13:30:00,2024-09-05 18:00:00],[2024-09-06 09:00:00,2024-09-06 18:00:00]]"

    parsed = parse_leave_range(raw, timezone)

    assert len(parsed) == 2
    assert parsed[0][0].isoformat() == "2024-09-05T13:30:00+08:00"
    assert parsed[1][1].isoformat() == "2024-09-06T18:00:00+08:00"


def test_build_segments_from_event_uses_leave_range() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    payload = {
        "uuid": "uuid-1",
        "event": {
            "type": "leave_approvalV2",
            "instance_code": "instance-1",
            "open_id": "ou_123",
            "leave_range": "[[2024-09-05 13:30:00,2024-09-05 18:00:00],[2024-09-06 09:00:00,2024-09-06 18:00:00]]",
            "leave_start_time": "2024-09-05 00:00:00",
            "leave_end_time": "2024-09-07 00:00:00",
        },
    }

    segments = build_segments_from_event(payload, timezone, source="event")

    assert len(segments) == 2
    assert segments[0].instance_code == "instance-1"
    assert segments[0].user_id == "ou_123"
    assert segments[0].start_at.isoformat() == "2024-09-05T13:30:00+08:00"
    assert segments[1].end_at.isoformat() == "2024-09-06T18:00:00+08:00"


def test_build_segment_from_instance_detail_parses_leave_group() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    detail = {
        "data": {
            "instance_code": "instance-2",
            "open_id": "ou_456",
            "form": """
            [
              {
                "id": "widgetLeaveGroupV2",
                "type": "leaveGroupV2",
                "value": [
                  {"id": "widgetLeaveGroupStartTime", "type": "date", "value": "2024-09-05T09:00:00+08:00"},
                  {"id": "widgetLeaveGroupEndTime", "type": "date", "value": "2024-09-05T18:00:00+08:00"}
                ]
              }
            ]
            """,
        }
    }

    segment = build_segment_from_instance_detail(detail, timezone, source="reconcile")

    assert segment is not None
    assert segment.instance_code == "instance-2"
    assert segment.user_id == "ou_456"
    assert segment.start_at.isoformat() == "2024-09-05T09:00:00+08:00"
    assert segment.end_at.isoformat() == "2024-09-05T18:00:00+08:00"


def test_build_segment_from_instance_detail_parses_normalized_leave_group_object() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    detail = {
        "instance_code": "instance-3",
        "open_id": "ou_789",
        "form": """
        [
          {
            "id": "widgetLeaveGroupV2",
            "name": "说明",
            "type": "leaveGroupV2",
            "value": {
              "start": "2026-04-21T10:00:00+08:00",
              "end": "2026-04-21T19:00:00+08:00",
              "name": "事假",
              "reason": "学校期末项目验收，需线下到场展示",
              "unit": "HOUR"
            }
          }
        ]
        """,
    }

    segment = build_segment_from_instance_detail(detail, timezone, source="reconcile")

    assert segment is not None
    assert segment.instance_code == "instance-3"
    assert segment.user_id == "ou_789"
    assert segment.start_at.isoformat() == "2026-04-21T10:00:00+08:00"
    assert segment.end_at.isoformat() == "2026-04-21T19:00:00+08:00"


def test_build_segment_from_instance_detail_rejects_invalid_form_json() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    detail = {
        "instance_code": "instance-bad",
        "open_id": "ou_bad",
        "form": "{not-json",
    }

    with pytest.raises(ValueError, match="instance-bad"):
        build_segment_from_instance_detail(detail, timezone, source="reconcile")


def test_build_segments_from_event_prefers_user_id_over_open_id() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    payload = {
        "uuid": "uuid-user-id",
        "event": {
            "type": "leave_approvalV2",
            "instance_code": "instance-user-id",
            "user_id": "user_123",
            "open_id": "ou_should_not_win",
            "leave_range": "[[2024-09-05 13:30:00,2024-09-05 18:00:00]]",
        },
    }

    segments = build_segments_from_event(payload, timezone, source="event")

    assert len(segments) == 1
    assert segments[0].user_id == "user_123"


def test_build_segment_from_instance_detail_prefers_user_id_over_open_id() -> None:
    timezone = ZoneInfo("Asia/Shanghai")
    detail = {
        "instance_code": "instance-prefer-user-id",
        "user_id": "user_456",
        "open_id": "ou_should_not_win",
        "form": """
        [
          {
            "id": "widgetLeaveGroupV2",
            "type": "leaveGroupV2",
            "value": {
              "start": "2026-04-21T10:00:00+08:00",
              "end": "2026-04-21T19:00:00+08:00"
            }
          }
        ]
        """,
    }

    segment = build_segment_from_instance_detail(detail, timezone, source="reconcile")

    assert segment is not None
    assert segment.user_id == "user_456"
