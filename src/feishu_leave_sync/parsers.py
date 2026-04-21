from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence
from zoneinfo import ZoneInfo

from feishu_leave_sync.models import LeaveSegment


DATETIME_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


def _normalize_datetime_string(value: str) -> str:
    return value.strip().replace("Z", "+00:00")


def parse_datetime(value: str, timezone: ZoneInfo) -> datetime:
    raw = _normalize_datetime_string(value)
    if "T" in raw or "+" in raw:
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone)
        return parsed.astimezone(timezone)
    parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
    return parsed.replace(tzinfo=timezone)


def parse_leave_range(value: Any, timezone: ZoneInfo) -> List[tuple[datetime, datetime]]:
    if value in (None, "", []):
        return []

    pairs: List[tuple[str, str]] = []
    if isinstance(value, list):
        for item in value:
            if isinstance(item, Sequence) and len(item) >= 2:
                start_raw = str(item[0])
                end_raw = str(item[1])
                pairs.append((start_raw, end_raw))
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            decoded = json.loads(text)
        except json.JSONDecodeError:
            matches = DATETIME_PATTERN.findall(text)
            if len(matches) % 2 != 0:
                raise ValueError(f"Unable to parse leave_range value: {value}")
            for index in range(0, len(matches), 2):
                pairs.append((matches[index], matches[index + 1]))
        else:
            return parse_leave_range(decoded, timezone)
    else:
        raise ValueError(f"Unsupported leave_range payload: {value!r}")

    results: List[tuple[datetime, datetime]] = []
    for start_raw, end_raw in pairs:
        start_at = parse_datetime(start_raw, timezone)
        end_at = parse_datetime(end_raw, timezone)
        if end_at <= start_at:
            raise ValueError(f"Invalid leave range: {start_raw} -> {end_raw}")
        results.append((start_at, end_at))
    return results


def _pick_user_identifier(data: Dict[str, Any]) -> str | None:
    for key in ("user_id", "employee_id", "open_id"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def build_segments_from_event(payload: Dict[str, Any], timezone: ZoneInfo, source: str) -> List[LeaveSegment]:
    event = payload["event"]
    instance_code = event["instance_code"]
    user_id = _pick_user_identifier(event)
    if not user_id:
        raise ValueError(f"Event {instance_code} missing user identifier")

    ranges = parse_leave_range(event.get("leave_range"), timezone)
    if not ranges:
        start_raw = event.get("leave_start_time")
        end_raw = event.get("leave_end_time")
        if not start_raw or not end_raw:
            raise ValueError(f"Event {instance_code} missing leave_range and leave_start/end_time")
        ranges = [(parse_datetime(start_raw, timezone), parse_datetime(end_raw, timezone))]

    segments = [
        LeaveSegment(
            instance_code=instance_code,
            user_id=user_id,
            start_at=start_at,
            end_at=end_at,
            timezone_name=timezone.key,
            source=source,
        )
        for start_at, end_at in ranges
    ]
    return segments


def _find_widget_value(items: Iterable[Dict[str, Any]], widget_id: str) -> str | None:
    for item in items:
        if item.get("id") == widget_id:
            value = item.get("value")
            if isinstance(value, str):
                return value
    return None


def build_segment_from_instance_detail(
    instance_detail: Dict[str, Any],
    timezone: ZoneInfo,
    source: str,
) -> LeaveSegment | None:
    data = instance_detail.get("data", instance_detail)
    form_raw = data.get("form")
    if not form_raw:
        return None

    if isinstance(form_raw, str):
        try:
            form_items = json.loads(form_raw)
        except json.JSONDecodeError as exc:
            instance_code = data.get("instance_code", "unknown")
            raise ValueError(f"Instance {instance_code} has invalid form JSON") from exc
    elif isinstance(form_raw, list):
        form_items = form_raw
    else:
        return None

    leave_group = None
    for item in form_items:
        if item.get("type") in {"leaveGroupV2", "leaveGroup"}:
            leave_group = item
            break
    if not leave_group:
        return None

    group_values = leave_group.get("value")
    start_raw = None
    end_raw = None

    if isinstance(group_values, list):
        start_raw = _find_widget_value(group_values, "widgetLeaveGroupStartTime")
        end_raw = _find_widget_value(group_values, "widgetLeaveGroupEndTime")
    elif isinstance(group_values, dict):
        # Some tenants return normalized leaveGroupV2 values directly as a range object
        # instead of an array of nested widget payloads.
        start_raw = group_values.get("start")
        end_raw = group_values.get("end")

    if not start_raw or not end_raw:
        return None

    start_at = parse_datetime(start_raw, timezone)
    end_at = parse_datetime(end_raw, timezone)
    if end_at <= start_at:
        return None

    user_id = _pick_user_identifier(data)
    if not user_id:
        return None

    return LeaveSegment(
        instance_code=data["instance_code"],
        user_id=user_id,
        start_at=start_at,
        end_at=end_at,
        timezone_name=timezone.key,
        source=source,
    )
