from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Tuple


@dataclass(frozen=True)
class LeaveSegment:
    instance_code: str
    user_id: str
    start_at: datetime
    end_at: datetime
    timezone_name: str
    source: str

    @property
    def key(self) -> Tuple[str, str, str]:
        return (
            self.instance_code,
            self.start_at.isoformat(),
            self.end_at.isoformat(),
        )

    def as_record(self) -> Dict[str, str]:
        return {
            "instance_code": self.instance_code,
            "user_id": self.user_id,
            "start_time": self.start_at.isoformat(),
            "end_time": self.end_at.isoformat(),
            "timezone": self.timezone_name,
            "source": self.source,
        }


@dataclass(frozen=True)
class TimeoffMapping:
    instance_code: str
    user_id: str
    start_at: datetime
    end_at: datetime
    timezone_name: str
    timeoff_event_id: str
    source: str

    @property
    def key(self) -> Tuple[str, str, str]:
        return (
            self.instance_code,
            self.start_at.isoformat(),
            self.end_at.isoformat(),
        )


@dataclass(frozen=True)
class PendingTimeoffCreate:
    instance_code: str
    user_id: str
    start_at: datetime
    end_at: datetime
    timezone_name: str
    source: str
    remote_timeoff_event_id: str | None

    @property
    def key(self) -> Tuple[str, str, str]:
        return (
            self.instance_code,
            self.start_at.isoformat(),
            self.end_at.isoformat(),
        )


@dataclass(frozen=True)
class PendingJobRun:
    job_name: str
    period_key: str
    status: str
    details: Dict[str, object]
