from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

from feishu_leave_sync.models import LeaveSegment, PendingJobRun, PendingTimeoffCreate, TimeoffMapping


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class SQLiteStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(str(db_path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    def initialize(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA foreign_keys=ON;

                CREATE TABLE IF NOT EXISTS processed_events (
                    event_uuid TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    processed_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS leave_segments (
                    instance_code TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    timezone_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (instance_code, start_time, end_time)
                );

                CREATE TABLE IF NOT EXISTS timeoff_events (
                    instance_code TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    timezone_name TEXT NOT NULL,
                    timeoff_event_id TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (instance_code, start_time, end_time)
                );

                CREATE TABLE IF NOT EXISTS pending_timeoff_creates (
                    instance_code TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    timezone_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    remote_timeoff_event_id TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (instance_code, start_time, end_time)
                );

                CREATE TABLE IF NOT EXISTS job_runs (
                    job_name TEXT NOT NULL,
                    period_key TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    PRIMARY KEY (job_name, period_key)
                );

                CREATE TABLE IF NOT EXISTS pending_job_runs (
                    job_name TEXT NOT NULL,
                    period_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (job_name, period_key)
                );
                """
            )
            self._ensure_column(
                "pending_timeoff_creates",
                "remote_timeoff_event_id",
                "TEXT",
            )
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def has_processed_event(self, event_uuid: str) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM processed_events WHERE event_uuid = ?",
                (event_uuid,),
            ).fetchone()
            return row is not None

    def mark_event_processed(self, event_uuid: str, event_type: str, payload: dict) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO processed_events (event_uuid, event_type, payload_json, processed_at)
                VALUES (?, ?, ?, ?)
                """,
                (event_uuid, event_type, json.dumps(payload, ensure_ascii=False), _utc_now()),
            )
            self._connection.commit()

    def _ensure_column(self, table_name: str, column_name: str, column_sql: str) -> None:
        columns = {
            row["name"]
            for row in self._connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name in columns:
            return
        self._connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
        )

    def has_completed_job(self, job_name: str, period_key: str) -> bool:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT 1
                FROM job_runs
                WHERE job_name = ? AND period_key = ?
                """,
                (job_name, period_key),
            ).fetchone()
            return row is not None

    def mark_job_completed(self, job_name: str, period_key: str, details: dict | None = None) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO job_runs (job_name, period_key, details_json, completed_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    job_name,
                    period_key,
                    json.dumps(details or {}, ensure_ascii=False),
                    _utc_now(),
                ),
            )
            self._connection.commit()

    def get_pending_job(self, job_name: str, period_key: str) -> PendingJobRun | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT job_name, period_key, status, details_json
                FROM pending_job_runs
                WHERE job_name = ? AND period_key = ?
                """,
                (job_name, period_key),
            ).fetchone()
        if row is None:
            return None
        return PendingJobRun(
            job_name=row["job_name"],
            period_key=row["period_key"],
            status=row["status"],
            details=json.loads(row["details_json"]),
        )

    def mark_pending_job(
        self,
        job_name: str,
        period_key: str,
        *,
        status: str,
        details: dict | None = None,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO pending_job_runs (job_name, period_key, status, details_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    job_name,
                    period_key,
                    status,
                    json.dumps(details or {}, ensure_ascii=False),
                    _utc_now(),
                ),
            )
            self._connection.commit()

    def clear_pending_job(self, job_name: str, period_key: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                DELETE FROM pending_job_runs
                WHERE job_name = ? AND period_key = ?
                """,
                (job_name, period_key),
            )
            self._connection.commit()

    def replace_segments_for_instance(self, instance_code: str, segments: Iterable[LeaveSegment]) -> None:
        segment_list = list(segments)
        with self._lock:
            self._connection.execute(
                "DELETE FROM leave_segments WHERE instance_code = ?",
                (instance_code,),
            )
            for segment in segment_list:
                self._connection.execute(
                    """
                    INSERT INTO leave_segments (
                        instance_code, user_id, start_time, end_time, timezone_name, source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segment.instance_code,
                        segment.user_id,
                        segment.start_at.isoformat(),
                        segment.end_at.isoformat(),
                        segment.timezone_name,
                        segment.source,
                        _utc_now(),
                    ),
                )
            self._connection.commit()

    def replace_all_segments(self, segments: Iterable[LeaveSegment]) -> None:
        segment_list = list(segments)
        with self._lock:
            self._connection.execute("DELETE FROM leave_segments")
            for segment in segment_list:
                self._connection.execute(
                    """
                    INSERT INTO leave_segments (
                        instance_code, user_id, start_time, end_time, timezone_name, source, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segment.instance_code,
                        segment.user_id,
                        segment.start_at.isoformat(),
                        segment.end_at.isoformat(),
                        segment.timezone_name,
                        segment.source,
                        _utc_now(),
                    ),
                )
            self._connection.commit()

    def delete_segments_for_instance(self, instance_code: str) -> None:
        with self._lock:
            self._connection.execute(
                "DELETE FROM leave_segments WHERE instance_code = ?",
                (instance_code,),
            )
            self._connection.commit()

    def list_segments(self) -> List[LeaveSegment]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT instance_code, user_id, start_time, end_time, timezone_name, source
                FROM leave_segments
                ORDER BY start_time ASC
                """
            ).fetchall()
        return [
            LeaveSegment(
                instance_code=row["instance_code"],
                user_id=row["user_id"],
                start_at=datetime.fromisoformat(row["start_time"]),
                end_at=datetime.fromisoformat(row["end_time"]),
                timezone_name=row["timezone_name"],
                source=row["source"],
            )
            for row in rows
        ]

    def list_segments_for_instance(self, instance_code: str) -> List[LeaveSegment]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT instance_code, user_id, start_time, end_time, timezone_name, source
                FROM leave_segments
                WHERE instance_code = ?
                ORDER BY start_time ASC
                """,
                (instance_code,),
            ).fetchall()
        return [
            LeaveSegment(
                instance_code=row["instance_code"],
                user_id=row["user_id"],
                start_at=datetime.fromisoformat(row["start_time"]),
                end_at=datetime.fromisoformat(row["end_time"]),
                timezone_name=row["timezone_name"],
                source=row["source"],
            )
            for row in rows
        ]

    def upsert_timeoff_event(self, segment: LeaveSegment, timeoff_event_id: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO timeoff_events (
                    instance_code, user_id, start_time, end_time, timezone_name, timeoff_event_id, source, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment.instance_code,
                    segment.user_id,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                    segment.timezone_name,
                    timeoff_event_id,
                    segment.source,
                    _utc_now(),
                ),
            )
            self._connection.commit()

    def has_pending_timeoff_create(self, segment: LeaveSegment) -> bool:
        return self.get_pending_timeoff_create(segment) is not None

    def get_pending_timeoff_create(self, segment: LeaveSegment) -> PendingTimeoffCreate | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT instance_code, user_id, start_time, end_time, timezone_name, source, remote_timeoff_event_id
                FROM pending_timeoff_creates
                WHERE instance_code = ? AND start_time = ? AND end_time = ?
                """,
                (
                    segment.instance_code,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                ),
            ).fetchone()
        return self._row_to_pending_timeoff_create(row)

    def list_pending_timeoff_creates_for_instance(self, instance_code: str) -> List[PendingTimeoffCreate]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT instance_code, user_id, start_time, end_time, timezone_name, source, remote_timeoff_event_id
                FROM pending_timeoff_creates
                WHERE instance_code = ?
                ORDER BY start_time ASC
                """,
                (instance_code,),
            ).fetchall()
        return [
            pending
            for row in rows
            if (pending := self._row_to_pending_timeoff_create(row)) is not None
        ]

    def mark_pending_timeoff_create(
        self,
        segment: LeaveSegment,
        *,
        remote_timeoff_event_id: str | None = None,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO pending_timeoff_creates (
                    instance_code, user_id, start_time, end_time, timezone_name, source, remote_timeoff_event_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    segment.instance_code,
                    segment.user_id,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                    segment.timezone_name,
                    segment.source,
                    remote_timeoff_event_id,
                    _utc_now(),
                ),
            )
            self._connection.commit()

    def set_pending_timeoff_remote_event_id(self, segment: LeaveSegment, remote_timeoff_event_id: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE pending_timeoff_creates
                SET remote_timeoff_event_id = ?, updated_at = ?
                WHERE instance_code = ? AND start_time = ? AND end_time = ?
                """,
                (
                    remote_timeoff_event_id,
                    _utc_now(),
                    segment.instance_code,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                ),
            )
            self._connection.commit()

    def clear_pending_timeoff_create(self, segment: LeaveSegment) -> None:
        with self._lock:
            self._connection.execute(
                """
                DELETE FROM pending_timeoff_creates
                WHERE instance_code = ? AND start_time = ? AND end_time = ?
                """,
                (
                    segment.instance_code,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                ),
            )
            self._connection.commit()

    def clear_pending_timeoff_creates_for_instance(self, instance_code: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                DELETE FROM pending_timeoff_creates
                WHERE instance_code = ?
                """,
                (instance_code,),
            )
            self._connection.commit()

    def list_timeoff_events(self) -> List[TimeoffMapping]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT instance_code, user_id, start_time, end_time, timezone_name, timeoff_event_id, source
                FROM timeoff_events
                ORDER BY start_time ASC
                """
            ).fetchall()
        return [
            TimeoffMapping(
                instance_code=row["instance_code"],
                user_id=row["user_id"],
                start_at=datetime.fromisoformat(row["start_time"]),
                end_at=datetime.fromisoformat(row["end_time"]),
                timezone_name=row["timezone_name"],
                timeoff_event_id=row["timeoff_event_id"],
                source=row["source"],
            )
            for row in rows
        ]

    def list_timeoff_events_for_instance(self, instance_code: str) -> List[TimeoffMapping]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT instance_code, user_id, start_time, end_time, timezone_name, timeoff_event_id, source
                FROM timeoff_events
                WHERE instance_code = ?
                ORDER BY start_time ASC
                """,
                (instance_code,),
            ).fetchall()
        return [
            TimeoffMapping(
                instance_code=row["instance_code"],
                user_id=row["user_id"],
                start_at=datetime.fromisoformat(row["start_time"]),
                end_at=datetime.fromisoformat(row["end_time"]),
                timezone_name=row["timezone_name"],
                timeoff_event_id=row["timeoff_event_id"],
                source=row["source"],
            )
            for row in rows
        ]

    def delete_timeoff_event_mapping(self, instance_code: str, start_time: str, end_time: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                DELETE FROM timeoff_events
                WHERE instance_code = ? AND start_time = ? AND end_time = ?
                """,
                (instance_code, start_time, end_time),
            )
            self._connection.commit()

    def delete_all_timeoff_mappings_for_instance(self, instance_code: str) -> None:
        with self._lock:
            self._connection.execute(
                "DELETE FROM timeoff_events WHERE instance_code = ?",
                (instance_code,),
            )
            self._connection.commit()

    @staticmethod
    def _row_to_pending_timeoff_create(row: sqlite3.Row | None) -> PendingTimeoffCreate | None:
        if row is None:
            return None
        return PendingTimeoffCreate(
            instance_code=row["instance_code"],
            user_id=row["user_id"],
            start_at=datetime.fromisoformat(row["start_time"]),
            end_at=datetime.fromisoformat(row["end_time"]),
            timezone_name=row["timezone_name"],
            source=row["source"],
            remote_timeoff_event_id=row["remote_timeoff_event_id"],
        )
