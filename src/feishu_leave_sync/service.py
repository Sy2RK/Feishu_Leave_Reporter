from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Tuple

from feishu_leave_sync.api import FeishuApiClient
from feishu_leave_sync.config import Settings
from feishu_leave_sync.db import SQLiteStore
from feishu_leave_sync.models import LeaveSegment, TimeoffMapping
from feishu_leave_sync.parsers import build_segment_from_instance_detail, build_segments_from_event
from feishu_leave_sync.reports import (
    build_weekly_leave_report_card,
    get_next_weekly_report_run_at,
    is_weekly_report_due,
    select_weekly_report_segments,
)

if TYPE_CHECKING:
    import lark_oapi as lark


LOGGER = logging.getLogger(__name__)
DAILY_RECONCILE_TIMES = (time(hour=8, minute=0), time(hour=18, minute=0))
WEEKLY_REPORT_JOB_NAME = "weekly_leave_report"


@dataclass(frozen=True)
class ReconcileStats:
    expected_segments: int
    created_events: int
    deleted_events: int


@dataclass(frozen=True)
class ReconcilePlan:
    desired_segments_by_instance: Dict[str, Tuple[LeaveSegment, ...]]
    authoritative_instance_codes: Tuple[str, ...]
    skipped_instance_codes: Tuple[str, ...]

    @property
    def expected_segments(self) -> int:
        return sum(len(segments) for segments in self.desired_segments_by_instance.values())


class LeaveSyncService:
    def __init__(
        self,
        settings: Settings,
        store: SQLiteStore,
        api_client: FeishuApiClient,
    ) -> None:
        self._settings = settings
        self._store = store
        self._api = api_client
        self._lock = threading.RLock()
        self._scheduler_stop = threading.Event()
        self._scheduler_thread: threading.Thread | None = None
        self._weekly_report_thread: threading.Thread | None = None
        self._has_successful_reconcile = False

    def bootstrap(self) -> None:
        LOGGER.info("Bootstrapping service")
        self._api.get_tenant_access_token()
        for approval_code in self._settings.approval_codes:
            self._api.subscribe_approval(approval_code)

    def run_startup_reconcile(self) -> ReconcileStats:
        LOGGER.info("Starting startup reconcile")
        stats = self._run_full_reconcile(reason="startup")
        self._on_successful_reconcile(reason="startup")
        return stats

    def run_scheduled_reconcile(self) -> ReconcileStats:
        LOGGER.info("Starting scheduled reconcile")
        stats = self._run_full_reconcile(reason="scheduled")
        self._on_successful_reconcile(reason="scheduled")
        return stats

    def start_periodic_reconcile_scheduler(self) -> None:
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            LOGGER.info("Periodic reconcile scheduler already running")
            return

        self._scheduler_stop.clear()
        self._scheduler_thread = threading.Thread(
            target=self._periodic_reconcile_loop,
            name="feishu-periodic-reconcile",
            daemon=True,
        )
        self._scheduler_thread.start()

    def stop_periodic_reconcile_scheduler(self) -> None:
        self._scheduler_stop.set()
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=5)
        if self._weekly_report_thread and self._weekly_report_thread.is_alive():
            self._weekly_report_thread.join(timeout=5)

    def start_weekly_report_scheduler(self) -> None:
        if not self._settings.weekly_report_webhook_url:
            LOGGER.info("Weekly leave report scheduler disabled because FEISHU_WEEKLY_REPORT_WEBHOOK_URL is not set")
            return

        if self._weekly_report_thread and self._weekly_report_thread.is_alive():
            LOGGER.info("Weekly leave report scheduler already running")
            return

        self._scheduler_stop.clear()
        self._weekly_report_thread = threading.Thread(
            target=self._weekly_report_loop,
            name="feishu-weekly-leave-report",
            daemon=True,
        )
        self._weekly_report_thread.start()

    def _run_full_reconcile(self, *, reason: str) -> ReconcileStats:
        reconcile_plan = self._build_reconcile_plan()
        created = 0
        deleted = 0
        with self._lock:
            for instance_code in reconcile_plan.authoritative_instance_codes:
                desired_segments = reconcile_plan.desired_segments_by_instance.get(instance_code, ())
                self._store.replace_segments_for_instance(instance_code, desired_segments)
                instance_created, instance_deleted = self._sync_instance_locked(instance_code)
                created += instance_created
                deleted += instance_deleted
        LOGGER.info(
            "%s reconcile finished: expected=%s authoritative_instances=%s skipped_instances=%s created=%s deleted=%s",
            reason,
            reconcile_plan.expected_segments,
            len(reconcile_plan.authoritative_instance_codes),
            len(reconcile_plan.skipped_instance_codes),
            created,
            deleted,
        )
        return ReconcileStats(
            expected_segments=reconcile_plan.expected_segments,
            created_events=created,
            deleted_events=deleted,
        )

    def _periodic_reconcile_loop(self) -> None:
        while not self._scheduler_stop.is_set():
            now = datetime.now(tz=self._settings.timezone)
            next_run_at = get_next_reconcile_run_at(now)
            wait_seconds = max((next_run_at - now).total_seconds(), 1.0)
            LOGGER.info(
                "Periodic reconcile scheduler armed: next_run_at=%s wait_seconds=%.0f",
                next_run_at.isoformat(),
                wait_seconds,
            )
            if self._scheduler_stop.wait(wait_seconds):
                break

            try:
                self.run_scheduled_reconcile()
            except Exception:
                LOGGER.exception("Scheduled reconcile failed")

    def _weekly_report_loop(self) -> None:
        try:
            self.run_weekly_report_if_due(reason="startup")
        except Exception:
            LOGGER.exception("Weekly leave report startup check failed")

        while not self._scheduler_stop.is_set():
            now = datetime.now(tz=self._settings.timezone)
            next_run_at = get_next_weekly_report_run_at(now)
            wait_seconds = max((next_run_at - now).total_seconds(), 1.0)
            LOGGER.info(
                "Weekly leave report scheduler armed: next_run_at=%s wait_seconds=%.0f",
                next_run_at.isoformat(),
                wait_seconds,
            )
            if self._scheduler_stop.wait(wait_seconds):
                break

            try:
                self.run_weekly_report_if_due(reason="scheduled")
            except Exception:
                LOGGER.exception("Weekly leave report send failed")

    def run_weekly_report_if_due(self, *, reason: str, now: datetime | None = None) -> bool:
        webhook_url = self._settings.weekly_report_webhook_url
        if not webhook_url:
            return False

        current_time = now or datetime.now(tz=self._settings.timezone)
        if not is_weekly_report_due(current_time):
            return False
        if not self._has_successful_reconcile:
            LOGGER.warning(
                "Skipping weekly leave report because no successful reconcile has completed in this process yet"
            )
            return False

        with self._lock:
            segments = select_weekly_report_segments(self._store.list_segments(), current_time)
            report = build_weekly_leave_report_card(segments, current_time)
            if self._store.has_completed_job(WEEKLY_REPORT_JOB_NAME, report.period_key):
                try:
                    self._store.clear_pending_job(WEEKLY_REPORT_JOB_NAME, report.period_key)
                except Exception:
                    LOGGER.exception(
                        "Failed to clear stale weekly leave report pending state after completion was already recorded: period=%s",
                        report.period_key,
                    )
                LOGGER.info(
                    "Skipping weekly leave report because period has already been sent: period=%s",
                    report.period_key,
                )
                return False

            pending_job = self._store.get_pending_job(WEEKLY_REPORT_JOB_NAME, report.period_key)
            if pending_job is not None:
                if pending_job.status == "sent_remote":
                    LOGGER.warning(
                        "Finalizing weekly leave report after a previous webhook send already succeeded remotely: period=%s",
                        report.period_key,
                    )
                    self._store.mark_job_completed(
                        WEEKLY_REPORT_JOB_NAME,
                        report.period_key,
                        pending_job.details,
                    )
                    self._store.clear_pending_job(WEEKLY_REPORT_JOB_NAME, report.period_key)
                    return False

                LOGGER.warning(
                    "Skipping weekly leave report because a previous send attempt is still pending and remote delivery is unknown: period=%s",
                    report.period_key,
                )
                return False

            report_details = {
                "reason": reason,
                "segment_count": report.segment_count,
                "distinct_user_count": report.distinct_user_count,
                "overlapping_today_count": report.overlapping_today_count,
                "omitted_segment_count": report.omitted_segment_count,
            }
            self._store.mark_pending_job(
                WEEKLY_REPORT_JOB_NAME,
                report.period_key,
                status="sending",
                details=report_details,
            )

            LOGGER.info(
                "Sending weekly leave report reason=%s period=%s segments=%s users=%s",
                reason,
                report.period_key,
                report.segment_count,
                report.distinct_user_count,
            )
            self._api.send_bot_webhook_card(webhook_url, report.card)
            pending_status_persisted = False
            try:
                self._store.mark_pending_job(
                    WEEKLY_REPORT_JOB_NAME,
                    report.period_key,
                    status="sent_remote",
                    details=report_details,
                )
                pending_status_persisted = True
            except Exception:
                LOGGER.exception(
                    "Failed to persist weekly leave report remote-send marker; keeping pending state to avoid duplicates: period=%s",
                    report.period_key,
                )

            try:
                self._store.mark_job_completed(
                    WEEKLY_REPORT_JOB_NAME,
                    report.period_key,
                    report_details,
                )
            except Exception:
                LOGGER.exception(
                    "Failed to persist weekly leave report completion after webhook send; pending_status_persisted=%s period=%s",
                    pending_status_persisted,
                    report.period_key,
                )
                return True

            try:
                self._store.clear_pending_job(WEEKLY_REPORT_JOB_NAME, report.period_key)
            except Exception:
                LOGGER.exception(
                    "Failed to clear weekly leave report pending state after completion; completed marker is already durable: period=%s",
                    report.period_key,
                )
        return True

    def process_customized_event(self, payload: Dict[str, Any]) -> None:
        event_type = payload.get("event", {}).get("type")
        event_uuid = payload.get("uuid")
        if not event_type or not event_uuid:
            raise ValueError(f"Unexpected event payload shape: {payload}")

        with self._lock:
            if self._store.has_processed_event(event_uuid):
                LOGGER.info("Skipping duplicate event uuid=%s type=%s", event_uuid, event_type)
                return

            if event_type == "leave_approval":
                self._handle_leave_approval_locked(payload)
                self._store.mark_event_processed(event_uuid, event_type, payload)
                return

            if event_type == "leave_approvalV2":
                self._handle_leave_approval_v2_locked(payload)
                self._store.mark_event_processed(event_uuid, event_type, payload)
                return

            if event_type == "leave_approval_revert":
                self._handle_leave_revert_locked(payload)
                self._store.mark_event_processed(event_uuid, event_type, payload)
                return

            raise ValueError(f"Unsupported event type: {event_type}")

    def build_event_handler(self) -> "lark.EventDispatcherHandler":
        import lark_oapi as lark

        def _handle_customized_event(data: "lark.CustomizedEvent") -> None:
            payload = json.loads(lark.JSON.marshal(data))
            event_type = payload.get("event", {}).get("type")
            LOGGER.info("Received event type=%s uuid=%s", event_type, payload.get("uuid"))
            self.process_customized_event(payload)

        return (
            lark.EventDispatcherHandler.builder("", "")
            .register_p1_customized_event("leave_approval", _handle_customized_event)
            .register_p1_customized_event("leave_approvalV2", _handle_customized_event)
            .register_p1_customized_event("leave_approval_revert", _handle_customized_event)
            .build()
        )

    def _handle_leave_approval_v2_locked(self, payload: Dict[str, Any]) -> None:
        now = datetime.now(tz=self._settings.timezone)
        all_segments = build_segments_from_event(payload, self._settings.timezone, source="event")
        active_segments = [segment for segment in all_segments if segment.end_at >= now]
        instance_code = payload["event"]["instance_code"]

        LOGGER.info(
            "Processing leave_approvalV2 instance=%s total_segments=%s active_segments=%s",
            instance_code,
            len(all_segments),
            len(active_segments),
        )
        self._store.replace_segments_for_instance(instance_code, active_segments)
        self._sync_instance_locked(instance_code)

    def _handle_leave_approval_locked(self, payload: Dict[str, Any]) -> None:
        instance_code = payload["event"]["instance_code"]
        existing_segments = self._store.list_segments_for_instance(instance_code)
        if any(segment.source == "event" for segment in existing_segments):
            LOGGER.info(
                "Ignoring leave_approval fallback because leave_approvalV2 data already exists for instance=%s",
                instance_code,
            )
            return

        now = datetime.now(tz=self._settings.timezone)
        all_segments = self._build_segments_for_legacy_event(payload)
        active_segments = [segment for segment in all_segments if segment.end_at >= now]

        LOGGER.info(
            "Processing leave_approval fallback instance=%s total_segments=%s active_segments=%s",
            instance_code,
            len(all_segments),
            len(active_segments),
        )
        self._store.replace_segments_for_instance(instance_code, active_segments)
        self._sync_instance_locked(instance_code)

    def _build_segments_for_legacy_event(self, payload: Dict[str, Any]) -> List[LeaveSegment]:
        instance_code = payload["event"]["instance_code"]
        try:
            detail = self._api.get_instance_detail(instance_code)
            segment = build_segment_from_instance_detail(
                detail,
                self._settings.timezone,
                source="legacy_event_detail",
            )
        except Exception:
            LOGGER.warning(
                "Falling back to leave_approval payload because instance detail lookup failed: instance=%s",
                instance_code,
                exc_info=True,
            )
            segment = None

        if segment is not None:
            return [segment]

        return build_segments_from_event(payload, self._settings.timezone, source="legacy_event")

    def _handle_leave_revert_locked(self, payload: Dict[str, Any]) -> None:
        instance_code = payload["event"]["instance_code"]
        LOGGER.info("Processing leave_approval_revert instance=%s", instance_code)
        for mapping in self._store.list_timeoff_events_for_instance(instance_code):
            LOGGER.info(
                "Deleting timeoff event instance=%s event_id=%s",
                mapping.instance_code,
                mapping.timeoff_event_id,
            )
            self._api.delete_timeoff_event(mapping.timeoff_event_id)

        for pending in self._store.list_pending_timeoff_creates_for_instance(instance_code):
            if pending.remote_timeoff_event_id:
                LOGGER.info(
                    "Deleting pending remote timeoff event instance=%s event_id=%s",
                    pending.instance_code,
                    pending.remote_timeoff_event_id,
                )
                self._api.delete_timeoff_event(pending.remote_timeoff_event_id)
            else:
                LOGGER.warning(
                    "Clearing pending timeoff create without remote event id because revert arrived before local recovery could complete: instance=%s start=%s end=%s",
                    pending.instance_code,
                    pending.start_at.isoformat(),
                    pending.end_at.isoformat(),
                )

        self._store.delete_all_timeoff_mappings_for_instance(instance_code)
        self._store.clear_pending_timeoff_creates_for_instance(instance_code)
        self._store.delete_segments_for_instance(instance_code)

    def _build_reconcile_plan(self) -> ReconcilePlan:
        now = datetime.now(tz=self._settings.timezone)
        window_end = now
        window_start = now - timedelta(days=self._settings.lookback_days)
        desired_by_instance: dict[str, Tuple[LeaveSegment, ...]] = {}
        authoritative_instance_codes: list[str] = []
        skipped_instance_codes: list[str] = []
        visited_instance_codes: set[str] = set()

        for approval_code in self._settings.approval_codes:
            current_start = window_start
            while current_start <= window_end:
                current_end = min(current_start + timedelta(days=30), window_end)
                start_ms = int(current_start.timestamp() * 1000)
                end_ms = int(current_end.timestamp() * 1000)
                LOGGER.info(
                    "Reconciling approval=%s window_start=%s window_end=%s",
                    approval_code,
                    current_start.isoformat(),
                    current_end.isoformat(),
                )
                for instance_code in self._api.iter_instance_codes(
                    approval_code,
                    start_ms=start_ms,
                    end_ms=end_ms,
                ):
                    if instance_code in visited_instance_codes:
                        continue
                    visited_instance_codes.add(instance_code)

                    try:
                        detail = self._api.get_instance_detail(instance_code)
                        segment = build_segment_from_instance_detail(
                            detail,
                            self._settings.timezone,
                            source="reconcile",
                        )
                    except Exception:
                        skipped_instance_codes.append(instance_code)
                        LOGGER.warning(
                            "Skipping instance during reconcile because detail parsing failed: instance=%s",
                            instance_code,
                            exc_info=True,
                        )
                        continue

                    if segment is None:
                        skipped_instance_codes.append(instance_code)
                        LOGGER.warning(
                            "Skipping instance during reconcile because detail was not authoritative: instance=%s",
                            instance_code,
                        )
                        continue

                    authoritative_instance_codes.append(instance_code)
                    if segment.end_at >= now:
                        desired_by_instance[instance_code] = (segment,)
                    else:
                        desired_by_instance[instance_code] = ()
                current_start = current_end + timedelta(milliseconds=1)

        return ReconcilePlan(
            desired_segments_by_instance=desired_by_instance,
            authoritative_instance_codes=tuple(authoritative_instance_codes),
            skipped_instance_codes=tuple(skipped_instance_codes),
        )

    def _sync_instance_locked(self, instance_code: str) -> tuple[int, int]:
        desired_segments = self._store.list_segments_for_instance(instance_code)
        actual_events = self._store.list_timeoff_events_for_instance(instance_code)
        return self._sync_desired_vs_actual_locked(desired_segments, actual_events)

    def _sync_desired_vs_actual_locked(
        self,
        desired_segments: Iterable[LeaveSegment],
        actual_events: Iterable[TimeoffMapping],
    ) -> tuple[int, int]:
        desired_map = {segment.key: segment for segment in desired_segments}
        actual_map = {mapping.key: mapping for mapping in actual_events}

        created_count = 0
        deleted_count = 0

        for key in sorted(desired_map.keys() & actual_map.keys()):
            pending_create = self._store.get_pending_timeoff_create(desired_map[key])
            if pending_create is None:
                continue
            LOGGER.info(
                "Clearing stale pending timeoff create because local mapping already exists: instance=%s start=%s end=%s",
                pending_create.instance_code,
                pending_create.start_at.isoformat(),
                pending_create.end_at.isoformat(),
            )
            self._store.clear_pending_timeoff_create(desired_map[key])

        for key in sorted(desired_map.keys() - actual_map.keys()):
            segment = desired_map[key]
            pending_create = self._store.get_pending_timeoff_create(segment)
            if pending_create is not None:
                if pending_create.remote_timeoff_event_id:
                    LOGGER.warning(
                        "Recovering local timeoff mapping from pending remote create: instance=%s start=%s end=%s event_id=%s",
                        segment.instance_code,
                        segment.start_at.isoformat(),
                        segment.end_at.isoformat(),
                        pending_create.remote_timeoff_event_id,
                    )
                    self._store.upsert_timeoff_event(segment, pending_create.remote_timeoff_event_id)
                    self._store.clear_pending_timeoff_create(segment)
                    continue

                LOGGER.warning(
                    "Skipping create because a previous attempt may have already succeeded remotely: instance=%s start=%s end=%s",
                    segment.instance_code,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                )
                continue

            LOGGER.info(
                "Creating timeoff event instance=%s start=%s end=%s",
                segment.instance_code,
                segment.start_at.isoformat(),
                segment.end_at.isoformat(),
            )
            self._store.mark_pending_timeoff_create(segment)
            try:
                timeoff_event_id = self._api.create_timeoff_event(segment)
            except Exception:
                self._store.clear_pending_timeoff_create(segment)
                raise
            self._store.set_pending_timeoff_remote_event_id(segment, timeoff_event_id)

            try:
                self._store.upsert_timeoff_event(segment, timeoff_event_id)
            except Exception:
                LOGGER.exception(
                    "Failed to persist timeoff mapping after remote create; leaving pending marker in place: instance=%s start=%s end=%s event_id=%s",
                    segment.instance_code,
                    segment.start_at.isoformat(),
                    segment.end_at.isoformat(),
                    timeoff_event_id,
                )
                raise

            self._store.clear_pending_timeoff_create(segment)
            created_count += 1

        for key in sorted(actual_map.keys() - desired_map.keys()):
            mapping = actual_map[key]
            LOGGER.info(
                "Removing stale timeoff event instance=%s start=%s end=%s event_id=%s",
                mapping.instance_code,
                mapping.start_at.isoformat(),
                mapping.end_at.isoformat(),
                mapping.timeoff_event_id,
            )
            self._api.delete_timeoff_event(mapping.timeoff_event_id)
            self._store.delete_timeoff_event_mapping(
                mapping.instance_code,
                mapping.start_at.isoformat(),
                mapping.end_at.isoformat(),
            )
            deleted_count += 1

        return created_count, deleted_count

    def _on_successful_reconcile(self, *, reason: str) -> None:
        self._has_successful_reconcile = True
        try:
            self.run_weekly_report_if_due(reason=f"{reason}_reconcile")
        except Exception:
            LOGGER.exception("Weekly leave report follow-up after successful reconcile failed")


def get_next_reconcile_run_at(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")

    current_date = now.date()
    for scheduled_time in DAILY_RECONCILE_TIMES:
        candidate = datetime.combine(current_date, scheduled_time, tzinfo=now.tzinfo)
        if candidate > now:
            return candidate

    tomorrow = current_date + timedelta(days=1)
    return datetime.combine(tomorrow, DAILY_RECONCILE_TIMES[0], tzinfo=now.tzinfo)
