from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import pytest

from feishu_leave_sync.config import Settings
from feishu_leave_sync.db import SQLiteStore
from feishu_leave_sync.models import LeaveSegment
from feishu_leave_sync.service import LeaveSyncService, get_next_reconcile_run_at


@dataclass
class DummyApiClient:
    created_payloads: list = field(default_factory=list)
    deleted_ids: list = field(default_factory=list)
    webhook_payloads: list = field(default_factory=list)
    instance_codes: tuple[str, ...] = ()
    instance_details: dict[str, object] = field(default_factory=dict)

    def get_tenant_access_token(self, *, force_refresh: bool = False) -> str:
        return "tenant-token"

    def subscribe_approval(self, approval_code: str) -> None:
        return None

    def create_timeoff_event(self, segment):  # noqa: ANN001
        event_id = f"timeoff-{len(self.created_payloads) + 1}"
        self.created_payloads.append((segment.instance_code, segment.start_at.isoformat(), segment.end_at.isoformat()))
        return event_id

    def delete_timeoff_event(self, timeoff_event_id: str) -> None:
        self.deleted_ids.append(timeoff_event_id)

    def send_bot_webhook_card(self, webhook_url: str, card):  # noqa: ANN001
        self.webhook_payloads.append((webhook_url, card))

    def iter_instance_codes(self, approval_code: str, *, start_ms: int, end_ms: int):  # noqa: ANN001
        return iter(self.instance_codes)

    def get_instance_detail(self, instance_code: str):  # noqa: ANN001
        return self.instance_details[instance_code]

    def close(self) -> None:
        return None


class FailingSQLiteStore(SQLiteStore):
    def __init__(self, db_path: Path) -> None:
        super().__init__(db_path)
        self.fail_upsert_once = True

    def upsert_timeoff_event(self, segment: LeaveSegment, timeoff_event_id: str) -> None:
        if self.fail_upsert_once:
            self.fail_upsert_once = False
            raise RuntimeError("simulated sqlite write failure")
        super().upsert_timeoff_event(segment, timeoff_event_id)


class FailingJobCompletionStore(SQLiteStore):
    def __init__(self, db_path: Path) -> None:
        super().__init__(db_path)
        self.fail_mark_job_completed_once = True

    def mark_job_completed(self, job_name: str, period_key: str, details: dict | None = None) -> None:
        if self.fail_mark_job_completed_once:
            self.fail_mark_job_completed_once = False
            raise RuntimeError("simulated job completion persist failure")
        super().mark_job_completed(job_name, period_key, details)


class LegacyPendingRemoteIdUpdateStore(SQLiteStore):
    def set_pending_timeoff_remote_event_id(self, segment: LeaveSegment, remote_timeoff_event_id: str) -> None:
        raise AssertionError("legacy pending remote id update path should not be used")


class FailingWebhookApiClient(DummyApiClient):
    def __init__(self) -> None:
        super().__init__()
        self.fail_webhook_once = True

    def send_bot_webhook_card(self, webhook_url: str, card):  # noqa: ANN001
        if self.fail_webhook_once:
            self.fail_webhook_once = False
            raise RuntimeError("simulated webhook send failure")
        super().send_bot_webhook_card(webhook_url, card)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        app_id="cli_123",
        app_secret="secret",
        approval_codes=("APPROVAL_1",),
        weekly_report_webhook_url=None,
        timezone_name="Asia/Shanghai",
        lookback_days=365,
        db_path=tmp_path / "var" / "state" / "leave-sync.db",
        log_level="INFO",
        launchd_label="com.example.test",
    )


def _settings_with_weekly_report(tmp_path: Path) -> Settings:
    return Settings(
        app_id="cli_123",
        app_secret="secret",
        approval_codes=("APPROVAL_1",),
        weekly_report_webhook_url="https://example.com/webhook",
        timezone_name="Asia/Shanghai",
        lookback_days=365,
        db_path=tmp_path / "var" / "state" / "leave-sync.db",
        log_level="INFO",
        launchd_label="com.example.test",
    )


def _segment(
    instance_code: str,
    *,
    user_id: str = "ou_123",
    start_at: str = "2099-09-05T09:00:00+08:00",
    end_at: str = "2099-09-05T18:00:00+08:00",
    source: str = "test",
) -> LeaveSegment:
    return LeaveSegment(
        instance_code=instance_code,
        user_id=user_id,
        start_at=datetime.fromisoformat(start_at),
        end_at=datetime.fromisoformat(end_at),
        timezone_name="Asia/Shanghai",
        source=source,
    )


def test_leave_event_is_idempotent_and_revert_deletes_events() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient(created_payloads=[], deleted_ids=[])
        service = LeaveSyncService(settings, store, api)

        payload = {
            "uuid": "uuid-1",
            "event": {
                "type": "leave_approvalV2",
                "instance_code": "instance-1",
                "open_id": "ou_123",
                "leave_range": "[[2099-09-05 13:30:00,2099-09-05 18:00:00],[2099-09-06 09:00:00,2099-09-06 18:00:00]]",
                "leave_start_time": "2099-09-05 00:00:00",
                "leave_end_time": "2099-09-07 00:00:00",
            },
        }

        service.process_customized_event(payload)
        service.process_customized_event(payload)

        assert len(api.created_payloads) == 2
        assert len(store.list_timeoff_events_for_instance("instance-1")) == 2

        revert_payload = {
            "uuid": "uuid-2",
            "event": {
                "type": "leave_approval_revert",
                "instance_code": "instance-1",
            },
        }
        service.process_customized_event(revert_payload)

        assert len(api.deleted_ids) == 2
        assert store.list_timeoff_events_for_instance("instance-1") == []
        assert store.list_segments_for_instance("instance-1") == []

        store.close()


def test_leave_approval_fallback_creates_timeoff_event() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient(created_payloads=[], deleted_ids=[])
        service = LeaveSyncService(settings, store, api)

        api.instance_details["instance-legacy"] = {
            "instance_code": "instance-legacy",
            "user_id": "user_legacy",
            "open_id": "ou_should_not_win",
            "form": """
            [
              {
                "id": "widgetLeaveGroupV2",
                "type": "leaveGroupV2",
                "value": {
                  "start": "2099-09-05T09:00:00+08:00",
                  "end": "2099-09-05T18:00:00+08:00"
                }
              }
            ]
            """,
        }

        payload = {
            "uuid": "uuid-legacy",
            "event": {
                "type": "leave_approval",
                "instance_code": "instance-legacy",
                "open_id": "ou_legacy",
                "leave_start_time": "2099-09-05 09:00:00",
                "leave_end_time": "2099-09-05 18:00:00",
            },
        }

        service.process_customized_event(payload)

        assert len(api.created_payloads) == 1
        assert store.list_segments_for_instance("instance-legacy")[0].user_id == "user_legacy"
        assert len(store.list_timeoff_events_for_instance("instance-legacy")) == 1
        assert store.has_processed_event("uuid-legacy") is True

        store.close()


def test_leave_approval_v2_replaces_legacy_fallback_segments() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient(created_payloads=[], deleted_ids=[])
        service = LeaveSyncService(settings, store, api)

        legacy_payload = {
            "uuid": "uuid-legacy",
            "event": {
                "type": "leave_approval",
                "instance_code": "instance-legacy-upgrade",
                "open_id": "ou_legacy",
                "leave_start_time": "2099-09-05 09:00:00",
                "leave_end_time": "2099-09-05 18:00:00",
            },
        }
        v2_payload = {
            "uuid": "uuid-v2",
            "event": {
                "type": "leave_approvalV2",
                "instance_code": "instance-legacy-upgrade",
                "open_id": "ou_legacy",
                "leave_range": "[[2099-09-05 09:00:00,2099-09-05 12:00:00],[2099-09-05 14:00:00,2099-09-05 18:00:00]]",
                "leave_start_time": "2099-09-05 09:00:00",
                "leave_end_time": "2099-09-05 18:00:00",
            },
        }

        service.process_customized_event(legacy_payload)
        service.process_customized_event(v2_payload)

        assert len(api.created_payloads) == 3
        assert len(api.deleted_ids) == 1
        assert len(store.list_timeoff_events_for_instance("instance-legacy-upgrade")) == 2

        store.close()


def test_leave_approval_fallback_is_ignored_after_v2_segments_exist() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient(created_payloads=[], deleted_ids=[])
        service = LeaveSyncService(settings, store, api)

        v2_payload = {
            "uuid": "uuid-v2-first",
            "event": {
                "type": "leave_approvalV2",
                "instance_code": "instance-v2-first",
                "open_id": "ou_v2",
                "leave_range": "[[2099-09-05 09:00:00,2099-09-05 12:00:00],[2099-09-05 14:00:00,2099-09-05 18:00:00]]",
                "leave_start_time": "2099-09-05 09:00:00",
                "leave_end_time": "2099-09-05 18:00:00",
            },
        }
        legacy_payload = {
            "uuid": "uuid-legacy-second",
            "event": {
                "type": "leave_approval",
                "instance_code": "instance-v2-first",
                "open_id": "ou_v2",
                "leave_start_time": "2099-09-05 09:00:00",
                "leave_end_time": "2099-09-05 18:00:00",
            },
        }

        service.process_customized_event(v2_payload)
        service.process_customized_event(legacy_payload)

        assert len(api.created_payloads) == 2
        assert api.deleted_ids == []
        assert len(store.list_timeoff_events_for_instance("instance-v2-first")) == 2

        store.close()


def test_next_reconcile_run_at_uses_same_day_windows_before_cutoff() -> None:
    tz = ZoneInfo("Asia/Shanghai")

    early_morning = datetime(2026, 4, 21, 7, 30, tzinfo=tz)
    midday = datetime(2026, 4, 21, 12, 0, tzinfo=tz)

    assert get_next_reconcile_run_at(early_morning) == datetime(2026, 4, 21, 8, 0, tzinfo=tz)
    assert get_next_reconcile_run_at(midday) == datetime(2026, 4, 21, 18, 0, tzinfo=tz)


def test_next_reconcile_run_at_rolls_to_next_day_after_evening_window() -> None:
    tz = ZoneInfo("Asia/Shanghai")
    after_evening_run = datetime(2026, 4, 21, 18, 0, tzinfo=tz)

    assert get_next_reconcile_run_at(after_evening_run) == datetime(2026, 4, 22, 8, 0, tzinfo=tz)


def test_startup_reconcile_does_not_delete_mapping_for_unseen_instance() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        existing_segment = _segment("instance-unseen")
        store.replace_segments_for_instance("instance-unseen", [existing_segment])
        store.upsert_timeoff_event(existing_segment, "timeoff-existing")

        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)

        stats = service.run_startup_reconcile()

        assert stats.deleted_events == 0
        assert api.deleted_ids == []
        assert len(store.list_timeoff_events_for_instance("instance-unseen")) == 1
        assert len(store.list_segments_for_instance("instance-unseen")) == 1

        store.close()


def test_startup_reconcile_deletes_mapping_for_authoritative_expired_instance() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        expired_segment = _segment(
            "instance-expired",
            start_at="2020-09-05T09:00:00+08:00",
            end_at="2020-09-05T18:00:00+08:00",
        )
        store.replace_segments_for_instance("instance-expired", [expired_segment])
        store.upsert_timeoff_event(expired_segment, "timeoff-expired")

        api = DummyApiClient(
            instance_codes=("instance-expired",),
            instance_details={
                "instance-expired": {
                    "instance_code": "instance-expired",
                    "open_id": "ou_123",
                    "form": """
                    [
                      {
                        "id": "widgetLeaveGroupV2",
                        "type": "leaveGroupV2",
                        "value": {
                          "start": "2020-09-05T09:00:00+08:00",
                          "end": "2020-09-05T18:00:00+08:00"
                        }
                      }
                    ]
                    """,
                }
            },
        )
        service = LeaveSyncService(settings, store, api)

        stats = service.run_startup_reconcile()

        assert stats.deleted_events == 1
        assert api.deleted_ids == ["timeoff-expired"]
        assert store.list_timeoff_events_for_instance("instance-expired") == []
        assert store.list_segments_for_instance("instance-expired") == []

        store.close()


def test_failed_local_persist_after_remote_create_recovers_without_duplicate_remote_create() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = FailingSQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)

        payload = {
            "uuid": "uuid-fail-once",
            "event": {
                "type": "leave_approvalV2",
                "instance_code": "instance-fail-once",
                "open_id": "ou_123",
                "leave_range": "[[2099-09-05 13:30:00,2099-09-05 18:00:00]]",
            },
        }

        with pytest.raises(RuntimeError, match="simulated sqlite write failure"):
            service.process_customized_event(payload)

        segment = store.list_segments_for_instance("instance-fail-once")[0]
        assert len(api.created_payloads) == 1
        assert store.has_pending_timeoff_create(segment) is True
        assert store.get_pending_timeoff_create(segment).remote_timeoff_event_id == "timeoff-1"

        service.process_customized_event(payload)

        assert len(api.created_payloads) == 1
        mappings = store.list_timeoff_events_for_instance("instance-fail-once")
        assert len(mappings) == 1
        assert mappings[0].timeoff_event_id == "timeoff-1"
        assert store.has_pending_timeoff_create(segment) is False
        assert store.has_processed_event("uuid-fail-once") is True

        store.close()


def test_remote_create_does_not_depend_on_legacy_pending_remote_id_update() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = LegacyPendingRemoteIdUpdateStore(settings.db_path)
        store.initialize()
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)

        payload = {
            "uuid": "uuid-no-legacy-remote-id-update",
            "event": {
                "type": "leave_approvalV2",
                "instance_code": "instance-no-legacy-remote-id-update",
                "open_id": "ou_123",
                "leave_range": "[[2099-09-05 13:30:00,2099-09-05 18:00:00]]",
            },
        }

        service.process_customized_event(payload)

        assert len(api.created_payloads) == 1
        mappings = store.list_timeoff_events_for_instance("instance-no-legacy-remote-id-update")
        assert len(mappings) == 1
        assert mappings[0].timeoff_event_id == "timeoff-1"
        assert store.list_pending_timeoff_creates_for_instance("instance-no-legacy-remote-id-update") == []
        assert store.has_processed_event("uuid-no-legacy-remote-id-update") is True

        store.close()


def test_revert_cleans_pending_remote_timeoff_create() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = FailingSQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)

        payload = {
            "uuid": "uuid-pending-revert",
            "event": {
                "type": "leave_approvalV2",
                "instance_code": "instance-pending-revert",
                "open_id": "ou_123",
                "leave_range": "[[2099-09-05 13:30:00,2099-09-05 18:00:00]]",
            },
        }

        with pytest.raises(RuntimeError, match="simulated sqlite write failure"):
            service.process_customized_event(payload)

        revert_payload = {
            "uuid": "uuid-pending-revert-2",
            "event": {
                "type": "leave_approval_revert",
                "instance_code": "instance-pending-revert",
            },
        }
        service.process_customized_event(revert_payload)

        assert api.deleted_ids == ["timeoff-1"]
        assert store.list_timeoff_events_for_instance("instance-pending-revert") == []
        assert store.list_segments_for_instance("instance-pending-revert") == []
        assert store.list_pending_timeoff_creates_for_instance("instance-pending-revert") == []

        store.close()


def test_startup_reconcile_skips_bad_instance_detail_and_continues() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient(
            instance_codes=("instance-bad", "instance-good"),
            instance_details={
                "instance-bad": {
                    "instance_code": "instance-bad",
                    "open_id": "ou_bad",
                    "form": "{not-json",
                },
                "instance-good": {
                    "instance_code": "instance-good",
                    "open_id": "ou_good",
                    "form": """
                    [
                      {
                        "id": "widgetLeaveGroupV2",
                        "type": "leaveGroupV2",
                        "value": {
                          "start": "2099-09-06T09:00:00+08:00",
                          "end": "2099-09-06T18:00:00+08:00"
                        }
                      }
                    ]
                    """,
                },
            },
        )
        service = LeaveSyncService(settings, store, api)

        stats = service.run_startup_reconcile()

        assert stats.expected_segments == 1
        assert stats.created_events == 1
        assert stats.deleted_events == 0
        assert len(api.created_payloads) == 1
        assert api.created_payloads[0][0] == "instance-good"
        assert len(store.list_timeoff_events_for_instance("instance-good")) == 1
        assert store.list_timeoff_events_for_instance("instance-bad") == []

        store.close()


def test_weekly_report_sends_once_per_monday_period() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings_with_weekly_report(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        store.replace_segments_for_instance(
            "instance-report",
            [
                _segment(
                    "instance-report",
                    user_id="user_1",
                    start_at="2026-04-20T14:00:00+08:00",
                    end_at="2026-04-20T18:00:00+08:00",
                )
            ],
        )
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)
        service._has_successful_reconcile = True  # noqa: SLF001
        monday = datetime(2026, 4, 20, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        assert service.run_weekly_report_if_due(reason="scheduled", now=monday) is True
        assert service.run_weekly_report_if_due(reason="scheduled", now=monday) is False
        assert len(api.webhook_payloads) == 1
        assert store.has_completed_job("weekly_leave_report", "2026-04-20") is True

        store.close()


def test_weekly_report_skips_when_not_due() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings_with_weekly_report(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)
        service._has_successful_reconcile = True  # noqa: SLF001
        tuesday = datetime(2026, 4, 21, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        assert service.run_weekly_report_if_due(reason="scheduled", now=tuesday) is False
        assert api.webhook_payloads == []

        store.close()


def test_weekly_report_requires_successful_reconcile_in_current_process() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings_with_weekly_report(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        store.replace_segments_for_instance(
            "instance-report",
            [
                _segment(
                    "instance-report",
                    user_id="user_1",
                    start_at="2026-04-20T14:00:00+08:00",
                    end_at="2026-04-20T18:00:00+08:00",
                )
            ],
        )
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)
        monday = datetime(2026, 4, 20, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        assert service.run_weekly_report_if_due(reason="startup", now=monday) is False
        assert api.webhook_payloads == []
        assert store.has_completed_job("weekly_leave_report", "2026-04-20") is False

        store.close()


def test_weekly_report_does_not_resend_when_completion_persist_fails_once() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings_with_weekly_report(temp_path)
        store = FailingJobCompletionStore(settings.db_path)
        store.initialize()
        store.replace_segments_for_instance(
            "instance-report",
            [
                _segment(
                    "instance-report",
                    user_id="user_1",
                    start_at="2026-04-20T14:00:00+08:00",
                    end_at="2026-04-20T18:00:00+08:00",
                )
            ],
        )
        api = DummyApiClient()
        service = LeaveSyncService(settings, store, api)
        service._has_successful_reconcile = True  # noqa: SLF001
        monday = datetime(2026, 4, 20, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        assert service.run_weekly_report_if_due(reason="scheduled", now=monday) is True
        assert len(api.webhook_payloads) == 1
        pending_job = store.get_pending_job("weekly_leave_report", "2026-04-20")
        assert pending_job is not None
        assert pending_job.status == "sent_remote"
        assert store.has_completed_job("weekly_leave_report", "2026-04-20") is False

        assert service.run_weekly_report_if_due(reason="scheduled", now=monday) is False
        assert len(api.webhook_payloads) == 1
        assert store.has_completed_job("weekly_leave_report", "2026-04-20") is True
        assert store.get_pending_job("weekly_leave_report", "2026-04-20") is None

        store.close()


def test_weekly_report_retries_after_webhook_send_failure() -> None:
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        settings = _settings_with_weekly_report(temp_path)
        store = SQLiteStore(settings.db_path)
        store.initialize()
        store.replace_segments_for_instance(
            "instance-report",
            [
                _segment(
                    "instance-report",
                    user_id="user_1",
                    start_at="2026-04-20T14:00:00+08:00",
                    end_at="2026-04-20T18:00:00+08:00",
                )
            ],
        )
        api = FailingWebhookApiClient()
        service = LeaveSyncService(settings, store, api)
        service._has_successful_reconcile = True  # noqa: SLF001
        monday = datetime(2026, 4, 20, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        with pytest.raises(RuntimeError, match="simulated webhook send failure"):
            service.run_weekly_report_if_due(reason="scheduled", now=monday)

        assert api.webhook_payloads == []
        assert store.get_pending_job("weekly_leave_report", "2026-04-20") is None
        assert store.has_completed_job("weekly_leave_report", "2026-04-20") is False

        assert service.run_weekly_report_if_due(reason="scheduled", now=monday) is True
        assert len(api.webhook_payloads) == 1
        assert store.get_pending_job("weekly_leave_report", "2026-04-20") is None
        assert store.has_completed_job("weekly_leave_report", "2026-04-20") is True

        store.close()
