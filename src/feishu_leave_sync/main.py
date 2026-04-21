from __future__ import annotations

import logging
import sys

from feishu_leave_sync.api import FeishuApiClient
from feishu_leave_sync.config import ConfigError, Settings
from feishu_leave_sync.db import SQLiteStore
from feishu_leave_sync.logging_utils import setup_logging
from feishu_leave_sync.service import LeaveSyncService


LOGGER = logging.getLogger(__name__)


def main() -> int:
    try:
        settings = Settings.from_env()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
    setup_logging(settings.log_level, settings.log_file_path)

    store = SQLiteStore(settings.db_path)
    store.initialize()
    api_client = FeishuApiClient(settings)
    service = LeaveSyncService(settings, store, api_client)

    try:
        service.bootstrap()
        try:
            service.run_startup_reconcile()
        except Exception:
            LOGGER.exception("Startup reconcile failed; continuing with realtime websocket processing")
        service.start_periodic_reconcile_scheduler()
        service.start_weekly_report_scheduler()

        LOGGER.info("Connecting to Feishu websocket event stream")
        event_handler = service.build_event_handler()
        import lark_oapi as lark

        client = lark.ws.Client(
            settings.app_id,
            settings.app_secret,
            event_handler=event_handler,
            log_level=_to_lark_log_level(settings.log_level),
        )
        client.start()
    finally:
        service.stop_periodic_reconcile_scheduler()
        api_client.close()
        store.close()

    return 0


def _to_lark_log_level(level: str) -> int:
    import lark_oapi as lark

    normalized = level.upper()
    if normalized == "DEBUG":
        return lark.LogLevel.DEBUG
    if normalized in {"WARN", "WARNING"}:
        return lark.LogLevel.WARNING
    if normalized == "ERROR":
        return lark.LogLevel.ERROR
    return lark.LogLevel.INFO


if __name__ == "__main__":
    raise SystemExit(main())
