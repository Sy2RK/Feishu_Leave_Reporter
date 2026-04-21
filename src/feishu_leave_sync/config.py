from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class ConfigError(ValueError):
    """Raised when required configuration is missing or invalid."""


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _parse_csv(value: str) -> Tuple[str, ...]:
    parts = [item.strip() for item in value.split(",")]
    filtered = tuple(item for item in parts if item)
    if not filtered:
        raise ConfigError("FEISHU_APPROVAL_CODES must contain at least one approval code")
    return filtered


@dataclass(frozen=True)
class Settings:
    app_id: str
    app_secret: str
    approval_codes: Tuple[str, ...]
    weekly_report_webhook_url: str | None
    timezone_name: str
    lookback_days: int
    db_path: Path
    log_level: str
    launchd_label: str

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @property
    def log_file_path(self) -> Path:
        return self.db_path.parent.parent / "log" / "feishu-leave-sync.log"

    @property
    def stdout_log_path(self) -> Path:
        return self.db_path.parent.parent / "log" / "feishu-leave-sync.stdout.log"

    @property
    def stderr_log_path(self) -> Path:
        return self.db_path.parent.parent / "log" / "feishu-leave-sync.stderr.log"

    @property
    def runtime_root(self) -> Path:
        return self.db_path.parent.parent

    @classmethod
    def from_env(cls) -> "Settings":
        app_id = _require_env("FEISHU_APP_ID")
        app_secret = _require_env("FEISHU_APP_SECRET")
        approval_codes = _parse_csv(_require_env("FEISHU_APPROVAL_CODES"))
        weekly_report_webhook_url = os.getenv("FEISHU_WEEKLY_REPORT_WEBHOOK_URL", "").strip() or None
        timezone_name = os.getenv("FEISHU_TIMEZONE", "Asia/Shanghai").strip() or "Asia/Shanghai"

        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ConfigError(f"Unknown FEISHU_TIMEZONE: {timezone_name}") from exc

        lookback_raw = os.getenv("LOOKBACK_DAYS", "365").strip() or "365"
        try:
            lookback_days = int(lookback_raw)
        except ValueError as exc:
            raise ConfigError("LOOKBACK_DAYS must be an integer") from exc
        if lookback_days <= 0:
            raise ConfigError("LOOKBACK_DAYS must be greater than 0")

        db_path_raw = os.getenv("DB_PATH", "").strip()
        if db_path_raw:
            db_path = Path(db_path_raw).expanduser().resolve()
        else:
            db_path = (Path.cwd() / "var" / "state" / "leave-sync.db").resolve()

        log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
        launchd_label = os.getenv("LAUNCHD_LABEL", "com.ggbond.feishu-leave-sync").strip()
        if not launchd_label:
            raise ConfigError("LAUNCHD_LABEL must not be empty")

        return cls(
            app_id=app_id,
            app_secret=app_secret,
            approval_codes=approval_codes,
            weekly_report_webhook_url=weekly_report_webhook_url,
            timezone_name=timezone_name,
            lookback_days=lookback_days,
            db_path=db_path,
            log_level=log_level,
            launchd_label=launchd_label,
        )
