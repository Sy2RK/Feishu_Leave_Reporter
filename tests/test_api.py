from __future__ import annotations

from pathlib import Path

import httpx

from feishu_leave_sync.api import FeishuApiClient, FeishuApiError, user_id_type_for_identifier
from feishu_leave_sync.config import Settings


def test_user_id_type_for_identifier_detects_open_id() -> None:
    assert user_id_type_for_identifier("ou_123") == "open_id"


def test_user_id_type_for_identifier_detects_union_id() -> None:
    assert user_id_type_for_identifier("on_123") == "union_id"


def test_user_id_type_for_identifier_defaults_to_user_id() -> None:
    assert user_id_type_for_identifier("26af7364") == "user_id"


def test_send_bot_webhook_card_accepts_status_code_response() -> None:
    client = FeishuApiClient(_settings())
    original_client = client._client  # noqa: SLF001
    client._client = httpx.Client(  # noqa: SLF001
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"StatusCode": 0, "StatusMessage": "success"})
        )
    )

    try:
        original_client.close()
        client.send_bot_webhook_card(
            "https://open.feishu.cn/open-apis/bot/v2/hook/12345678-1234-1234-1234-1234567890ab",
            {"schema": "2.0", "body": {"direction": "vertical", "elements": []}},
        )
    finally:
        client.close()


def test_send_bot_webhook_card_masks_webhook_on_error() -> None:
    client = FeishuApiClient(_settings())
    original_client = client._client  # noqa: SLF001
    client._client = httpx.Client(  # noqa: SLF001
        transport=httpx.MockTransport(
            lambda request: httpx.Response(400, json={"StatusCode": 9499, "StatusMessage": "invalid request"})
        )
    )

    try:
        original_client.close()
        try:
            client.send_bot_webhook_card(
                "https://open.feishu.cn/open-apis/bot/v2/hook/abcdef12-3456-7890-abcd-ef1234567890",
                {"schema": "2.0", "body": {"direction": "vertical", "elements": []}},
            )
        except FeishuApiError as exc:
            message = str(exc)
            assert "9499" in message
            assert "invalid request" in message
            assert "abcdef12-3456-7890-abcd-ef1234567890" not in message
        else:
            raise AssertionError("Expected FeishuApiError")
    finally:
        client.close()


def _settings() -> Settings:
    return Settings(
        app_id="cli_test",
        app_secret="secret",
        approval_codes=("approval",),
        weekly_report_webhook_url=None,
        timezone_name="Asia/Shanghai",
        lookback_days=365,
        db_path=Path("/tmp/leave-sync-test.db"),
        log_level="INFO",
        launchd_label="com.example.test",
    )
