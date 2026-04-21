from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, Optional
from urllib.parse import urlsplit

import httpx

from feishu_leave_sync.config import Settings
from feishu_leave_sync.models import LeaveSegment


LOGGER = logging.getLogger(__name__)


def user_id_type_for_identifier(identifier: str) -> str:
    normalized = identifier.strip()
    if normalized.startswith("ou_"):
        return "open_id"
    if normalized.startswith("on_"):
        return "union_id"
    return "user_id"


class FeishuApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class FeishuApiClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.Client(
            base_url="https://open.feishu.cn",
            timeout=httpx.Timeout(20.0, connect=10.0),
        )
        self._tenant_access_token: Optional[str] = None
        self._tenant_token_expires_at: Optional[datetime] = None

    def close(self) -> None:
        self._client.close()

    def _auth_headers(self) -> Dict[str, str]:
        token = self.get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    def get_tenant_access_token(self, *, force_refresh: bool = False) -> str:
        now = datetime.now(tz=timezone.utc)
        if (
            not force_refresh
            and self._tenant_access_token
            and self._tenant_token_expires_at
            and now < self._tenant_token_expires_at
        ):
            return self._tenant_access_token

        payload = {
            "app_id": self._settings.app_id,
            "app_secret": self._settings.app_secret,
        }
        response = self._request(
            "POST",
            "/open-apis/auth/v3/tenant_access_token/internal/",
            headers={"Content-Type": "application/json; charset=utf-8"},
            json_body=payload,
            authenticated=False,
        )
        self._tenant_access_token = response["tenant_access_token"]
        expires_in = int(response.get("expire", 7200))
        self._tenant_token_expires_at = now + timedelta(seconds=max(expires_in - 60, 60))
        return self._tenant_access_token

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        authenticated: bool = True,
        ok_codes: Iterable[int] = (),
        missing_ok_codes: Iterable[int] = (),
    ) -> Dict[str, Any]:
        request_headers = dict(headers or {})
        if authenticated:
            request_headers.update(self._auth_headers())

        for attempt in range(5):
            try:
                response = self._client.request(
                    method=method,
                    url=path,
                    params=params,
                    json=json_body,
                    headers=request_headers,
                )
            except httpx.HTTPError as exc:
                if attempt == 4:
                    raise FeishuApiError(f"HTTP request failed: {exc}") from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == 4:
                    raise FeishuApiError(
                        f"Feishu API temporary failure: {response.status_code} {response.text}",
                        status_code=response.status_code,
                    )
                self._sleep_before_retry(attempt)
                continue

            try:
                payload = response.json()
            except json.JSONDecodeError as exc:
                raise FeishuApiError(
                    f"Feishu API returned invalid JSON: {response.text}",
                    status_code=response.status_code,
                ) from exc

            code = int(payload.get("code", 0))
            if code == 0 or code in ok_codes:
                return payload.get("data", payload)
            if code in missing_ok_codes:
                return payload.get("data", payload)

            if code in {1395001, 190004, 190005, 190010} and attempt < 4:
                self._sleep_before_retry(attempt)
                continue

            raise FeishuApiError(
                f"Feishu API error code={code}: {payload.get('msg', 'unknown error')}",
                status_code=response.status_code,
                code=code,
            )

        raise AssertionError("unreachable")

    @staticmethod
    def _sleep_before_retry(attempt: int) -> None:
        delay = (2 ** attempt) * 0.5 + random.uniform(0.0, 0.25)
        time.sleep(delay)

    def subscribe_approval(self, approval_code: str) -> None:
        LOGGER.info("Ensuring approval subscription for %s", approval_code)
        self._request(
            "POST",
            f"/open-apis/approval/v4/approvals/{approval_code}/subscribe",
            ok_codes=(1390007,),
        )

    def create_timeoff_event(self, segment: LeaveSegment) -> str:
        user_id_type = user_id_type_for_identifier(segment.user_id)
        payload = {
            "user_id": segment.user_id,
            "timezone": segment.timezone_name,
            "start_time": str(int(segment.start_at.timestamp())),
            "end_time": str(int(segment.end_at.timestamp())),
        }
        try:
            data = self._request(
                "POST",
                "/open-apis/calendar/v4/timeoff_events",
                params={"user_id_type": user_id_type},
                json_body=payload,
            )
        except FeishuApiError as exc:
            if user_id_type == "user_id" and exc.code == 99991672:
                raise FeishuApiError(
                    "Creating timeoff events with user_id requires scope "
                    "contact:user.employee_id:readonly; enable the scope, publish the app, "
                    "and restart the service.",
                    status_code=exc.status_code,
                    code=exc.code,
                ) from exc
            raise
        return data["timeoff_event_id"]

    def delete_timeoff_event(self, timeoff_event_id: str) -> None:
        self._request(
            "DELETE",
            f"/open-apis/calendar/v4/timeoff_events/{timeoff_event_id}",
            missing_ok_codes=(190002,),
        )

    def send_bot_webhook_card(self, webhook_url: str, card: dict[str, Any]) -> None:
        webhook_headers = {"Content-Type": "application/json; charset=utf-8"}
        payload = {
            "msg_type": "interactive",
            "card": card,
        }

        for attempt in range(5):
            try:
                response = self._client.request(
                    "POST",
                    webhook_url,
                    headers=webhook_headers,
                    json=payload,
                )
            except httpx.HTTPError as exc:
                if attempt == 4:
                    raise FeishuApiError(
                        f"Bot webhook request failed for {self._mask_webhook_url(webhook_url)}: {exc.__class__.__name__}"
                    ) from exc
                self._sleep_before_retry(attempt)
                continue

            if response.status_code in {429, 500, 502, 503, 504}:
                if attempt == 4:
                    raise FeishuApiError(
                        "Bot webhook temporary failure: "
                        f"{response.status_code} {self._mask_webhook_url(webhook_url)}"
                    )
                self._sleep_before_retry(attempt)
                continue

            try:
                response_payload = response.json()
            except json.JSONDecodeError:
                if response.is_success:
                    return
                raise FeishuApiError(
                    "Bot webhook returned invalid JSON: "
                    f"status={response.status_code} webhook={self._mask_webhook_url(webhook_url)}"
                )

            code = response_payload.get("code", response_payload.get("StatusCode", 0))
            if response.is_success and code in {None, 0}:
                return

            message = response_payload.get("msg") or response_payload.get("StatusMessage") or "unknown error"
            raise FeishuApiError(
                f"Bot webhook error code={code}: {message}",
                status_code=response.status_code,
                code=code if isinstance(code, int) else None,
            )

    def iter_instance_codes(
        self,
        approval_code: str,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterator[str]:
        page_token: str | None = None
        while True:
            data = self._request(
                "POST",
                "/open-apis/approval/v4/instances/query",
                params={
                    "page_size": 200,
                    **({"page_token": page_token} if page_token else {}),
                },
                json_body={
                    "approval_code": approval_code,
                    "instance_status": "APPROVED",
                    "instance_start_time_from": str(start_ms),
                    "instance_start_time_to": str(end_ms),
                    "locale": "zh-CN",
                },
            )
            for item in data.get("instance_list", []):
                instance = item.get("instance", {})
                code = instance.get("code")
                if code:
                    yield code
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
            if not page_token:
                break

    def get_instance_detail(self, instance_code: str) -> Dict[str, Any]:
        return self._request(
            "GET",
            f"/open-apis/approval/v4/instances/{instance_code}",
            params={"locale": "zh-CN"},
        )

    @staticmethod
    def _mask_webhook_url(webhook_url: str) -> str:
        parsed = urlsplit(webhook_url)
        token = parsed.path.rsplit("/", 1)[-1]
        masked = f"{token[:6]}..." if token else "unknown"
        return f"{parsed.scheme}://{parsed.netloc}/.../{masked}"
