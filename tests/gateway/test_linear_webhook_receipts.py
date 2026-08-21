"""RED→GREEN tests for Linear Issue→Go receipt-only webhook intake.

Covers the first safe ENG-13 slice: a valid signed + allowlisted Linear Issue
state transition persists exactly one profile-local durable receipt and never
dispatches handle_message. Duplicates survive store/adapter reconstruction.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import time
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.webhook import WebhookAdapter, _INSECURE_NO_AUTH


GO_STATE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
OTHER_STATE_ID = "11111111-2222-3333-4444-555555555555"
ISSUE_ID = "issue-uuid-001"
ISSUE_IDENTIFIER = "ENG-13"
SECRET = "linear-receipt-secret"
PROVIDER = "linear"


def _svix_signature(body: bytes, secret: str, msg_id: str, timestamp: str) -> str:
    key = (
        base64.b64decode(secret.removeprefix("whsec_"))
        if secret.startswith("whsec_")
        else secret.encode()
    )
    signed = msg_id.encode() + b"." + timestamp.encode() + b"." + body
    digest = hmac.new(key, signed, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


def _linear_issue_update_payload(
    *,
    state_id: str = GO_STATE_ID,
    action: str = "update",
    event_type: str = "Issue",
    issue_id: str = ISSUE_ID,
    identifier: str = ISSUE_IDENTIFIER,
    previous_state_id: str = OTHER_STATE_ID,
    include_updated_from_state: bool = True,
) -> dict:
    data = {
        "id": issue_id,
        "identifier": identifier,
        "title": "Receipt-only intake",
        "stateId": state_id,
        "number": 13,
    }
    payload: dict = {
        "action": action,
        "type": event_type,
        "data": data,
        "url": f"https://linear.app/team/issue/{identifier}",
        "createdAt": "2026-08-11T12:00:00.000Z",
        "organizationId": "org-uuid",
        "webhookTimestamp": int(time.time() * 1000),
        "webhookId": "webhook-uuid",
    }
    if include_updated_from_state:
        payload["updatedFrom"] = {"stateId": previous_state_id}
    return payload


def _receipt_route(**overrides) -> dict:
    route = {
        "secret": SECRET,
        "receipt_only": "linear_issue_go",
        "allowed_state_ids": [GO_STATE_ID],
        "events": ["Issue"],
    }
    route.update(overrides)
    return route


def _make_adapter(tmp_path: Path, routes=None, **extra_kwargs) -> WebhookAdapter:
    config = PlatformConfig(
        enabled=True,
        extra={
            "host": "127.0.0.1",
            "port": 0,
            "routes": routes or {"linear-go": _receipt_route()},
            "rate_limit": 30,
            "max_body_bytes": 1_048_576,
            **extra_kwargs,
        },
    )
    adapter = WebhookAdapter(config)
    # Fresh profile-local store rooted at the test HERMES_HOME.
    from gateway.webhook_receipts import WebhookReceiptStore

    adapter._receipt_store = WebhookReceiptStore(
        db_path=tmp_path / "webhook_receipts.db"
    )
    return adapter


def _create_app(adapter: WebhookAdapter) -> web.Application:
    app = web.Application(client_max_size=adapter._max_body_bytes)
    app.router.add_get("/health", adapter._handle_health)
    app.router.add_post("/webhooks/{route_name}", adapter._handle_webhook)
    return app


async def _post_signed(
    cli,
    *,
    payload: dict,
    delivery_id: str = "msg_linear_delivery_1",
    secret: str = SECRET,
    route: str = "linear-go",
    include_svix: bool = True,
    bad_signature: bool = False,
    auth_without_svix: bool = False,
    timestamp: str | None = None,
):
    timestamp = timestamp or str(int(time.time() * 1000))
    payload = dict(payload)
    payload["webhookTimestamp"] = int(timestamp)
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if include_svix:
        sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        if bad_signature:
            sig = "not-the-real-digest"
        headers.update(
            {
                "Linear-Delivery": delivery_id,
                "Linear-Timestamp": timestamp,
                "Linear-Signature": sig,
            }
        )
    elif auth_without_svix:
        # Pass Linear HMAC auth while omitting its provider delivery id.
        headers["Linear-Signature"] = hmac.new(
            secret.encode(), body, hashlib.sha256
        ).hexdigest()
        headers["Linear-Timestamp"] = timestamp
    return await cli.post(f"/webhooks/{route}", data=body, headers=headers)


async def _post_linear_signed(
    cli,
    *,
    payload: dict,
    delivery_id: str = "linear_delivery_1",
    secret: str = SECRET,
    timestamp: str | None = None,
):
    """Send the actual Linear webhook header contract."""
    timestamp = timestamp or str(int(time.time() * 1000))
    payload = dict(payload)
    payload["webhookTimestamp"] = int(timestamp)
    body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return await cli.post(
        "/webhooks/linear-go",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Linear-Delivery": delivery_id,
            "Linear-Signature": signature,
            "Linear-Timestamp": timestamp,
        },
    )


def _count_receipts(db_path: Path) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT COUNT(*) FROM webhook_receipts").fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


@pytest.fixture
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


# ---------------------------------------------------------------------------
# 1) Valid signed + allowlisted event → one receipt, no handle_message
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_valid_linear_issue_go_creates_one_receipt_no_handle_message(
    hermes_home,
):
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()
    db_path = hermes_home / "webhook_receipts.db"

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await _post_signed(cli, payload=_linear_issue_update_payload())
        assert resp.status == 202
        data = await resp.json()
        assert data["status"] == "received"
        assert data.get("receipt_id")
        assert isinstance(data["receipt_id"], str)
        assert data["receipt_id"]

    adapter.handle_message.assert_not_called()
    assert _count_receipts(db_path) == 1


# ---------------------------------------------------------------------------
# 2) Same Svix ID duplicates after fresh store/adapter; still one row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_duplicate_svix_id_after_store_reconstruction_is_idempotent(
    hermes_home,
):
    delivery_id = "msg_linear_dup_42"
    payload = _linear_issue_update_payload()
    timestamp = str(int(time.time() * 1000))
    db_path = hermes_home / "webhook_receipts.db"

    adapter1 = _make_adapter(hermes_home)
    adapter1.handle_message = AsyncMock()
    app1 = _create_app(adapter1)
    async with TestClient(TestServer(app1)) as cli:
        resp1 = await _post_signed(
            cli,
            payload=payload,
            delivery_id=delivery_id,
            timestamp=timestamp,
        )
        assert resp1.status == 202
        first = await resp1.json()
        assert first["status"] == "received"
        receipt_id = first["receipt_id"]

    # Reconstruct adapter + store against the same profile-local DB.
    adapter2 = _make_adapter(hermes_home)
    adapter2.handle_message = AsyncMock()
    app2 = _create_app(adapter2)
    async with TestClient(TestServer(app2)) as cli:
        resp2 = await _post_signed(
            cli,
            payload=payload,
            delivery_id=delivery_id,
            timestamp=timestamp,
        )
        assert resp2.status == 200
        second = await resp2.json()
        assert second["status"] == "duplicate"
        assert second.get("receipt_id") == receipt_id

    adapter1.handle_message.assert_not_called()
    adapter2.handle_message.assert_not_called()
    assert _count_receipts(db_path) == 1


# ---------------------------------------------------------------------------
# 3) Fail-closed validation: no receipt for bad/missing fields
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload_mutator,include_svix,bad_signature,auth_without_svix",
    [
        (lambda p: p.update({"type": "Comment"}) or p, True, False, False),
        (lambda p: p.update({"action": "create"}) or p, True, False, False),
        (
            lambda p: (
                p["data"].__setitem__("stateId", OTHER_STATE_ID) or p
            ),
            True,
            False,
            False,
        ),
        (lambda p: (p.pop("updatedFrom", None) or True) and p, True, False, False),
        (
            lambda p: p.update({"updatedFrom": {"stateId": GO_STATE_ID}}) or p,
            True,
            False,
            False,
        ),
        (
            lambda p: (p["data"].pop("id", None) or True) and p,
            True,
            False,
            False,
        ),
        (
            lambda p: (p["data"].pop("identifier", None) or True) and p,
            True,
            False,
            False,
        ),
        (
            lambda p: (p["data"].pop("stateId", None) or True) and p,
            True,
            False,
            False,
        ),
        (lambda p: p, False, False, True),  # missing Svix delivery ID
        (lambda p: p, True, True, False),  # invalid signature
    ],
    ids=[
        "wrong_event_type",
        "wrong_action",
        "state_not_allowlisted",
        "missing_state_transition",
        "no_actual_state_transition",
        "missing_issue_id",
        "missing_identifier",
        "missing_state_id",
        "missing_delivery_id",
        "invalid_signature",
    ],
)
async def test_invalid_linear_receipt_inputs_create_no_receipt(
    hermes_home,
    payload_mutator,
    include_svix,
    bad_signature,
    auth_without_svix,
):
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()
    db_path = hermes_home / "webhook_receipts.db"

    payload = _linear_issue_update_payload()
    payload_mutator(payload)

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await _post_signed(
            cli,
            payload=payload,
            include_svix=include_svix,
            bad_signature=bad_signature,
            auth_without_svix=auth_without_svix,
        )
        assert resp.status != 202
        body = await resp.json()
        assert body.get("status") != "received"

    adapter.handle_message.assert_not_called()
    assert _count_receipts(db_path) == 0


# ---------------------------------------------------------------------------
# 4) Storage failure is safe and never invokes handle_message
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_storage_failure_is_safe_and_skips_handle_message(hermes_home):
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        with patch.object(
            adapter._receipt_store,
            "record_linear_issue_go",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            resp = await _post_signed(cli, payload=_linear_issue_update_payload())
        assert resp.status >= 500
        data = await resp.json()
        assert data.get("status") != "received"
        assert "error" in data

    adapter.handle_message.assert_not_called()
    assert _count_receipts(hermes_home / "webhook_receipts.db") == 0


# ---------------------------------------------------------------------------
# 5) Normal generic webhook behavior remains unchanged
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_normal_webhook_behavior_still_accepts_and_dispatches(hermes_home):
    routes = {
        "gh": {
            "secret": _INSECURE_NO_AUTH,
            "events": ["pull_request"],
            "prompt": "PR: {action}",
        }
    }
    adapter = _make_adapter(hermes_home, routes=routes)
    adapter.handle_message = AsyncMock()

    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        resp = await cli.post(
            "/webhooks/gh",
            json={"action": "opened"},
            headers={
                "X-GitHub-Event": "pull_request",
                "X-GitHub-Delivery": "normal-webhook-1",
            },
        )
        assert resp.status == 202
        data = await resp.json()
        assert data["status"] == "accepted"

    adapter.handle_message.assert_awaited()
    assert _count_receipts(hermes_home / "webhook_receipts.db") == 0


@pytest.mark.asyncio
async def test_actual_linear_headers_are_required_and_timestamp_is_fresh(hermes_home):
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()
    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        accepted = await _post_linear_signed(
            cli,
            payload=_linear_issue_update_payload(),
            delivery_id="linear-real-1",
        )
        assert accepted.status == 202

        stale = await _post_linear_signed(
            cli,
            payload=_linear_issue_update_payload(issue_id="issue-stale"),
            delivery_id="linear-stale-1",
            timestamp=str(int((time.time() - 301) * 1000)),
        )
        assert stale.status == 401


def test_receipt_store_singletons_are_profile_scoped(hermes_home):
    adapter = _make_adapter(hermes_home)
    adapter._receipt_store = None
    default_store = adapter._get_receipt_store("default")
    coder_store = adapter._get_receipt_store("coder")
    assert default_store is not coder_store
    assert default_store.db_path == hermes_home / "webhook_receipts.db"
    assert coder_store.db_path == hermes_home / "profiles" / "coder" / "webhook_receipts.db"


@pytest.mark.asyncio
async def test_receipt_sqlite_write_runs_off_event_loop(hermes_home, monkeypatch):
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()
    called = False
    real_to_thread = __import__("asyncio").to_thread

    async def spy_to_thread(func, /, *args, **kwargs):
        nonlocal called
        if getattr(func, "__name__", "") == "record_linear_issue_go":
            called = True
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr("gateway.platforms.webhook.asyncio.to_thread", spy_to_thread)
    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        response = await _post_linear_signed(cli, payload=_linear_issue_update_payload())
    assert response.status == 202
    assert called is True


@pytest.mark.asyncio
async def test_same_delivery_id_with_different_payload_is_conflict(hermes_home):
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()
    app = _create_app(adapter)
    async with TestClient(TestServer(app)) as cli:
        first = await _post_linear_signed(
            cli,
            payload=_linear_issue_update_payload(),
            delivery_id="linear-conflict-1",
        )
        second = await _post_linear_signed(
            cli,
            payload=_linear_issue_update_payload(issue_id="different-issue"),
            delivery_id="linear-conflict-1",
        )
    assert first.status == 202
    assert second.status == 409


@pytest.mark.asyncio
async def test_linear_replay_cannot_bypass_receipt_with_new_headers(hermes_home):
    """Unsigned delivery/timestamp headers cannot mint another receipt."""
    adapter = _make_adapter(hermes_home)
    adapter.handle_message = AsyncMock()
    app = _create_app(adapter)
    payload = _linear_issue_update_payload()
    timestamp = str(int(time.time() * 1000))
    payload["webhookTimestamp"] = int(timestamp)
    body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()

    async with TestClient(TestServer(app)) as cli:
        first = await cli.post(
            "/webhooks/linear-go",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Linear-Delivery": "linear-replay-original",
                "Linear-Signature": signature,
                "Linear-Timestamp": timestamp,
            },
        )
        replay = await cli.post(
            "/webhooks/linear-go",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Linear-Delivery": "linear-replay-attacker-changed",
                "Linear-Signature": signature,
                "Linear-Timestamp": timestamp,
            },
        )
        replay_body = await replay.json()

    assert first.status == 202
    assert replay.status == 200
    assert replay_body["status"] == "duplicate"
    assert _count_receipts(hermes_home / "webhook_receipts.db") == 1


@pytest.mark.asyncio
async def test_linear_unsigned_timestamp_must_match_signed_payload(hermes_home):
    adapter = _make_adapter(hermes_home)
    app = _create_app(adapter)
    payload = _linear_issue_update_payload()
    signed_timestamp = str(int(time.time() * 1000))
    payload["webhookTimestamp"] = int(signed_timestamp)
    body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()

    async with TestClient(TestServer(app)) as cli:
        response = await cli.post(
            "/webhooks/linear-go",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Linear-Delivery": "linear-timestamp-mismatch",
                "Linear-Signature": signature,
                "Linear-Timestamp": str(int(signed_timestamp) + 1),
            },
        )

    assert response.status == 401
    assert _count_receipts(hermes_home / "webhook_receipts.db") == 0
