from __future__ import annotations

import json

import pytest

from agent.orchestration.linear_mcp_projection import (
    LinearMCPProjectionConfig,
    LinearMCPProjectionOwner,
)
from agent.orchestration.production_handoff_composition import (
    LiveAdapterUnavailable,
    LiveEffectAuthority,
    ProductionRequestAuthority,
)


class FakeRequestMCPCaller:
    def __init__(self) -> None:
        self.calls = []
        self.comments = []

    @staticmethod
    def envelope(payload):
        return {"content": [{"type": "text", "text": json.dumps(payload)}]}

    def call_tool(
        self, *, request_id, session_id, server_name, tool_name, arguments
    ):
        self.calls.append(
            (request_id, session_id, server_name, tool_name, dict(arguments))
        )
        if tool_name == "list_comments":
            assert arguments["limit"] == 250
            assert arguments["orderBy"] == "createdAt"
            assert "cursor" not in arguments
            return self.envelope(
                {"comments": list(self.comments), "hasNextPage": False, "cursor": None}
            )
        if tool_name == "save_comment":
            self.comments.append({"body": arguments["body"]})
            return self.envelope({"id": f"comment-{len(self.comments)}"})
        raise AssertionError(f"unexpected tool: {tool_name}")


def _authority(*, terminal=False):
    return ProductionRequestAuthority("request-128", "session-128", True, terminal)


def _live(*, request_id="request-128", session_id="session-128", approved=True):
    return LiveEffectAuthority(request_id, session_id, "linear-go-128", approved)


def _owner(caller, **kwargs):
    return LinearMCPProjectionOwner(
        LinearMCPProjectionConfig(True, "live"),
        authority=kwargs.get("authority", _authority()),
        live_authority=kwargs.get("live_authority", _live()),
        caller=caller,
    )


def test_default_off_owner_is_zero_touch_and_config_is_strict():
    caller = FakeRequestMCPCaller()
    owner = LinearMCPProjectionOwner(caller=caller)

    with pytest.raises(LiveAdapterUnavailable, match="default-off"):
        owner.start()
    assert caller.calls == []

    for enabled in (1, "true", None, object()):
        with pytest.raises(LiveAdapterUnavailable, match="literal bool"):
            LinearMCPProjectionConfig(enabled=enabled)
    with pytest.raises(LiveAdapterUnavailable, match="disabled/off or enabled/live"):
        LinearMCPProjectionConfig(True, "off")


@pytest.mark.parametrize(
    "authority,live_authority,error",
    [
        (_authority(terminal=True), _live(), "not active"),
        (_authority(), _live(request_id="other"), "not bound"),
        (_authority(), _live(session_id="other"), "not bound"),
        (_authority(), _live(approved=False), "not bound"),
    ],
)
def test_start_rejects_nonmatching_request_authority_without_mcp_calls(
    authority, live_authority, error
):
    caller = FakeRequestMCPCaller()
    owner = _owner(caller, authority=authority, live_authority=live_authority)

    with pytest.raises(LiveAdapterUnavailable, match=error):
        owner.start()
    assert caller.calls == []


def test_projection_routes_every_call_through_one_request_and_reads_back():
    caller = FakeRequestMCPCaller()
    owner = _owner(caller)
    owner.start()
    key = "a" * 64

    assert owner.upsert_handoff(
        issue="ENG-128", canonical='{"handoff":"digest"}', idempotency_key=key
    ) == key
    assert owner.read_handoff(issue="ENG-128", idempotency_key=key) == '{"handoff":"digest"}'

    assert [call[3] for call in caller.calls] == [
        "list_comments", "save_comment", "list_comments", "list_comments"
    ]
    assert {(call[0], call[1], call[2]) for call in caller.calls} == {
        ("request-128", "session-128", "linear")
    }
    body = caller.comments[0]["body"]
    assert body.startswith("<!-- hermes:linear-mcp-handoff-projection:v1 -->\n")
    assert body.endswith("\n<!-- /hermes:linear-mcp-handoff-projection:v1 -->")
    projected = json.loads(body.splitlines()[1])
    assert projected == {
        "canonical": '{"handoff":"digest"}',
        "idempotency_key": key,
        "schema": "hermes.linear-mcp-handoff-projection.v1",
    }


def test_readback_selects_the_requested_projection_when_issue_has_multiple_jobs():
    caller = FakeRequestMCPCaller()
    caller.comments = [
        {"body": LinearMCPProjectionOwner._document("golden", "a" * 64)},
        {"body": LinearMCPProjectionOwner._document("restart", "b" * 64)},
    ]
    owner = _owner(caller)
    owner.start()

    assert owner.read_handoff(
        issue="ENG-128", idempotency_key="b" * 64
    ) == "restart"


@pytest.mark.parametrize(
    "comments",
    [
        [{"body": LinearMCPProjectionOwner._document("other", "a" * 64)}],
        [
            {"body": LinearMCPProjectionOwner._document("first", "b" * 64)},
            {"body": LinearMCPProjectionOwner._document("second", "b" * 64)},
        ],
    ],
)
def test_readback_fails_closed_without_exactly_one_requested_projection(comments):
    caller = FakeRequestMCPCaller()
    caller.comments = comments
    owner = _owner(caller)
    owner.start()

    with pytest.raises(ValueError, match="exactly one matching projection"):
        owner.read_handoff(issue="ENG-128", idempotency_key="b" * 64)


def test_readback_rejects_invalid_key_before_calling_linear():
    caller = FakeRequestMCPCaller()
    owner = _owner(caller)
    owner.start()

    with pytest.raises(ValueError, match="SHA-256 idempotency key"):
        owner.read_handoff(issue="ENG-128", idempotency_key="not-a-key")
    assert caller.calls == []


def test_projection_decodes_handle_function_call_nested_result_shape():
    class RuntimeShapeCaller(FakeRequestMCPCaller):
        def call_tool(self, **kwargs):
            self.calls.append(kwargs)
            provider_json = json.dumps(
                {
                    "comments": [
                        {
                            "body": LinearMCPProjectionOwner._document(
                                "runtime projection", "f" * 64
                            )
                        }
                    ],
                    "hasNextPage": False,
                    "cursor": None,
                }
            )
            return json.dumps({"result": provider_json})

    caller = RuntimeShapeCaller()
    owner = _owner(caller)
    owner.start()

    assert owner.read_handoff(issue="ENG-128", idempotency_key="f" * 64) == "runtime projection"
    assert caller.calls[0]["tool_name"] == "list_comments"


def test_retry_is_read_only_and_divergent_idempotency_fails_closed():
    caller = FakeRequestMCPCaller()
    owner = _owner(caller)
    owner.start()
    key = "b" * 64
    owner.upsert_handoff(issue="ENG-128", canonical="first", idempotency_key=key)
    caller.calls.clear()

    assert owner.upsert_handoff(
        issue="ENG-128", canonical="first", idempotency_key=key
    ) == key
    assert [call[3] for call in caller.calls] == ["list_comments"]

    with pytest.raises(ValueError, match="different bytes"):
        owner.upsert_handoff(
            issue="ENG-128", canonical="second", idempotency_key=key
        )
    assert [call[3] for call in caller.calls] == ["list_comments", "list_comments"]


def test_owner_cannot_be_used_outside_its_lifecycle():
    caller = FakeRequestMCPCaller()
    owner = _owner(caller)
    with pytest.raises(LiveAdapterUnavailable, match="not started"):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)
    owner.start()
    with pytest.raises(LiveAdapterUnavailable, match="already started"):
        owner.start()
    owner.shutdown()
    with pytest.raises(LiveAdapterUnavailable, match="not started"):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)
    with pytest.raises(LiveAdapterUnavailable, match="cannot restart"):
        owner.start()
    assert caller.calls == []


def test_malformed_readback_fails_closed():
    caller = FakeRequestMCPCaller()
    caller.comments = [{"body": "ordinary issue comment"}]
    owner = _owner(caller)
    owner.start()

    with pytest.raises(ValueError, match="exactly one matching projection"):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)
    assert [call[3] for call in caller.calls] == ["list_comments"]

    assert owner.upsert_handoff(
        issue="ENG-128", canonical="safe", idempotency_key="c" * 64
    ) == "c" * 64
    assert [call[3] for call in caller.calls] == [
        "list_comments", "list_comments", "save_comment", "list_comments"
    ]


def test_projection_never_reads_or_edits_issue_description():
    caller = FakeRequestMCPCaller()
    owner = _owner(caller)
    owner.start()

    owner.upsert_handoff(
        issue="ENG-128", canonical="comment only", idempotency_key="d" * 64
    )

    assert {call[3] for call in caller.calls} == {"list_comments", "save_comment"}
    assert all("description" not in call[4] for call in caller.calls)


def test_terminal_comments_page_accepts_provider_omitted_cursor():
    class OmittedTerminalCursorCaller(FakeRequestMCPCaller):
        def call_tool(self, **kwargs):
            self.calls.append(kwargs)
            assert kwargs["tool_name"] == "list_comments"
            return self.envelope({
                "comments": [
                    {
                        "body": LinearMCPProjectionOwner._document(
                            "provider terminal page", "9" * 64
                        )
                    }
                ],
                "hasNextPage": False,
            })

    owner = _owner(OmittedTerminalCursorCaller())
    owner.start()

    assert owner.read_handoff(issue="ENG-128", idempotency_key="9" * 64) == "provider terminal page"


def test_comment_pages_follow_mcp_json_cursor_until_exhausted():
    class PaginatedCaller(FakeRequestMCPCaller):
        def call_tool(self, **kwargs):
            arguments = kwargs["arguments"]
            self.calls.append(
                (
                    kwargs["request_id"],
                    kwargs["session_id"],
                    kwargs["server_name"],
                    kwargs["tool_name"],
                    dict(arguments),
                )
            )
            assert kwargs["tool_name"] == "list_comments"
            assert arguments["limit"] == 250
            assert arguments["orderBy"] == "createdAt"
            if "cursor" not in arguments:
                return self.envelope({
                    "comments": [{"body": "ordinary"}],
                    "hasNextPage": True,
                    "cursor": "page-2",
                })
            assert arguments["cursor"] == "page-2"
            return self.envelope(
                {
                    "comments": [
                        {
                            "body": LinearMCPProjectionOwner._document(
                                "latest", "e" * 64
                            )
                        }
                    ],
                    "hasNextPage": False,
                    "cursor": None,
                }
            )

    caller = PaginatedCaller()
    owner = _owner(caller)
    owner.start()

    assert owner.read_handoff(issue="ENG-128", idempotency_key="e" * 64) == "latest"
    assert [call[4].get("cursor") for call in caller.calls] == [None, "page-2"]


@pytest.mark.parametrize(
    "result,error",
    [
        ({"comments": [], "hasNextPage": "false", "cursor": None}, "pagination"),
        ({"comments": [], "hasNextPage": True, "cursor": None}, "pagination"),
        ({"comments": []}, "invalid schema"),
    ],
)
def test_malformed_comment_pagination_fails_closed(result, error):
    class Caller(FakeRequestMCPCaller):
        def call_tool(self, **kwargs):
            return self.envelope(result)

    owner = _owner(Caller())
    owner.start()
    with pytest.raises(ValueError, match=error):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)


def test_repeated_cursor_and_page_bound_fail_closed(monkeypatch):
    class Caller(FakeRequestMCPCaller):
        def call_tool(self, **kwargs):
            return self.envelope({
                "comments": [], "hasNextPage": True, "cursor": "same"
            })

    owner = _owner(Caller())
    owner.start()
    with pytest.raises(ValueError, match="invalid pagination"):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)

    monkeypatch.setattr("agent.orchestration.linear_mcp_projection._MAX_COMMENT_PAGES", 1)
    owner = _owner(Caller())
    owner.start()
    with pytest.raises(ValueError, match="exceeded its bound"):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)


@pytest.mark.parametrize(
    "envelope,error",
    [
        ({"content": [{"type": "text", "text": "not-json"}]}, "not valid JSON"),
        ({"content": [], "isError": False}, "exactly one text block"),
        ({"content": [{"type": "text", "text": "{}"}], "isError": True}, "error result"),
        ({"comments": []}, "MCP result envelope"),
    ],
)
def test_malformed_mcp_result_envelope_fails_closed(envelope, error):
    class Caller(FakeRequestMCPCaller):
        def call_tool(self, **kwargs):
            return envelope

    owner = _owner(Caller())
    owner.start()
    with pytest.raises(ValueError, match=error):
        owner.read_handoff(issue="ENG-128", idempotency_key="a" * 64)
