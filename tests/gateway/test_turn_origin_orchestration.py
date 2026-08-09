"""Gateway stamps trusted TurnOrigin for Adaptive Orchestrator V1.1."""

from __future__ import annotations

from gateway.config import Platform
from gateway.session import SessionSource


def test_session_source_to_trusted_turn_origin_for_slack_dm():
    from agent.orchestration.origin import turn_origin_from_session_source

    source = SessionSource(
        platform=Platform.SLACK,
        chat_id="D0BNXU62YLD",
        user_id="U0BNXPWV8N9",
        chat_type="dm",
        scope_id="T0BP4UYH012",
    )
    origin = turn_origin_from_session_source(
        source, session_key="agent:main:slack:dm:T0BP4UYH012:D0BNXU62YLD"
    )
    assert origin.trusted is True
    assert origin.platform == "slack"
    assert origin.workspace_id == "T0BP4UYH012"
    assert origin.channel_id == "D0BNXU62YLD"
    assert origin.user_id == "U0BNXPWV8N9"


def test_empty_session_source_is_untrusted():
    from agent.orchestration.origin import turn_origin_from_session_source

    origin = turn_origin_from_session_source(None)
    assert origin.trusted is False
    assert origin.platform == ""
