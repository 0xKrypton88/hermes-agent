"""Tests for the local, fail-closed ENG-13 launch-gate preflight.

The gate inspects configuration only. It never opens a listener, registers a
Linear webhook, starts an agent, or changes an issue state.
"""

from __future__ import annotations

from gateway.linear_go_launch_gate import assess_linear_go_launch_gate


GO_STATE_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def test_gate_blocks_when_webhook_platform_is_disabled():
    result = assess_linear_go_launch_gate({
        "platforms": {
            "webhook": {
                "enabled": False,
                "extra": {
                    "host": "127.0.0.1",
                    "routes": {
                        "linear-go": {
                            "secret": "configured-secret",
                            "receipt_only": "linear_issue_go",
                            "allowed_state_ids": [GO_STATE_ID],
                            "events": ["Issue"],
                        }
                    },
                },
            }
        }
    })

    assert result.status == "BLOCKED"
    assert result.listener_ready is False
    assert "webhook_platform_disabled" in result.blockers
    assert result.external_activation_ready is False


def test_gate_is_local_ready_only_with_loopback_receipt_route():
    result = assess_linear_go_launch_gate({
        "platforms": {
            "webhook": {
                "enabled": True,
                "extra": {
                    "host": "127.0.0.1",
                    "routes": {
                        "linear-go": {
                            "secret": "configured-secret",
                            "receipt_only": "linear_issue_go",
                            "allowed_state_ids": [GO_STATE_ID],
                            "events": ["Issue"],
                        }
                    },
                },
            }
        }
    })

    assert result.status == "LOCAL_READY"
    assert result.listener_ready is True
    assert result.external_activation_ready is False
    assert result.blockers == ()


def test_gate_blocks_broad_bind_and_missing_receipt_contract():
    result = assess_linear_go_launch_gate({
        "platforms": {
            "webhook": {
                "enabled": True,
                "host": "0.0.0.0",
                "extra": {"routes": {"linear-go": {}}},
            }
        }
    })

    assert result.status == "BLOCKED"
    assert "non_loopback_host" in result.blockers
    assert "missing_receipt_only_mode" in result.blockers
    assert "missing_allowed_state_ids" in result.blockers
    assert "missing_route_secret" in result.blockers


def test_gate_requires_exactly_one_linear_go_route():
    result = assess_linear_go_launch_gate({
        "platforms": {
            "webhook": {
                "enabled": True,
                "extra": {
                    "host": "127.0.0.1",
                    "routes": {
                        "linear-go-a": {
                            "secret": "configured-secret",
                            "receipt_only": "linear_issue_go",
                            "allowed_state_ids": [GO_STATE_ID],
                        },
                        "linear-go-b": {
                            "secret": "configured-secret",
                            "receipt_only": "linear_issue_go",
                            "allowed_state_ids": [GO_STATE_ID],
                        },
                    },
                },
            }
        }
    })

    assert result.status == "BLOCKED"
    assert "linear_go_route_count_not_one" in result.blockers
