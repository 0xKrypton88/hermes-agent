"""ENG-25 — realistic DSN/password redaction, not placeholder-only checks."""

from __future__ import annotations

import pytest

from agent.durable_jobs.redaction import redact_payload, redact_secret_text

URI_AT = "postgresql://hermes:p@ssword@127.0.0.1:5432/durable_jobs"
URI_ENCODED = "postgresql://hermes:p%40ss%20word@127.0.0.1:5432/durable_jobs"
LIBPQ_QUOTED = "host=127.0.0.1 dbname=durable_jobs password='secret with spaces'"
LIBPQ_DOUBLE = 'host=127.0.0.1 dbname=durable_jobs password="quoted secret"'
PLAIN_PASSWORD = "p@ssword"
ENCODED_PASSWORD = "p%40ss%20word"
QUOTED_PASSWORD = "secret with spaces"
DOUBLE_PASSWORD = "quoted secret"


def test_redact_secret_text_uri_password_containing_at():
    redacted = redact_secret_text(f"failed dsn={URI_AT}")
    assert PLAIN_PASSWORD not in redacted
    assert "p@ssword" not in redacted
    assert "ssword@" not in redacted
    assert "[REDACTED]" in redacted
    assert "127.0.0.1" in redacted


def test_redact_secret_text_percent_encoded_userinfo_password():
    redacted = redact_secret_text(URI_ENCODED)
    assert ENCODED_PASSWORD not in redacted
    assert "p%40ss" not in redacted
    assert "[REDACTED]" in redacted


def test_redact_secret_text_quoted_libpq_password_with_spaces():
    redacted = redact_secret_text(LIBPQ_QUOTED)
    assert QUOTED_PASSWORD not in redacted
    assert "secret" not in redacted
    assert "spaces" not in redacted
    assert "[REDACTED]" in redacted
    assert "dbname=durable_jobs" in redacted


def test_redact_secret_text_double_quoted_libpq_password():
    redacted = redact_secret_text(LIBPQ_DOUBLE)
    assert DOUBLE_PASSWORD not in redacted
    assert "quoted" not in redacted.lower() or "[REDACTED]" in redacted
    assert "secret" not in redacted
    assert "[REDACTED]" in redacted


def test_redact_payload_scans_ordinary_string_leaves():
    payload = {
        "job_id": "dj_keep",
        "note": f"reconnect with {URI_AT}",
        "nested": {"detail": LIBPQ_QUOTED},
        "items": [URI_ENCODED],
    }
    redacted = redact_payload(payload)
    dumped = str(redacted)
    assert redacted["job_id"] == "dj_keep"
    assert PLAIN_PASSWORD not in dumped
    assert QUOTED_PASSWORD not in dumped
    assert ENCODED_PASSWORD not in dumped
    assert "[REDACTED]" in dumped


def test_config_error_redacts_realistic_passwords():
    from agent.durable_jobs.config import DurableJobsConfigError, load_durable_jobs_config

    with pytest.raises(DurableJobsConfigError) as exc:
        load_durable_jobs_config(
            {
                "durable_jobs": {
                    "backend": "postgresql",
                    "postgres_dsn": URI_AT,
                    "postgres_schema": "public",
                    "checkpoint_postgres_dsn": URI_ENCODED,
                    "checkpoint_postgres_schema": "durable_jobs_ckpt",
                    "postgres_storage_id": "app",
                    "checkpoint_postgres_storage_id": "ckpt",
                    "postgres_environment_id": "test",
                }
            }
        )
    text = str(exc.value)
    assert PLAIN_PASSWORD not in text
    assert ENCODED_PASSWORD not in text
    assert "p@ssword" not in text
