"""ENG-25 — dedicated schema domain/ownership fail-closed classification."""

from __future__ import annotations

import pytest

from agent.durable_jobs.postgres_domain import (
    APPLICATION_DOMAIN,
    CHECKPOINTER_DOMAIN,
    SchemaOccupancy,
    classify_schema_occupancy,
)


def test_vacant_schema_is_creatable():
    decision = classify_schema_occupancy(
        schema_exists=False,
        table_names=frozenset(),
        markers={},
        owner_role=None,
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.VACANT


def test_empty_existing_schema_is_foreign():
    decision = classify_schema_occupancy(
        schema_exists=True,
        table_names=frozenset(),
        markers={},
        owner_role="other",
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.EMPTY


def test_unrelated_tables_without_marker_are_foreign():
    decision = classify_schema_occupancy(
        schema_exists=True,
        table_names=frozenset({"widgets", "orders"}),
        markers={},
        owner_role="hermes",
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.UNRELATED


def test_unmarked_durable_tables_fail_closed():
    decision = classify_schema_occupancy(
        schema_exists=True,
        table_names=frozenset({"durable_jobs", "durable_jobs_meta"}),
        markers={"schema_version": "9"},
        owner_role="hermes",
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.UNMARKED


def test_wrong_domain_marker_is_foreign():
    decision = classify_schema_occupancy(
        schema_exists=True,
        table_names=frozenset({"durable_jobs_meta"}),
        markers={
            "schema_version": "9",
            "domain": CHECKPOINTER_DOMAIN,
            "owner_role": "hermes",
        },
        owner_role="hermes",
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.FOREIGN_DOMAIN


def test_wrong_owner_fails_closed():
    decision = classify_schema_occupancy(
        schema_exists=True,
        table_names=frozenset({"durable_jobs_meta"}),
        markers={
            "schema_version": "9",
            "domain": APPLICATION_DOMAIN,
            "owner_role": "alice",
        },
        owner_role="alice",
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.WRONG_OWNER


def test_owned_application_schema_may_reopen():
    decision = classify_schema_occupancy(
        schema_exists=True,
        table_names=frozenset({"durable_jobs_meta", "durable_jobs"}),
        markers={
            "schema_version": "9",
            "domain": APPLICATION_DOMAIN,
            "owner_role": "hermes",
        },
        owner_role="hermes",
        current_role="hermes",
        expected_domain=APPLICATION_DOMAIN,
    )
    assert decision is SchemaOccupancy.OWNED


def test_fail_closed_helper_rejects_non_owned(monkeypatch):
    from agent.durable_jobs.config import DurableJobsConfigError
    from agent.durable_jobs.postgres_domain import require_owned_or_vacant

    with pytest.raises(DurableJobsConfigError):
        require_owned_or_vacant(SchemaOccupancy.EMPTY, schema="djapp")
    with pytest.raises(DurableJobsConfigError):
        require_owned_or_vacant(SchemaOccupancy.UNRELATED, schema="djapp")
    with pytest.raises(DurableJobsConfigError):
        require_owned_or_vacant(SchemaOccupancy.UNMARKED, schema="djapp")
    with pytest.raises(DurableJobsConfigError):
        require_owned_or_vacant(SchemaOccupancy.FOREIGN_DOMAIN, schema="djapp")
    with pytest.raises(DurableJobsConfigError):
        require_owned_or_vacant(SchemaOccupancy.WRONG_OWNER, schema="djapp")
    require_owned_or_vacant(SchemaOccupancy.VACANT, schema="djapp")
    require_owned_or_vacant(SchemaOccupancy.OWNED, schema="djapp")
