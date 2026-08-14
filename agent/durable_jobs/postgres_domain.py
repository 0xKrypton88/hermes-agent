"""Dedicated PostgreSQL schema domain / ownership markers (ENG-25)."""

from __future__ import annotations

from enum import Enum
from typing import Mapping, Optional

from agent.durable_jobs.config import DurableJobsConfigError

APPLICATION_DOMAIN = "hermes.durable_jobs.application"
CHECKPOINTER_DOMAIN = "hermes.durable_jobs.checkpointer"
DOMAIN_META_KEY = "domain"
OWNER_META_KEY = "owner_role"


class SchemaOccupancy(Enum):
    VACANT = "vacant"
    OWNED = "owned"
    EMPTY = "empty"
    UNRELATED = "unrelated"
    UNMARKED = "unmarked"
    FOREIGN_DOMAIN = "foreign_domain"
    WRONG_OWNER = "wrong_owner"


def classify_schema_occupancy(
    *,
    schema_exists: bool,
    table_names: frozenset[str],
    markers: Mapping[str, str],
    owner_role: Optional[str],
    current_role: str,
    expected_domain: str,
) -> SchemaOccupancy:
    if not schema_exists:
        return SchemaOccupancy.VACANT
    domain = (markers.get(DOMAIN_META_KEY) or "").strip()
    marked_owner = (markers.get(OWNER_META_KEY) or "").strip()
    if not domain:
        if not table_names:
            return SchemaOccupancy.EMPTY
        if "durable_jobs_meta" in table_names or "durable_checkpoint_meta" in table_names:
            return SchemaOccupancy.UNMARKED
        return SchemaOccupancy.UNRELATED
    if domain != expected_domain:
        return SchemaOccupancy.FOREIGN_DOMAIN
    expected_owner = marked_owner or (owner_role or "")
    if current_role != expected_owner or (owner_role and owner_role != current_role):
        return SchemaOccupancy.WRONG_OWNER
    return SchemaOccupancy.OWNED


def require_owned_or_vacant(occupancy: SchemaOccupancy, *, schema: str) -> None:
    if occupancy in {SchemaOccupancy.VACANT, SchemaOccupancy.OWNED}:
        return
    raise DurableJobsConfigError(
        f"PostgreSQL schema {schema!r} occupancy is {occupancy.value}; "
        "refusing to adopt empty, unmarked, foreign, or wrong-owner schemas"
    )
