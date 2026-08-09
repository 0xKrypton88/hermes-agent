from __future__ import annotations

from hermes_cli.projects_db import Project, ProjectFolder
from tui_gateway.project_routing import choose_project


def _project(
    project_id: str,
    name: str,
    path: str,
    *,
    aliases: list[str] | None = None,
) -> Project:
    return Project(
        id=project_id,
        name=name,
        slug=name.lower().replace(" ", "-"),
        description=None,
        icon=None,
        color=None,
        board_slug=None,
        primary_path=path,
        archived=False,
        created_at=1,
        folders=[ProjectFolder(path=path, label=None, is_primary=True, added_at=1)],
        aliases=aliases or [],
    )


def test_project_area_title_prefix_beats_the_current_workspace() -> None:
    hermes = _project("hermes", "Hermes Agent", "/repos/hermes-agent", aliases=["Hermes"])
    mcc = _project("mcc", "Mission Control", "/repos/mission-control", aliases=["MCC"])

    decision = choose_project(
        [hermes, mcc],
        title="HERMES - PROJECT ROUTING",
        user_text="Make the Hermes sidebar easier to navigate",
        cwd="/repos/mission-control/src",
        git_repo_root="/repos/mission-control",
    )

    assert decision is not None
    assert decision.project_id == "hermes"
    assert decision.confidence >= 0.9
    assert "title prefix" in decision.reason
    assert "message mention" in decision.reason


def test_custom_alias_can_route_a_project_without_matching_its_full_name() -> None:
    mcc = _project("mcc", "Mission Control", "/repos/mission-control", aliases=["MCC"])
    quantcore = _project("qc", "QuantCore", "/repos/quantcore")

    decision = choose_project(
        [mcc, quantcore],
        user_text="Continue the MCC work statistics page",
        assistant_text="I will inspect the dashboard implementation.",
        cwd="/repos/quantcore",
    )

    assert decision is not None
    assert decision.project_id == "mcc"


def test_workspace_only_evidence_does_not_create_an_explicit_assignment() -> None:
    hermes = _project("hermes", "Hermes Agent", "/repos/hermes-agent")

    decision = choose_project(
        [hermes],
        cwd="/repos/hermes-agent/apps/desktop",
        git_repo_root="/repos/hermes-agent",
    )

    assert decision is None


def test_ambiguous_semantic_mentions_wait_for_more_evidence() -> None:
    hermes = _project("hermes", "Hermes Agent", "/repos/hermes-agent")
    mcc = _project("mcc", "Mission Control", "/repos/mission-control")

    decision = choose_project(
        [hermes, mcc],
        user_text="Compare Hermes Agent with Mission Control",
    )

    assert decision is None


def test_activity_and_message_evidence_can_combine_without_a_title_prefix() -> None:
    quantcore = _project("qc", "QuantCore", "/repos/quantcore", aliases=["QC"])
    norna = _project("norna", "Norna Agency", "/repos/norna")

    decision = choose_project(
        [quantcore, norna],
        user_text="Fix the QuantCore campaign status",
        activity_text="edited /repos/quantcore/src/campaigns.py and ran focused tests",
        cwd="/tmp/scratch",
    )

    assert decision is not None
    assert decision.project_id == "qc"
    assert "activity" in decision.reason
