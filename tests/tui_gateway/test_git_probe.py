from tui_gateway import git_probe


def test_negative_root_cache_survives_a_full_cold_project_tree_build(monkeypatch):
    """A slow Windows tree build must not re-probe the same dead root mid-request.

    A projects.tree overview has multiple resolution phases and a 120-second
    client deadline. On a host where failed git subprocess cleanup is slow, the
    old 30-second negative TTL expired between phases and expanded one cold tree
    request to 163 seconds. Keep the failed result alive through the complete
    request; known Git/worktree mutations call git_probe.invalidate() explicitly.
    """
    now = [0.0]
    probes = 0
    cache = git_probe._RootCache()

    monkeypatch.setattr(git_probe.time, "monotonic", lambda: now[0])

    def miss() -> str:
        nonlocal probes
        probes += 1
        return ""

    assert cache.resolve("C:/deleted-worktree", miss) == ""

    # Later phases of the same cold tree build revisit the root after the old
    # 30-second TTL, but still within the request's bounded recovery window.
    now[0] = 180.0
    assert cache.resolve("C:/deleted-worktree", miss) == ""
    assert probes == 1
