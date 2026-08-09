"""Process-level integration: hermes chat -Q launch title seed persistence.

Exercises the real CLI argv path against a temp HERMES_HOME and a local fake
OpenAI-compatible model seam. No external models or credentials.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml

from hermes_state import SessionDB

REPO_ROOT = Path(__file__).resolve().parents[1]


def _resolve_hermes_cmd() -> list[str]:
    """Prefer a local hermes entrypoint; fall back to the active interpreter."""
    candidates = [
        REPO_ROOT / ".venv" / "Scripts" / "hermes.exe",
        REPO_ROOT / "venv" / "Scripts" / "hermes.exe",
        REPO_ROOT / ".venv" / "bin" / "hermes",
        REPO_ROOT / "venv" / "bin" / "hermes",
    ]
    for path in candidates:
        if path.exists():
            return [str(path)]
    return [sys.executable, "-c", "from hermes_cli.main import main; main()"]


class _FakeChatHandler(BaseHTTPRequestHandler):
    """Minimal OpenAI-compatible chat completions seam (text-only, no tools)."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b"{}"
        try:
            request = json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            request = {}
        content = "Seeded session title check complete."
        if request.get("stream"):
            chunks = [
                {
                    "id": "chatcmpl-fake-title-seed",
                    "object": "chat.completion.chunk",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"role": "assistant", "content": content},
                            "finish_reason": None,
                        }
                    ],
                },
                {
                    "id": "chatcmpl-fake-title-seed",
                    "object": "chat.completion.chunk",
                    "choices": [
                        {
                            "index": 0,
                            "delta": {},
                            "finish_reason": "stop",
                        }
                    ],
                },
            ]
            payload = b"".join(
                f"data: {json.dumps(chunk)}\n\n".encode("utf-8") for chunk in chunks
            ) + b"data: [DONE]\n\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        body = {
            "id": "chatcmpl-fake-title-seed",
            "object": "chat.completion",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": content,
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 8,
                "completion_tokens": 6,
                "total_tokens": 14,
            },
        }
        payload = json.dumps(body).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        # Some clients probe /models; keep the seam quiet.
        body = json.dumps({"data": [{"id": "fake-title-model"}]}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):  # noqa: A003 - stdlib signature
        return


@pytest.fixture()
def fake_openai_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeChatHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}/v1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _scrubbed_env(hermes_home: Path) -> dict[str, str]:
    """Build a hermetic child env: keep path/locale, drop provider credentials."""
    keep_prefixes = ("PATH", "SYSTEMROOT", "SYSTEMDRIVE", "WINDIR", "TEMP", "TMP", "COMSPEC")
    env = {
        key: value
        for key, value in os.environ.items()
        if key.upper().startswith(keep_prefixes)
        or key
        in {
            "USERPROFILE",
            "HOMEDRIVE",
            "HOMEPATH",
            "LOCALAPPDATA",
            "APPDATA",
            "PATHEXT",
            "NUMBER_OF_PROCESSORS",
        }
    }
    env.update(
        {
            "HERMES_HOME": str(hermes_home),
            "HOME": str(hermes_home.parent),
            "USERPROFILE": str(hermes_home.parent),
            "TZ": "UTC",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONHASHSEED": "0",
            "PYTHONUTF8": "1",
            "PYTHONPATH": str(REPO_ROOT),
            "HERMES_YOLO_MODE": "1",
            # Stale leftovers must not leak into the child and affect seeding.
            "HERMES_TITLE_PROJECT": "STALE-SHOULD-NOT-WIN",
            "HERMES_TITLE_AREA": "Stale Area",
        }
    )
    return env


def _write_temp_hermes_home(home: Path, *, base_url: str) -> None:
    home.mkdir(parents=True, exist_ok=True)
    (home / ".env").write_text("OPENAI_API_KEY=test-fake-key\n", encoding="utf-8")
    config = {
        "model": {
            "default": "fake-title-model",
            "provider": "fake-title-local",
            "base_url": base_url,
            "api_key": "test-fake-key",
        },
        "providers": {
            "fake-title-local": {
                "api": base_url,
                "api_key": "test-fake-key",
            }
        },
        "auxiliary": {
            "title_generation": {"enabled": False},
        },
        "memory": {"memory_enabled": False, "user_profile_enabled": False},
        "compression": {"enabled": False},
        "display": {"interface": "cli", "tool_progress": "off"},
        "agent": {"max_turns": 2},
        "tools": {
            "cli": {"enabled": []},
        },
    }
    (home / "config.yaml").write_text(
        yaml.safe_dump(config, sort_keys=False), encoding="utf-8"
    )


def test_hermes_chat_quiet_oneshot_persists_launch_title_meta(
    tmp_path, fake_openai_server
):
    """Real ``hermes chat -Q ... --title-project/--title-area -q`` process path.

    After the child exits, reopen state.db and prove title / title_source /
    title_meta were already persisted (no async title daemon dependency).
    """
    hermes_home = tmp_path / "hermes-home"
    _write_temp_hermes_home(hermes_home, base_url=fake_openai_server)

    cmd = _resolve_hermes_cmd()
    cmd.extend(
        [
            "chat",
            "-Q",
            "--cli",
            "--source",
            "mcc-hermes",
            "--title-project",
            "MCC",
            "--title-area",
            "Agent Sessions",
            "-q",
            "Confirm the launch title seed is persisted.",
            "--provider",
            "fake-title-local",
            "--model",
            "fake-title-model",
            "--yolo",
            "--ignore-rules",
            "--toolsets",
            "todo",
        ]
    )

    result = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=_scrubbed_env(hermes_home),
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )

    assert result.returncode == 0, (
        "hermes chat -Q oneshot failed\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )

    # Process has exited — reopen the DB from disk and assert durable seed.
    db = SessionDB(hermes_home / "state.db")
    try:
        sessions = [
            row
            for row in db.list_sessions_rich(limit=20, source="mcc-hermes")
            if (row.get("source") or "") == "mcc-hermes"
        ]
        if not sessions:
            # Fallback for stores that ignore source filters in list helpers.
            with db._lock:
                rows = db._conn.execute(
                    "SELECT id, title, title_source, title_meta, source "
                    "FROM sessions WHERE source = ? ORDER BY started_at DESC",
                    ("mcc-hermes",),
                ).fetchall()
            sessions = [dict(r) for r in rows]

        assert sessions, (
            "expected an mcc-hermes session row after process exit\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        row = sessions[0]
        session_id = row["id"]
        assert db.get_session_title(session_id) == "MCC - Agent Sessions"
        assert db.get_session_title_source(session_id) == "llm"
        meta = db.get_session_title_meta(session_id)
        assert meta is not None
        assert meta["project"] == "MCC"
        assert meta["area"] == "Agent Sessions"
        assert meta.get("project_owned") is True
        assert not meta.get("executor")
        assert not meta.get("model")
    finally:
        db.close()
