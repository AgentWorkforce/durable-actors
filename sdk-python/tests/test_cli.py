"""Installed CLI acceptance tests; every child runs without JavaScript on PATH."""

import json
import os
import re
import shutil
import subprocess
import sys
import time
import tomllib
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path

import httpx
import pytest

from durable_actors.cli import native_executable
from durable_actors.client import ActorInvocationError, Client

SOURCE = """from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
    def label(self) -> str:
        return "first"
"""


@pytest.fixture
def cli_env(tmp_path):
    environment = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("DURABLE_ACTORS_") and k not in {"PYTHONPATH", "PYTHONHOME"}
    }
    empty = tmp_path / "empty-path"
    empty.mkdir()
    environment["PATH"] = str(empty)
    assert shutil.which("bun", path=environment["PATH"]) is None
    assert shutil.which("node", path=environment["PATH"]) is None
    if runtime := os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"):
        environment["DURABLE_ACTORS_BINARY"] = runtime
    environment["DURABLE_ACTORS_SECRET"] = "cli-test-key"
    return environment


def cli(project, environment, *arguments, check=True):
    result = subprocess.run(
        [sys.executable, "-m", "durable_actors", *arguments],
        cwd=project,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if check:
        assert result.returncode == 0, result.stdout + result.stderr
    return result


def require_runtime():
    if os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"):
        return
    try:
        assert native_executable().is_file()
    except ImportError:
        if os.environ.get("DURABLE_ACTORS_TEST_WHEEL"):
            raise
        pytest.skip("requires native runtime wheel or DURABLE_ACTORS_TEST_RUNTIME")


@contextmanager
def process(project, environment, *arguments):
    log = project / f"{arguments[0]}.log"
    with log.open("w") as output:
        child = subprocess.Popen(
            [sys.executable, "-m", "durable_actors", *arguments],
            cwd=project,
            env=environment,
            stdout=output,
            stderr=output,
        )
        try:
            yield child, log
        finally:
            child.terminate()
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
                pytest.fail(f"CLI failed to shut down: {log.read_text()}")


def eventually(read, child, log, *, timeout=45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        assert child.poll() is None, log.read_text()
        try:
            result = read()
            if result:
                return result
        except (httpx.HTTPError, ValueError) as error:
            last = error
        except ActorInvocationError as error:
            # Reload briefly withdraws the old host before publishing its replacement.
            if error.code != "unavailable":
                raise
            last = error
        time.sleep(0.1)
    pytest.fail(f"Timed out: {last}\n{log.read_text()}")


def runtime_origin(log):
    match = re.search(
        r"DURABLE_ACTORS_CONTROL_PLANE_URL=(http://127\.0\.0\.1:\d+)", log.read_text()
    )
    return match.group(1) if match else None


def test_init_and_local_generation_need_only_python(tmp_path, cli_env):
    assert version("durable-actors") in cli(tmp_path, cli_env, "--version").stdout
    assert "--no-watch" in cli(tmp_path, cli_env, "dev", "--help").stdout
    invalid_port = cli(tmp_path, cli_env, "dev", "--port", "65536", check=False)
    assert invalid_port.returncode == 2
    assert "--port" in invalid_port.stderr
    assert "Traceback" not in invalid_port.stderr
    cli(tmp_path, cli_env, "init", "counter")
    project = tmp_path / "counter"
    metadata = tomllib.loads((project / "pyproject.toml").read_text())
    assert metadata["dependency-groups"]["dev"] == [
        f"durable-actors[cli]=={version('durable-actors')}"
    ]
    assert metadata["project"]["dependencies"] == [f"durable-actors=={version('durable-actors')}"]
    assert not (project / "package.json").exists()
    assert not (project / "bunfig.toml").exists()
    assert not (project / "node_modules").exists()
    assert (project / ".gitignore").is_file()
    # Wheels cannot include files outside sdk-python, so the package keeps a copy of the root skill.
    skill = Path(__file__).parents[2] / "skills/durable-actors-quickstart/SKILL.md"
    for agent in (".agents", ".claude"):
        installed = project / agent / "skills/durable-actors-quickstart/SKILL.md"
        assert installed.read_text() == skill.read_text()
    existing = cli(tmp_path, cli_env, "init", "counter", check=False)
    assert existing.returncode != 0
    assert "Traceback" not in existing.stderr
    conflicting_source = cli(
        project,
        cli_env,
        "generate",
        "src/actors.py",
        "--control-plane-url",
        "http://localhost",
        check=False,
    )
    assert conflicting_source.returncode == 2
    assert "cannot be combined" in conflicting_source.stderr
    cli(project, cli_env, "generate", "src/actors.py")
    assert (project / "generated/py.typed").is_file()
    (project / "src/actors.py").write_text(SOURCE.replace("return self.count", 'return "wrong"'))
    invalid = cli(project, cli_env, "generate", "src/actors.py", check=False)
    assert invalid.returncode != 0
    assert "Incompatible return value" in invalid.stderr


def test_dev_reload_last_good_build_restart_and_remote_generate(tmp_path, cli_env):
    require_runtime()
    cli(tmp_path, cli_env, "init", "counter")
    project = tmp_path / "counter"
    source = project / "src/actors.py"
    source.write_text(SOURCE)
    for restart in (False, True):
        with process(project, cli_env, "dev", "--port", "0") as (child, log):
            origin = eventually(lambda: runtime_origin(log), child, log)
            with Client(origin, api_key="cli-test-key") as client:
                assert client.invoke("Counter", "one", "increment", [0 if restart else 2]) == 2
                if restart:
                    continue
                source.write_text(SOURCE.replace("return self.count", 'return "wrong"'))
                eventually(lambda: "Actor source update failed" in log.read_text(), child, log)
                assert client.invoke("Counter", "one", "increment", [0]) == 2
                source.write_text(SOURCE.replace('return "first"', 'return "second"'))
                eventually(
                    lambda: client.invoke("Counter", "one", "label", []) == "second", child, log
                )
                assert client.invoke("Counter", "one", "increment", [0]) == 2
                # API generation must not compile local source.
                source.write_text("invalid python source")
                cli(project, {**cli_env, "DURABLE_ACTORS_CONTROL_PLANE_URL": origin}, "generate")
                assert (project / "generated/_counter.py").is_file()
                # .env.local beats .env; explicit process settings beat both.
                (project / ".env").write_text(
                    "DURABLE_ACTORS_CONTROL_PLANE_URL=http://unreachable.invalid\n"
                )
                (project / ".env.local").write_text(f"DURABLE_ACTORS_CONTROL_PLANE_URL={origin}\n")
                cli(project, cli_env, "generate")
                (project / ".env.local").write_text(
                    "DURABLE_ACTORS_CONTROL_PLANE_URL=http://unreachable.invalid\n"
                )
                cli(project, {**cli_env, "DURABLE_ACTORS_CONTROL_PLANE_URL": origin}, "generate")
                (project / ".env").unlink()
                (project / ".env.local").unlink()
                source.write_text(SOURCE)


def test_no_watch_keeps_running_code(tmp_path, cli_env):
    require_runtime()
    (tmp_path / "actors.py").write_text(SOURCE)
    with process(tmp_path, cli_env, "dev", "--port", "0", "--no-watch") as (child, log):
        origin = eventually(lambda: runtime_origin(log), child, log)
        with Client(origin, api_key="cli-test-key") as client:
            assert client.invoke("Counter", "one", "label", []) == "first"
            (tmp_path / "actors.py").write_text(SOURCE.replace('return "first"', 'return "second"'))
            time.sleep(1)
            assert client.invoke("Counter", "one", "label", []) == "first"


def test_observer_assets_proxy_streaming_and_browser_boundaries(tmp_path, cli_env):
    require_runtime()
    from importlib.resources import files

    try:
        assert files("durable_actors_runtime").joinpath("observer/index.html").is_file()
        assert (
            files("durable_actors_runtime").joinpath("observer/THIRD-PARTY-LICENSES.md").is_file()
        )
    except ImportError:
        if os.environ.get("DURABLE_ACTORS_TEST_WHEEL"):
            raise
        pytest.skip("requires packaged observer assets")
    (tmp_path / "actors.py").write_text(SOURCE)
    with process(tmp_path, cli_env, "dev", "--port", "0") as (runtime, runtime_log):
        origin = eventually(lambda: runtime_origin(runtime_log), runtime, runtime_log)
        with process(
            tmp_path,
            {**cli_env, "DURABLE_ACTORS_CONTROL_PLANE_URL": origin},
            "observe",
            "--no-open",
        ) as (observer, log):
            match = eventually(
                lambda: re.search(r"Observe: (http://127\.0\.0\.1:\d+)", log.read_text()),
                observer,
                log,
            )
            ui = match.group(1)
            eventually(lambda: httpx.get(ui).status_code == 200, observer, log)
            with httpx.Client(base_url=ui, timeout=10) as http:
                page = http.get("/")
                assert "<html" in page.text
                assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
                assert http.get("/app.js").status_code == 200
                assert http.get("/api/observe/connection").json() == {"connected": True}
                assert http.get("/api/observe/actors").status_code == 200
                assert http.get("/api/observe/residency?actorName=Counter").json() == {
                    "periods": [],
                    "truncated": False,
                }
                assert http.get("/", headers={"host": "evil.example"}).status_code == 403
                assert http.get("/", headers={"origin": "https://evil.example"}).status_code == 403
                assert http.get("/", headers={"sec-fetch-site": "cross-site"}).status_code == 403
                assert http.post("/api/observe/actors").status_code == 405
                assert http.get("/api/observe/../../deployment").status_code == 404
                assert http.get("/%2e%2e/pyproject.toml").status_code == 404
                assert (
                    http.head("/api/observe/events")
                    .headers["content-type"]
                    .startswith("text/event-stream")
                )
                with http.stream("GET", "/api/observe/events") as stream:
                    assert stream.status_code == 200
                    assert next(stream.iter_bytes())
                runtime.terminate()
                runtime.wait(timeout=15)
                assert http.get("/api/observe/connection").status_code == 503


def test_start_uses_configured_binary_interpreter_and_exit_status(tmp_path, cli_env):
    executable = tmp_path / "native"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "print(json.dumps({key: os.environ.get(key) for key in ['DURABLE_ACTORS_PYTHON', 'DURABLE_ACTORS_PROCESS_ROLE', 'DURABLE_ACTORS_SECRET']}))\n"
        "sys.exit(7)\n"
    )
    executable.chmod(0o755)
    environment = {
        **cli_env,
        "DURABLE_ACTORS_BINARY": str(executable),
        "DURABLE_ACTORS_API_KEY": "alias-key",
    }
    del environment["DURABLE_ACTORS_SECRET"]
    result = cli(tmp_path, environment, "start", check=False)
    assert result.returncode == 7
    assert json.loads(result.stdout) == {
        "DURABLE_ACTORS_PYTHON": sys.executable,
        "DURABLE_ACTORS_PROCESS_ROLE": "control_plane",
        "DURABLE_ACTORS_SECRET": "alias-key",
    }
