"""Python project tooling. Install with durable-actors[cli]."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from importlib.metadata import distribution, version
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import httpx
from dotenv import load_dotenv

from .client import Client
from .codegen import generate_client
from .inspect import run_inspect


@click.group()
@click.version_option(None, "-V", "--version", package_name="durable-actors")
def cli() -> None:
    """Develop and inspect Python durable actors."""
    # Explicit process variables win; local settings override shared settings.
    load_dotenv(Path.cwd() / ".env.local")
    load_dotenv(Path.cwd() / ".env")


@cli.command()
@click.option("--watch/--no-watch", default=True, help="Reload source changes.")
@click.option(
    "--port", type=click.IntRange(0, 65535), help="Local server port; 0 selects a free port."
)
@click.option("--project", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--entrypoint", help="Actor source path relative to the project.")
@click.option("--data-dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--storage", type=click.Choice(["local", "gcs"]))
def dev(
    watch: bool,
    port: int | None,
    project: Path | None,
    entrypoint: str | None,
    data_dir: Path | None,
    storage: str | None,
) -> None:
    """Run local actors and reload source changes."""
    arguments = ["dev"]
    for flag, value in (
        ("port", port),
        ("project", project),
        ("entrypoint", entrypoint),
        ("data-dir", data_dir),
        ("storage", storage),
    ):
        if value is not None:
            arguments.extend([f"--{flag}", str(value)])
    if watch:
        arguments.append("--watch")
    execute_runtime(arguments)


@cli.command()
def start() -> None:
    """Run the control plane using its environment settings."""
    execute_runtime([])


@cli.command()
@click.option("--open/--no-open", "open_browser", default=True, help="Open a browser on startup.")
@click.option("--control-plane-url", help="Override the configured control plane URL.")
def observe(control_plane_url: str | None, open_browser: bool) -> None:
    """Open the actor observer."""
    from .observer import observe as serve_observer

    serve_observer(control_plane_url, open_browser=open_browser)


@cli.command("inspect")
@click.argument("selector", required=False, metavar="ActorName/actorId")
@click.option("--json", "json_output", is_flag=True, help="Print machine-readable JSON.")
@click.option("--control-plane-url", help="Override the configured control plane URL.")
def inspect(selector: str | None, json_output: bool, control_plane_url: str | None) -> None:
    """Print actor inventory and the latest persisted state."""
    text, warning = run_inspect(control_plane_url, selector, json_output)
    if warning:
        click.echo(warning, err=True)
    click.echo(text)


def main() -> None:
    try:
        cli(prog_name="durable-actors")
    except (ImportError, ModuleNotFoundError) as error:
        click.ClickException(
            f"{error}. Install durable-actors[cli] in this Python environment."
        ).show()
        raise SystemExit(1) from error
    except subprocess.CalledProcessError as error:
        click.ClickException(error.stderr or error.stdout or str(error)).show()
        raise SystemExit(1) from error
    except (ValueError, OSError, RuntimeError, httpx.HTTPError) as error:
        click.ClickException(str(error)).show()
        raise SystemExit(1) from error


def native_executable() -> Path:
    override = os.environ.get("DURABLE_ACTORS_BINARY")
    if override:
        return Path(override).absolute()
    runtime = distribution("durable-actors-runtime")
    if runtime.version != version("durable-actors"):
        raise RuntimeError("durable-actors and durable-actors-runtime versions must match")
    for file in runtime.files or []:
        if file.name == "durable-actors-native":
            return Path(str(runtime.locate_file(file)))
    raise RuntimeError("Native runtime is missing. Reinstall durable-actors[cli].")


def execute_runtime(arguments: list[str]) -> None:
    executable = native_executable()
    environment = {**os.environ, "DURABLE_ACTORS_PYTHON": sys.executable}
    environment.setdefault("DURABLE_ACTORS_PROCESS_ROLE", "control_plane")
    if "DURABLE_ACTORS_SECRET" not in environment and "DURABLE_ACTORS_API_KEY" in environment:
        environment["DURABLE_ACTORS_SECRET"] = environment["DURABLE_ACTORS_API_KEY"]
    # Replace the launcher so the native runtime owns signals, hosts and reloads.
    os.execve(executable, [str(executable), *arguments], environment)


@cli.command("init")
@click.argument("directory", type=click.Path(file_okay=False, path_type=Path))
def initialize(directory: Path) -> None:
    """Create a Python actor project in DIRECTORY."""
    directory = directory.absolute()
    directory.mkdir(parents=True)  # Refuse existing directories, including empty ones.
    try:
        shutil.copytree(
            Path(__file__).with_name("template"),
            directory,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        manifest = directory / "pyproject.toml"
        name = re.sub(r"[^a-z0-9]+", "-", directory.name.lower()).strip("-") or "actors"
        manifest.write_text(
            manifest.read_text()
            .replace("PROJECT_NAME", name)
            .replace("SDK_VERSION", version("durable-actors"))
        )
        (directory / "gitignore").rename(directory / ".gitignore")
        for agent in (".agents", ".claude"):
            shutil.copytree(Path(__file__).with_name("skills"), directory / agent / "skills")
    except BaseException:
        shutil.rmtree(directory)
        raise
    click.echo(
        f"Created Python actors in {directory}.\n\n  cd {shlex.quote(str(directory))}\n  uv run durable-actors dev\n\nEdit src/actors.py. Generate clients with: uv run durable-actors generate"
    )


@cli.command()
@click.argument("entrypoint", required=False, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--out-dir",
    "output",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path("generated"),
    show_default=True,
)
@click.option("--control-plane-url", "origin", help="Override the configured control plane URL.")
def generate(entrypoint: str | None, output: Path, origin: str | None) -> None:
    """Generate typed clients from ENTRYPOINT or the running server."""
    if entrypoint and origin:
        raise click.UsageError("--control-plane-url cannot be combined with a source entrypoint")
    if entrypoint:
        with TemporaryDirectory(prefix="durable-actors-build-") as temporary:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "durable_actors.build",
                    str(Path.cwd()),
                    entrypoint,
                    temporary,
                    "local",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            contract = json.loads(result.stdout)
    else:
        with Client(origin) as client:
            contract = client.get_contract()
    generate_client(contract, output.resolve())
    from .build import check_actor

    check_actor(Path.cwd(), str(output.resolve()))
    click.echo(f"Generated {len(contract['actors'])} actor contract(s) in {output.resolve()}.")


if __name__ == "__main__":
    main()
