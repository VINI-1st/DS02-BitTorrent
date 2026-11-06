"""Cross-platform subprocess helpers for the simulator (C1/C2/C3).

Why this exists
---------------
* On Windows ``Popen.terminate()`` is ``TerminateProcess`` – an abrupt kill, so
  a peer could never log ``stopped``. A *graceful* leave therefore needs a
  different mechanism per OS:

  - POSIX: ``SIGTERM``.
  - Windows: the child is started in its own process group
    (``CREATE_NEW_PROCESS_GROUP``) and receives ``CTRL_BREAK_EVENT``
    (``signal.SIGBREAK`` in Python).
  - Both: a *stop file* (``<dir>/.sim_stop``) is touched first. The stub
    processes poll for it, so a graceful stop works even when the launcher has
    no console attached (IDE, CI, scheduled task) and console signals fail.

* A *crash* is always ``Popen.kill()`` (SIGKILL / TerminateProcess).

Entry points can be a script path (``src/sim/stub_peer.py``) or a module
(``-m peer`` / ``module:peer``) because Role B's contract runs ``python -m peer``.
"""

from __future__ import annotations

import os
import shlex
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Sequence

from src.sim.common import REPO_ROOT

IS_WINDOWS = os.name == "nt"
STOP_FILE_NAME = ".sim_stop"


@dataclass(frozen=True)
class Entrypoint:
    """A Python program the simulator can launch: a script file or a module."""

    kind: str  # "script" | "module"
    target: str

    @classmethod
    def parse(cls, spec: str) -> "Entrypoint":
        spec = spec.strip()
        if spec.startswith("-m "):
            return cls("module", spec[3:].strip())
        if spec.startswith("module:"):
            return cls("module", spec[len("module:"):].strip())
        p = Path(spec)
        if spec.endswith(".py") or p.exists() or "/" in spec or "\\" in spec:
            if not p.is_absolute():
                p = (REPO_ROOT / p) if (REPO_ROOT / p).exists() else p.resolve()
            return cls("script", str(p))
        return cls("module", spec)

    def argv(self) -> list[str]:
        if self.kind == "module":
            return [sys.executable, "-u", "-m", self.target]
        return [sys.executable, "-u", self.target]

    @property
    def is_stub_peer(self) -> bool:
        return self.target.replace("\\", "/").endswith("stub_peer.py") or self.target.endswith("stub_peer")

    @property
    def is_stub_tracker(self) -> bool:
        return self.target.replace("\\", "/").endswith("stub_tracker.py") or self.target.endswith("stub_tracker")

    def __str__(self) -> str:
        return f"-m {self.target}" if self.kind == "module" else self.target


def child_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Environment for children: UTF-8 I/O, unbuffered, repo root + src on PYTHONPATH."""
    env = dict(os.environ)
    paths = [str(REPO_ROOT), str(REPO_ROOT / "src")]
    if env.get("PYTHONPATH"):
        paths.append(env["PYTHONPATH"])
    env["PYTHONPATH"] = os.pathsep.join(paths)
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    env["PYTHONUNBUFFERED"] = "1"
    if extra:
        env.update(extra)
    return env


def spawn(
    argv: Sequence[str],
    cwd: str | Path | None = None,
    stdout: int | IO | None = subprocess.PIPE,
    stderr: int | IO | None = None,
) -> subprocess.Popen:
    """Start a child process ready for graceful stop on any OS."""
    kwargs: dict = {}
    if IS_WINDOWS:
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
    return subprocess.Popen(
        list(argv),
        cwd=str(cwd) if cwd else str(REPO_ROOT),
        stdout=stdout,
        stderr=stderr,
        stdin=subprocess.DEVNULL,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        env=child_env(),
        **kwargs,
    )


def touch_stop_file(directory: str | Path | None) -> None:
    if not directory:
        return
    try:
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)
        (d / STOP_FILE_NAME).write_text(str(time.time()), encoding="utf-8")
    except OSError:
        pass


def send_graceful_signal(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    try:
        if IS_WINDOWS:
            proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
        else:
            proc.send_signal(signal.SIGTERM)
    except (OSError, ValueError):
        pass


def graceful_stop(
    proc: subprocess.Popen, stop_dir: str | Path | None = None, timeout: float = 4.0
) -> str:
    """Ask a process to leave gracefully; kill it if it does not exit in time.

    Returns ``"already_exited"``, ``"graceful"`` or ``"killed"``.
    """
    if proc.poll() is not None:
        return "already_exited"
    touch_stop_file(stop_dir)
    send_graceful_signal(proc)
    try:
        proc.wait(timeout=timeout)
        return "graceful"
    except subprocess.TimeoutExpired:
        hard_kill(proc)
        return "killed"


def hard_kill(proc: subprocess.Popen, timeout: float = 3.0) -> None:
    """Abrupt crash (SIGKILL / TerminateProcess)."""
    if proc.poll() is not None:
        return
    try:
        proc.kill()
    except OSError:
        pass
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pass


def is_port_free(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if not IS_WINDOWS:
            # same option the peers use: ignore TIME_WAIT leftovers of the previous run
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
            return True
        except OSError:
            return False


def find_free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


def wait_for_port(host: str, port: int, timeout: float = 6.0, proc: subprocess.Popen | None = None) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc is not None and proc.poll() is not None:
            return False
        try:
            with socket.create_connection((host, port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def split_args(extra: str | None) -> list[str]:
    if not extra:
        return []
    return shlex.split(extra, posix=not IS_WINDOWS)
