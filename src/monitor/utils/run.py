"""Run utilities for the monitor package."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def run_with_tee(
    args: str | Sequence[str],
    *,
    text: bool = False,
    input: str | bytes | None = None,
    check: bool = False,
    timeout: float | None = None,
    shell: bool = False,
    env: dict[str, str] | None = None,
    cwd: str | Path | None = None,
    capture_output: bool = True,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    """Run command, streaming output to stdout/stderr while capturing it.

    Like subprocess.run but tees output to console while capturing.
    """
    # Set up pipes for capturing
    stdout_data = []
    stderr_data = []

    popen_kwargs = {
        "stdin": subprocess.PIPE if input is not None else None,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
        "text": text,
        "env": env,
        "cwd": cwd,
        "shell": shell,
        **kwargs,
    }

    proc = subprocess.Popen(args, **popen_kwargs)

    try:
        if input is not None:
            if text and isinstance(input, str):
                input_bytes = input.encode() if not text else input
            else:
                input_bytes = input
            stdout, stderr = proc.communicate(input=input_bytes, timeout=timeout)
        else:
            stdout, stderr = proc.communicate(timeout=timeout)

        # Tee output to console
        if stdout:
            if text:
                sys.stdout.write(stdout)
            else:
                sys.stdout.buffer.write(stdout)
            sys.stdout.flush()

        if stderr:
            if text:
                sys.stderr.write(stderr)
            else:
                sys.stderr.buffer.write(stderr)
            sys.stderr.flush()

    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        raise

    result = subprocess.CompletedProcess(
        args=args,
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
    )

    if check and proc.returncode != 0:
        raise subprocess.CalledProcessError(
            proc.returncode, args, output=stdout, stderr=stderr
        )

    return result


def run_with_log(
    args: Sequence[str],
    log_path: str | Path,
    *,
    env: dict[str, str] | None = None,
    cwd: str | Path | None = None,
) -> int:
    """Run command, capturing output to log file while streaming to stdout.

    This is the original run_with_tee behavior for backwards compatibility.
    """
    import os
    import pty
    import select

    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    master_fd, slave_fd = pty.openpty()

    process = subprocess.Popen(
        args,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        env=env,
        cwd=cwd,
        close_fds=True,
    )

    os.close(slave_fd)

    with log_path.open("w", encoding="utf-8") as f:
        try:
            while process.poll() is None:
                r, _, _ = select.select([master_fd], [], [], 0.1)
                if master_fd in r:
                    try:
                        data = os.read(master_fd, 8192).decode("utf-8", errors="replace")
                        if data:
                            sys.stdout.write(data)
                            sys.stdout.flush()
                            f.write(data)
                            f.flush()
                    except OSError:  # pragma: no cover
                        break
        finally:
            # Read remaining output
            while True:
                r, _, _ = select.select([master_fd], [], [], 0)
                if master_fd not in r:
                    break
                try:
                    data = os.read(master_fd, 8192).decode("utf-8", errors="replace")
                    if not data:  # pragma: no cover
                        break
                    sys.stdout.write(data)
                    sys.stdout.flush()
                    f.write(data)
                    f.flush()
                except OSError:  # pragma: no cover
                    break
            os.close(master_fd)

    return process.wait()
