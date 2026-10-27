#!/usr/bin/env python3
"""Reap this invocation's CLI descendants even when container PID 1 is not an init."""
import ctypes
import os
import signal
import subprocess
import sys
import time


def has_children():
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return False
        if pid == 0:
            return True


def cleanup(group):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 2
        while has_children() and time.monotonic() < deadline:
            time.sleep(0.025)
        if not has_children():
            return
    raise RuntimeError("Owned descendants did not drain; retain occupancy")


def main():
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "Cannot establish owned CLI subreaper")
    interrupted = False

    def interrupt(_signal, _frame):
        nonlocal interrupted
        interrupted = True

    signal.signal(signal.SIGTERM, interrupt)
    signal.signal(signal.SIGINT, interrupt)
    timeout = int(sys.argv[1]) / 1000
    if timeout <= 0:
        raise ValueError("A positive deadline is required")
    child = subprocess.Popen(
        sys.argv[2:], start_new_session=True,
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    print(child.pid, flush=True)
    try:
        deadline = time.monotonic() + timeout
        while child.poll() is None and not interrupted and time.monotonic() < deadline:
            time.sleep(0.025)
        status = child.poll()
        descendants = has_children() if status is not None else True
        return 0 if status == 0 and not interrupted and not descendants else 1
    finally:
        cleanup(child.pid)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"Owned process supervision failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(2)
