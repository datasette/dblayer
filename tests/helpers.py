"""Test helpers."""

from __future__ import annotations

import os


def count_open_fds() -> int:
    """Number of open file descriptors in this process.

    Counts entries in ``/proc/self/fd`` where it exists (Linux) and otherwise
    ``/dev/fd`` (macOS). Listing the directory itself opens one descriptor;
    that is the same on every call, so differences between calls are exact.
    """
    for path in ("/proc/self/fd", "/dev/fd"):
        if os.path.isdir(path):
            return len(os.listdir(path))
    raise RuntimeError("no /proc/self/fd or /dev/fd on this platform")
