"""``docs/worker-protocol.md`` is checked against the code: every message
type appears in its table, every worker handler is a documented message,
and the recorded session still matches what a real worker exchanges."""

from __future__ import annotations

import re

from dblayer.worker import __main__ as worker
from dblayer.worker.protocol import MESSAGE_TYPES

from . import protocol_session as ps

TEXT = ps.DOC.read_text()


def _table_names() -> set[str]:
    names = set()
    for line in TEXT.splitlines():
        if line.startswith("| `"):
            first = line.split("|")[1]
            names.update(re.findall(r"`(\w+)`", first))
    return names


def test_every_message_type_is_in_the_table():
    missing = set(MESSAGE_TYPES) - _table_names()
    assert not missing, f"undocumented message types: {missing}"


def test_every_worker_handler_is_a_message_type():
    handlers = {n[3:] for n in dir(worker.Server) if n.startswith("do_")}
    internal = {"close_stream", "debug"}  # cancel of a paused stream; --debug hooks
    assert handlers - internal <= set(MESSAGE_TYPES)
    requests = {t for t, kind in MESSAGE_TYPES.items() if kind == "request"}
    assert requests - {"hello"} <= handlers  # hello is answered by the reader


def _sections(text: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    current = "start"
    for line in text.splitlines():
        if line.startswith("# "):
            current = line[2:]
            continue
        out.setdefault(current, []).append(line)
    return {k: ps.shape("\n".join(v)) for k, v in out.items()}


def test_recorded_session_matches_a_fresh_recording():
    fresh = _sections(ps.generate())
    documented = _sections(ps.current_section())
    assert fresh.keys() == documented.keys()
    for name in fresh:
        if "deadline" in name:
            # Timing decides whether the core's own cancel goes out before
            # the worker's notice arrives; the notice itself must be there.
            assert "← expired" in fresh[name] and "← expired" in documented[name]
            continue
        assert fresh[name] == documented[name], name
