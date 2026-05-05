"""Helpers for sink wrappers."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

_LOG = logging.getLogger(__name__)


def inner_name(sink: Any) -> str:
    return str(getattr(sink, "name", type(sink).__name__))


def open_all(sinks: Iterable[Any]) -> None:
    opened: list[Any] = []
    try:
        for sink in sinks:
            sink.open()
            opened.append(sink)
    except BaseException:
        for sink in reversed(opened):
            try:
                sink.close()
            except Exception:
                _LOG.exception("error closing sink %r during rollback", inner_name(sink))
        raise


def close_all(sinks: Iterable[Any]) -> None:
    for sink in reversed(list(sinks)):
        try:
            sink.close()
        except Exception:
            _LOG.exception("error closing sink %r", inner_name(sink))
