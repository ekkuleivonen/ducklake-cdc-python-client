"""Callable sinks."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

from ducklake_cdc.sinks.base import BaseDDLSink, BaseDMLSink
from ducklake_cdc.types import DDLBatch, DMLBatch, SinkContext

DMLCallable = Callable[..., None]
DDLCallable = Callable[..., None]


class CallableDMLSink(BaseDMLSink):
    """Wrap a function ``(batch, ctx) -> None`` or ``(batch) -> None``."""

    require_ack = True

    def __init__(
        self,
        fn: DMLCallable,
        *,
        name: str | None = None,
        require_ack: bool = True,
    ) -> None:
        if not callable(fn):
            raise TypeError("CallableDMLSink expects a callable")
        self._fn = fn
        self._wants_ctx = _callable_wants_context(fn)
        self.name = name or _callable_name(fn)
        self.require_ack = require_ack

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        if self._wants_ctx:
            self._fn(batch, ctx)
        else:
            self._fn(batch)


class CallableDDLSink(BaseDDLSink):
    """Wrap a function ``(batch, ctx) -> None`` or ``(batch) -> None``."""

    require_ack = True

    def __init__(
        self,
        fn: DDLCallable,
        *,
        name: str | None = None,
        require_ack: bool = True,
    ) -> None:
        if not callable(fn):
            raise TypeError("CallableDDLSink expects a callable")
        self._fn = fn
        self._wants_ctx = _callable_wants_context(fn)
        self.name = name or _callable_name(fn)
        self.require_ack = require_ack

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        if self._wants_ctx:
            self._fn(batch, ctx)
        else:
            self._fn(batch)


def _callable_name(fn: Callable[..., Any]) -> str:
    name = getattr(fn, "__name__", None)
    if isinstance(name, str) and name and name != "<lambda>":
        return name
    return type(fn).__name__


def _callable_wants_context(fn: Callable[..., Any]) -> bool:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return True

    positional: list[inspect.Parameter] = []
    saw_var_positional = False
    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional.append(param)
        elif param.kind is inspect.Parameter.VAR_POSITIONAL:
            saw_var_positional = True

    required = [p for p in positional if p.default is inspect.Parameter.empty]

    if saw_var_positional:
        return True
    if len(required) > 2:
        raise TypeError(
            "callable sink takes at most 2 positional arguments (batch, ctx); "
            f"{_callable_name(fn)!r} requires {len(required)}"
        )
    if len(positional) >= 2:
        return True
    if len(positional) == 1:
        return False
    raise TypeError(
        "callable sink must accept (batch) or (batch, ctx); "
        f"{_callable_name(fn)!r} accepts no positional arguments"
    )
