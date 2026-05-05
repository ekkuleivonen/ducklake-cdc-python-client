"""Small sink primitives for the high-level CDC client."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from ducklake_cdc_client.types import DDLBatch, DDLTickBatch, DMLBatch, DMLTickBatch, SinkContext

type Batch = DMLBatch | DDLBatch | DMLTickBatch | DDLTickBatch
type SinkLike = Sink | Callable[..., None]


@runtime_checkable
class Sink(Protocol):
    """A batch destination.

    Users do not need to inherit from this. Any object with ``write(batch, ctx)``
    works; ``open`` and ``close`` are optional lifecycle hooks. Plain callables
    are accepted by consumers and are wrapped with :func:`sink`.
    """

    def write(self, batch: Batch, ctx: SinkContext) -> None: ...


@dataclass
class _CallableSink:
    fn: Callable[..., None]
    name: str
    required: bool = True

    def __post_init__(self) -> None:
        self._wants_ctx = _callable_wants_context(self.fn)

    def write(self, batch: Batch, ctx: SinkContext) -> None:
        if self._wants_ctx:
            self.fn(batch, ctx)
        else:
            self.fn(batch)


def sink(
    fn: Callable[..., None],
    *,
    name: str | None = None,
    required: bool = True,
) -> Sink:
    """Wrap ``fn(batch)`` or ``fn(batch, ctx)`` as a sink."""

    if not callable(fn):
        raise TypeError("sink() expects a callable")
    return _CallableSink(fn=fn, name=name or _callable_name(fn), required=required)


def as_sink(value: SinkLike) -> Sink:
    if hasattr(value, "write"):
        return value  # type: ignore[return-value]
    if callable(value):
        return sink(value)
    raise TypeError("sink must be callable or expose write(batch, ctx)")


def open_sink(value: Sink) -> None:
    open_ = getattr(value, "open", None)
    if callable(open_):
        open_()


def close_sink(value: Sink) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()


def write_sink(value: Sink, batch: Batch, ctx: SinkContext) -> None:
    value.write(batch, ctx)


def sink_required(value: Sink) -> bool:
    return bool(getattr(value, "required", True))


def sink_name(value: object) -> str:
    name = getattr(value, "name", None)
    if isinstance(name, str) and name:
        return name
    return type(value).__name__


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
