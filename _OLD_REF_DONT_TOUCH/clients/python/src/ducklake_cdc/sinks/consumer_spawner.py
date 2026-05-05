"""Spawner sink for dynamic CDCApp topologies."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable
from typing import Any

from ducklake_cdc.app import CDCApp, Consumer
from ducklake_cdc.consumers import DDLConsumer, DMLConsumer
from ducklake_cdc.types import SinkContext

SpawnerHook = Callable[..., Consumer | Iterable[Consumer] | None]


class ConsumerSpawner:
    """Attach to any consumer and add hook-returned consumers to a :class:`CDCApp`."""

    name = "consumer_spawner"
    require_ack = True
    batch_kind = "any"

    def __init__(
        self,
        *,
        app: CDCApp,
        on_event: SpawnerHook,
        name: str | None = None,
        require_ack: bool = True,
    ) -> None:
        if not callable(on_event):
            raise TypeError("ConsumerSpawner expects a callable on_event hook")
        self._app = app
        self._on_event = on_event
        self._call_shape = _hook_call_shape(on_event)
        self.name = name or self.name
        self.require_ack = require_ack

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, batch: Any, ctx: SinkContext) -> None:
        for item in batch:
            result = self._call_hook(item, batch, ctx)
            for consumer in _iter_consumers(result):
                self._app.add_consumer(consumer)

    def _call_hook(self, item: Any, batch: Any, ctx: SinkContext) -> Any:
        if self._call_shape == 1:
            return self._on_event(item)
        if self._call_shape == 2:
            return self._on_event(item, ctx)
        return self._on_event(item, batch, ctx)


def _iter_consumers(result: Consumer | Iterable[Consumer] | None) -> Iterable[Consumer]:
    if result is None:
        return ()
    if isinstance(result, (DMLConsumer, DDLConsumer)):
        return (result,)
    if isinstance(result, str | bytes):
        raise TypeError("ConsumerSpawner hook must return Consumer objects, not strings")
    return tuple(result)


def _hook_call_shape(fn: Callable[..., Any]) -> int:
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return 3

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

    required = [param for param in positional if param.default is inspect.Parameter.empty]
    if saw_var_positional:
        return 3
    if len(required) > 3:
        raise TypeError("ConsumerSpawner hook accepts at most (item, batch, ctx)")
    if len(positional) >= 3:
        return 3
    if len(positional) == 2:
        return 2
    if len(positional) == 1:
        return 1
    raise TypeError("ConsumerSpawner hook must accept at least the delivered item")
