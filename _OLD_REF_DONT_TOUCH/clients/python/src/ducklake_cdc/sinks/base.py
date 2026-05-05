"""Base sink protocols and convenience classes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from ducklake_cdc.types import DDLBatch, DDLTickBatch, DMLBatch, DMLTickBatch, SinkContext

SinkBatchKind = Literal["dml_changes", "ddl_changes", "dml_ticks", "ddl_ticks", "any"]


@runtime_checkable
class DMLSink(Protocol):
    """Protocol for a sink that consumes DML batches."""

    name: str
    require_ack: bool

    def open(self) -> None: ...

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class DDLSink(Protocol):
    """Protocol for a sink that consumes DDL batches."""

    name: str
    require_ack: bool

    def open(self) -> None: ...

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class DMLTickSink(Protocol):
    """Protocol for a sink that consumes DML tick batches."""

    name: str
    require_ack: bool

    def open(self) -> None: ...

    def write(self, batch: DMLTickBatch, ctx: SinkContext) -> None: ...

    def close(self) -> None: ...


@runtime_checkable
class DDLTickSink(Protocol):
    """Protocol for a sink that consumes DDL tick batches."""

    name: str
    require_ack: bool

    def open(self) -> None: ...

    def write(self, batch: DDLTickBatch, ctx: SinkContext) -> None: ...

    def close(self) -> None: ...


class BaseDMLSink:
    """Convenience base class for DML sinks."""

    name: str = "sink"
    require_ack: bool = True
    batch_kind: SinkBatchKind = "dml_changes"

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, batch: DMLBatch, ctx: SinkContext) -> None:
        raise NotImplementedError(f"sink {self.name!r} must implement write(batch, ctx)")


class BaseDDLSink:
    """Convenience base class for DDL sinks."""

    name: str = "sink"
    require_ack: bool = True
    batch_kind: SinkBatchKind = "ddl_changes"

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, batch: DDLBatch, ctx: SinkContext) -> None:
        raise NotImplementedError(f"sink {self.name!r} must implement write(batch, ctx)")


class BaseDMLTickSink:
    """Convenience base class for DML tick sinks."""

    name: str = "sink"
    require_ack: bool = True
    batch_kind: SinkBatchKind = "dml_ticks"

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, batch: DMLTickBatch, ctx: SinkContext) -> None:
        raise NotImplementedError(f"sink {self.name!r} must implement write(batch, ctx)")


class BaseDDLTickSink:
    """Convenience base class for DDL tick sinks."""

    name: str = "sink"
    require_ack: bool = True
    batch_kind: SinkBatchKind = "ddl_ticks"

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def write(self, batch: DDLTickBatch, ctx: SinkContext) -> None:
        raise NotImplementedError(f"sink {self.name!r} must implement write(batch, ctx)")
