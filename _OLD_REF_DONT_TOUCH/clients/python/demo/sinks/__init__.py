"""User-defined CDC sinks for the demo consumer."""

from .dashboard import DemoDashboard, DemoSink
from .stats import StatsSink

__all__ = ["DemoDashboard", "DemoSink", "StatsSink"]
