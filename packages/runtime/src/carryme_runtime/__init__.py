"""Shared runtime services for carryme."""

from carryme_connectors import ConnectorError

from carryme_runtime.candidates import filter_candidate_records
from carryme_runtime.execution import ExecutionAdapter, MockExecutionAdapter
from carryme_runtime.intents import build_trade_intent
from carryme_runtime.opportunities import OpportunityService, fetch_live_snapshot
from carryme_runtime.preflight import (
    LiveExecutionConfigMap,
    build_paper_trade_execution_preflight,
    build_venue_execution_preflights,
)

__all__ = [
    "ExecutionAdapter",
    "LiveExecutionConfigMap",
    "MockExecutionAdapter",
    "build_paper_trade_execution_preflight",
    "build_trade_intent",
    "build_venue_execution_preflights",
    "ConnectorError",
    "OpportunityService",
    "fetch_live_snapshot",
    "filter_candidate_records",
]
