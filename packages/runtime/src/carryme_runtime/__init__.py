"""Shared runtime services for carryme."""

from carryme_connectors import ConnectorError

from carryme_runtime.candidates import filter_candidate_records
from carryme_runtime.confirmation import require_confirmed_preview
from carryme_runtime.execution import ExecutionAdapter, MockExecutionAdapter
from carryme_runtime.intents import InvalidTradeCandidateError, build_trade_intent
from carryme_runtime.opportunities import OpportunityService, UpstreamDataError, fetch_live_snapshot
from carryme_runtime.order_preview import OrderPreviewService
from carryme_runtime.preflight import (
    LiveExecutionConfigMap,
    build_live_execution_configs,
    build_paper_trade_execution_preflight,
    build_venue_execution_preflights,
)

__all__ = [
    "ExecutionAdapter",
    "LiveExecutionConfigMap",
    "MockExecutionAdapter",
    "OrderPreviewService",
    "build_live_execution_configs",
    "build_paper_trade_execution_preflight",
    "build_trade_intent",
    "build_venue_execution_preflights",
    "ConnectorError",
    "InvalidTradeCandidateError",
    "OpportunityService",
    "UpstreamDataError",
    "fetch_live_snapshot",
    "filter_candidate_records",
    "require_confirmed_preview",
]
