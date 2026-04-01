"""Shared database schema helpers for storage-backed services."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Protocol

from carryme_storage.alerts import CandidateAlertStore
from carryme_storage.approved_canaries import ApprovedCanaryStore
from carryme_storage.approved_canary_alerts import ApprovedCanaryAlertStore
from carryme_storage.balance_snapshots import BalanceSnapshotStore
from carryme_storage.canary_basket_launches import CanaryBasketLaunchStore
from carryme_storage.cleanup_preview_confirmations import CleanupPreviewConfirmationStore
from carryme_storage.execution_alerts import ExecutionAlertStore
from carryme_storage.execution_observations import ExecutionObservationStore
from carryme_storage.executions import ExecutionJournalStore
from carryme_storage.history import OpportunityHistoryStore
from carryme_storage.launch_ready_canaries import LaunchReadyCanaryStore
from carryme_storage.pair_close_preview_confirmations import (
    PairClosePreviewConfirmationStore,
)
from carryme_storage.paper_trades import PaperTradeStore
from carryme_storage.preview_confirmations import PreviewConfirmationStore
from carryme_storage.route_approvals import RouteApprovalStore
from carryme_storage.stable_canary_launches import StableCanaryLaunchStore
from carryme_storage.stable_launch_ready_alerts import StableLaunchReadyAlertStore
from carryme_storage.system_state_alerts import SystemStateAlertStore


class InitializableStore(Protocol):
    """Protocol for stores that can bootstrap their own schema."""

    def initialize(self) -> None: ...


StoreFactory = Callable[[str | Path], InitializableStore]

SCHEMA_TABLES: Sequence[str] = (
    "candidate_alert_events",
    "approved_canary_snapshots",
    "approved_canary_alert_events",
    "balance_snapshot_entries",
    "canary_basket_launch_records",
    "cleanup_preview_confirmation_entries",
    "execution_alert_events",
    "execution_observation_entries",
    "execution_journal_entries",
    "opportunity_history",
    "launch_ready_canary_snapshots",
    "pair_close_preview_confirmation_entries",
    "paper_trade_entries",
    "preview_confirmation_entries",
    "route_approval_entries",
    "stable_canary_launch_records",
    "stable_launch_ready_alert_events",
    "system_state_alert_events",
)

_STORE_FACTORIES: Sequence[StoreFactory] = (
    CandidateAlertStore,
    ApprovedCanaryStore,
    ApprovedCanaryAlertStore,
    BalanceSnapshotStore,
    CanaryBasketLaunchStore,
    CleanupPreviewConfirmationStore,
    ExecutionAlertStore,
    ExecutionObservationStore,
    ExecutionJournalStore,
    OpportunityHistoryStore,
    LaunchReadyCanaryStore,
    PairClosePreviewConfirmationStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    RouteApprovalStore,
    StableCanaryLaunchStore,
    StableLaunchReadyAlertStore,
    SystemStateAlertStore,
)


def initialize_database_schema(database: str | Path) -> None:
    """Create the storage schema on the provided database target."""

    for store_factory in _STORE_FACTORIES:
        store = store_factory(database)
        store.initialize()
