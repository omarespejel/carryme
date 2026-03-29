"""Shared models for the carryme workspace."""

from carryme_models.confirmation import PreviewConfirmationEntry
from carryme_models.execution import ExecutionJournalEntry, ExecutionLegResult
from carryme_models.health import AppDescriptor, ServiceHealth
from carryme_models.history import (
    CandidateAlertEvent,
    FundingPairSpec,
    OpportunityRecord,
    WatchlistDocument,
)
from carryme_models.intent import FundingPairTradeIntent, PaperTradeEntry, TradeLegIntent
from carryme_models.market import MarketStats, TopOfBook
from carryme_models.normalization import (
    FundingRateNormalization,
    MarketIdentity,
    NormalizedMarketSnapshot,
    TradingFeeProfile,
)
from carryme_models.opportunity import CapacityEstimate, FundingArbOpportunity
from carryme_models.preflight import (
    CredentialRequirementStatus,
    PaperTradeExecutionPreflight,
    VenueExecutionPreflight,
)
from carryme_models.preview import PaperTradeOrderPreview, VenueOrderPreview

__all__ = [
    "AppDescriptor",
    "CapacityEstimate",
    "CandidateAlertEvent",
    "CredentialRequirementStatus",
    "ExecutionJournalEntry",
    "ExecutionLegResult",
    "FundingPairSpec",
    "FundingRateNormalization",
    "FundingArbOpportunity",
    "FundingPairTradeIntent",
    "MarketIdentity",
    "MarketStats",
    "NormalizedMarketSnapshot",
    "OpportunityRecord",
    "PaperTradeEntry",
    "PaperTradeExecutionPreflight",
    "PaperTradeOrderPreview",
    "PreviewConfirmationEntry",
    "ServiceHealth",
    "TopOfBook",
    "TradeLegIntent",
    "TradingFeeProfile",
    "VenueExecutionPreflight",
    "VenueOrderPreview",
    "WatchlistDocument",
]
