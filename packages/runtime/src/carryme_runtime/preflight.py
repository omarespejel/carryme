"""Live execution preflight helpers."""

from __future__ import annotations

from typing import NamedTuple, TypedDict

from carryme_models import (
    CredentialRequirementStatus,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    VenueExecutionPreflight,
)


class VenueCredentialConfig(TypedDict):
    """Configured live credentials for one venue."""

    enabled: bool
    credentials: dict[str, str | None]


LiveExecutionConfigMap = dict[str, VenueCredentialConfig]

class RequirementSpec(NamedTuple):
    """One required credential for a live venue."""

    key: str
    env_var: str
    description: str
    secret: bool


class VenueSpec(TypedDict):
    """Static preflight specification for one supported venue."""

    requirements: list[RequirementSpec]
    notes: list[str]


_VENUE_SPECS: dict[str, VenueSpec] = {
    "extended": {
        "requirements": [
            RequirementSpec(
                "api_key",
                "CARRYME_API_EXTENDED_API_KEY",
                "Extended API key for the target trading subaccount.",
                True,
            ),
            RequirementSpec(
                "stark_private_key",
                "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                "Extended Stark signing key for authenticated trading requests.",
                True,
            ),
        ],
        "notes": [
            (
                "Extended live trading requires an API key and Stark signing key "
                "for the chosen subaccount."
            ),
        ],
    },
    "paradex": {
        "requirements": [
            RequirementSpec(
                "account_address",
                "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                "Paradex main account address paired with the trading subkey.",
                False,
            ),
            RequirementSpec(
                "private_key",
                "CARRYME_API_PARADEX_PRIVATE_KEY",
                "Paradex trading subkey private key used to derive authenticated API access.",
                True,
            ),
        ],
        "notes": [
            (
                "Paradex live trading requires the main account address and the "
                "trading subkey private key used for authenticated API access."
            ),
        ],
    },
    "hyperliquid": {
        "requirements": [
            RequirementSpec(
                "account_address",
                "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                "Hyperliquid master or subaccount address for the trading account.",
                False,
            ),
            RequirementSpec(
                "api_wallet_private_key",
                "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                "Hyperliquid API wallet private key used to sign live actions.",
                True,
            ),
        ],
        "notes": [
            "Hyperliquid live trading requires the account address and an API wallet private key.",
        ],
    },
}


def build_venue_execution_preflights(
    configs: LiveExecutionConfigMap,
) -> list[VenueExecutionPreflight]:
    """Build live-readiness status for all supported venues."""

    statuses: list[VenueExecutionPreflight] = []
    for venue, spec in _VENUE_SPECS.items():
        config = configs.get(venue, {"enabled": False, "credentials": {}})
        credentials = config["credentials"]
        requirements = [
            CredentialRequirementStatus(
                key=requirement.key,
                env_var=requirement.env_var,
                description=requirement.description,
                secret=requirement.secret,
                present=bool(credentials.get(requirement.key)),
            )
            for requirement in spec["requirements"]
        ]
        missing_env_vars = [item.env_var for item in requirements if not item.present]
        enabled = bool(config["enabled"])
        ready = enabled and not missing_env_vars
        statuses.append(
            VenueExecutionPreflight(
                venue=venue,
                enabled=enabled,
                ready=ready,
                missing_env_vars=missing_env_vars,
                requirements=requirements,
                notes=spec["notes"],
            )
        )
    return statuses


def build_paper_trade_execution_preflight(
    paper_trade: PaperTradeEntry,
    configs: LiveExecutionConfigMap,
) -> PaperTradeExecutionPreflight:
    """Build live-readiness status for the exact venues touched by a saved paper trade."""

    all_statuses = {item.venue: item for item in build_venue_execution_preflights(configs)}
    venue_names = [paper_trade.intent.long_leg.venue, paper_trade.intent.short_leg.venue]
    selected_names: list[str] = []
    for venue in venue_names:
        if venue not in selected_names:
            selected_names.append(venue)
    selected = [all_statuses[venue] for venue in selected_names]

    blocking_reasons: list[str] = []
    for status in selected:
        if not status.enabled:
            blocking_reasons.append(f"Venue {status.venue} live execution is not enabled")
        if status.missing_env_vars:
            blocking_reasons.append(
                f"Venue {status.venue} is missing required credentials: "
                + ", ".join(status.missing_env_vars)
            )

    return PaperTradeExecutionPreflight(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        ready=not blocking_reasons,
        venues=selected,
        blocking_reasons=blocking_reasons,
    )
