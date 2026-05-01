"""Live execution preflight helpers."""

from __future__ import annotations

import logging
from typing import NamedTuple, TypedDict

from carryme_models import (
    CredentialRequirementStatus,
    PaperTradeEntry,
    PaperTradeExecutionPreflight,
    VenueExecutionPreflight,
)

logger = logging.getLogger(__name__)


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
    alternative_keys: tuple[str, ...] = ()


class VenueSpec(TypedDict):
    """Static preflight specification for one supported venue."""

    enabled_setting: str
    credential_settings: dict[str, str]
    requirements: list[RequirementSpec]
    notes: list[str]


LIVE_EXECUTION_VENUE_SPECS: dict[str, VenueSpec] = {
    "extended": {
        "enabled_setting": "extended_live_enabled",
        "credential_settings": {
            "api_key": "extended_api_key",
            "stark_private_key": "extended_stark_private_key",
        },
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
        "enabled_setting": "paradex_live_enabled",
        "credential_settings": {
            "account_address": "paradex_account_address",
            "private_key": "paradex_private_key",
            "bearer_token": "paradex_bearer_token",
        },
        "requirements": [
            RequirementSpec(
                "account_address",
                "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                "Paradex main account address paired with the trading subkey.",
                False,
            ),
            RequirementSpec(
                "private_key",
                "CARRYME_API_PARADEX_PRIVATE_KEY|CARRYME_API_PARADEX_BEARER_TOKEN",
                (
                    "Paradex trading subkey private key or pre-issued bearer token used "
                    "for authenticated API access."
                ),
                True,
                ("bearer_token",),
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
        "enabled_setting": "hyperliquid_live_enabled",
        "credential_settings": {
            "account_address": "hyperliquid_account_address",
            "api_wallet_private_key": "hyperliquid_api_wallet_private_key",
        },
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
            (
                "Hyperliquid live trading requires the trading account address and an API "
                "wallet private key."
            ),
            (
                "If you trade through a subaccount or vault, also set the optional "
                "CARRYME_API_HYPERLIQUID_VAULT_ADDRESS so signed exchange actions target "
                "the correct account."
            ),
        ],
    },
}


def build_live_execution_configs(settings: object) -> LiveExecutionConfigMap:
    """Build live execution config directly from the shared venue specification."""

    configs: LiveExecutionConfigMap = {}
    for venue, spec in LIVE_EXECUTION_VENUE_SPECS.items():
        configs[venue] = {
            "enabled": bool(getattr(settings, spec["enabled_setting"])),
            "credentials": {
                key: getattr(settings, attribute_name)
                for key, attribute_name in spec["credential_settings"].items()
            },
        }
    return configs


def build_venue_execution_preflights(
    configs: LiveExecutionConfigMap,
) -> list[VenueExecutionPreflight]:
    """Build live-readiness status for all supported venues."""

    statuses: list[VenueExecutionPreflight] = []
    for venue, spec in LIVE_EXECUTION_VENUE_SPECS.items():
        config = configs.get(venue, {"enabled": False, "credentials": {}})
        credentials = config["credentials"]
        requirements = [
            CredentialRequirementStatus(
                key=requirement.key,
                env_var=requirement.env_var,
                description=requirement.description,
                secret=requirement.secret,
                present=any(
                    bool(credentials.get(key))
                    for key in (requirement.key, *requirement.alternative_keys)
                ),
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

    if paper_trade.entry_id is None:
        raise ValueError("paper_trade.entry_id must be set for preflight")

    all_statuses = {item.venue: item for item in build_venue_execution_preflights(configs)}
    venue_names = [paper_trade.intent.long_leg.venue, paper_trade.intent.short_leg.venue]
    selected: list[VenueExecutionPreflight] = []
    for venue in venue_names:
        if any(status.venue == venue for status in selected):
            continue
        status = all_statuses.get(venue)
        if status is None:
            logger.warning(
                "Skipping unsupported live execution venue %s for paper trade %s",
                venue,
                paper_trade.entry_id,
            )
            continue
        selected.append(status)

    blocking_reasons = [
        f"Venue {venue} is not supported for live execution"
        for venue in venue_names
        if venue not in all_statuses
    ]
    for status in selected:
        if not status.enabled:
            blocking_reasons.append(f"Venue {status.venue} live execution is not enabled")
        if status.missing_env_vars:
            blocking_reasons.append(
                f"Venue {status.venue} is missing required credentials: "
                + ", ".join(status.missing_env_vars)
            )

    return PaperTradeExecutionPreflight(
        paper_trade_id=paper_trade.entry_id,
        label=paper_trade.intent.label,
        ready=not blocking_reasons,
        venues=selected,
        blocking_reasons=blocking_reasons,
    )
