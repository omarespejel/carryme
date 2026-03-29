"""Extended signed order helpers built from the official settlement schema."""

from __future__ import annotations

import math
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any

from fast_stark_crypto import get_order_msg_hash, sign

from carryme_connectors.base import ConnectorError

EXTENDED_API_BASE_URL = "https://api.starknet.extended.exchange"
EXTENDED_ORDER_PATH = "/api/v1/user/order"


@dataclass(frozen=True)
class ExtendedStarknetDomain:
    """Mainnet Stark domain used by Extended settlement signatures."""

    name: str = "Perpetuals"
    version: str = "v0"
    chain_id: str = "SN_MAIN"
    revision: str = "1"


EXTENDED_MAINNET_DOMAIN = ExtendedStarknetDomain()


def build_signed_extended_order_payload(
    *,
    api_key: str,
    stark_private_key: str,
    account_payload: dict[str, Any],
    market_payload: dict[str, Any],
    order_payload: dict[str, Any],
    taker_fee_rate: Decimal | float | str,
    nonce: int | None = None,
    expire_time: datetime | None = None,
    domain: ExtendedStarknetDomain = EXTENDED_MAINNET_DOMAIN,
) -> dict[str, Any]:
    """Build the live Extended `/user/order` request body from a confirmed preview leg."""

    if not api_key:
        raise ConnectorError("Extended API key is required to build a signed order")
    if not stark_private_key:
        raise ConnectorError("Extended Stark private key is required to build a signed order")

    account_body = _unwrap_payload(account_payload, "Extended account")
    market_body = _unwrap_payload(market_payload, "Extended market")
    market_name = _require_string(market_body, "name")
    side = _normalize_side(_require_string(order_payload, "side"))
    order_type = _normalize_order_type(_require_string(order_payload, "type"))
    if order_type != "LIMIT":
        raise ConnectorError(f"Unsupported Extended order type: {order_type}")

    time_in_force = _normalize_time_in_force(_require_string(order_payload, "time_in_force"))
    if time_in_force != "IOC":
        raise ConnectorError(f"Unsupported Extended time in force: {time_in_force}")

    price = Decimal(_require_string(order_payload, "price"))
    quantity = Decimal(_require_string(order_payload, "size"))
    reduce_only = bool(order_payload.get("reduce_only", False))
    post_only = bool(order_payload.get("post_only", False))
    order_external_id = _require_string(order_payload, "client_order_id")
    fee_rate = Decimal(str(taker_fee_rate))
    expiry = (expire_time or datetime.now(UTC) + timedelta(hours=1)).astimezone(UTC)
    nonce_value = secrets.randbelow(2**32) if nonce is None else nonce

    if price <= 0:
        raise ConnectorError("Extended order price must be positive")
    if quantity <= 0:
        raise ConnectorError("Extended order quantity must be positive")
    if fee_rate < 0:
        raise ConnectorError("Extended taker fee rate must be non-negative")

    l2_config = _require_mapping(market_body, "l2Config")
    synthetic_id = _parse_hex(_require_string(l2_config, "syntheticId"), "Extended syntheticId")
    collateral_id = _parse_hex(_require_string(l2_config, "collateralId"), "Extended collateralId")
    synthetic_resolution = _require_int(l2_config, "syntheticResolution")
    collateral_resolution = _require_int(l2_config, "collateralResolution")

    stark_key_text = _require_string(account_body, "l2Key")
    stark_key = _parse_hex(stark_key_text, "Extended l2Key")
    collateral_position = _require_int(account_body, "l2Vault")
    private_key = _parse_hex(stark_private_key, "Extended Stark private key")

    collateral_human = quantity * price
    fee_human = collateral_human * fee_rate
    synthetic_stark = _to_stark_amount(
        quantity,
        resolution=synthetic_resolution,
        rounding=ROUND_UP if side == "BUY" else ROUND_DOWN,
    )
    collateral_stark = _to_stark_amount(
        collateral_human,
        resolution=collateral_resolution,
        rounding=ROUND_UP if side == "BUY" else ROUND_DOWN,
    )
    fee_stark = _to_stark_amount(
        fee_human,
        resolution=collateral_resolution,
        rounding=ROUND_UP,
    )

    if side == "BUY":
        collateral_stark = -collateral_stark
    else:
        synthetic_stark = -synthetic_stark

    order_hash = get_order_msg_hash(
        position_id=collateral_position,
        base_asset_id=synthetic_id,
        base_amount=synthetic_stark,
        quote_asset_id=collateral_id,
        quote_amount=collateral_stark,
        fee_amount=fee_stark,
        fee_asset_id=collateral_id,
        expiration=_hash_expiration(expiry),
        salt=nonce_value,
        user_public_key=stark_key,
        domain_name=domain.name,
        domain_version=domain.version,
        domain_chain_id=domain.chain_id,
        domain_revision=domain.revision,
    )
    signature_r, signature_s = sign(private_key=private_key, msg_hash=order_hash)

    return {
        "id": order_external_id,
        "market": market_name,
        "type": order_type,
        "side": side,
        "qty": _format_decimal(quantity),
        "price": _format_decimal(price),
        "reduceOnly": reduce_only,
        "postOnly": post_only,
        "timeInForce": time_in_force,
        "expiryEpochMillis": _to_epoch_millis(expiry),
        "fee": _format_decimal(fee_rate, normalize=True),
        "nonce": str(nonce_value),
        "selfTradeProtectionLevel": "ACCOUNT",
        "settlement": {
            "signature": {
                "r": hex(signature_r),
                "s": hex(signature_s),
            },
            "starkKey": stark_key_text,
            "collateralPosition": str(collateral_position),
        },
        "debuggingAmounts": {
            "collateralAmount": str(collateral_stark),
            "feeAmount": str(fee_stark),
            "syntheticAmount": str(synthetic_stark),
        },
    }


def _unwrap_payload(payload: dict[str, Any], label: str) -> dict[str, Any]:
    body = payload.get("data", payload)
    if not isinstance(body, dict):
        raise ConnectorError(f"{label} payload must contain a data object")
    return body


def _require_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise ConnectorError(f"Extended payload missing object field {key!r}")
    return value


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ConnectorError(f"Extended payload missing string field {key!r}")
    return value


def _require_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value:
        return int(value)
    raise ConnectorError(f"Extended payload missing integer field {key!r}")


def _normalize_side(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ConnectorError(f"Unsupported Extended order side: {value}")
    return normalized


def _normalize_order_type(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"LIMIT"}:
        raise ConnectorError(f"Unsupported Extended order type: {value}")
    return normalized


def _normalize_time_in_force(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"IOC"}:
        raise ConnectorError(f"Unsupported Extended time in force: {value}")
    return normalized


def _parse_hex(value: str, label: str) -> int:
    stripped = value.strip().lower()
    if not stripped.startswith("0x"):
        raise ConnectorError(f"{label} must be a hex string")
    return int(stripped, 16)


def _to_stark_amount(value: Decimal, *, resolution: int, rounding: str) -> int:
    scaled = (value * Decimal(resolution)).to_integral_value(rounding=rounding)
    return int(scaled)


def _hash_expiration(expiry: datetime) -> int:
    return math.ceil((expiry + timedelta(days=14)).timestamp())


def _to_epoch_millis(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _format_decimal(value: Decimal, *, normalize: bool = False) -> str:
    if normalize:
        return format(value.normalize(), "f")
    return format(value, "f")
