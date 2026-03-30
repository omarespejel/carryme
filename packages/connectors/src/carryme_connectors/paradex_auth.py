"""Paradex authenticated JWT helpers built from the official subkey auth flow."""

from __future__ import annotations

import functools
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from Crypto.Hash import keccak
from fast_stark_crypto import get_public_key, pedersen_hash, sign

from carryme_connectors.base import ConnectorError

PARADEX_API_BASE_URL = "https://api.prod.paradex.trade"
PARADEX_SYSTEM_CONFIG_PATH = "/v1/system/config"
PARADEX_AUTH_PATH = "/v1/auth"
PARADEX_ORDER_PATH = "/v1/orders"
PARADEX_AUTH_TOKEN_LIFETIME_SECONDS = 24 * 60 * 60
STARK_FIELD_MASK = (1 << 250) - 1
STARK_CURVE_ORDER = 0x800000000000010FFFFFFFFFFFFFFFFB781126DCAE7B2321E66A241ADC64D2F
PARADEX_DEFAULT_RECV_WINDOW_MS = 300_000

_AUTH_TYPES: dict[str, list[dict[str, str]]] = {
    "StarkNetDomain": [
        {"name": "name", "type": "felt"},
        {"name": "chainId", "type": "felt"},
        {"name": "version", "type": "felt"},
    ],
    "Request": [
        {"name": "method", "type": "felt"},
        {"name": "path", "type": "felt"},
        {"name": "body", "type": "felt"},
        {"name": "timestamp", "type": "felt"},
        {"name": "expiration", "type": "felt"},
    ],
}

_ORDER_TYPES: dict[str, list[dict[str, str]]] = {
    "StarkNetDomain": [
        {"name": "name", "type": "felt"},
        {"name": "chainId", "type": "felt"},
        {"name": "version", "type": "felt"},
    ],
    "Order": [
        {"name": "timestamp", "type": "felt"},
        {"name": "market", "type": "felt"},
        {"name": "side", "type": "felt"},
        {"name": "orderType", "type": "felt"},
        {"name": "size", "type": "felt"},
        {"name": "price", "type": "felt"},
    ],
}


@dataclass(frozen=True)
class ParadexSystemConfig:
    """Minimal Paradex system config required for JWT issuance."""

    starknet_chain_id: str


class ParadexJwtTokenProvider:
    """Mint short-lived Paradex JWTs directly from the trading subkey."""

    def __init__(
        self,
        *,
        base_url: str = PARADEX_API_BASE_URL,
        system_config_path: str = PARADEX_SYSTEM_CONFIG_PATH,
        auth_path: str = PARADEX_AUTH_PATH,
        token_lifetime_seconds: int = PARADEX_AUTH_TOKEN_LIFETIME_SECONDS,
        token_usage: str | None = None,
    ) -> None:
        self._base_url = base_url
        self._system_config_path = system_config_path
        self._auth_path = auth_path
        self._token_lifetime_seconds = token_lifetime_seconds
        self._token_usage = token_usage.strip().lower() if token_usage else None

    async def fetch_system_config(
        self,
        client: httpx.AsyncClient | None = None,
    ) -> ParadexSystemConfig:
        """Return the live system config required for signature domain building."""

        payload = await self._request_json(client, "GET", self._system_config_path)
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex system config payload must be an object")
        chain_id = payload.get("starknet_chain_id")
        if not isinstance(chain_id, str) or not chain_id:
            raise ConnectorError("Paradex system config missing starknet_chain_id")
        return ParadexSystemConfig(starknet_chain_id=chain_id)

    async def issue_jwt_token(
        self,
        *,
        account_address: str,
        private_key: str,
        client: httpx.AsyncClient | None = None,
        now: int | None = None,
    ) -> str:
        """Issue a short-lived Paradex JWT for authenticated REST reads/writes."""

        config = await self.fetch_system_config(client)
        issued_at = int(time.time()) if now is None else now
        expires_at = issued_at + self._token_lifetime_seconds
        auth_path = build_paradex_auth_request_path(
            private_key=private_key,
            auth_path=self._auth_path,
            token_usage=self._token_usage,
        )
        headers = build_paradex_auth_headers(
            account_address=account_address,
            private_key=private_key,
            starknet_chain_id=config.starknet_chain_id,
            issued_at=issued_at,
            expires_at=expires_at,
            auth_path=self._auth_path,
        )
        payload = await self._request_json(client, "POST", auth_path, headers=headers)
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex auth payload must be an object")
        jwt_token = payload.get("jwt_token")
        if not isinstance(jwt_token, str) or not jwt_token:
            raise ConnectorError("Paradex auth response missing jwt_token")
        return jwt_token

    async def _request_json(
        self,
        client: httpx.AsyncClient | None,
        method: str,
        path: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any] | list[Any]:
        if client is not None:
            response = await client.request(method, path, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict | list):
                raise ConnectorError("Paradex returned an unexpected payload shape")
            return payload

        async with httpx.AsyncClient(base_url=self._base_url, timeout=15.0) as owned_client:
            response = await owned_client.request(method, path, headers=headers)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict | list):
                raise ConnectorError("Paradex returned an unexpected payload shape")
            return payload


def build_paradex_auth_request_path(
    *,
    private_key: str,
    auth_path: str = PARADEX_AUTH_PATH,
    token_usage: str | None = None,
) -> str:
    """Return the Paradex auth endpoint path that includes the subkey public key."""

    private_key_int = _parse_hex_value(private_key, "Paradex private key")
    public_key_int = get_public_key(private_key_int)
    path = f"{auth_path}/{hex(public_key_int)}"
    if token_usage:
        return f"{path}?token_usage={token_usage.strip().lower()}"
    return path


def build_paradex_auth_headers(
    *,
    account_address: str,
    private_key: str,
    starknet_chain_id: str,
    issued_at: int,
    expires_at: int,
    auth_path: str = PARADEX_AUTH_PATH,
) -> dict[str, str]:
    """Build the official Paradex `/v1/auth` signature headers."""

    account_address_int = _parse_hex_value(account_address, "Paradex account address")
    private_key_int = _parse_hex_value(private_key, "Paradex private key")
    chain_id_int = int.from_bytes(starknet_chain_id.encode("utf-8"), "big")
    typed_data = _build_auth_typed_data(
        account_address=account_address_int,
        domain={
            "name": "Paradex",
            "chainId": hex(chain_id_int),
            "version": "1",
        },
        primary_type="Request",
        message={
            "method": "POST",
            "path": auth_path,
            "body": "",
            "timestamp": issued_at,
            "expiration": expires_at,
        },
        types=_AUTH_TYPES,
    )
    signature = sign(private_key=private_key_int, msg_hash=typed_data.message_hash)
    flattened_signature = f'["{signature[0]}","{signature[1]}"]'
    return {
        "PARADEX-STARKNET-ACCOUNT": account_address,
        "PARADEX-STARKNET-SIGNATURE": flattened_signature,
        "PARADEX-TIMESTAMP": str(issued_at),
        "PARADEX-SIGNATURE-EXPIRATION": str(expires_at),
    }


def build_signed_paradex_order_payload(
    *,
    account_address: str,
    private_key: str,
    starknet_chain_id: str,
    order_payload: Mapping[str, Any],
    signature_timestamp_ms: int | None = None,
    recv_window_ms: int = PARADEX_DEFAULT_RECV_WINDOW_MS,
) -> dict[str, Any]:
    """Build a signed Paradex order payload from an unsigned preview payload."""

    market = _require_string(order_payload, "market")
    side = _normalize_order_side(_require_string(order_payload, "side"))
    order_type = _normalize_order_type(_require_string(order_payload, "type"))
    size = _normalize_order_decimal(_require_decimal(order_payload, "size"), key="size")
    if size <= 0:
        raise ConnectorError("Paradex order payload field size must be greater than zero")
    price = (
        _normalize_order_decimal(_require_decimal(order_payload, "price"), key="price")
        if order_type != "MARKET"
        else Decimal("0")
    )
    if order_type != "MARKET" and price <= 0:
        raise ConnectorError("Paradex order payload field price must be greater than zero")
    instruction = _require_string(order_payload, "instruction")
    client_id = _require_string(order_payload, "client_id")
    reduce_only_value = order_payload.get("reduce_only", False)
    if not isinstance(reduce_only_value, bool):
        raise ConnectorError("Paradex order payload field reduce_only must be a boolean")
    reduce_only = reduce_only_value
    signature_timestamp = (
        int(time.time() * 1000) if signature_timestamp_ms is None else signature_timestamp_ms
    )
    signature = build_paradex_order_signature(
        account_address=account_address,
        private_key=private_key,
        starknet_chain_id=starknet_chain_id,
        market=market,
        side=side,
        order_type=order_type,
        size=size,
        price=price,
        signature_timestamp_ms=signature_timestamp,
    )

    signed_payload: dict[str, Any] = {
        "market": market,
        "side": side,
        "type": order_type,
        "size": _format_order_decimal(size),
        "price": _format_order_decimal(price),
        "instruction": instruction,
        "client_id": client_id,
        "signature": signature,
        "signature_timestamp": signature_timestamp,
        "recv_window": recv_window_ms,
    }
    if reduce_only:
        signed_payload["flags"] = ["REDUCE_ONLY"]
    return signed_payload


def build_paradex_order_signature(
    *,
    account_address: str,
    private_key: str,
    starknet_chain_id: str,
    market: str,
    side: str,
    order_type: str,
    size: Decimal,
    price: Decimal,
    signature_timestamp_ms: int,
) -> str:
    """Return the Paradex typed-data order signature for one order payload."""

    account_address_int = _parse_hex_value(account_address, "Paradex account address")
    private_key_int = _parse_hex_value(private_key, "Paradex private key")
    chain_id_int = int.from_bytes(starknet_chain_id.encode("utf-8"), "big")
    typed_data = _build_auth_typed_data(
        account_address=account_address_int,
        domain={
            "name": "Paradex",
            "chainId": hex(chain_id_int),
            "version": "1",
        },
        primary_type="Order",
        message={
            "timestamp": str(signature_timestamp_ms),
            "market": market,
            "side": "1" if side == "BUY" else "2",
            "orderType": order_type,
            "size": str(_to_chain_decimal(size)),
            "price": "0" if order_type == "MARKET" else str(_to_chain_decimal(price)),
        },
        types=_ORDER_TYPES,
    )
    signature = sign(private_key=private_key_int, msg_hash=typed_data.message_hash)
    return f'["{signature[0]}","{signature[1]}"]'


def _parse_hex_value(value: str, label: str) -> int:
    try:
        return int(value, 16)
    except ValueError as exc:  # pragma: no cover - defensive only
        raise ConnectorError(f"{label} must be a hex string") from exc


def _require_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConnectorError(f"Paradex order payload missing required string field: {key}")
    return value


def _require_decimal(payload: Mapping[str, Any], key: str) -> Decimal:
    value = payload.get(key)
    if value is None:
        raise ConnectorError(f"Paradex order payload missing required decimal field: {key}")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise ConnectorError(f"Paradex order payload field {key} must be a decimal") from exc
    if not parsed.is_finite():
        raise ConnectorError(f"Paradex order payload field {key} must be finite")
    return parsed


def _normalize_order_side(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"BUY", "SELL"}:
        raise ConnectorError(f"Unsupported Paradex order side: {value}")
    return normalized


def _normalize_order_type(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"LIMIT", "MARKET"}:
        raise ConnectorError(f"Unsupported Paradex order type: {value}")
    return normalized


def _to_chain_decimal(value: Decimal) -> int:
    return int(value.scaleb(8))


def _format_order_decimal(value: Decimal) -> str:
    return format(value, "f")


def _normalize_order_decimal(value: Decimal, *, key: str) -> Decimal:
    normalized = Decimal(_to_chain_decimal(value)).scaleb(-8)
    if normalized != value:
        raise ConnectorError(f"Paradex order payload field {key} exceeds 8 decimal places")
    return normalized


@dataclass(frozen=True)
class AuthTypedData:
    """Minimal typed-data container for Paradex auth signing."""

    message_hash: int


def _build_auth_typed_data(
    *,
    account_address: int,
    domain: Mapping[str, int | str],
    primary_type: str,
    message: Mapping[str, int | str],
    types: dict[str, list[dict[str, str]]],
) -> AuthTypedData:
    full_message_hash = _compute_hash_on_elements(
        [
            _encode_shortstring("StarkNet Message"),
            _struct_hash(types, "StarkNetDomain", domain),
            account_address,
            _struct_hash(types, primary_type, message),
        ]
    )
    return AuthTypedData(message_hash=full_message_hash)


def _struct_hash(
    types: dict[str, list[dict[str, str]]],
    type_name: str,
    data: Mapping[str, int | str],
) -> int:
    return _compute_hash_on_elements(
        [_type_hash(types, type_name), *_encode_data(types, type_name, data)]
    )


def _type_hash(types: dict[str, list[dict[str, str]]], type_name: str) -> int:
    return _get_selector_from_name(_encode_type(types, type_name))


def _encode_data(
    types: dict[str, list[dict[str, str]]],
    type_name: str,
    data: Mapping[str, int | str],
) -> list[int]:
    values: list[int] = []
    for param in types[type_name]:
        values.append(_encode_value(types, param["type"], data[param["name"]]))
    return values


def _encode_value(
    types: dict[str, list[dict[str, str]]],
    type_name: str,
    value: Any,
) -> int:
    if _is_pointer(type_name) and isinstance(value, list):
        stripped_type = _strip_pointer(type_name)
        if stripped_type in types:
            struct_items = [
                _struct_hash(types, stripped_type, item)
                for item in value
                if isinstance(item, dict)
            ]
            return _compute_hash_on_elements(struct_items)
        return _compute_hash_on_elements([int(_get_hex(item), 16) for item in value])

    if type_name in types and isinstance(value, dict):
        return _struct_hash(types, type_name, value)

    return int(_get_hex(value), 16)


def _encode_type(types: dict[str, list[dict[str, str]]], type_name: str) -> str:
    primary, *dependencies = _get_dependencies(types, type_name)
    ordered = [primary, *sorted(dependencies)]

    def render_dependency(name: str) -> str:
        rendered = [f"{item['name']}:{item['type']}" for item in types[name]]
        return f"{name}({','.join(rendered)})"

    return "".join(render_dependency(name) for name in ordered)


def _get_dependencies(types: dict[str, list[dict[str, str]]], type_name: str) -> list[str]:
    if type_name not in types:
        return []

    dependencies: set[str] = set()

    def collect_deps(current: str) -> None:
        for param in types[current]:
            param_type = _strip_pointer(param["type"])
            if param_type in types and param_type not in dependencies:
                dependencies.add(param_type)
                collect_deps(param_type)

    collect_deps(type_name)
    return [type_name, *list(dependencies)]


def _compute_hash_on_elements(data: Sequence[int]) -> int:
    return functools.reduce(pedersen_hash, [*data, len(data)], 0)


def _get_selector_from_name(name: str) -> int:
    digest = keccak.new(digest_bits=256, data=name.encode("ascii")).digest()
    return int.from_bytes(digest, "big") & STARK_FIELD_MASK


def _get_hex(value: int | str) -> str:
    if isinstance(value, int):
        return hex(value)
    if value.startswith("0x"):
        return value
    if value.isnumeric():
        return hex(int(value))
    return hex(_encode_shortstring(value))


def _encode_shortstring(value: str) -> int:
    if not value:
        return 0
    encoded = value.encode("ascii")
    if len(encoded) > 31:
        raise ConnectorError("Short string values must be 31 ASCII bytes or fewer")
    return int.from_bytes(encoded, "big")


def _is_pointer(value: str) -> bool:
    return bool(value) and value.endswith("*")


def _strip_pointer(value: str) -> str:
    return value[:-1] if _is_pointer(value) else value
