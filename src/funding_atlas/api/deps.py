"""Settings, shared connections and the authentication seam.

Three jobs, in order of how much trouble each one saves later:

1. **One DuckDB connection per worker.** Opening a connection per request means
   re-parsing Parquet footers on every page load, and under the dashboard's
   polling that is the difference between a 5ms response and a 500ms one. The
   store is cached per `(data_dir, venue)` in process state and handed to routes
   by `Depends`, so a test can substitute its own without touching a route.

2. **Settings from the environment.** The API runs in the same container shape
   as the recorder, so it reads the same `FA_*` variables and the same
   `config/recorder.yaml` when one is mounted. The config file is optional:
   `load_config` raises if it is missing, and an API that cannot start without
   the recorder's YAML is an API that cannot be deployed on its own.

3. **A place for API keys that is not a route body.** There is no auth today.
   But paid tiers are the stated plan, and retrofitting auth into fifteen route
   functions is how an endpoint gets missed. `require_principal` is already a
   dependency on every router (see `main.py`), already returns a `Principal`
   carrying a tier, and already enforces a key *if* one is configured. Turning
   on paid tiers means populating `FA_API_KEYS` and reading `principal.tier` —
   no route body changes.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, Header, HTTPException, status

from funding_atlas.api.duck import DuckStore
from funding_atlas.core.config import RecorderConfig, load_config

__all__ = [
    "Principal",
    "PrincipalDep",
    "Settings",
    "SettingsDep",
    "StoreDep",
    "get_settings",
    "get_store",
    "require_principal",
    "reset_state",
    "set_settings",
    "uptime_s",
]

#: Comma-separated in the environment, because that is what docker-compose and
#: every orchestrator can express without a nested config format.
_LIST_SEP = ","

#: Default CORS origins: the local dashboard dev server only. Not `*` — the API
#: is read-only today, but a wildcard that someone later copies onto an
#: authenticated endpoint turns into credential leakage, and the correct value
#: for production is known only to the deployment.
DEFAULT_CORS_ORIGINS = ("http://localhost:3000", "http://127.0.0.1:3000")

_PROCESS_START_NS = time.monotonic_ns()


@dataclass(frozen=True, slots=True)
class Settings:
    """Everything the API needs to start, resolved once."""

    data_dir: Path
    venue: str
    network: str
    cors_origins: tuple[str, ...] = DEFAULT_CORS_ORIGINS
    #: Empty means open access. See `require_principal`.
    api_keys: frozenset[str] = field(default_factory=frozenset)


def _recorder_config() -> RecorderConfig | None:
    # Optional on purpose: the API is deployable without the recorder's YAML.
    try:
        return load_config()
    except Exception:  # noqa: BLE001 - absent or invalid config is a fallback, not a failure
        return None


def _split(raw: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in raw.split(_LIST_SEP) if part.strip())


def _load_settings() -> Settings:
    config = _recorder_config()
    # `load_config` has already applied the `FA_*` overrides, so its values win
    # over the defaults here; the env reads below cover the API-only variables
    # the recorder's model does not know about.
    data_dir = config.data_dir if config else Path(os.environ.get("FA_DATA_DIR", "./data"))
    venue = config.venue if config else os.environ.get("FA_VENUE", "hyperliquid")
    network = config.network if config else os.environ.get("FA_HL_NETWORK", "mainnet")
    origins = _split(os.environ.get("FA_API_CORS_ORIGINS", "")) or DEFAULT_CORS_ORIGINS
    return Settings(
        data_dir=Path(data_dir),
        venue=venue,
        network=network,
        cors_origins=origins,
        api_keys=frozenset(_split(os.environ.get("FA_API_KEYS", ""))),
    )


_lock = threading.Lock()
_settings: Settings | None = None
_stores: dict[tuple[Path, str], DuckStore] = {}


def get_settings() -> Settings:
    global _settings
    with _lock:
        if _settings is None:
            _settings = _load_settings()
        return _settings


def set_settings(settings: Settings) -> None:
    """Install settings explicitly, for `create_app(settings=...)` and tests."""
    global _settings
    with _lock:
        _settings = settings


def reset_state() -> None:
    """Drop cached settings and close cached connections.

    Exists for tests and for a clean shutdown; a cached DuckDB handle that
    outlives its app keeps file descriptors on partitions the recorder may be
    rotating.
    """
    global _settings
    with _lock:
        for store in _stores.values():
            store.close()
        _stores.clear()
        _settings = None


SettingsDep = Annotated[Settings, Depends(get_settings)]


def get_store(settings: SettingsDep) -> DuckStore:
    """The per-worker DuckDB store. Created once, reused for the process life."""
    key = (settings.data_dir, settings.venue)
    with _lock:
        store = _stores.get(key)
        if store is None:
            store = DuckStore(settings.data_dir, settings.venue)
            _stores[key] = store
        return store


StoreDep = Annotated[DuckStore, Depends(get_store)]


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling. Anonymous until paid tiers exist."""

    tier: Literal["public", "paid"]
    key_id: str | None = None


def require_principal(
    settings: SettingsDep,
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> Principal:
    """Resolve the caller's tier, rejecting a bad key when keys are configured.

    With `FA_API_KEYS` unset this is a no-op that returns the public principal,
    which is today's behaviour: no authentication, by design. Set the variable
    and the same dependency starts enforcing, on every router, without a route
    body changing — which is the only reason it is wired up before it is needed.
    """
    if not settings.api_keys:
        return Principal(tier="public")
    if x_api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="missing X-API-Key header"
        )
    if x_api_key not in settings.api_keys:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unknown API key")
    # Only a prefix is retained: the full key must not reach a log line or an
    # error body from here.
    return Principal(tier="paid", key_id=x_api_key[:8])


PrincipalDep = Annotated[Principal, Depends(require_principal)]


def uptime_s() -> float:
    """Seconds since this process started.

    `monotonic_ns`, so an NTP step during the night cannot make the health
    endpoint report a negative uptime.
    """
    return (time.monotonic_ns() - _PROCESS_START_NS) / 1e9
