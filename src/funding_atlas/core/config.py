"""Typed recorder configuration.

A recorder that dies at 3am because `max_silence_seconds` was a string has lost
book data that cannot be refetched. So validation here is strict and total, and
it all happens before the first subscription: every model forbids unknown keys,
every bound is checked, and every combination that cannot be satisfied at
runtime is rejected with a message that names the field.

`extra="forbid"` is the single most valuable property of this module. A typo'd
key in a YAML file is otherwise the worst possible failure mode — the recorder
starts, looks healthy, and silently records with the default.

Precedence, highest first:

  1. environment variables (`FA_*`)
  2. values in the YAML file
  3. model defaults

Environment wins because the file is committed and the deployment is not: the
container sets `FA_DATA_DIR` and `FA_HL_NETWORK` for the host it runs on, and
must not be overruled by whatever the repository happens to ship.

Secrets (`FA_PG_DSN`, `FA_S3_SECRET`) are `SecretStr` and are accepted *only*
from the environment. They therefore cannot be committed to the config file, and
cannot be printed by a stray f-string or a pydantic traceback. Use `redacted()`
for the startup log line.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator

from funding_atlas.venues.records import STREAMS

__all__ = [
    "ConfigError",
    "FundingStream",
    "IntegrityConfig",
    "L2Stream",
    "MidsStream",
    "OpenInterestStream",
    "RecorderConfig",
    "RotationConfig",
    "StreamsConfig",
    "TradesStream",
    "UniverseConfig",
    "load_config",
]

DEFAULT_CONFIG_PATH = Path("config/recorder.yaml")

#: Env var -> top-level `RecorderConfig` field. `FA_CONFIG` is absent on purpose:
#: it selects the file rather than overriding a value inside it.
ENV_OVERRIDES: dict[str, str] = {
    "FA_ENV": "env",
    "FA_DATA_DIR": "data_dir",
    "FA_LOG_LEVEL": "log_level",
    "FA_HL_NETWORK": "network",
    "FA_PG_DSN": "pg_dsn",
    "FA_S3_ENDPOINT": "s3_endpoint",
    "FA_S3_BUCKET": "s3_bucket",
    "FA_S3_KEY_ID": "s3_key_id",
    "FA_S3_SECRET": "s3_secret",
}

#: Fields that may only arrive from the environment. A DSN with a password in a
#: committed YAML file is a leak, so it is an error rather than a warning.
SECRET_FIELDS: frozenset[str] = frozenset({"pg_dsn", "s3_secret"})


class ConfigError(Exception):
    """Raised for problems with the config *source* — missing file, bad YAML.

    Field-level problems are left to pydantic's `ValidationError`, whose message
    already names the exact path that is wrong.
    """


class _Base(BaseModel):
    # frozen: config read once at startup. A value that changes under a running
    # stream is indistinguishable from a bug in that stream.
    model_config = ConfigDict(frozen=True, extra="forbid")


class RotationConfig(_Base):
    """Parquet rotation bounds. Whichever is hit first closes the file.

    Both are `gt=0`: a zero bound means "rotate every row" or "rotate every
    instant", which produces millions of empty files and fills the inode table
    long before the disk.
    """

    # PyYAML resolves YAML 1.1 ints, so `500_000` in the file arrives here as the
    # int 500000 — verified, no explicit unwrapping needed. `strict=False`
    # (pydantic's default) would also accept the string form from another loader.
    max_rows: int = Field(gt=0)
    max_seconds: int = Field(gt=0)


class UniverseConfig(_Base):
    """Which assets get recorded."""

    mode: Literal["all_perps", "explicit"] = "all_perps"
    explicit: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _check_mode_matches_list(self) -> UniverseConfig:
        if self.mode == "explicit" and not self.explicit:
            raise ValueError("universe.mode is 'explicit' but universe.explicit is empty")
        # Not an unused field but a contradiction: someone listed assets and
        # expects them to be the universe. Recording everything instead is the
        # kind of surprise that is only noticed from the disk graph.
        if self.mode == "all_perps" and self.explicit:
            raise ValueError(
                "universe.explicit is set but universe.mode is 'all_perps'; "
                "the list would be ignored — set mode: explicit or clear the list"
            )
        if len(set(self.explicit)) != len(self.explicit):
            raise ValueError("universe.explicit contains duplicates")
        return self


class _StreamBase(_Base):
    enabled: bool = True


class FundingStream(_StreamBase):
    poll_seconds: int = Field(default=60, ge=0)

    @model_validator(mode="after")
    def _check_interval(self) -> FundingStream:
        if self.enabled and self.poll_seconds == 0:
            raise ValueError("streams.funding is enabled but poll_seconds is 0")
        return self


class TradesStream(_StreamBase):
    """Trades are pushed, not sampled, so there is no interval to set."""


class MidsStream(_StreamBase):
    sample_seconds: int = Field(default=1, ge=0)

    @model_validator(mode="after")
    def _check_interval(self) -> MidsStream:
        if self.enabled and self.sample_seconds == 0:
            raise ValueError("streams.mids is enabled but sample_seconds is 0")
        return self


class L2Stream(_StreamBase):
    """Book snapshots — the volume driver, hence opt-in per asset."""

    # Defaults off, unlike the other streams: a book snapshot stream with no
    # asset list is the one configuration that can fill a disk in week one, so
    # it has to be asked for by name.
    enabled: bool = False
    assets: tuple[str, ...] = ()
    sample_ms: int = Field(default=500, ge=0)
    depth: int = Field(default=20, gt=0)

    @model_validator(mode="after")
    def _check_enabled_is_actionable(self) -> L2Stream:
        if self.enabled and self.sample_ms == 0:
            raise ValueError("streams.l2 is enabled but sample_ms is 0")
        if self.enabled and not self.assets:
            raise ValueError(
                "streams.l2 is enabled but its asset list is empty; "
                "it would record nothing while reporting healthy"
            )
        if len(set(self.assets)) != len(self.assets):
            raise ValueError("streams.l2.assets contains duplicates")
        return self


class OpenInterestStream(_StreamBase):
    # Defaults off: `config/recorder.yaml` does not mention this stream, and
    # enabling a stream nobody asked for is not the loader's call to make.
    enabled: bool = False
    poll_seconds: int = Field(default=60, ge=0)

    @model_validator(mode="after")
    def _check_interval(self) -> OpenInterestStream:
        if self.enabled and self.poll_seconds == 0:
            raise ValueError("streams.open_interest is enabled but poll_seconds is 0")
        return self


class StreamsConfig(_Base):
    """One entry per stream in `records.STREAMS`.

    The canonical name is `l2`, matching `records.STREAMS`, and the shipped YAML
    now uses it. The `l2_book` alias is kept only so an older config file still
    loads; new files should not use it. Both spellings are accepted rather
    than renaming anything: the config file is the published interface, and
    `STREAMS` is the internal one.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    funding: FundingStream = FundingStream()
    trades: TradesStream = TradesStream()
    mids: MidsStream = MidsStream()
    l2: L2Stream = Field(default_factory=L2Stream, alias="l2_book")
    open_interest: OpenInterestStream = OpenInterestStream()

    @model_validator(mode="after")
    def _check_something_is_enabled(self) -> StreamsConfig:
        if not any(
            s.enabled for s in (self.funding, self.trades, self.mids, self.l2, self.open_interest)
        ):
            raise ValueError("every stream is disabled; the recorder would record nothing")
        return self


# A stream added to `records.STREAMS` without a home here would be unconfigurable
# and silently never recorded. Fail at import, where it is cheap to notice.
_UNMODELLED = frozenset(STREAMS) - frozenset(StreamsConfig.model_fields)
if _UNMODELLED:
    raise RuntimeError(
        f"records.STREAMS has streams with no config model: {sorted(_UNMODELLED)}. "
        "Add them to StreamsConfig."
    )


class IntegrityConfig(_Base):
    """Gap detection. Lost book data is unrecoverable, so the recorder must at
    least know *that* it was lost."""

    heartbeat_seconds: int = Field(default=30, gt=0)
    max_silence_seconds: int = Field(default=90, gt=0)

    @model_validator(mode="after")
    def _check_silence_exceeds_heartbeat(self) -> IntegrityConfig:
        # Strictly greater: equal leaves zero slack for scheduling jitter, so
        # every heartbeat races the silence timer and declares phantom gaps.
        if self.max_silence_seconds <= self.heartbeat_seconds:
            raise ValueError(
                f"integrity.max_silence_seconds ({self.max_silence_seconds}) must be greater "
                f"than heartbeat_seconds ({self.heartbeat_seconds}); otherwise a healthy "
                "stream is declared silent between two heartbeats"
            )
        return self


class RecorderConfig(_Base):
    """The whole recorder configuration: file values plus environment."""

    venue: str = Field(min_length=1)
    network: Literal["mainnet", "testnet"] = "mainnet"

    # Deployment-shaped, so these come from the environment in practice.
    env: Literal["dev", "staging", "prod"] = "dev"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    data_dir: Path = Path("./data")

    rotation: RotationConfig
    universe: UniverseConfig = UniverseConfig()
    streams: StreamsConfig = StreamsConfig()
    integrity: IntegrityConfig = IntegrityConfig()

    pg_dsn: SecretStr | None = None
    s3_endpoint: str | None = None
    s3_bucket: str | None = None
    s3_key_id: str | None = None
    s3_secret: SecretStr | None = None

    @model_validator(mode="after")
    def _check_l2_assets_are_in_universe(self) -> RecorderConfig:
        # Only checkable for an explicit universe; under all_perps the set is
        # whatever the venue lists at startup, which is not knowable here.
        if self.universe.mode == "explicit":
            universe = {a.upper() for a in self.universe.explicit}
            unknown = sorted({a.upper() for a in self.streams.l2.assets} - universe)
            if unknown:
                raise ValueError(f"streams.l2.assets not present in universe.explicit: {unknown}")
        return self

    @model_validator(mode="after")
    def _check_cold_storage_is_complete(self) -> RecorderConfig:
        # Half-configured offload is worse than none: partitions are deleted
        # locally only after a verified upload, so an upload that cannot
        # authenticate stalls the disk reclaim it was added to provide.
        s3 = {
            "s3_endpoint": self.s3_endpoint,
            "s3_bucket": self.s3_bucket,
            "s3_key_id": self.s3_key_id,
            "s3_secret": self.s3_secret,
        }
        if any(s3.values()) and not all(s3.values()):
            missing = sorted(k for k, v in s3.items() if not v)
            raise ValueError(f"cold storage is partially configured; missing: {missing}")
        return self

    def redacted(self) -> dict[str, Any]:
        """Log-safe dump. Secrets become a marker, never a value."""
        dumped = self.model_dump(mode="json", by_alias=False)
        for name in SECRET_FIELDS:
            if dumped.get(name) is not None:
                dumped[name] = "***redacted***"
        return dumped


def _env_overrides(environ: dict[str, str] | None = None) -> dict[str, Any]:
    environ = dict(os.environ) if environ is None else environ
    out: dict[str, Any] = {}
    for var, field in ENV_OVERRIDES.items():
        raw = environ.get(var)
        # An empty value means unset, not "". `.env.example` ships
        # `FA_S3_KEY_ID=` commented-out-but-empty, and docker-compose happily
        # forwards blank variables; "" would otherwise look like a configured
        # bucket with no credentials.
        if raw is None or raw.strip() == "":
            continue
        out[field] = raw.strip()
    return out


def load_config(path: str | Path | None = None) -> RecorderConfig:
    """Load, merge and validate. Raises before any stream is opened.

    `path` wins; otherwise `FA_CONFIG`; otherwise the repository default.

    A `str` is accepted alongside `Path` because this is the first call in every
    entry point, and a startup failure here should name the file rather than
    surface an AttributeError from somewhere inside the loader.
    """
    if path is None:
        env_path = os.environ.get("FA_CONFIG", "").strip()
        path = Path(env_path) if env_path else DEFAULT_CONFIG_PATH
    else:
        path = Path(path)

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config file {path}: {exc}") from exc

    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path} is not valid YAML: {exc}") from exc

    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(
            f"{path} must contain a YAML mapping at the top level, got {type(raw).__name__}"
        )

    leaked = SECRET_FIELDS & set(raw)
    if leaked:
        raise ConfigError(
            f"{path} sets secret field(s) {sorted(leaked)}; secrets come from the environment "
            "(FA_PG_DSN, FA_S3_SECRET) so they are never committed"
        )

    merged: dict[str, Any] = {**raw, **_env_overrides()}
    return RecorderConfig.model_validate(merged)
