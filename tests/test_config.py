"""Config loader tests.

The important one is `test_shipped_config_loads`: it loads the real
`config/recorder.yaml`, so a field renamed in the loader without touching the
file (or the reverse) fails here rather than at 3am on a deployment.
"""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest
from pydantic import ValidationError

from funding_atlas.core.config import (
    ConfigError,
    IntegrityConfig,
    L2Stream,
    MidsStream,
    RecorderConfig,
    RotationConfig,
    StreamsConfig,
    UniverseConfig,
    load_config,
)
from funding_atlas.venues.records import STREAMS

REPO_ROOT = Path(__file__).resolve().parents[1]
SHIPPED_CONFIG = REPO_ROOT / "config" / "recorder.yaml"

# A minimal valid file, used as the base for the negative cases below.
BASE_YAML = """\
venue: hyperliquid
network: mainnet
rotation:
  max_rows: 1000
  max_seconds: 60
universe:
  mode: all_perps
  explicit: []
streams:
  funding:
    enabled: true
    poll_seconds: 60
  trades:
    enabled: true
  mids:
    enabled: true
    sample_seconds: 1
  l2_book:
    enabled: true
    assets: [BTC]
    sample_ms: 500
    depth: 20
integrity:
  heartbeat_seconds: 30
  max_silence_seconds: 90
"""


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """No FA_* variable from the developer's shell may reach a test."""
    for var in list(os.environ):
        if var.startswith("FA_"):
            monkeypatch.delenv(var, raising=False)


def write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "recorder.yaml"
    path.write_text(body, encoding="utf-8")
    return path


# --- the regression test that matters -------------------------------------------------


def test_shipped_config_loads() -> None:
    cfg = load_config(SHIPPED_CONFIG)

    assert cfg.venue == "hyperliquid"
    assert cfg.network == "mainnet"
    assert cfg.universe.mode == "all_perps"
    assert cfg.streams.l2.enabled is True
    assert cfg.streams.l2.depth == 20
    assert "HYPE" in cfg.streams.l2.assets
    assert cfg.integrity.max_silence_seconds == 90


def test_yaml_underscore_int_is_parsed_as_int() -> None:
    # `max_rows: 500_000` in the file. PyYAML's YAML 1.1 int resolver accepts
    # underscores; this asserts we get the number and not the string "500_000".
    cfg = load_config(SHIPPED_CONFIG)
    assert isinstance(cfg.rotation.max_rows, int)
    assert cfg.rotation.max_rows == 500_000


def test_every_stream_in_records_has_a_config_field() -> None:
    assert frozenset(STREAMS) <= frozenset(StreamsConfig.model_fields)


# --- extra="forbid" ------------------------------------------------------------------


def test_unknown_top_level_key_raises(tmp_path: Path) -> None:
    path = write(tmp_path, BASE_YAML + "max_rowz: 10\n")
    with pytest.raises(ValidationError, match="max_rowz"):
        load_config(path)


def test_unknown_nested_key_raises(tmp_path: Path) -> None:
    body = BASE_YAML.replace("  max_seconds: 60\n", "  max_seconds: 60\n  max_second: 60\n")
    with pytest.raises(ValidationError, match="max_second"):
        load_config(write(tmp_path, body))


def test_typod_stream_name_raises(tmp_path: Path) -> None:
    body = BASE_YAML.replace("  trades:\n", "  trade:\n")
    with pytest.raises(ValidationError, match="trade"):
        load_config(write(tmp_path, body))


def test_canonical_l2_spelling_is_also_accepted(tmp_path: Path) -> None:
    body = BASE_YAML.replace("  l2_book:\n", "  l2:\n")
    assert load_config(write(tmp_path, body)).streams.l2.enabled is True


# --- cross-field rules ---------------------------------------------------------------


def test_explicit_mode_with_empty_list_raises() -> None:
    with pytest.raises(ValidationError, match="explicit"):
        UniverseConfig(mode="explicit", explicit=())


def test_all_perps_with_a_list_raises() -> None:
    with pytest.raises(ValidationError, match="would be ignored"):
        UniverseConfig(mode="all_perps", explicit=("BTC",))


def test_duplicate_universe_assets_raise() -> None:
    with pytest.raises(ValidationError, match="duplicates"):
        UniverseConfig(mode="explicit", explicit=("BTC", "BTC"))


def test_l2_asset_outside_universe_raises(tmp_path: Path) -> None:
    body = BASE_YAML.replace(
        "  mode: all_perps\n  explicit: []\n",
        "  mode: explicit\n  explicit: [ETH]\n",
    )
    with pytest.raises(ValidationError, match=r"not present in universe.explicit.*BTC"):
        load_config(write(tmp_path, body))


def test_l2_asset_inside_universe_is_accepted(tmp_path: Path) -> None:
    body = BASE_YAML.replace(
        "  mode: all_perps\n  explicit: []\n",
        "  mode: explicit\n  explicit: [BTC, ETH]\n",
    )
    assert load_config(write(tmp_path, body)).universe.explicit == ("BTC", "ETH")


def test_l2_assets_unchecked_under_all_perps(tmp_path: Path) -> None:
    # Universe is whatever the venue lists at startup, so nothing to check here.
    body = BASE_YAML.replace("assets: [BTC]", "assets: [BTC, WIF]")
    assert load_config(write(tmp_path, body)).streams.l2.assets == ("BTC", "WIF")


def test_silence_below_heartbeat_raises() -> None:
    with pytest.raises(ValidationError, match="must be greater"):
        IntegrityConfig(heartbeat_seconds=30, max_silence_seconds=10)


def test_silence_equal_to_heartbeat_raises() -> None:
    with pytest.raises(ValidationError, match="must be greater"):
        IntegrityConfig(heartbeat_seconds=30, max_silence_seconds=30)


@pytest.mark.parametrize("field", ["heartbeat_seconds", "max_silence_seconds"])
def test_non_positive_integrity_bounds_raise(field: str) -> None:
    kwargs = {"heartbeat_seconds": 30, "max_silence_seconds": 90, field: 0}
    with pytest.raises(ValidationError, match="greater than 0"):
        IntegrityConfig(**kwargs)


def test_enabled_stream_with_zero_interval_raises() -> None:
    with pytest.raises(ValidationError, match="sample_seconds is 0"):
        MidsStream(enabled=True, sample_seconds=0)
    with pytest.raises(ValidationError, match="sample_ms is 0"):
        L2Stream(enabled=True, assets=("BTC",), sample_ms=0)


def test_disabled_stream_with_zero_interval_is_fine() -> None:
    assert MidsStream(enabled=False, sample_seconds=0).enabled is False


def test_enabled_l2_without_assets_raises() -> None:
    with pytest.raises(ValidationError, match="asset list is empty"):
        L2Stream(enabled=True, assets=())


def test_all_streams_disabled_raises() -> None:
    with pytest.raises(ValidationError, match="record nothing"):
        StreamsConfig(
            funding={"enabled": False},
            trades={"enabled": False},
            mids={"enabled": False},
            l2={"enabled": False},
            open_interest={"enabled": False},
        )


@pytest.mark.parametrize("value", [0, -1])
@pytest.mark.parametrize("field", ["max_rows", "max_seconds"])
def test_non_positive_rotation_bounds_raise(field: str, value: int) -> None:
    kwargs = {"max_rows": 1000, "max_seconds": 60, field: value}
    with pytest.raises(ValidationError, match="greater than 0"):
        RotationConfig(**kwargs)


def test_partial_cold_storage_raises() -> None:
    with pytest.raises(ValidationError, match="partially configured"):
        RecorderConfig(
            venue="hyperliquid",
            rotation=RotationConfig(max_rows=1, max_seconds=1),
            s3_bucket="funding-atlas",
        )


# --- environment overrides -----------------------------------------------------------


def test_env_overrides_file_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write(tmp_path, BASE_YAML)  # network: mainnet
    monkeypatch.setenv("FA_HL_NETWORK", "testnet")
    monkeypatch.setenv("FA_ENV", "prod")
    monkeypatch.setenv("FA_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("FA_DATA_DIR", "/data")

    cfg = load_config(path)
    assert cfg.network == "testnet"
    assert cfg.env == "prod"
    assert cfg.log_level == "DEBUG"
    assert cfg.data_dir == Path("/data")


def test_bad_env_value_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write(tmp_path, BASE_YAML)
    monkeypatch.setenv("FA_HL_NETWORK", "mainnnet")
    with pytest.raises(ValidationError, match="network"):
        load_config(path)


def test_empty_env_value_is_treated_as_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # docker-compose forwards blank variables; "" must not look configured.
    path = write(tmp_path, BASE_YAML)
    monkeypatch.setenv("FA_HL_NETWORK", "")
    monkeypatch.setenv("FA_S3_BUCKET", "")
    cfg = load_config(path)
    assert cfg.network == "mainnet"
    assert cfg.s3_bucket is None


def test_fa_config_is_used_when_no_path_given(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write(tmp_path, BASE_YAML.replace("venue: hyperliquid", "venue: from-fa-config"))
    monkeypatch.setenv("FA_CONFIG", str(path))
    assert load_config().venue == "from-fa-config"


def test_explicit_path_beats_fa_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    other = tmp_path / "other.yaml"
    other.write_text(BASE_YAML.replace("venue: hyperliquid", "venue: explicit"), encoding="utf-8")
    monkeypatch.setenv("FA_CONFIG", str(write(tmp_path, BASE_YAML)))
    assert load_config(other).venue == "explicit"


def test_missing_file_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read config file"):
        load_config(tmp_path / "nope.yaml")


def test_malformed_yaml_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(write(tmp_path, "venue: [unclosed\n"))


def test_non_mapping_yaml_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="mapping at the top level"):
        load_config(write(tmp_path, "- hyperliquid\n"))


# --- secrets -------------------------------------------------------------------------


SECRET_DSN = "postgresql://atlas:hunter2@localhost:5432/atlas"  # fake, test-only


def test_secrets_load_from_env_and_never_print(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write(tmp_path, BASE_YAML)
    monkeypatch.setenv("FA_PG_DSN", SECRET_DSN)
    monkeypatch.setenv("FA_S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("FA_S3_BUCKET", "funding-atlas")
    monkeypatch.setenv("FA_S3_KEY_ID", "key-id")
    monkeypatch.setenv("FA_S3_SECRET", "s3-secret-value")

    cfg = load_config(path)

    assert cfg.pg_dsn is not None and cfg.pg_dsn.get_secret_value() == SECRET_DSN
    assert cfg.s3_secret is not None and cfg.s3_secret.get_secret_value() == "s3-secret-value"

    for rendered in (repr(cfg), str(cfg), repr(cfg.pg_dsn), str(cfg.s3_secret)):
        assert "hunter2" not in rendered
        assert SECRET_DSN not in rendered
        assert "s3-secret-value" not in rendered

    redacted = cfg.redacted()
    assert redacted["pg_dsn"] == "***redacted***"
    assert redacted["s3_secret"] == "***redacted***"
    assert "hunter2" not in str(redacted)
    assert "s3-secret-value" not in str(redacted)
    # Non-secret values must still be there — the point is a usable log line.
    assert redacted["s3_bucket"] == "funding-atlas"
    assert redacted["venue"] == "hyperliquid"


def test_validation_error_does_not_leak_a_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A bad sibling value makes pydantic render the whole model's input.
    body = BASE_YAML.replace("  max_rows: 1000\n", "  max_rows: 0\n")
    monkeypatch.setenv("FA_PG_DSN", SECRET_DSN)
    with pytest.raises(ValidationError) as excinfo:
        load_config(write(tmp_path, body))
    assert "hunter2" not in str(excinfo.value)


def test_redacted_omits_nothing_when_no_secrets_are_set(tmp_path: Path) -> None:
    redacted = load_config(write(tmp_path, BASE_YAML)).redacted()
    assert redacted["pg_dsn"] is None
    assert redacted["s3_secret"] is None


def test_secret_in_config_file_raises(tmp_path: Path) -> None:
    body = BASE_YAML + textwrap.dedent(f"""\
        pg_dsn: {SECRET_DSN}
        """)
    with pytest.raises(ConfigError, match="secrets come from the environment"):
        load_config(write(tmp_path, body))
