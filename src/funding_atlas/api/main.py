"""The public Data API: FastAPI app, CORS, and the `fa-api` entry point.

Read-only by construction. The recorder owns `FA_DATA_DIR`; this process opens
an in-memory DuckDB whose spill directory is forced elsewhere and only ever
issues `SELECT` over `read_parquet`. Nothing here can create a byte inside the
history it serves, which is what makes it safe to point at a live recorder's
disk.

Two structural choices worth stating:

* **Authentication is wired before it exists.** `deps.require_principal` is a
  dependency on every router already. It returns the public principal while
  `FA_API_KEYS` is unset — today's "no auth yet" — and starts enforcing the
  moment that variable is set, without a route body changing. Adding auth later
  by editing every handler is how one endpoint ends up unprotected.
* **Routes are sync (`def`, not `async def`).** A DuckDB scan is blocking CPU
  and IO work; running it in a coroutine would stall the event loop and make one
  slow screener query hold up every other request. FastAPI runs sync handlers in
  a threadpool, and `DuckStore` serialises access with a lock because a DuckDB
  connection is not safe to share across threads.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from funding_atlas.api.deps import Settings, get_settings, require_principal, set_settings
from funding_atlas.api.routes import assets, coverage, funding, health

__all__ = ["app", "cli", "create_app"]

TITLE = "funding-atlas Data API"
VERSION = "0.0.1"

# Binds all interfaces because it runs in a container, where the published
# port is the actual network boundary; a loopback default would make the
# service unreachable from outside its own namespace.
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the app. `settings` overrides environment resolution, for tests."""
    if settings is not None:
        set_settings(settings)
    resolved = get_settings()

    app = FastAPI(
        title=TITLE,
        version=VERSION,
        description=(
            "Read-only funding, open-interest and coverage data for perpetual futures. "
            "Decimal values are JSON strings; timestamps are int64 nanoseconds."
        ),
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(resolved.cors_origins),
        allow_credentials=False,  # no cookies, no sessions; an API key is a header
        allow_methods=["GET"],  # the API is read-only, and the preflight should say so
        allow_headers=["X-API-Key", "Content-Type"],
    )

    # The auth seam, applied per router rather than per handler: a new route in
    # any of these modules inherits it without anyone remembering to.
    guard = [Depends(require_principal)]
    for module in (health, assets, funding, coverage):
        app.include_router(module.router, dependencies=guard)
    return app


app = create_app()


def cli(argv: Sequence[str] | None = None) -> int:
    """`fa-api` entry point. Returns the process exit code."""
    import uvicorn

    parser = argparse.ArgumentParser(
        prog="fa-api", description="Serve the funding-atlas read-only Data API."
    )
    parser.add_argument(
        "--host", default=DEFAULT_HOST, help=f"bind address (default {DEFAULT_HOST})"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help=f"bind port (default {DEFAULT_PORT})"
    )
    args = parser.parse_args(argv)

    uvicorn.run(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(cli())
