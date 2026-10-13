# Deployment

One host, three containers behind Caddy: the recorder, the read-only Data API,
and Postgres. The dashboard is static files, not a running application — see
`web/next.config.ts`.

## DNS

Two A records, both to the host:

| Name | Type | Value |
|---|---|---|
| `@` | A | the host IP |
| `api` | A | the host IP |

`api` is a separate name rather than a path under the main domain so the API can
be moved to its own machine later by changing one DNS record. A path would break
every consumer that had integrated against it.

## First run

```bash
cp .env.example .env            # then fill FA_DOMAIN, FA_API_DOMAIN, FA_ACME_EMAIL
docker compose --profile edge up -d
```

Caddy obtains certificates on first start, which needs ports 80 and 443 reachable
from the internet. Open them before starting, or the ACME challenge fails and
Caddy retries with a backoff.

## Known gap: no rate limiting

Caddy's core has none, and adding it means building a custom image with the
`caddy-ratelimit` module. The API is read-only and holds no secrets, so abuse
costs CPU rather than data — but every request is a DuckDB scan, so a determined
scraper can make the host unpleasant. Worth fixing before the API is advertised
anywhere.
