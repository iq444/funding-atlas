# Stack decisions

What is in, what is deferred, and why. Written down so each piece has to earn
its place rather than arrive by habit — every component is a thing that can
break at 3am, and the system is operated by one person.

## In now

| Layer | Choice | Why |
|---|---|---|
| Recorder, engine, research | Python 3.12 | one language for data, research and execution; the venue SDK lives here |
| Hot data | Parquet + DuckDB | research is columnar scans over immutable history; zero running cost |
| Operational state, event log | PostgreSQL | transactions, constraints, and the audit trail that must not be lost |
| Migrations | Alembic + SQLAlchemy | schema changes have to be reviewable and reversible |
| API | FastAPI + Pydantic | the data API and the dashboard's backend |
| Dashboard | Next.js, TypeScript, Tailwind, shadcn/ui, Lightweight Charts | standard, fast to build, deploys free |
| Venue | Hyperliquid SDK | the only supported path to the exchange |
| Runtime | Docker + Compose | one command to reproduce a host |
| Reverse proxy | Caddy | automatic TLS, one config file |
| CI | GitHub Actions | tests and linting on every push |

## Deferred, with the trigger that brings it in

| Component | Deferred because | Bring it in when |
|---|---|---|
| **NATS JetStream** | durable messaging between processes that do not yet exist; everything runs in one asyncio process | workers genuinely need to span hosts, and Postgres `LISTEN/NOTIFY` has stopped being enough |
| **Redis** | nothing to cache and no cross-process state yet | there is a measured hot path, not an assumed one |
| **MinIO** | self-hosting object storage on the host whose disk we are trying to empty defeats the purpose | never — cold data goes to S3-compatible storage **off** the box (Cloudflare R2: cheap, zero egress) |
| **TimescaleDB** | Parquet+DuckDB already covers history and Postgres covers state; this is a third system that beats neither | a workload appears that is genuinely time-series-in-Postgres shaped |
| **Prometheus, Grafana, Loki** | real value, but monitoring a system that does not run yet | the recorder has been live long enough to have behaviour worth graphing |
| **Sentry** | external dependency and cost; structured logs cover a single-operator system | there are users whose errors you cannot see |
| **Solidity, Foundry** | no contract has a job to do yet | an on-chain component has a concrete, stated purpose |
| **Base RPC, viem, web3.py** | Base is optional in the original design, not a trading dependency | the same trigger as above |

## The rule behind the table

A component is added when something concrete breaks without it, not when the
architecture diagram looks incomplete. On a single host operated by one person,
every additional moving part is paid for twice: once to build, and again every
time it fails at an inconvenient hour.

The deferrals above are cheap to reverse. The interfaces the system is built on
— `MarketFeed`, `Broker`, `Clock`, the event log — do not assume a single
process, so splitting workers across hosts later is a wiring change rather than
a rewrite.
