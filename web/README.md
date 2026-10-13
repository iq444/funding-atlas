# funding-atlas — public funding dashboard

The web front end for the funding-atlas data API: Hyperliquid perpetual funding
rates across every listed perp, a carry screener that nets fees out of the
headline number, per-asset funding history, and the recorder's own health and
**known gaps**.

The dataset's claim is that it is a continuous, honest record. So the
non-negotiable rule in this UI is that any range containing a hole in the
record says so — on the chart itself, not in a footnote. A line drawn smoothly
across missing data is a false statement about what was observed.

## Run it

```bash
cd web
npm install
cp .env.example .env.local      # then edit NEXT_PUBLIC_API_BASE_URL
npm run dev                     # http://localhost:3000
```

| Script | What it does |
|---|---|
| `npm run dev` | dev server against the live API |
| `npm run dev:mock` | dev server against the in-browser fixture layer |
| `npm run build` | production build |
| `npm run build:mock` | production build with mock mode baked in (for a demo deploy) |
| `npm start` | serve the production build |
| `npm run lint` | ESLint |
| `npm run typecheck` | `tsc --noEmit` |

### Environment

| Variable | Default | Meaning |
|---|---|---|
| `NEXT_PUBLIC_API_BASE_URL` | `http://127.0.0.1:8000` | Base URL of the data API, no trailing slash. |
| `NEXT_PUBLIC_MOCK_API` | unset (`0`) | `1` serves every endpoint from local fixtures instead of the API. |

Both are `NEXT_PUBLIC_*` because the dashboard reads the API **from the
browser**. That keeps the deployment a static bundle with no server-side
secret, and it means a reader can watch the requests in their own devtools —
which matters for a dataset whose whole claim is that it can be checked. It
also means the API must allow the dashboard's origin via CORS.

Next.js inlines `NEXT_PUBLIC_*` at build time, so changing either variable
requires a rebuild, not just a restart.

## Mock mode

`NEXT_PUBLIC_MOCK_API=1` swaps the typed client's implementation
(`src/lib/api/client.ts`) for the fixture layer in `src/lib/api/mock.ts`. No
network request is made. It exists so the whole app can be built, reviewed and
tested before the API is deployed.

- **Deterministic.** Values come from a seeded PRNG keyed on asset and on the
  current hour, never `Math.random()`, so two renders agree.
- **Shaped like the wire.** Monetary values are emitted as *strings* with
  plausible per-asset precision, so the string-parsing path is the one being
  exercised rather than a convenient number path.
- **Includes the awkward states** on purpose: a stale `l2` stream, two recorder
  gaps inside the default chart window, a per-asset listing-churn gap, and
  carries that do not cover their fees.
- **Loud.** Every page is stamped `MOCK DATA` across the top and in the footer
  while it is on. Synthetic numbers that do not announce themselves are exactly
  the failure this project is built against.

## Routes

| Route | What it is |
|---|---|
| `/` | Funding overview — every listed perp, sortable and filterable, with the payment direction spelled out per row. |
| `/screener` | Carry screener — notional / holding period / side in, ranked by **net** APR after fees, with breakeven hours and the full assumptions block. |
| `/asset/[symbol]` | Funding history chart, current stats, venue trading rules, and coverage for exactly the plotted window. |
| `/status` | Recorder health, per-stream lag and row counts, and every known gap. |

## How gaps are surfaced

`GET /coverage` is queried for the same window any chart or range describes,
and the result drives three things:

1. The history series is built over the **complete bucket grid** for the range,
   with a Lightweight Charts *whitespace* point wherever no row exists. The
   line breaks at a gap rather than interpolating across it, and the time axis
   stays linear in time (the library spaces points by index, so simply omitting
   a bucket would compress the gap away to nothing).
2. Each gap is drawn as a hatched band over the plot area, pinned to the time
   axis as you pan and zoom, and listed underneath with its recorded cause.
3. Window statistics state their sample size: *"computed over 157 recorded
   buckets out of 168 the window would hold"*, rather than quietly averaging
   over whatever survived.

## Numbers

Monetary values arrive from the API as strings to avoid precision loss, and
`src/lib/num.ts` keeps it that way: each value retains its original text, and
formatting is **capped at the precision the string carried**. A mark quoted as
`3.4210` is never rendered as `3.42100000`. The parsed double is used for
sorting and arithmetic only.

Timestamps are nanoseconds (`*_ns`). A current nanosecond timestamp is ~1.8e18,
well past `Number.MAX_SAFE_INTEGER`, so `src/lib/time.ts` accepts both strings
and numbers and does the conversion with `BigInt` whenever it is given a
string. Query parameters are round-tripped as text, never through a double.

All times displayed are UTC and labelled as such.

## Layout

```
src/
  app/
    page.tsx                 /          funding overview
    screener/page.tsx        /screener  carry screener
    asset/[symbol]/page.tsx  /asset/*   history, rules, coverage
    status/page.tsx          /status    recorder health and gaps
  components/
    ui/                      shadcn/ui primitives (new-york, zinc)
    funding-chart.tsx        Lightweight Charts + gap bands
    funding-sign.tsx         who-pays-whom, in words
    gap-notice.tsx           coverage, stated plainly
  lib/
    api/{client,mock,types}.ts
    num.ts  time.ts  use-api.ts  use-now.ts
```

Stack: Next.js 15 (App Router) · TypeScript · Tailwind CSS v4 · shadcn/ui ·
Lightweight Charts v4. All versions are pinned exactly; no canary builds.

## Notes

- `npm audit` reports advisories reaching the build toolchain through
  `next` and `eslint-config-next` transitively. They are dev-time only and
  cannot be resolved without a major Next.js bump; nothing is shipped to the
  browser from those packages.
- Dark theme only. This is an instrument panel, and the palette is tuned for
  reading dense numeric tables for a long time.

## Where the client is deliberately tolerant

The contract this app was written against and the API's response models differ
in a few places, so the client accepts both shapes rather than breaking on
whichever arrives:

| Field | Contract as published | What the API emits | How the client handles it |
|---|---|---|---|
| `coverage.covered` | unspecified — read as a 0..1 fraction | `bool` | Both accepted. The displayed percentage is derived from `missing_s` over the window the caller asked for, which is well-defined either way. |
| `*_ns` | nanosecond integers | JSON `int` | Accepted as `string` or `number`; converted with `BigInt` when given a string. A nanosecond JSON number is already past `Number.MAX_SAFE_INTEGER`. |
| history / screener `coverage` | separate `GET /coverage` | also embedded in the response | Embedded coverage is preferred; the standalone call is the fallback. |
| screener `assumptions` | free-form map | fixed model, `funding_interval_s` is a list | Rendered whatever it is — known keys get a label, unknown keys are shown under their own name rather than dropped. |
| `mark`, `open_interest`, `oi_notional`, `as_of_ns` | required | nullable | Rendered as `—` when absent. |

Monetary strings are also normalised server-side (`Decimal.normalize()`), which
drops trailing zeros. That means the string no longer carries the precision the
venue quoted, and this client's "never display more digits than arrived" rule
degrades to "never display more digits than survived normalisation".
