# Contributing

## Licensing

This project is dual licensed under **MIT OR Apache-2.0**, at the user's option.

By submitting a contribution you agree it is dual licensed on the same terms,
unless you say otherwise in the pull request.

This matters more than the usual boilerplate: a contribution offered under only
one of the two licences cannot later be redistributed under the other. A project
that accepts such contributions without noticing ends up with files under
different terms and no way to state a single licence honestly — and that cannot
be repaired without tracking down every contributor.

## What the code has to hold to

Three rules carry real consequences here, so they are not style preferences:

- **`Decimal`, never `float`, for prices and sizes.** Float rounding on a
  two-leg hedge produces a position that is nearly neutral, which is the same
  thing as directional.
- **Time comes from a `Clock`, never `time.time()`.** This is what lets backtest,
  paper and live share one code path, and it is what structurally prevents a
  strategy from reading a price stamped later than the moment it is reasoning
  about.
- **Nothing in this repository signs, holds a key, or places an order.** The
  boundary in `venues/base.py` is why this repository can be public. Execution
  lives elsewhere.

Comments explain *why*, not *what*. Existing modules are the reference for tone.

## Before opening a pull request

```bash
ruff check src tests
ruff format --check src tests
pytest -q
```

## Reporting a data gap

Gaps in recorded history are first-class events, not bugs to be smoothed over.
If you find a period where data is missing or wrong, open an issue with the
stream, asset and time range — a silently patched hole is worse than a recorded
one, because research cannot tell the difference between data that is absent and
data that was invented.
