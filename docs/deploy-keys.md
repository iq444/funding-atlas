# Credentials

Which machine holds what, and why. One table to answer the only question that
matters during an incident: *this host is compromised — what else just fell?*

## The map

| Host | Needs | Credential | Access | Name |
|---|---|---|---|---|
| Workstation | push both repos | fine-grained PAT, 90-day expiry | read/write | `mac-dev-rw-2026-10` |
| Recorder VPS | clone funding-atlas | deploy key | **read-only** | `deploy-vps-recorder-ro` |
| Execution host | clone the engine | deploy key | **read-only** | `deploy-vps-exec-ro` |
| Watchdog host | clone the engine | deploy key | **read-only** | `deploy-vps-watchdog-ro` |
| GitHub Actions | CI | built-in `GITHUB_TOKEN` | per-workflow | — (no PAT) |

Naming: `<host>-<access>-<yyyy-mm>` for tokens, `deploy-<host>-<access>` for
keys. Host first, because during an incident you work backwards from a machine
to the credentials it held.

## Why the workstation gets a token and servers get keys

A deploy key cannot expire. Stolen, it works until someone notices and revokes
it. The workstation is the machine most exposed to ordinary risk — browsing,
downloads, installed tooling — and it is the one place that needs **write**
access. There, automatic expiry is worth more than per-repository isolation, so
it gets a 90-day fine-grained token.

Servers are the inverse: narrow, unattended, and they only ever need to *read*
code. A read-only deploy key scoped to one repository means a compromised
server cannot modify the code it runs, and cannot reach any other repository.

**No host other than the workstation ever gets write access.** A server that
can push is a server that can rewrite the strategy it is about to execute.

## Provisioning

Run on the target host, never on a workstation:

```bash
./scripts/provision-deploy-key.sh iq444/funding-atlas deploy-vps-recorder-ro
```

The private key is generated on the host and never leaves it. A key generated
elsewhere and copied over has been exposed to every machine, clipboard and
channel it crossed, and there is no way to un-expose it.

The script writes an SSH host alias per key, because GitHub refuses to reuse a
deploy key across repositories — a host cloning two repos needs two keys, and
SSH has to be told which key belongs to which clone URL.

## Rotation

| Credential | When |
|---|---|
| Workstation PAT | every 90 days, at expiry |
| Deploy keys | on host rebuild, on staff change, immediately on suspected exposure |
| Hyperliquid agent wallet | see the engine's `SECURITY.md` — separate schedule, separate blast radius |

Revoking a deploy key is one click in the repository's Settings → Deploy keys
and affects exactly one host. That narrowness is the point.

## What this does not protect against

Scoping limits the damage from a stolen credential; it does not prevent the
theft. A process running as your user can read credentials your tools read
silently — that is why those tools do not prompt.

So the real control is upstream: **no machine holds a secret whose loss
matters.** For this project the GitHub credentials are the cheap ones. The
expensive secret is the trading key, and it is handled on the assumption that
the host it sits on will eventually be compromised — which is why it is an
agent wallet that can trade but cannot withdraw, on a host that serves no
public traffic, funded only with pilot capital.
