#!/usr/bin/env bash
# Create a read-only deploy key for one repository, on the host that will use it.
#
# Run this ON the server, never on a workstation. The private key is generated
# here and must never leave: a key that travels has been exposed to every
# machine and channel it crossed.
#
# Usage:
#   ./provision-deploy-key.sh <owner/repo> <key-name>
#
# Example:
#   ./provision-deploy-key.sh iq444/funding-atlas deploy-vps-recorder-ro
#
# Naming scheme: deploy-<host>-<access>  (see docs/deploy-keys.md)

set -euo pipefail

if [[ $# -ne 2 ]]; then
    sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
    exit 64
fi

REPO="$1"          # owner/repo
NAME="$2"          # deploy-vps-recorder-ro
ALIAS="${NAME}.github.com"
KEY="$HOME/.ssh/${NAME}"
SSH_CONFIG="$HOME/.ssh/config"

if [[ ! "$REPO" =~ ^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$ ]]; then
    echo "error: expected <owner/repo>, got '$REPO'" >&2
    exit 64
fi

if [[ -e "$KEY" ]]; then
    echo "error: $KEY already exists. Remove it deliberately or pick another name." >&2
    exit 1
fi

mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

# No passphrase: this key is used by an unattended service, so a passphrase
# would have to be stored next to it to be useful. The protection here is the
# key being read-only and scoped to a single repository, not secrecy of a
# passphrase that lives on the same disk.
ssh-keygen -t ed25519 -f "$KEY" -N "" -C "$NAME" >/dev/null
chmod 600 "$KEY"
chmod 644 "$KEY.pub"

# A host alias per key. GitHub refuses to reuse one deploy key across repos, so
# a host cloning two repositories needs two keys — and ssh needs to be told
# which key goes with which clone URL.
if ! grep -qs "^Host ${ALIAS}\$" "$SSH_CONFIG" 2>/dev/null; then
    cat >> "$SSH_CONFIG" <<EOF

Host ${ALIAS}
    HostName github.com
    User git
    IdentityFile ${KEY}
    IdentitiesOnly yes
EOF
    chmod 600 "$SSH_CONFIG"
fi

cat <<EOF

────────────────────────────────────────────────────────────────────────
Keypair created. Private key stays here: ${KEY}

1. Open:
     https://github.com/${REPO}/settings/keys/new

2. Title:
     ${NAME}

3. Key — paste exactly this:

$(cat "${KEY}.pub")

4. Leave "Allow write access" UNCHECKED.
   A host that only runs code never needs to push. Granting write here is
   how a compromised server becomes a compromised repository.

5. Then clone using the alias, not github.com:

     git clone git@${ALIAS}:${REPO}.git

   Verify first:
     ssh -T git@${ALIAS}
   Expect: "Hi ${REPO}! You've successfully authenticated, but GitHub does
   not provide shell access." — that wording confirms it is a deploy key
   scoped to one repository rather than account-wide access.
────────────────────────────────────────────────────────────────────────
EOF
