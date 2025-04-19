#!/usr/bin/env sh
# ------------------------------------------------------------------------------
# A unified entry‑point wrapper for every runtime container.
#   • Loads /mnt/data/inputs/.env (if it exists) and exports its keys.
#   • Establishes project‑wide defaults (e.g., PYTHONUNBUFFERED=1).
#   • Logs what it is about to run, then execs the real command.
# ------------------------------------------------------------------------------

set -euo pipefail

# ── 1)  Source user‑supplied environment  ─────────────────────────────────────
if [ -f /mnt/data/inputs/.env ]; then
  set -a                       # export everything we source
  . /mnt/data/inputs/.env
  set +a
fi

# ── 2)  Trace and hand off  ───────────────────────────────────────────────────
echo "[entrypoint] $(date -Iseconds)  →  exec $*"
exec "$@"
