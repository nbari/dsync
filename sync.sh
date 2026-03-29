#!/usr/bin/env bash
set -euo pipefail

# Source and destination PGDATA roots for the migration.
SRC_PGDATA="/var/lib/postgresql/15/main"
DST_HOST="postgres@newserver"
DST_PGDATA="/var/lib/postgresql/15/main"
PGUSER="postgres"
PXS_BIN="${PXS_BIN:-pxs}"
SSH_BIN="${SSH_BIN:-ssh}"
BACKUP_LABEL_NAME="pxs_migration"

# Keep the same exclusions as the original rsync-based workflow so we do not
# overwrite destination-specific config or transient postmaster state.
PXS_IGNORE_ARGS=(
  --ignore postmaster.pid
  --ignore postmaster.opts
  --ignore pg_hba.conf
  --ignore pg_ident.conf
  --ignore postgresql.conf
)

# Speed-first transfer profile: mirror the destination with --delete, but skip
# --fsync on all passes so the first benchmarks reflect raw transfer speed.
PXS_SYNC_ARGS=(
  --delete
  "${PXS_IGNORE_ARGS[@]}"
)

tmpfile=""
script_started=$SECONDS

cleanup() {
  # Remove the temporary backup_label file and shut down the persistent psql
  # coprocess on normal exit or interruption.
  if [[ -n "$tmpfile" && -f "$tmpfile" ]]; then
    rm -f "$tmpfile"
  fi

  if [[ -n "${PG_PID:-}" ]]; then
    if [[ -n "${PG[1]:-}" ]]; then
      printf '\\q\n' >&"${PG[1]}" || true
    fi
    wait "$PG_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# Quote a value for safe interpolation into a remote shell command.
shell_quote() {
  local value=${1//\'/\'\"\'\"\'}
  printf "'%s'" "$value"
}

# Fail early if a required command is not available locally.
need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[-] Missing required command: $1" >&2
    exit 1
  }
}

# Render elapsed seconds as HH:MM:SS so long PGDATA copies are easy to compare.
format_duration() {
  local total_seconds=$1
  local hours=$((total_seconds / 3600))
  local minutes=$(((total_seconds % 3600) / 60))
  local seconds=$((total_seconds % 60))

  printf "%02d:%02d:%02d" "$hours" "$minutes" "$seconds"
}

# Time a single step and print a compact summary after it completes.
run_timed_step() {
  local label=$1
  shift

  local started=$SECONDS
  echo "[*] ${label}..."
  "$@"
  echo "[*] ${label} completed in $(format_duration "$((SECONDS - started))")"
}

# Preflight warnings are informational: they surface the few parity gaps that
# materially matter for this PGDATA workflow without blocking the run.
warn_if_detected() {
  local label=$1
  local example=${2:-}
  echo "[!] ${label}" >&2
  if [[ -n "$example" ]]; then
    echo "    example: $example" >&2
  fi
}

detect_tablespaces() {
  # PostgreSQL tablespaces are symlinks under pg_tblspc. pxs will preserve the
  # symlink entries, but the referenced targets must exist on the destination.
  local tablespace_root=${SRC_PGDATA%/}/pg_tblspc
  [[ -d "$tablespace_root" ]] || return 1

  local sample=""
  sample=$(find "$tablespace_root" -mindepth 1 -maxdepth 1 -type l -print -quit 2>/dev/null || true)
  [[ -n "$sample" ]] || return 1

  warn_if_detected \
    "Tablespace symlinks detected under pg_tblspc. pxs will preserve the symlinks, but the tablespace targets must exist and be handled consistently on the destination host." \
    "$sample -> $(readlink "$sample" 2>/dev/null || printf 'unresolved')"
}

detect_hardlinks() {
  # Hard links are uncommon in PGDATA, but if present they are a known parity
  # gap versus rsync -H.
  local sample=""
  sample=$(find "$SRC_PGDATA" -type f -links +1 -print -quit 2>/dev/null || true)
  [[ -n "$sample" ]] || return 1

  warn_if_detected \
    "Hard-linked files were detected. pxs does not preserve hard-link topology yet, so this workflow is not a strict replacement for rsync -H." \
    "$sample"
}

run_preflight() {
  # Validate the local/remote environment before we start the PostgreSQL backup
  # window. Any hard failure should happen here, before data movement begins.
  need_cmd "$PXS_BIN"
  need_cmd "$SSH_BIN"
  need_cmd psql
  need_cmd mktemp
  need_cmd find

  [[ -d "$SRC_PGDATA" ]] || {
    echo "[-] Source PGDATA does not exist or is not a directory: $SRC_PGDATA" >&2
    exit 1
  }

  local local_user
  local remote_user
  local local_version
  local remote_version
  local remote_parent

  local_user=$(id -un)
  if [[ "$local_user" != "$PGUSER" ]]; then
    echo "[!] Script is running as '$local_user', not '$PGUSER'." >&2
    echo "    The intended workflow is to run the transfer as the PostgreSQL OS user." >&2
  fi

  local_version=$("$PXS_BIN" --version 2>/dev/null || true)
  remote_version=$("$SSH_BIN" "$DST_HOST" "pxs --version" 2>/dev/null || true)
  remote_user=$("$SSH_BIN" "$DST_HOST" "id -un" 2>/dev/null || true)

  [[ -n "$remote_version" ]] || {
    echo "[-] Could not execute 'pxs --version' on $DST_HOST. Ensure pxs is installed and in PATH remotely." >&2
    exit 1
  }

  if [[ -n "$remote_user" && "$remote_user" != "$PGUSER" ]]; then
    echo "[!] Remote SSH session resolves to '$remote_user', not '$PGUSER'." >&2
    echo "    The intended workflow is for the destination transfer to run as the PostgreSQL OS user." >&2
  fi

  echo "[*] Local pxs:  $local_version"
  echo "[*] Remote pxs: $remote_version"

  if ! "$PXS_BIN" sync --help 2>&1 | grep -q -- '--delete'; then
    echo "[-] Local pxs does not advertise sync --delete support." >&2
    exit 1
  fi

  remote_parent=$(dirname "$DST_PGDATA")
  echo "[*] Ensuring remote destination directory exists..."
  "$SSH_BIN" "$DST_HOST" "mkdir -p -- $(shell_quote "$remote_parent") $(shell_quote "$DST_PGDATA")"

  echo "[*] Capability summary:"
  echo "    - supported here: recursive SSH sync to remote destinations, symlinks, perms, mtimes, remote --delete, repeated PGDATA passes"
  echo "    - speed mode: --fsync is disabled for all passes"
  echo "    - not guaranteed here: hard links (-H)"

  detect_tablespaces || true
  detect_hardlinks || true
}

run_pxs_sync() {
  # Repeated runs are expected to get faster as the destination converges
  # because pxs skips unchanged files and can send deltas for changed ones.
  echo "[*] pxs sync: $SRC_PGDATA -> $DST_HOST:$DST_PGDATA (--delete, no --fsync)"
  "$PXS_BIN" sync "$SRC_PGDATA" "$DST_HOST:$DST_PGDATA" "${PXS_SYNC_ARGS[@]}"
}

# Preflight first so we fail fast on missing tools, unsupported pxs builds, or
# obvious environmental surprises before touching PostgreSQL backup state.
run_preflight

echo "[*] Starting persistent PostgreSQL session..."
coproc PG { psql -U "$PGUSER" -d postgres -t -A -q; }
PG_PID=$!

# First pass copies the bulk of PGDATA before backup mode starts.
run_timed_step "Pre-syncing data directory (this may take a while)" run_pxs_sync

# Enter PostgreSQL backup mode so the following passes produce a consistent base
# backup workflow instead of a raw live-filesystem copy.
echo "[*] Starting PostgreSQL backup mode..."
printf "SELECT pg_backup_start('%s', true);\n" "$BACKUP_LABEL_NAME" >&"${PG[1]}"
read -r -u "${PG[0]}" _

# Second pass should be much smaller because most data already exists remotely.
run_timed_step "Syncing data directory again (this should take less time)" run_pxs_sync

# Capture the backup label emitted by pg_backup_stop(). It is written after the
# final data pass so the destination reflects the completed backup window.
echo "[*] Stopping PostgreSQL backup mode and capturing label..."
printf "SELECT labelfile FROM pg_backup_stop();\n" >&"${PG[1]}"
printf "SELECT 'END_OF_LABEL';\n" >&"${PG[1]}"

backup_label=""
while read -r -u "${PG[0]}" line; do
  if [[ "$line" == "END_OF_LABEL" ]]; then
    break
  fi
  backup_label+="$line"$'\n'
done

tmpfile=$(mktemp)
printf "%s" "$backup_label" >"$tmpfile"

# Final catch-up pass after pg_backup_stop() should only need the remaining
# changed files/blocks created during the backup window.
run_timed_step "Last sync of data directory" run_pxs_sync

# Install backup_label only after the final pass so PostgreSQL sees a coherent
# base-backup marker on the destination.
run_timed_step "Installing backup_label on destination" \
  "$SSH_BIN" "$DST_HOST" "cat > $(shell_quote "$DST_PGDATA/backup_label")" <"$tmpfile"

echo
echo "[*] Migration complete!"
echo "    Destination host: $DST_HOST"
echo "    Destination PGDATA: $DST_PGDATA"
echo "    Total elapsed: $(format_duration "$((SECONDS - script_started))")"
echo
echo "[*] You can now start PostgreSQL on the destination host with:"
echo "    pg_ctl -D $DST_PGDATA start"
echo
echo "[!] Reminder:"
echo "    This script is a PGDATA-focused pxs workflow."
echo "    It does not yet guarantee rsync parity for hard links."
