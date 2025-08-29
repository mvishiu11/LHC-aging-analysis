#!/usr/bin/env bash
# make_runs_index.sh — build two indexes:
#  1) <OUT_FILE>        : runId -> absolute path to .lst file
#  2) <OUT_SAMPLE_FILE> : runId -> one random line from inside the .lst (alien://...)
#
# NEW: If a .lst is empty (0 bytes) or contains no selectable entries
#      (only comments/blank lines), we SKIP that run in BOTH files.

set -euo pipefail

SEED=""
if [[ "${1:-}" == "--seed" ]]; then
  [[ $# -ge 3 ]] || { echo "Usage: $0 [--seed N] <dir_path> [output_file]" >&2; exit 1; }
  SEED="$2"
  shift 2
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 [--seed N] <dir_path> [output_file]" >&2
  exit 1
fi

ROOT_DIR="$1"
OUT_FILE="${2:-runs.index}"

ROOT_DIR="$(realpath "$ROOT_DIR")"

# Decide the sample index filename: <base>_sample.index if OUT_FILE ends with .index, else <OUT_FILE>_sample.index
if [[ "$OUT_FILE" == *.index ]]; then
  OUT_SAMPLE_FILE="${OUT_FILE%.index}_sample.index"
else
  OUT_SAMPLE_FILE="${OUT_FILE}_sample.index"
fi

TMP_MAIN="$(mktemp)"
TMP_SAMP="$(mktemp)"

cleanup() {
  rm -f "$TMP_MAIN" "$TMP_SAMP"
}
trap cleanup EXIT

# Helper: pick one random non-empty, non-comment line from a .lst using awk reservoir sampling
pick_random_line() {
  local f="$1"
  if [[ -n "$SEED" ]]; then
    awk -v seed="$SEED" '
      BEGIN { srand(seed) }
      /^[[:space:]]*#/ { next }    # skip comments
      /^[[:space:]]*$/ { next }    # skip blanks
      { n++; if (rand()*n < 1) line=$0 }
      END { if (n>0) print line }
    ' "$f"
  else
    awk '
      BEGIN { srand() }            # time-based seed
      /^[[:space:]]*#/ { next }
      /^[[:space:]]*$/ { next }
      { n++; if (rand()*n < 1) line=$0 }
      END { if (n>0) print line }
    ' "$f"
  fi
}

# Find matching .lst files safely
while IFS= read -r -d '' lst; do
  base="$(basename "$lst")"
  parent="$(basename "$(dirname "$lst")")"
  run_id=""

  if [[ "$base" =~ ^run_([A-Za-z0-9]+)_ctf_full\.lst$ ]]; then
    run_id="${BASH_REMATCH[1]}"
  elif [[ "$parent" =~ ^run_([A-Za-z0-9]+)$ ]]; then
    run_id="${BASH_REMATCH[1]}"
  fi

  [[ -n "$run_id" ]] || continue

  # Skip if file is literally empty (0 bytes) to avoid awk work
  if [[ ! -s "$lst" ]]; then
    printf "WARN: %s is empty (0 bytes) — skipping run %s\n" "$lst" "$run_id" >&2
    continue
  fi

  # Sample index: choose one random selectable entry
  sample_line="$(pick_random_line "$lst" || true)"

  # If the .lst has no selectable entries (only comments/blank), skip this run entirely
  if [[ -z "$sample_line" ]]; then
    printf "WARN: %s has no selectable entries — skipping run %s\n" "$lst" "$run_id" >&2
    continue
  fi

  # Only now add to BOTH indexes
  printf "%s\t%s\n" "$run_id" "$lst" >> "$TMP_MAIN"
  printf "%s\t%s\n" "$run_id" "$sample_line" >> "$TMP_SAMP"

done < <(find "$ROOT_DIR" -type f -name 'run_*_ctf_full.lst' -print0)

# Sort & write atomically
if [[ -s "$TMP_MAIN" ]]; then
  sort -t$'\t' -k1,1 "$TMP_MAIN" > "$OUT_FILE"
  echo "Indexed $(wc -l < "$OUT_FILE") runs -> $OUT_FILE"
else
  echo "No valid (non-empty) .lst files found under: $ROOT_DIR" >&2
  # still create empty outputs for predictability
  : > "$OUT_FILE"
  : > "$OUT_SAMPLE_FILE"
  exit 2
fi

if [[ -s "$TMP_SAMP" ]]; then
  sort -t$'\t' -k1,1 "$TMP_SAMP" > "$OUT_SAMPLE_FILE"
  echo "Sampled $(wc -l < "$OUT_SAMPLE_FILE") runs -> $OUT_SAMPLE_FILE"
else
  : > "$OUT_SAMPLE_FILE"
  echo "No sample entries created -> $OUT_SAMPLE_FILE (empty)" >&2
fi
