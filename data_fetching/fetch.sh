#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run_ft0_qc.sh
#
# Iterate over all AliEn paths in laser_paths.lst and, for each FT0 run,
#   1. fire up the digits-reader workflow,
#   2. pipe it into your FT0 Digit QC task,
#   3. save stdout+stderr in per-run logfiles.
#
# Usage ........: ./run_ft0_qc.sh [listfile] [logdir]
#   listfile ...: default = laser_paths.lst
#   logdir  .....: default = logs
#
# Prerequisites : • grid token / JAliEn access
#                 • QC JSON at $QC_JSON (or default path below)
#                 • O2 / QC env already loaded (alienv/aliBuild shell …)
# -----------------------------------------------------------------------------

set -euo pipefail

LISTFILE="${1:-laser_paths.lst}"
LOGDIR="${2:-logs}"
QC_JSON="${QC_JSON:-$HOME/alice/QualityControl/Modules/FIT/FT0/etc/ft0-digits.json}"

mkdir -p "$LOGDIR"

echo ">>>  Running FT0 QC for every path in  $LISTFILE"
echo ">>>  Logs  →  $LOGDIR/*.log"
echo ">>>  QC cfg: $QC_JSON"
echo

# --- main loop ---------------------------------------------------------------
while read -r PATH_ALIEN; do
  [[ -z "$PATH_ALIEN" ]] && continue            # skip empty lines
  RUN=$(basename "$PATH_ALIEN")                 # e.g. 564587
  echo -e "\e[1;34m→ processing run $RUN\e[0m"

  # Use the reader’s --infile / --input (name changed between O2 tags – try both)
  # Redirect *everything* into a per-run logfile for post-mortem inspection.
  {
      echo "### $(date -u)  run $RUN  $PATH_ALIEN"
      o2-ft0-digits-reader-workflow --input "$PATH_ALIEN" -b \
          | o2-qc --config "json://$QC_JSON" -b
      echo "### exit code: $?"
  } &> "$LOGDIR/run_${RUN}.log"

  if grep -q "FATAL" "$LOGDIR/run_${RUN}.log"; then
      echo -e "   \e[31m✗ run $RUN – Fatal error, see log\e[0m"
  else
      echo -e "   \e[32m✓ run $RUN finished OK\e[0m"
  fi
done < "$LISTFILE"
