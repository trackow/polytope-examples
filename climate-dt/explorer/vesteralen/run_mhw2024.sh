#!/usr/bin/env bash
# August 2024 marine heatwave, storyline figures (see MHW2024.md). Downloads are cached in ./cache
set -u
cd "$(dirname "$0")"
PY="${PYTHON:-../../../.venv/bin/python}"
fail=0
for s in mhw_fig1a_sst_timeseries mhw_fig1b_sst_maps mhw_fig1c_transect mhw_nao; do
  echo "=== $s"
  "$PY" "$s.py" || { echo "FAILED: $s"; fail=1; }
done
exit $fail
