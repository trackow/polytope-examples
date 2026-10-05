#!/usr/bin/env bash
# Produce all Vesterålen plots (monthly + hourly) into ./plots
set -u
cd "$(dirname "$0")"
PY="${PYTHON:-../../../.venv/bin/python}"
fail=0
for s in plot_bbox_healpix plot_polygon_healpix plot_area_regridded plot_point_timeseries; do
  for f in "" "--hourly"; do
    echo "=== $s $f"
    "$PY" "$s.py" $f || { echo "FAILED: $s $f"; fail=1; }
  done
done
exit $fail
