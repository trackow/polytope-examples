"""Vesterålen — IFS-NEMO daily ocean field from EarthDataHub (Climate DT, high res, 1990–2014).

EarthDataHub serves the data as Zarr v3 on a regular ~0.04° lat/lon grid, so there's no
Polytope request here, only a lazy xarray open plus a subset. Needs zarr >= 3, so it runs in
its own venv (.venv-edh, see README), not the Polytope one.

The API key is not the Polytope key. Get it on the DestinE platform under "Quota & API Keys",
then either export EDH_API_KEY=... or put it in ~/.netrc (see README).
"""
import argparse
import os
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import xarray as xr

HERE = Path(__file__).resolve().parent
DATASET = "climate-dt-2/IFS-NEMO-hist-o2d-daily-high-v0.zarr"

# same region as vesteralen_common.py (kept separate, that module needs the Polytope stack)
REGION = "Vesterålen"
SOUTH, WEST, NORTH, EAST = 68.3, 13.8, 69.4, 16.6
EXTENT = [13.5, 17.0, 68.2, 69.5]


def open_edh():
    key = os.environ.get("EDH_API_KEY")
    if key:
        return xr.open_dataset(f"https://edh:{key}@api.earthdatahub.destine.eu/{DATASET}",
                               chunks={}, engine="zarr", zarr_format=3)
    # no env var: fall back to ~/.netrc
    return xr.open_dataset(f"https://api.earthdatahub.destine.eu/{DATASET}",
                           chunks={}, engine="zarr", zarr_format=3,
                           storage_options={"client_kwargs": {"trust_env": True}})


def subset(da):
    lat, lon = da["latitude"], da["longitude"]
    west, east = WEST, EAST
    if float(lon.max()) > 180:  # 0..360 convention
        west, east = west % 360, east % 360
    lat_slice = slice(SOUTH, NORTH) if lat[0] < lat[-1] else slice(NORTH, SOUTH)
    return da.sel(latitude=lat_slice, longitude=slice(west, east))


p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--var", default="avg_tos", help="variable name (default: avg_tos, SST)")
p.add_argument("--date", default="2014-01-15")
args = p.parse_args()

ds = open_edh()
print(ds)
if args.var not in ds:
    raise SystemExit(f"{args.var} not in dataset, pick one of: {', '.join(ds.data_vars)}")

da = subset(ds[args.var]).sel(time=args.date, method="nearest").load()
units = da.attrs.get("units", "")
print(f"\n{args.var} on {str(da['time'].values)[:10]}: grid {dict(da.sizes)}, "
      f"{float(da.min()):.2f}–{float(da.max()):.2f} {units}")

fig, ax = plt.subplots(subplot_kw={"projection": ccrs.Mercator()}, figsize=(9, 8))
da.plot(ax=ax, transform=ccrs.PlateCarree(), cmap="RdYlBu_r",
        cbar_kwargs={"label": f"{args.var} ({units})", "shrink": 0.8})
ax.add_feature(cfeature.LAND.with_scale("10m"), facecolor="0.85", zorder=2)
ax.add_feature(cfeature.COASTLINE.with_scale("10m"), linewidth=0.7, zorder=3)
ax.set_extent(EXTENT, crs=ccrs.PlateCarree())
gl = ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
gl.top_labels = False
gl.right_labels = False
ax.set_title(f"IFS-NEMO — {args.var} — {str(da['time'].values)[:10]} — {REGION}\n"
             "(EarthDataHub, Climate DT hist, daily, ~0.04°)")

out = HERE / "plots" / f"vesteralen_edh_nemo_{args.var}.png"
out.parent.mkdir(exist_ok=True)
fig.tight_layout()
fig.savefig(out, dpi=150)
print(f"saved {out}")
