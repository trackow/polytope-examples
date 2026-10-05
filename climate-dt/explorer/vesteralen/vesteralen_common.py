"""Shared settings and helpers for the Vesterålen (Norway) plot scripts.

Mirrors the setup of 03_lazy_browse_portfolio.ipynb (monthly, clmn) and
04_lazy_browse_portfolio_hourly.ipynb (hourly, clte); every script takes
``--hourly`` to switch between the two.
"""
import argparse
import logging
import sys
import warnings
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # polytope_zarr.py lives in climate-dt/explorer

import matplotlib
matplotlib.use("Agg")
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import earthkit.data
import healpy as hp
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import PolyCollection

from polytope_zarr import PolytopeZarrStore

# Disable earthkit disk cache (polytope_zarr caches decoded arrays in memory)
earthkit.data.config.set("cache-policy", "off")
for _ln in ("polytope", "polytope.api", "earthkit.data", "urllib3"):
    logging.getLogger(_ln).setLevel(logging.WARNING)
warnings.filterwarnings("ignore", category=DeprecationWarning)

OUT_DIR = HERE / "plots"

# ── Region ────────────────────────────────────────────────────────
REGION = "Vesterålen"
BBOX = (68.3, 13.8, 69.4, 16.6)    # (south, west, north, east) — Polytope feature
AREA = (69.4, 13.8, 68.3, 16.6)    # (north, west, south, east) — MARS area + regridding
EXTENT = [13.5, 17.0, 68.2, 69.5]  # map extent (lon0, lon1, lat0, lat1)
POINT = (68.70, 15.41)             # Sortland
POINT_NAME = "Sortland"
# (lat, lon) polygon around Andøya, Langøya, Hadseløya and N/W Hinnøya,
# in the same list-of-rings structure as earthkit.geo.cartography.country_polygons()
SHAPES = [[
    (68.45, 13.90), (68.45, 15.30), (68.30, 15.80), (68.30, 16.60),
    (68.95, 16.60), (69.40, 16.30), (69.40, 15.30), (69.05, 14.40),
    (68.70, 13.90), (68.45, 13.90),
]]

# ── Per-frequency request settings (as in notebooks 03 / 04) ──────
CONFIG = {
    "monthly": dict(var="avg_2t", model="IFS-FESOM", time="2010-06", label="Jun 2010",
                    ts_model="IFS-FESOM", ts_time=slice("2010-01", "2010-12"),
                    area_time="2010-06"),
    "hourly": dict(var="2t", model="ICON", time="2014-01-01T12:00", label="2014-01-01 12:00",
                   ts_model="IFS-NEMO", ts_time=slice("2014-01-01T00:00", "2014-01-02T23:00"),
                   area_time=slice("2014-01-01T00:00", "2014-01-01T23:00")),
}


def parse_args(description):
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--hourly", action="store_true",
                   help="use the hourly (clte) stream instead of monthly means (clmn)")
    args = p.parse_args()
    args.freq = "hourly" if args.hourly else "monthly"
    args.cfg = CONFIG[args.freq]
    return args


def open_store(freq, resolution="high"):
    kwargs = dict(models=["ICON", "IFS-FESOM", "IFS-NEMO"], experiment="hist",
                  resolution=resolution, levtype="sfc")
    if freq == "hourly":
        kwargs.update(frequency="hourly", start_date="1990-01-01T00:00:00",
                      end_date="2014-12-31T23:00:00")
    else:
        kwargs.update(years=range(1990, 2015))
    store = PolytopeZarrStore.from_climate_dt(**kwargs)
    return store, store.open()


def local_map(figsize=(9, 8)):
    """Mercator axes over Vesterålen with high-resolution coastlines."""
    fig, ax = plt.subplots(subplot_kw={"projection": ccrs.Mercator()}, figsize=figsize)
    return fig, ax


def finish_map(ax):
    ax.add_feature(cfeature.COASTLINE.with_scale("10m"), linewidth=0.7)
    ax.add_feature(cfeature.BORDERS.with_scale("10m"), linewidth=0.5, linestyle="--")
    ax.set_extent(EXTENT, crs=ccrs.PlateCarree())
    gl = ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5)
    gl.top_labels = False
    gl.right_labels = False


def plot_healpix_cells(result, var, nside, title, outfile, shapes=None):
    """Render native HEALPix (NESTED) cells from a bbox/polygon feature result."""
    da = result[var].squeeze()
    lats = result.coords["latitude"].values
    lons = result.coords["longitude"].values
    values = da.values.ravel()

    pix_ids = hp.ang2pix(nside, np.radians(90.0 - lats), np.radians(lons), nest=True)
    boundaries = hp.boundaries(nside, pix_ids, step=50, nest=True)
    polygons = [list(zip(np.degrees(np.arctan2(by, bx)), np.degrees(np.arcsin(bz))))
                for bx, by, bz in boundaries]

    fig, ax = local_map()
    coll = PolyCollection(polygons, transform=ccrs.PlateCarree(),
                          edgecolors="face", linewidths=0.1)
    coll.set_array(values)
    coll.set_cmap(plt.cm.RdYlBu_r)
    coll.set_norm(mcolors.Normalize(vmin=np.nanmin(values), vmax=np.nanmax(values)))
    ax.add_collection(coll)
    for shape in shapes or []:
        ax.plot([pt[1] for pt in shape], [pt[0] for pt in shape],
                color="black", linewidth=1.2, transform=ccrs.PlateCarree())
    finish_map(ax)
    plt.colorbar(coll, ax=ax, label="K", shrink=0.8, pad=0.02)
    ax.set_title(title)
    save(fig, outfile)
    print(f"{len(values)} HEALPix cells, {np.nanmin(values):.1f}–{np.nanmax(values):.1f} K")


def save(fig, name):
    OUT_DIR.mkdir(exist_ok=True)
    path = OUT_DIR / name
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"saved {path}")
