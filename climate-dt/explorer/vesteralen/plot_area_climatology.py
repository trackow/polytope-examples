"""Vesterålen — January and July 2 m temperature, 1990–2014 mean, server-side regridded to 0.05°.

The historical runs are free-running, so single dates differ mostly by weather.
A multi-year mean is the fairer way to compare the three models.
"""
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np

from vesteralen_common import (AREA, MODELS, REGION, finish_map, land_mask, open_store,
                               parse_args, save)

GRID = "0.05/0.05"
YEARS = slice("1990-01", "2014-12")
MONTHS = {1: "January", 7: "July"}

parse_args(__doc__)
store, ds = open_store("monthly", filter_months=list(MONTHS))

clim = {}  # (month, model) -> 2-D mean field
for model in MODELS:
    area_ds = ds["avg_2t"].polytope.sel(model=model, time=YEARS, area=AREA, grid=GRID)
    da = area_ds[list(area_ds.data_vars)[0]]
    for m in MONTHS:
        sel = da.sel(time=da["time"].dt.month == m)
        print(f"{model}: {MONTHS[m]} — {sel.sizes['time']} years")
        clim[m, model] = sel.mean("time")

f0 = next(iter(clim.values()))
is_land = land_mask(f0["longitude"].values, f0["latitude"].values)

fig, axes = plt.subplots(len(MONTHS), len(MODELS), figsize=(15, 10.5), layout="constrained",
                         subplot_kw={"projection": ccrs.Mercator()})
print(f"\n{'':18s} {'land':>6s} {'sea':>6s} {'sea-land':>8s}  (K)")
for row, m in zip(axes, MONTHS):
    vmin = min(float(clim[m, mod].min()) for mod in MODELS)
    vmax = max(float(clim[m, mod].max()) for mod in MODELS)
    for ax, model in zip(row, MODELS):
        field = clim[m, model]
        mesh = field.plot(ax=ax, transform=ccrs.PlateCarree(), cmap="RdYlBu_r",
                          vmin=vmin, vmax=vmax, add_colorbar=False)
        finish_map(ax)
        ax.set_title(f"{model} — {MONTHS[m]}")
        v = field.values
        t_land, t_sea = np.nanmean(v[is_land]), np.nanmean(v[~is_land])
        print(f"{MONTHS[m]:8s} {model:9s} {t_land:6.1f} {t_sea:6.1f} {t_sea - t_land:8.1f}")
    fig.colorbar(mesh, ax=row, label="K", shrink=0.9)

fig.suptitle(f"avg_2t — 1990–2014 mean — {REGION}, 0.05° grid, server-side regridding")
save(fig, "vesteralen_area_climatology.png", tight=False)
