"""Vesterålen — MARS area subset with server-side regridding to 0.05°, ICON vs IFS-FESOM vs IFS-NEMO."""
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np

from vesteralen_common import (AREA, MODELS, REGION, finish_map, land_mask, open_store,
                               parse_args, save)

GRID = "0.05/0.05"

args = parse_args(__doc__)
cfg = args.cfg
store, ds = open_store(args.freq)

fields = {}
for model in MODELS:
    area_ds = ds[cfg["var"]].polytope.sel(model=model, time=cfg["area_time"],
                                          area=AREA, grid=GRID)
    var_name = list(area_ds.data_vars)[0]
    field = area_ds[var_name]
    if args.hourly:
        field = field.sel(time=cfg["time"])
    fields[model] = field.squeeze()

f0 = next(iter(fields.values()))
is_land = land_mask(f0["longitude"].values, f0["latitude"].values)

vmin = min(float(f.min()) for f in fields.values())
vmax = max(float(f.max()) for f in fields.values())

fig, axes = plt.subplots(1, len(MODELS), figsize=(15, 5.5), layout="constrained",
                         subplot_kw={"projection": ccrs.Mercator()})
print(f"{'model':10s} {'min':>6s} {'max':>6s} {'land':>6s} {'sea':>6s} {'sea-land':>8s}  (K)")
for ax, (model, field) in zip(axes, fields.items()):
    mesh = field.plot(ax=ax, transform=ccrs.PlateCarree(), cmap="RdYlBu_r",
                      vmin=vmin, vmax=vmax, add_colorbar=False)
    finish_map(ax)
    ax.set_title(model)
    v = field.values
    t_land, t_sea = np.nanmean(v[is_land]), np.nanmean(v[~is_land])
    print(f"{model:10s} {np.nanmin(v):6.1f} {np.nanmax(v):6.1f} {t_land:6.1f} {t_sea:6.1f} {t_sea - t_land:8.1f}")

fig.colorbar(mesh, ax=axes, label="K", shrink=0.9)
fig.suptitle(f"{var_name} — {cfg['label']} — {REGION}, {GRID.split('/')[0]}° grid, server-side regridding")
save(fig, f"vesteralen_area_regridded_{args.freq}.png", tight=False)
