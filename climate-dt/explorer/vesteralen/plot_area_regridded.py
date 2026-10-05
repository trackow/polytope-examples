"""Vesterålen — MARS area subset with server-side regridding to a regular 0.05° lat/lon grid."""
import cartopy.crs as ccrs

from vesteralen_common import AREA, REGION, finish_map, local_map, open_store, parse_args, save

GRID = "0.05/0.05"

args = parse_args(__doc__)
cfg = args.cfg
store, ds = open_store(args.freq)

area_ds = ds[cfg["var"]].polytope.sel(model=cfg["model"], time=cfg["area_time"],
                                      area=AREA, grid=GRID)
var_name = list(area_ds.data_vars)[0]
field = area_ds[var_name]
if args.hourly:
    field = field.sel(time=cfg["time"])
field = field.squeeze()

fig, ax = local_map()
field.plot(ax=ax, transform=ccrs.PlateCarree(), cmap="RdYlBu_r", cbar_kwargs={"label": "K"})
finish_map(ax)
ax.set_title(f"{cfg['model']} — {var_name} — {cfg['label']}\n({REGION}, {GRID.split('/')[0]}° grid, server-side regridding)")
save(fig, f"vesteralen_area_regridded_{args.freq}.png")
print(f"grid {dict(field.sizes)}, {float(field.min()):.1f}–{float(field.max()):.1f} K")
