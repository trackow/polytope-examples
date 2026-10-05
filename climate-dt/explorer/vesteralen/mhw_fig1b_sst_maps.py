"""MHW 2024 (cf. Gonzalez et al. 2025, Fig. 1b): SST on the peak day (15 Aug 2024) for the three
storyline climates, plus the Tplus2.0K – hist difference. Box and Eggum transect drawn on top."""
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt

from mhw_common import (BOX, CLIMATE_LABEL, CLIMATES, EGGUM, MAP_AREA, PEAK, PERIOD, area_field,
                        open_storyline, save, transect_points)

store, ds = open_storyline("o2d")
# reuse the cached June–September download instead of a new request
sst = {c: area_field(ds, "avg_tos", c, slice(*PERIOD), MAP_AREA, tag="box").sel(time=PEAK) - 273.15
       for c in CLIMATES}
n, w, s, e = MAP_AREA
proj = ccrs.LambertConformal(central_longitude=17, central_latitude=70)


def decorate(ax):
    ax.add_feature(cfeature.LAND.with_scale("50m"), facecolor="0.85", zorder=2)
    ax.add_feature(cfeature.COASTLINE.with_scale("50m"), linewidth=0.5, zorder=3)
    ax.plot([p[1] for p in BOX], [p[0] for p in BOX], "k--", lw=1, transform=ccrs.PlateCarree(), zorder=4)
    lats, lons = transect_points()
    ax.plot(lons, lats, "s", ms=3, mfc="none", mec="k", transform=ccrs.PlateCarree(), zorder=4)
    ax.plot(EGGUM[1], EGGUM[0], "o", ms=4, color="tab:blue", transform=ccrs.PlateCarree(), zorder=5)
    ax.set_extent([w, e, s, n], crs=ccrs.PlateCarree())
    gl = ax.gridlines(draw_labels=True, linewidth=0.3, alpha=0.5, x_inline=False, y_inline=False)
    gl.top_labels = gl.right_labels = False


fig, axes = plt.subplots(1, 4, figsize=(22, 6.5), layout="constrained", subplot_kw={"projection": proj})
for ax, c in zip(axes, CLIMATES):
    m = sst[c].plot(ax=ax, transform=ccrs.PlateCarree(), cmap="coolwarm", vmin=4, vmax=19, add_colorbar=False)
    decorate(ax)
    ax.set_title(CLIMATE_LABEL[c])
fig.colorbar(m, ax=axes[:3], label="SST (°C)", shrink=0.85)

diff = sst["Tplus2.0K"] - sst["hist"]
d = diff.plot(ax=axes[3], transform=ccrs.PlateCarree(), cmap="RdBu_r", vmin=-3, vmax=3, add_colorbar=False)
decorate(axes[3])
axes[3].set_title("Tplus2.0K − hist")
fig.colorbar(d, ax=axes[3], label="ΔSST (K)", shrink=0.85)
fig.suptitle(f"IFS-FESOM storylines — SST on {PEAK} (peak of the observed MHW) — dashed: Fig. 1b box (traced), squares: Eggum transect")
save(fig, "mhw2024_fig1b_sst_maps.png", tight=False)

for c in CLIMATES:
    print(f"{c:10s} SST {PEAK}: {float(sst[c].min()):.1f}–{float(sst[c].max()):.1f} °C")
print(f"Tplus2.0K − hist: mean {float(diff.mean()):+.2f} K, range {float(diff.min()):+.2f} to {float(diff.max()):+.2f} K")
