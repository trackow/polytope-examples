"""MHW 2024 (cf. Gonzalez et al. 2025, Fig. 1c): temperature along the transect off Eggum, 0–260 m,
on the peak day (15 Aug 2024), for the three storyline climates and Tplus2.0K – hist.

One area request per FESOM level (1–33, i.e. 0–260 m) and climate, then the 0.05° field is
interpolated to the 13 stations."""
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from mhw_common import (CLIMATE_LABEL, CLIMATES, FESOM_BOUNDS, N_STATIONS, PEAK, TRANSECT_AREA,
                        area_field, open_storyline, save, transect_points)

LEVELS = range(1, 34)   # 0–260 m

store, ds = open_storyline("o3d")
lats, lons = transect_points()
pts_lat = xr.DataArray(lats, dims="station")
pts_lon = xr.DataArray(lons, dims="station")

sections = {}
for c in CLIMATES:
    cols = []
    for lev in LEVELS:
        f = area_field(ds, "avg_thetao", c, PEAK, TRANSECT_AREA, level=lev, tag="eggum")
        cols.append(f.interp(latitude=pts_lat, longitude=pts_lon, method="nearest").values - 273.15)
    sections[c] = np.array(cols)   # (level, station)
    print(f"{c}: done")

stations = np.arange(N_STATIONS)
depth_edges = FESOM_BOUNDS[: len(LEVELS) + 1]
fig, axes = plt.subplots(1, 4, figsize=(22, 5), layout="constrained", sharey=True)
for ax, c in zip(axes, CLIMATES):
    m = ax.pcolormesh(np.arange(N_STATIONS + 1) - 0.5, depth_edges, sections[c], cmap="coolwarm", vmin=0, vmax=17)
    ax.set_title(CLIMATE_LABEL[c])
fig.colorbar(m, ax=axes[:3], label="temperature (°C)", shrink=0.9)
diff = sections["Tplus2.0K"] - sections["hist"]
d = axes[3].pcolormesh(np.arange(N_STATIONS + 1) - 0.5, depth_edges, diff, cmap="RdBu_r", vmin=-3, vmax=3)
axes[3].set_title("Tplus2.0K − hist")
fig.colorbar(d, ax=axes[3], label="ΔT (K)", shrink=0.9)
for ax in axes:
    ax.set_xlim(N_STATIONS - 0.5, -0.5)   # offshore (12) on the left, Eggum (0) on the right, as in Fig. 1c
    ax.set_xticks(stations[::2])
    ax.set_xlabel("station")
    ax.set_facecolor("white")
axes[0].set_ylim(depth_edges[-1], 0)
axes[0].set_ylabel("depth (m)")
fig.suptitle(f"IFS-FESOM storylines — temperature along the Eggum transect on {PEAK} (white: below the model seafloor)")
save(fig, "mhw2024_fig1c_eggum_transect.png", tight=False)

top = slice(0, 6)   # levels 1-6, 0-30 m
for c in CLIMATES:
    s = sections[c]
    warm = [FESOM_BOUNDS[np.where(s[:, i] >= 12)[0].max() + 1] if np.any(s[:, i] >= 12) else 0 for i in stations]
    print(f"{c:10s} 0-30 m mean {np.nanmean(s[top]):5.2f} °C, max {np.nanmax(s):5.2f} °C, "
          f"depth of 12 °C water per station: {[int(x) for x in warm]}")
print(f"Tplus2.0K − hist: 0-30 m {np.nanmean(diff[top]):+.2f} K, 50-250 m {np.nanmean(diff[10:]):+.2f} K")
