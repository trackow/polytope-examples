"""MHW 2024: NAO and sea level pressure in the three storyline climates.

Gonzalez et al. point to a record positive NAO in July–August 2024 (the largest Azores–Iceland
pressure difference since 1948). The storyline atmosphere is nudged towards the observed weather,
so the large-scale pressure pattern should be almost the same in cont, hist and Tplus2.0K.

Two figures:
- mean sea level pressure, July–August 2024 mean (6-hourly), North Atlantic, 0.5° grid
- station-based NAO: hourly msl at Ponta Delgada (Azores) minus Reykjavík, June–September 2024
"""
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from mhw_common import (CLIMATE_COLOR, CLIMATE_LABEL, CLIMATES, EVENT, _cached, area_field,
                        open_storyline, save)

NATL = (80.0, -70.0, 20.0, 40.0)   # north, west, south, east
JA = ("2024-07-01T00:00", "2024-08-31T18:00")
JJAS = ("2024-06-01T00:00", "2024-09-30T23:00")
STATIONS = {"Ponta Delgada": (37.74, -25.67), "Reykjavík": (64.13, -21.94)}

# ── 1. SLP maps, July–August mean ─────────────────────────────────
store, ds = open_storyline("sfc", hours=[0, 6, 12, 18])
slp = {}
for c in CLIMATES:
    f = area_field(ds, "msl", c, slice(*JA), NATL, tag="natl_6h", grid="0.5/0.5")
    print(f"{c}: {f.sizes['time']} time steps")
    slp[c] = f.mean("time") / 100.0

proj = ccrs.LambertConformal(central_longitude=-15, central_latitude=50)
fig, axes = plt.subplots(1, 5, figsize=(26, 6), layout="constrained", subplot_kw={"projection": proj})
levels = np.arange(996, 1030, 2)
for ax, c in zip(axes, CLIMATES):
    m = slp[c].plot.contourf(ax=ax, transform=ccrs.PlateCarree(), levels=levels, cmap="RdYlBu_r",
                             extend="both", add_colorbar=False)
    slp[c].plot.contour(ax=ax, transform=ccrs.PlateCarree(), levels=levels, colors="k", linewidths=0.4)
    ax.set_title(CLIMATE_LABEL[c])
fig.colorbar(m, ax=axes[:3], label="msl (hPa)", shrink=0.85)
for ax, (a, b) in zip(axes[3:], [("Tplus2.0K", "hist"), ("cont", "hist")]):
    d = (slp[a] - slp[b]).plot(ax=ax, transform=ccrs.PlateCarree(), cmap="RdBu_r", vmin=-2, vmax=2,
                               add_colorbar=False)
    ax.set_title(f"{a} − {b}")
fig.colorbar(d, ax=axes[3:], label="Δmsl (hPa)", shrink=0.85)
for ax in axes:
    ax.add_feature(cfeature.COASTLINE.with_scale("110m"), linewidth=0.5)
    for name, (la, lo) in STATIONS.items():
        ax.plot(lo, la, "k^", ms=6, transform=ccrs.PlateCarree())
    ax.set_extent([NATL[1], NATL[3], NATL[2], NATL[0]], crs=ccrs.PlateCarree())
    ax.gridlines(linewidth=0.3, alpha=0.5)
fig.suptitle("IFS-FESOM storylines — mean sea level pressure, July–August 2024 mean (▲ Ponta Delgada, Reykjavík)")
save(fig, "mhw2024_nao_slp_maps.png", tight=False)

for a, b in [("Tplus2.0K", "hist"), ("cont", "hist")]:
    d = slp[a] - slp[b]
    print(f"{a} − {b}: mean {float(d.mean()):+.2f} hPa, range {float(d.min()):+.2f} to {float(d.max()):+.2f} hPa")

# ── 2. Station-based NAO (hourly) ─────────────────────────────────
store_h, ds_h = open_storyline("sfc")


def station_msl(c, name):
    lat, lon = STATIONS[name]

    def fetch():
        r = ds_h["msl"].polytope.sel(climate=c, time=slice(*JJAS), point=(lat, lon))
        cov = r["coverages"][0]
        t = pd.to_datetime(cov["domain"]["axes"]["t"]["values"])
        return xr.DataArray(np.array(cov["ranges"]["msl"]["values"], float) / 100.0,
                            dims="time", coords={"time": t.tz_localize(None)}, name="msl")

    return _cached(f"msl_{c}_{name.replace(' ', '_').replace('í', 'i')}_jjas", fetch)


fig, axes = plt.subplots(2, 1, figsize=(11, 7), sharex=True, layout="constrained")
print(f"\n{'climate':10s} {'Jul ΔSLP':>9s} {'Aug ΔSLP':>9s} {'Jul-Aug':>8s}  (hPa, Ponta Delgada − Reykjavík)")
nao = {}
for c in CLIMATES:
    az, ic = station_msl(c, "Ponta Delgada"), station_msl(c, "Reykjavík")
    nao[c] = (az - ic)
    daily = nao[c].resample(time="1D").mean()
    axes[0].plot(az["time"], az, color=CLIMATE_COLOR[c], lw=0.8, label=f"{CLIMATE_LABEL[c]}, Ponta Delgada")
    axes[0].plot(ic["time"], ic, color=CLIMATE_COLOR[c], lw=0.8, ls="--")
    axes[1].plot(daily["time"], daily, color=CLIMATE_COLOR[c], lw=1.6, label=CLIMATE_LABEL[c])
    m = nao[c].groupby("time.month").mean()
    print(f"{c:10s} {float(m.sel(month=7)):9.1f} {float(m.sel(month=8)):9.1f} "
          f"{float(nao[c].sel(time=slice('2024-07-01', '2024-08-31')).mean()):8.1f}")
for ax in axes:
    ax.axvspan(pd.Timestamp(EVENT[0]), pd.Timestamp(EVENT[1]), color="#f4a582", alpha=0.35)
    ax.grid(True, alpha=0.3)
axes[0].set_ylabel("msl (hPa)")
axes[0].set_title("Hourly msl: Ponta Delgada (solid) and Reykjavík (dashed)")
axes[0].legend(fontsize=8, ncol=3, loc="lower left")
axes[1].set_ylabel("Δmsl (hPa)")
axes[1].set_title("Station NAO: Ponta Delgada − Reykjavík, daily mean (shaded: observed MHW)")
axes[1].legend(fontsize=9)
axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
save(fig, "mhw2024_nao_station.png", tight=False)

r1 = np.corrcoef(nao["hist"], nao["cont"])[0, 1]
r2 = np.corrcoef(nao["hist"], nao["Tplus2.0K"])[0, 1]
print(f"hourly correlation with hist: cont {r1:.3f}, Tplus2.0K {r2:.3f}")
