"""MHW 2024 (cf. Gonzalez et al. 2025, Fig. 1a): daily SST averaged over the box, June–September 2024,
for the three storyline climates. Absolute temperatures, no baseline yet."""
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from mhw_common import (CLIMATE_COLOR, CLIMATE_LABEL, CLIMATES, EVENT, MAP_AREA, PEAK, PERIOD,
                        area_field, box_mean, open_storyline, save)

store, ds = open_storyline("o2d")

fig, ax = plt.subplots(figsize=(10, 4.2))
ax.axvspan(pd.Timestamp(EVENT[0]), pd.Timestamp(EVENT[1]), color="#f4a582", alpha=0.35,
           label="observed MHW (Gonzalez et al.)")
print(f"{'climate':10s} {'Jun mean':>8s} {'peak day':>11s} {'max':>6s} {PEAK:>11s}  (°C, box mean)")
for c in CLIMATES:
    sst = area_field(ds, "avg_tos", c, slice(*PERIOD), MAP_AREA, tag="box")
    ts = box_mean(sst) - 273.15
    ax.plot(ts["time"], ts, color=CLIMATE_COLOR[c], lw=1.8, label=CLIMATE_LABEL[c])
    jun = float(ts.sel(time=slice("2024-06-01", "2024-06-30")).mean())
    imax = int(ts.argmax())
    print(f"{c:10s} {jun:8.2f} {str(ts['time'].values[imax])[:10]:>11s} {float(ts[imax]):6.2f} "
          f"{float(ts.sel(time=PEAK, method='nearest')):11.2f}")

ax.set_ylabel("SST (°C)")
ax.set_title("IFS-FESOM storylines — SST averaged over the Fig. 1b box (northern Norway shelf), 2024")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
ax.grid(True, alpha=0.3)
ax.legend(loc="upper left", fontsize=9)
save(fig, "mhw2024_fig1a_box_sst.png")
