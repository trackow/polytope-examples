"""Vesterålen — Polytope timeseries feature at Sortland, ICON vs IFS-FESOM vs IFS-NEMO."""
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from vesteralen_common import MODELS, POINT, POINT_NAME, REGION, open_store, parse_args, save

args = parse_args(__doc__)
cfg = args.cfg
store, ds = open_store(args.freq)

fig, ax = plt.subplots(figsize=(10, 4))
for model in MODELS:
    ts_result = ds[cfg["var"]].polytope.sel(model=model, time=cfg["ts_time"], point=POINT)
    cov = ts_result["coverages"][0]
    times = pd.to_datetime(cov["domain"]["axes"]["t"]["values"])
    values = np.array(cov["ranges"][cfg["var"]]["values"], dtype=float)
    ax.plot(times, values, marker="o", markersize=3 if args.hourly else 5, label=model)
    print(f"{model:10s} min {values.min():.1f}  max {values.max():.1f}  mean {values.mean():.1f} K")

ax.set_ylabel(f"{cfg['var']} (K)")
ax.set_title(f"{cfg['var']} — {POINT_NAME}, {REGION} "
             f"({cfg['ts_time'].start} to {cfg['ts_time'].stop}, {args.freq})")
ax.legend()
ax.grid(True, alpha=0.3)
fig.autofmt_xdate()
save(fig, f"vesteralen_point_timeseries_{args.freq}.png")
