"""Vesterålen — Polytope timeseries feature at Sortland."""
import matplotlib.pyplot as plt
import pandas as pd

from vesteralen_common import POINT, POINT_NAME, REGION, open_store, parse_args, save

args = parse_args(__doc__)
cfg = args.cfg
# The notebooks take point timeseries from the standard-resolution store
store, ds = open_store(args.freq, resolution="standard")

ts_result = ds[cfg["var"]].polytope.sel(model=cfg["ts_model"], time=cfg["ts_time"], point=POINT)

cov = ts_result["coverages"][0]
times = pd.to_datetime(cov["domain"]["axes"]["t"]["values"])
values = cov["ranges"][cfg["var"]]["values"]

fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(times, values, marker="o", markersize=3 if args.hourly else 6)
ax.set_ylabel(f"{cfg['var']} (K)")
ax.set_title(f"{cfg['ts_model']} — {cfg['var']} — {POINT_NAME}, {REGION} "
             f"({cfg['ts_time'].start} to {cfg['ts_time'].stop}, {args.freq})")
ax.grid(True, alpha=0.3)
fig.autofmt_xdate()
save(fig, f"vesteralen_point_timeseries_{args.freq}.png")
