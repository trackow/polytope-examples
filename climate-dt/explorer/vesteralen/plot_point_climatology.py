"""Vesterålen — seasonal cycle of 2 m temperature at Sortland, 1990–2014, all three models.

Shows the mean for each calendar month, with the shaded band covering ±1 standard deviation
across the 25 years.
"""
import calendar

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from vesteralen_common import MODELS, POINT, POINT_NAME, REGION, open_store, parse_args, save

parse_args(__doc__)
store, ds = open_store("monthly")

fig, ax = plt.subplots(figsize=(10, 4.5))
months = np.arange(1, 13)
print(f"{'model':10s} {'Jan':>6s} {'Jul':>6s} {'Jul-Jan':>7s} {'annual':>7s}  (K, 1990–2014 mean)")
for model in MODELS:
    ts = ds["avg_2t"].polytope.sel(model=model, time=slice("1990-01", "2014-12"), point=POINT)
    cov = ts["coverages"][0]
    s = pd.Series(np.array(cov["ranges"]["avg_2t"]["values"], dtype=float),
                  index=pd.to_datetime(cov["domain"]["axes"]["t"]["values"]))
    by_month = s.groupby(s.index.month)
    mean, std = by_month.mean(), by_month.std()
    line, = ax.plot(months, mean.values, marker="o", markersize=4, label=f"{model} ({len(s)} months)")
    ax.fill_between(months, mean - std, mean + std, color=line.get_color(), alpha=0.15)
    print(f"{model:10s} {mean[1]:6.1f} {mean[7]:6.1f} {mean[7] - mean[1]:7.1f} {s.mean():7.1f}")

ax.set_xticks(months, [calendar.month_abbr[m] for m in months])
ax.set_ylabel("avg_2t (K)")
ax.set_title(f"avg_2t — {POINT_NAME}, {REGION} — 1990–2014 monthly climatology (±1σ)")
ax.legend()
ax.grid(True, alpha=0.3)
save(fig, "vesteralen_point_climatology.png")
