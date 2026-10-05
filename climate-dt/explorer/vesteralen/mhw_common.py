"""Shared settings for the August 2024 marine heatwave (MHW) scripts.

Storyline runs (IFS-FESOM, activity "story-nudging"): the atmosphere is nudged towards the
observed weather, the ocean runs freely. Three climates: cont (pre-industrial), hist (present
day), Tplus2.0K (+2 K). The set-up follows Gonzalez et al. (2025), Commun. Earth Environ. 6, 639,
Fig. 1: box mean SST, map on the peak day, and a 0–250 m transect off Eggum.

All downloads are cached as NetCDF in ./cache, so plots can be redone without new requests.
"""
from pathlib import Path

import numpy as np
import xarray as xr

from vesteralen_common import OUT_DIR, PolytopeZarrStore, save  # noqa: F401  (also sets up paths/logging)

CACHE = Path(__file__).resolve().parent / "cache"

CLIMATES = ["cont", "hist", "Tplus2.0K"]
CLIMATE_LABEL = {"cont": "pre-industrial (cont)", "hist": "present day (hist)", "Tplus2.0K": "+2 K (Tplus2.0K)"}
CLIMATE_COLOR = {"cont": "#4c72b0", "hist": "#222222", "Tplus2.0K": "#c44e52"}

PERIOD = ("2024-06-01", "2024-10-01")
EVENT = ("2024-08-05", "2024-08-26")   # MHW dates from Fig. 1a
PEAK = "2024-08-15"                    # day of maximum intensity

GRID = "0.05/0.05"
# area for the SST maps and box mean (north, west, south, east)
MAP_AREA = (76.0, 5.0, 65.0, 29.5)

# Fig. 1b box, traced by eye from the published map (corners: lat, lon) -- approximate
BOX = [(68.26, 5.94), (65.32, 16.41), (71.22, 28.87), (75.66, 18.54), (68.26, 5.94)]

# Fig. 1c transect off Eggum (Vestvågøy), station 0 at the coast, station 12 offshore
EGGUM = (68.31, 13.66)
TRANSECT_START = (68.36, 13.55)   # station 0
TRANSECT_END = (70.48, 9.45)      # station 12
N_STATIONS = 13
TRANSECT_AREA = (70.8, 9.0, 68.1, 14.0)

# FESOM (NG5) level bounds in m, levels 1-35 = 0-300 m (Climate DT user guide, Ocean Model Levels)
FESOM_BOUNDS = np.array([0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
                         95, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 220, 240, 260,
                         280, 300], dtype=float)
FESOM_MID = 0.5 * (FESOM_BOUNDS[:-1] + FESOM_BOUNDS[1:])   # index i -> level i+1

# depth bands of the aquaculture system types (infographic), as FESOM levels
SYSTEM_BANDS = {
    "A open cages (0-30 m)": (1, 6),
    "D semi-closed (10-50 m)": (3, 10),
    "C closed (20-100 m)": (5, 20),
    "E submerged (30-100 m)": (7, 20),
    "F offshore (50-200 m)": (11, 30),
    "B deep intake (100-200 m)": (21, 30),
}


def open_storyline(levtype, hours=None):
    """Storyline store, high resolution (nside 512 output), June-October 2024.

    hours: e.g. [0, 6, 12, 18] to request only those hours from hourly fields. The storyline
    path of from_climate_dt doesn't take filter_hours, so it is set on the store directly
    (area and point requests read it from there).
    """
    store = PolytopeZarrStore.from_climate_dt(
        models=["IFS-FESOM"], experiment=CLIMATES, activity="story-nudging",
        levtype=levtype, frequency="hourly", resolution="high",
        start_date="2024-06-01", end_date="2024-10-31")
    if hours is not None:
        store._filter_hours = hours
    return store, store.open()


def _cached(name, fetch):
    CACHE.mkdir(exist_ok=True)
    path = CACHE / f"{name}.nc"
    if path.exists():
        return xr.open_dataarray(path)
    da = fetch()
    da.name = da.name or "value"
    da = da.drop_attrs(deep=True)   # earthkit leaves GRIB handles in attrs
    da.to_netcdf(path)
    return da


def area_field(ds, var, climate, time, area, level=None, tag="", grid=GRID):
    """Server-side regridded field (area + grid), cached."""
    lev = f"_L{level}" if level else ""
    t = f"{time.start}_{time.stop}" if isinstance(time, slice) else time
    name = f"{var}_{climate}{lev}_{t}_{tag}".replace(":", "")

    def fetch():
        kw = {"level": level} if level else {}
        out = ds[var].polytope.sel(climate=climate, time=time, area=area, grid=grid, **kw)
        da = out[list(out.data_vars)[0]]
        drop = [d for d in da.dims if d not in ("time", "latitude", "longitude") and da.sizes[d] == 1]
        return da.squeeze(drop).load()

    return _cached(name, fetch)


def box_mask(lat, lon):
    import shapely
    poly = shapely.Polygon([(p[1], p[0]) for p in BOX])
    lon2d, lat2d = np.meshgrid(lon, lat)
    return shapely.contains_xy(poly, lon2d, lat2d)


def box_mean(da):
    """Area-weighted mean over the Fig. 1b box, ocean points only (land is NaN in the model)."""
    mask = xr.DataArray(box_mask(da.latitude.values, da.longitude.values),
                        dims=("latitude", "longitude"), coords={"latitude": da.latitude, "longitude": da.longitude})
    w = np.cos(np.deg2rad(da.latitude)) * mask
    return da.weighted(w.fillna(0)).mean(("latitude", "longitude"))


def transect_points():
    f = np.linspace(0, 1, N_STATIONS)
    lats = TRANSECT_START[0] + f * (TRANSECT_END[0] - TRANSECT_START[0])
    lons = TRANSECT_START[1] + f * (TRANSECT_END[1] - TRANSECT_START[1])
    return lats, lons
