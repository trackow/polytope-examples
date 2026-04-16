# DestinE Climate Change Analysis

Analyse climate change signals from **DestinE Climate DT Generation 2** simulations and browse the whole Climate DT catalogue via streaming.
The first example downloads monthly data (`clmn` stream) via Earthkit and [Polytope](https://github.com/ecmwf/polytope-client), computes 30-year mean differences between historical and SSP3-7.0 scenario experiments, and plots the results on the HEALPix grid.
The second example opens the full Generation 2 portfolio as a lazy xarray Dataset: monthly data are streamed on access and not saved to disk. Batched multi-year requests (via `.polytope.sel()`) make it efficient enough for full climate change analysis, entirely via streaming.

Supports the three Climate DT models **IFS-NEMO**, **IFS-FESOM**, and **ICON**.

## Quick start

### 1. Clone this repository

```bash
git clone https://github.com/trackow/polytope-climatedt-analysis.git
cd polytope-climatedt-analysis
```

### 2. Set up the Python environment

**Option A: Using conda**

```bash
conda env create -f environment.yaml
conda activate destine-analysis
```

**Option B: Using venv (no conda required)**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

> **Important:** `polytope_zarr.py` requires **zarr v2** (`zarr>=2.18,<3`) and `numcodecs<0.16`.
> These pins are included in `requirements.txt`.  Installing zarr v3 will cause a `TypeError: Unsupported type for store_like` error.

If you are running on the **DESP**, the packages may already be available. Just make sure your kernel has `earthkit-data` and `healpy` installed, and that `zarr<3` is pinned.

### 3. Authenticate (once per session)

Open and run **`01_key_destine_once.ipynb`**. This will:

1. Clone the [polytope-examples](https://github.com/destination-earth-digital-twins/polytope-examples) repository (once)
2. Run the DestinE authentication script. You will be prompted for your **DESP credentials**
3. Store the API key in `~/.polytopeapirc`

You only need to do this **once**. All subsequent notebooks will pick up the key automatically.

### 4. Run the climate change analysis

Open and run **`02_climate_change_destine.ipynb`**. This notebook:

1. Downloads monthly 2m temperature data for the configured models and time periods
2. Computes the climate change signal (scenario mean minus historical mean)
3. Plots the result on the HEALPix grid, as a Mollweide map
4. Prints global-mean temperature change per model

The Configuration is in the beginning: you can change models, variables, time periods, and experiments there.

### 5. Browse the full data portfolio (lazy)

Open **`03_lazy_browse_portfolio.ipynb`** (monthly) or **`04_lazy_browse_portfolio_hourly.ipynb`** (hourly/daily). These notebooks:

1. Opens the entire Climate DT monthly portfolio as an **instant xarray Dataset**, no data is downloaded
2. Variables, coordinates, and attributes appear immediately
3. Data is fetched via Polytope **only when you access values** (e.g. plotting, `.values`, `.compute()`)
4. Supports all 6 levtypes: `sfc`, `pl`, `hl`, `sol`, `o2d`, `o3d`: just uncomment the desired `LEVTYPE` in the configuration cell

The variable catalogue is defined in `destine_portfolio.py` (65 variables across all levtypes).

### 6. Look up variables and get access snippets

Open **`05_variable_lookup.ipynb`**. This notebook lets you:

1. Search for variables by **shortName** (e.g. `"2t"`, `"avg_tos"`) or by **keyword** in the long name (e.g. `"temperature"`, `"wind"`)
2. See which streams (monthly, hourly, storyline), levtypes, models, and experiments each variable is available in
3. Generate a **ready-to-copy** `from_climate_dt()` code snippet with `access_snippet()`
4. Browse the full catalogue of all ~190 variable × stream combinations
5. Inspect the **last Polytope request** sent by the store — useful for reuse with `earthkit.data` directly:
   ```python
   r = store.last_request
   data = earthkit.data.from_source("polytope", r["collection"], r["request"],
                                    address=r["address"], stream=False)
   field = data.to_numpy()
   ```

### CLTE (hourly) stream — variable summary

The hourly (`clte`) stream provides 64 variables across 6 levtypes.
Atmosphere fields (sfc, pl, hl, sol) are hourly; ocean/ice (o2d, o3d) are daily means.

| levtype | # vars | time res | description |
|---------|--------|----------|-------------|
| sfc (instant) | 14 | hourly | Standard shortNames (no `avg_` prefix): `tclw`, `tciw`, `sp`, `tcw`, `tcwv`, `sd`, `msl`, `tcc`, `10u`, `10v`, `2t`, `2d`, `10si`, `skt` |
| sfc (hourly mean) | 20 | hourly | Fluxes / radiation — keep `avg_` prefix: `avg_surfror` … `avg_tprate` |
| pl | 9 | hourly | 19 pressure levels (1000–1 hPa): `pv`, `z`, `t`, `u`, `v`, `q`, `w`, `r`, `clwc` |
| hl | 2 | hourly | 100 m only, IFS-only: `u`, `v` |
| sol | 2 | hourly | Snow (1–5) + soil (1–4/5): `sd`, `vsw` |
| o2d | 12 | daily | Sea ice (6) + ocean surface (6), `avg_` prefix |
| o3d | 5 | daily | 3-D ocean (up to 75 levels), `avg_` prefix |

> **Key difference from clmn:** SFC instantaneous, PL, HL, and SOL params use standard ECMWF shortNames
> (e.g. `2t` instead of `avg_2t`). Also `10si` instead of `10ws` for 10 m wind speed.
> Ocean/ice fields remain `avg_`-prefixed (daily means).

> **Note:** For multi-level levtypes (`pl`, `hl`, `sol`, `o3d`) you need to select a specific level when plotting, e.g.
> ```python
> ds["avg_t"].sel(model="ICON", time="2014-06-01", level=850)
> ```
> Without `.sel(level=...)`, xarray will try to fetch data for **all** levels at once.

## Files

| File | Description |
|------|-------------|
| `01_key_destine_once.ipynb` | One-time authentication — stores your API key in order to access Climate DT data |
| `02_climate_change_destine.ipynb` | Climate change analysis notebook (batch download, 30-year means) |
| `03_lazy_browse_portfolio.ipynb` | Lazy browsing of the full Climate DT monthly (clmn) portfolio |
| `04_lazy_browse_portfolio_hourly.ipynb` | Lazy browsing of the hourly (clte) portfolio |
| `05_variable_lookup.ipynb` | Variable discovery — search by name/keyword, generate `from_climate_dt()` snippets |
| `TEST_03_monthly_test_server.ipynb` | Monthly (clmn) tests on `polytope-test.mn5` — lazy browse, area, timeseries, bbox, polygon |
| `TEST_04_hourly_test_server.ipynb` | Hourly (clte) tests on `polytope-test.mn5` — lazy browse, area, timeseries, bbox, polygon |
| `destine_climate_helpers.py` | Helper module (polytope request handling, caching, data retrieval, chunking over years) |
| `destine_portfolio.py` | Data portfolio — clmn (65 vars) and clte (64 vars) across 6 levtypes, plus `find_variable()` and `access_snippet()` lookup helpers |
| `polytope_zarr.py` | Virtual zarr store backed by Polytope (lazy chunk fetching) |
| `requirements.txt` | Python dependencies with version pins (zarr v2, numcodecs) |

## Configuration options for `02_climate_change_destine.ipynb`

All options are in the configuration cell of the notebook:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PARAM` | `'avg_2t'` | Variable to analyse (e.g. `'avg_2t'`, `'235043'` for precip) |
| `MODELS` | `['IFS-NEMO', 'IFS-FESOM']` | Models to include (add `'ICON'` when available) |
| `RESOLUTION` | `'standard'` | Grid resolution (`'standard'` = H128, `'high'` = H1024) |
| `HIST_YEARS` | `range(1990, 2015)` | Historical period |
| `HIST_EXPERIMENT` | `'hist'` | Historical experiment name |
| `SCEN_YEARS` | `range(2015, 2050)` | Scenario period |
| `SCEN_EXPERIMENT` | `'SSP3-7.0'` | Scenario experiment name |
| `STORE_DATA` | `True` | Cache downloaded data as per-year NetCDF files |
| `DATA_DIR` | `'./data'` | Directory for cached data |

## Data caching

When `STORE_DATA = True`, downloaded data are saved as individual NetCDF files per year:

```
data/
├── IFS-NEMO/
│   ├── hist/clmn/standard/
│   │   ├── avg_2t_1990.nc
│   │   ├── avg_2t_1991.nc
│   │   └── ...
│   └── SSP3-7.0/clmn/standard/
│       ├── avg_2t_2015.nc
│       └── ...
├── IFS-FESOM/
│   └── ...
└── ICON/
    └── ...
```

Re-running the notebook skips years that are already cached. You do not need to think about this step.

## Requirements

- Python ≥ 3.10
- A valid [DESP account](https://platform.destine.eu/) for Climate DT data, with upgraded access
- Python packages defined in [requirements.txt](requirements.txt) or [environment.yaml](environment.yaml) respectively
