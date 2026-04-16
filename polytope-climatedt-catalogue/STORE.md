# How `polytope_zarr.py` works

`PolytopeZarrStore` is a virtual [zarr v2](https://zarr.readthedocs.io/en/v2/) store that presents DestinE Climate DT data as an xarray Dataset.
Metadata (variables, coordinates, dimensions, attributes) is available **instantly**; actual data is fetched from Polytope **only when accessed**.

The module also registers an xarray **`.polytope.sel()`** accessor that automatically batches multi-year requests into a single Polytope call.

This document walks through the full implementation (~700 lines).

---

## Table of contents

1. [The core idea](#the-core-idea)
2. [Factory classmethod](#factory-classmethod)
3. [Construction (low-level)](#construction)
4. [Two internal dicts](#two-internal-dicts)
5. [Metadata synthesis](#metadata-synthesis)
6. [Lazy chunk fetching & batching](#lazy-chunk-fetching--batching)
7. [Batch splitting](#batch-splitting-_split_batch)
8. [MutableMapping interface](#mutablemapping-interface)
9. [Convenience methods](#convenience-methods)
10. [The `.polytope.sel()` accessor](#the-polytopesel-accessor)
11. [Server-side spatial features](#server-side-spatial-features)
12. [Caching architecture](#caching-architecture)
13. [Data flow summary](#data-flow-summary)
14. [Why the zarr protocol limits us](#why-the-zarr-protocol-limits-us)

---

## The core idea

Zarr stores are just key → value mappings.  A zarr v2 store needs to serve:

| Key pattern | Content |
|---|---|
| `.zgroup` | `{"zarr_format": 2}` |
| `.zmetadata` | Consolidated metadata for all arrays |
| `<array>/.zarray` | Array shape, chunks, dtype, etc. |
| `<array>/.zattrs` | Array attributes (`_ARRAY_DIMENSIONS`, `long_name`, `units`) |
| `<array>/0` or `<array>/0.1.2` | Raw bytes for a data chunk |

`PolytopeZarrStore` implements Python's `MutableMapping` interface (`__getitem__`, `__contains__`, etc.), so zarr and xarray can treat it as a store.  The trick: **coordinate chunks are pre-computed in memory, but data variable chunks trigger a Polytope request on first access.**

---

## Factory classmethod

The recommended way to create a store for Climate DT data:

```python
store = PolytopeZarrStore.from_climate_dt(
    models=["ICON", "IFS-FESOM", "IFS-NEMO"],
    experiment="hist",
    levtype="sfc",
    years=range(1990, 2015),   # required for monthly
)
```

Or for hourly instantaneous data (clte stream):

```python
store = PolytopeZarrStore.from_climate_dt(
    models=["ICON"],
    experiment="hist",
    levtype="sfc",
    frequency="hourly",
    start_date="2020-01-01",
    end_date="2020-01-02",
)
```

### Factory parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `models` | `list[str]` | — | Model names (e.g. `["ICON", "IFS-FESOM", "IFS-NEMO"]`) |
| `experiment` | `str` | — | `"hist"`, `"cont"`, or `"SSP3-7.0"` |
| `levtype` | `str` | — | `"sfc"`, `"pl"`, `"hl"`, `"sol"`, `"o2d"`, or `"o3d"` |
| `frequency` | `str` | `"monthly"` | `"monthly"` (clmn stream) or `"hourly"` (clte stream) |
| `years` | range/list | — | Years to include (required for monthly) |
| `start_date` | `str` | — | ISO date (required for hourly) |
| `end_date` | `str` | — | ISO date (required for hourly) |
| `resolution` | `str` | `"standard"` | `"standard"` (nside 128) or `"high"` (nside 1024) |
| `realization` | `int` | `1` | Realization number |

### What the factory auto-derives

- **address** — IFS-NEMO → MN5; all others → LUMI
- **activity** — `"baseline"` if experiment ∈ {hist, cont}, else `"projections"`
- **n_cells** — `12 × 128²` (standard) or `12 × 1024²` (high)
- **stream** — `"clmn"` for monthly, `"clte"` for hourly
- **time_fields** — `["year", "month"]` for monthly, `["date", "time"]` for hourly
- **batch_size** — 12 for monthly, 24 for hourly
- **time axis** — `pd.date_range(freq="MS")` from years, or `freq="h"` from dates
- **coords, variables** — looked up from `PORTFOLIO_GEN2_CLMN[levtype]`
- **base_request** — all fixed Polytope fields (class, dataset, generation=2, etc.)

### Frequency-aware internals

The `_frequency` attribute (`"monthly"` or `"hourly"`) controls three internal methods:

1. **`_time_to_fields(ts)`** — converts a timestamp to Polytope request fields
2. **`_fetch_chunk`** batch grouping — groups by year-range (monthly) or day-range (hourly)
3. **`_split_batch`** field matching — matches by `(year, month)` or `(date, time)` metadata

Spatial features (`bbox`, `polygon`, `point`) are only available on hourly stores (clte stream).

---

## Construction

```python
store = PolytopeZarrStore(
    address=...,       # Polytope server URL, or {model: url} dict
    collection=...,    # e.g. "destination-earth"
    base_request=...,  # fixed request fields (class, dataset, experiment, …)
    coords=...,        # {dim_name: array} — time, cell, model, level, …
    variables=...,     # {var_name: {"dims": (...), "long_name": ..., "units": ...}}
    internal_dims=..., # dims that stay inside a single chunk (e.g. ["cell"])
    time_fields=...,   # which request fields to extract from timestamps
    batch_dim="time",  # dimension to batch over (default: "time")
    batch_size=12,     # max months per year in one request
    batch_years=1,     # how many consecutive years per request
)
```

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `address` | `str` or `dict` | — | Polytope server URL. If a dict, keys are model names for per-model routing (e.g. IFS-NEMO → MN5, others → LUMI) |
| `collection` | `str` | — | Polytope collection name (e.g. `"destination-earth"`) |
| `base_request` | `dict` | — | Fixed request fields (`class`, `dataset`, `experiment`, …). Do **not** include `year`/`month`/`param`/`model` — those are set per-chunk |
| `coords` | `dict` | — | `{dim_name: array-like}` — defines the shape and coordinate values of the xarray Dataset |
| `variables` | `dict` | — | `{var_name: {"dims": (...), "long_name": ..., "units": ...}}` |
| `internal_dims` | `list` | `[]` | Dims that form a single chunk (e.g. `["cell"]` — all 196 608 HEALPix cells in one chunk) |
| `time_fields` | `list` | `["year", "month"]` | Which Polytope request fields to extract from timestamps |
| `batch_dim` | `str` | `"time"` | Dimension to batch over. Set to `None` to disable batching |
| `batch_size` | `int` | `12` | Max months per year to fetch in one request (usually left at 12) |
| `batch_years` | `int` | `1` | Number of consecutive years to combine in a single Polytope request |

On construction, coordinates are normalised to numpy arrays and `_build_metadata()` is called to synthesise all zarr metadata.

---

## Two internal dicts

- **`self._kv`** — The "filesystem": all metadata keys (`.zgroup`, `.zmetadata`, per-array `.zarray`/`.zattrs`) plus coordinate data chunks.  Pre-populated at construction time.
- **`self._cache`** — Fetched data variable chunks, keyed by `"var_name/0.1.2"`.  Populated on demand by `_fetch_chunk()`.  This is the **only** cache layer — earthkit's disk cache should be set to `"off"` (see [Caching architecture](#caching-architecture)).

---

## Metadata synthesis

### `_build_metadata()`

Populates `self._kv` with everything zarr needs to describe the dataset — without any actual data.

1. Writes the root `.zgroup` and `.zattrs`
2. Iterates over **coordinates** → calls `_write_coord()` for each
3. Iterates over **data variables** → calls `_write_var_meta()` for each
4. Writes consolidated `.zmetadata` — the single JSON blob that xarray reads via `consolidated=True`, avoiding per-array round-trips

### `_write_coord()`

Writes a coordinate array's metadata **and data** into the store.  Three cases:

| dtype | Encoding | Notes |
|---|---|---|
| **datetime** | int64 nanoseconds since epoch | CF attributes: `units`, `calendar` |
| **string** | `VLenUTF8` (numcodecs) | Enables `ds.sel(model="ICON")`.  Falls back to fixed-length `\|S<n>` bytes |
| **numeric** | int32 or native float | Integer coords (cell indices, pressure levels) cast to int32 |

In all cases, the raw data bytes are stored under `"<coord>/0"` (a single chunk) — the coordinate is immediately available.

### `_write_var_meta()`

Writes **only metadata** for data variables (`.zarray` and `.zattrs`) — no data.  Chunk shape is determined by `internal_dims`:

- Dimensions in `internal_dims` (e.g. `cell`) → chunk size = full dimension size
- All other dimensions (e.g. `model`, `time`, `level`) → chunk size = 1

This means each chunk corresponds to **one model × one timestep × [one level] × all cells** — exactly what one Polytope request returns.

Attributes like `long_name` and `units` from the variable spec are propagated into `.zattrs`.

### `_encode_strings()`

Uses `numcodecs.VLenUTF8` for a compact binary encoding that zarr decodes back to Python strings.  Falls back to fixed-length `|S<n>` bytes if numcodecs is unavailable.

---

## Lazy chunk fetching & batching

### `_fetch_chunk(var_name, chunk_key)`

Called when xarray/zarr requests a data chunk that isn't in `self._kv` or `self._cache`.  This is where the Polytope HTTP request happens.

#### Step 1: Parse the chunk key

A zarr chunk key like `"avg_2t/1.5.0"` means variable `avg_2t`, chunk index `[1, 5, 0]` along the dimensions.  If dims are `("model", "time", "cell")`: model index 1, time index 5, cell chunk 0.

#### Step 2: Collect batch indices

When `batch_dim="time"` (the default), the store doesn't just fetch the one requested month — it finds **uncached neighbours** in the same year range and fetches them together.

The batching window is controlled by `batch_years`:
- `batch_years=1` → fetches all 12 months of the requested year (even if only 1 was asked for)
- `batch_years=5` → fetches up to 60 months (5 years × 12 months)
- `batch_years=25` → fetches up to 300 months (the full historical period in one call)

**How it works:**

1. Determine the reference year from the requested time index
2. Set `max_year = ref_year + batch_years - 1`
3. Scan all time indices within `[ref_year, max_year]`
4. Collect indices that are **not yet cached** (up to `batch_size × batch_years` total)
5. The originally requested index is always included

Already-cached months are skipped, so re-running the same cell never re-fetches.

#### Step 3: Build the Polytope request

Starts from `base_request` and adds per-chunk fields:

| Dimension | Request field | Example |
|---|---|---|
| `time` (batched) | `year=2010/2011/2012`, `month=1/2/.../12` | Multi-value `/`-separated syntax → Polytope returns all as separate GRIB fields |
| `time` (single) | `year=2014`, `month=6` | Single timestamp |
| `model` | `model=IFS-FESOM` | Model name string |
| `level` | `levelist=850` | Pressure/soil/ocean depth level |
| `param` | `param=avg_2t` | Variable name |
| internal dims (`cell`) | *(skipped)* | Full dimension returned in one go |

The multi-value year/month syntax (`year=2010/2011/.../2014, month=1/2/.../12`) creates a Cartesian product on the Polytope server — one GRIB field per year × month combination, all returned in a single HTTP response.

#### Step 4: Resolve server address

If `address` is a dict (per-model routing), the correct server is selected based on the model in the current request.

#### Step 5: Fetch via earthkit

```python
import earthkit.data
data = earthkit.data.from_source("polytope", collection, request, address=address, stream=False)
```

`earthkit.data` is imported **lazily inside this method**, not at module level.  This avoids a multi-second import delay when constructing the store or opening the dataset.

The response is an earthkit `FieldList` — one field per GRIB message.  For batched requests, this contains multiple fields that need to be split (see next section).

#### Step 6: Cache the result

- **Single-field response**: converted to `float32` bytes and stored as `self._cache["var_name/chunk_key"]`
- **Multi-field response**: passed to `_split_batch()` which distributes each field to the correct cache key

#### Error handling

If the fetch fails (network error, missing data, etc.), **all** batch indices are filled with `NaN`.  This means the dataset always opens successfully — missing fields just appear as NaN.

---

## Batch splitting: `_split_batch()`

After a batched Polytope request returns multiple GRIB fields, they need to be matched back to individual chunk cache keys.

### Time dimension (primary path)

1. Build a lookup table: `(year, month)` → time-axis index
2. For each returned field, read its `year` and `month` from the GRIB metadata
3. Match to the correct time index and store as `self._cache["var_name/0.idx.0"]`
4. Any unmatched batch indices are filled with NaN

This metadata-based matching is robust — it doesn't depend on the response ordering.

### Non-time dimensions

For non-time batch dimensions, fields are matched by position (1:1 with batch_indices).  A fallback treats the entire response as a single concatenated array and splits by expected chunk size.

---

## MutableMapping interface

These methods make `PolytopeZarrStore` look like a Python dict to zarr:

### `__getitem__(key)`

The main dispatch:
1. Check `self._kv` (metadata + coordinates) — instant
2. Check `self._cache` (previously fetched data) — instant
3. Parse as `"var_name/chunk_key"` and call `_fetch_chunk()` — triggers Polytope request
4. Raise `KeyError` for anything else

### `__contains__(key)`

Returns `True` for:
- Any key in `self._kv` (metadata/coordinates)
- Any key that looks like a valid data chunk (`"var_name/0.1.2"` where `var_name` is a known variable)

This is critical: zarr checks `key in store` before fetching.  By returning `True` for **all valid chunk keys**, we tell zarr "yes, that data exists" — even though it hasn't been fetched yet.

### `listdir(prefix)`

Used by zarr for directory-style discovery.  Lists "children" under a given prefix by scanning `self._kv` keys.

---

## Convenience methods

### `batch_years` property

```python
store.batch_years = 25  # change at any time — affects future fetches only
print(store.batch_years)
```

Getter/setter for `self._batch_years`.  Can be changed between accesses to tune prefetching without reconstructing the store.

### `open()`

Opens the store as an xarray Dataset (instant — no data fetched):

```python
ds = store.open()
# equivalent to:
ds = xr.open_dataset(store, engine="zarr", consolidated=True)
```

Additionally, `open()` attaches a reference to the store in `ds.attrs["_polytope_store"]` and in each DataArray's attrs.  This allows the `.polytope.sel()` accessor to find the store and auto-tune `batch_years`.

### `clear_cache()`

Frees memory by clearing `self._cache`.  Useful after plotting many fields.  Does not affect `self._kv` (metadata stays).

---

## The `.polytope.sel()` accessor

Registered at module level using xarray's `register_dataarray_accessor` and `register_dataset_accessor` decorators.  Available on any Dataset or DataArray returned by `store.open()`.

### The problem it solves

The zarr `MutableMapping` protocol is a key→value lookup — `__getitem__` receives **one chunk key at a time**.  When you write:

```python
ds["avg_2t"].sel(model="IFS-FESOM", time=slice("1990-01", "2014-12")).mean("time")
```

xarray resolves the time slice into ~300 indices and issues 300 separate `__getitem__` calls.  The store never sees the slice, it has no "query plan".  The `batch_years` parameter is a workaround: when the store is asked for January 1990, it speculatively prefetches all months within the `batch_years` window.  But the user has to set `batch_years` correctly upfront.

### How `.polytope.sel()` fixes this

```python
ds["avg_2t"].polytope.sel(model="IFS-FESOM", time=slice("1990-01", "2014-12"))
```

1. The accessor reads the `time=slice(...)` argument
2. Computes `batch_years = stop.year - start.year + 1` (e.g. `2014 - 1990 + 1 = 25`)
3. Sets `store.batch_years = 25`
4. Delegates to the normal `.sel()` method

Now when xarray makes the first `__getitem__` call, the store fetches all 25 years in a single Polytope request.  The remaining 299 calls are instant cache hits.

### Single timestamps

For `.sel(time="2010-06-01")` (not a slice), the accessor does nothing — `batch_years` stays at its current value (default 1), and the store fetches just the 12 months of that year.

### Dataset-level accessor

```python
ds.polytope.sel(var="avg_2t", model="IFS-FESOM", time=slice("1990-01", "2014-12"))
```

The optional `var` argument selects a single variable after `.sel()` — a shorthand for `ds.polytope.sel(...)[var]`.

### How the store reference is threaded

```
store.open()
  → ds.attrs["_polytope_store"] = store
  → ds[var].attrs["_polytope_store"] = store   (for each data variable)

_PolytopeDataArray.__init__(da)
  → self._store = da.attrs.get("_polytope_store")

_PolytopeDataArray.sel(**kwargs)
  → _infer_batch_years(self._store, kwargs)     # sets store.batch_years
  → return self._da.sel(**kwargs)                # normal xarray .sel()
```

---

## Server-side spatial features

`.polytope.sel()` also accepts **spatial keyword arguments** that bypass the zarr store entirely and execute a [Polytope feature request](https://polytope-examples.readthedocs.io/).  The result is an xarray Dataset on a **lat/lon grid** (not HEALPix).

### Important: stream differences

Feature requests use the **`clte` stream** (instantaneous forecast fields).  This is different from the zarr store's `clmn` stream (monthly means).  The Polytope server only supports features on `clte` — a `clmn` feature request is rejected with *"got stream : clmn, but expected one of ['clte']"*.

The accessor auto-switches `stream` from `clmn` → `clte`, removes `year`/`month` fields, and converts the `time` argument to `date` format (`YYYYMMDD`).

### Bounding box

```python
ds["avg_2t"].polytope.sel(
    model="IFS-FESOM",
    time="2010-06",
    bbox=(47, 5, 55, 15),   # (south, west, north, east) in degrees
)
```

Returns all grid points within the box for a single date.  Coordinates in the result are named `latitude` and `longitude`.

### Polygon

```python
ds["avg_2t"].polytope.sel(
    model="IFS-FESOM",
    time="2010-06",
    polygon=[(41.87, -8.88), (41.69, -8.82), ...],  # (lat, lon) vertices
)
```

Same as bounding box but returns only points inside the polygon boundary.

### Point timeseries

```python
ds["avg_2t"].polytope.sel(
    model="IFS-FESOM",
    time=slice("2010-01", "2010-12"),
    point=(52.5, 13.4),    # (lat, lon) — Berlin
)
```

Returns a timeseries at a single location over the requested date range.  The `time_axis` is set to `"date"` so the output has a time dimension.

### How it works internally

```
.polytope.sel(bbox=..., time="2010-06", model="IFS-FESOM")
  │
  ├─ copy store._base_request
  ├─ stream: clmn → clte
  ├─ remove year/month, add date: "20100601"
  ├─ add time: "0000"
  ├─ add feature: {type: "boundingbox", points: [...]}
  ├─ param: "avg_2t" (from DataArray name)
  │
  └─ earthkit.data.from_source("polytope", ...) → .to_xarray()
       → xarray Dataset with latitude/longitude coordinates
```

---

## Caching architecture

There are **two independent cache layers** to be aware of:

### Layer 1: `self._cache` (in-memory, managed by the store)

A plain Python `dict` mapping `"var_name/chunk_key"` → `bytes` (float32 numpy arrays serialised via `.tobytes()`).  This is populated by `_fetch_chunk()` and checked by `__getitem__()` on every access.

- **Scope**: lives as long as the store object
- **Size**: ~768 KB per field (196 608 cells × 4 bytes).  A full 25-year × 1-variable batch ≈ 230 MB
- **Cleared by**: `store.clear_cache()`

### Layer 2: earthkit disk cache (filesystem, managed by earthkit-data)

earthkit-data normally writes raw GRIB files to a temp directory.  With batch requests, this can quickly grow to many GiB and trigger cache eviction errors.

**Recommendation**: set `earthkit.data.config.set("cache-policy", "off")` in notebook 03.  The store's `self._cache` already prevents any re-fetching, so the earthkit disk cache is redundant overhead.

For notebook 02 (which uses `destine_climate_helpers.py` and doesn't have an in-memory cache layer), keep `"temporary"`: the earthkit cache helps if a download is interrupted before the result is saved to NetCDF.

### Cache flow diagram

```
xarray asks for chunk "avg_2t/1.5.0"
  │
  ├─ self._cache hit? ──→ YES → return bytes (instant, no network)
  │
  └─ NO → _fetch_chunk()
           │
           ├─ earthkit.data.from_source("polytope", ...)
           │     │
           │     ├─ HTTP request to Polytope API
           │     ├─ receives GRIB data
           │     ├─ decodes GRIB → numpy (in memory)
           │     └─ (if cache-policy != "off") writes GRIB to disk ← REDUNDANT
           │
           ├─ .to_numpy() → float32 → .tobytes()
           ├─ store in self._cache["avg_2t/1.5.0"]
           │   (+ neighbouring chunks if batched)
           └─ return bytes to zarr → xarray → user
```

---

## Data flow summary

```
store = PolytopeZarrStore(...)           → metadata synthesised in memory
ds    = store.open()                     → xarray reads .zmetadata (instant)
                                           store ref attached to ds.attrs

ds["avg_2t"]                             → returns lazy DataArray (no fetch)

ds["avg_2t"].sel(time="2010-06-01")      → zarr calls store["avg_2t/1.240.0"]
  .values                                → __getitem__ → _fetch_chunk()
                                         → batches 12 months of 2010
                                         → Polytope HTTP → earthkit → numpy → cache
                                         → returns requested chunk

ds["avg_2t"].polytope.sel(               → accessor sets store.batch_years = 25
    time=slice("1990-01", "2014-12")     → zarr calls store["avg_2t/1.0.0"]
).mean("time")                           → _fetch_chunk batches 300 months (1 request)
                                         → remaining 299 __getitem__ calls are cache hits
                                         → xarray computes mean over 300 cached chunks
```

---

## Why the zarr protocol limits us

In `destine_climate_helpers.py` (notebook 02), we build the Polytope request.  We see all requested years upfront and can join them into `year=2010/2011/.../2014` in a single API call.

In `polytope_zarr.py` (notebook 03), the store implements the `MutableMapping` protocol, essentially a dict.  xarray/zarr access it via `__getitem__(key)`, **one chunk key at a time**.  When you write:

```python
ds["avg_tprate"].sel(time=slice("2010-01", "2014-12")).mean("time")
```

Here's what happens:

1. `.sel(time=slice(...))` — xarray resolves this against the time coordinate (already in memory).  It determines it needs time indices 240, 241, …, 299
2. `.mean("time")` — xarray builds a computation graph to read each chunk and average them
3. **For each chunk**, xarray calls `store["avg_tprate/1.240.0"]`, `store["avg_tprate/1.241.0"]`, etc. — 60 separate `__getitem__` calls

The store never sees the slice.  There is no "query plan" passed from xarray to the store.  With plain `.sel()`, `batch_years` is a speculative prefetch: it assumes nearby time indices will be requested soon.

The `.polytope.sel()` accessor removes the guesswork: it **sees the slice before xarray resolves it**, so it knows exactly which years are needed and sets `batch_years` accordingly.  This isn't speculation, the data is being requested right now, we're just fetching it in one API call instead of letting xarray dealing with it one chunk at a time.
