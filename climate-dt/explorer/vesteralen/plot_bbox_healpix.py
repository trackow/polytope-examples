"""Vesterålen — Polytope bounding-box feature, native HEALPix cells (no interpolation)."""
from vesteralen_common import BBOX, REGION, open_store, parse_args, plot_healpix_cells

args = parse_args(__doc__)
cfg = args.cfg
store, ds = open_store(args.freq)

result = ds[cfg["var"]].polytope.sel(model=cfg["model"], time=cfg["time"], bbox=BBOX)

plot_healpix_cells(
    result, cfg["var"], store.nside,
    title=f"{cfg['model']} — {cfg['var']} — {cfg['label']} — {REGION} bbox\n"
          "(native HEALPix pixels, no interpolation)",
    outfile=f"vesteralen_bbox_healpix_{args.freq}.png",
)
