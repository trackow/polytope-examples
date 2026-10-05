"""Vesterålen — Polytope polygon feature, native HEALPix cells inside the region outline."""
from vesteralen_common import REGION, SHAPES, open_store, parse_args, plot_healpix_cells

args = parse_args(__doc__)
cfg = args.cfg
store, ds = open_store(args.freq)

result = ds[cfg["var"]].polytope.sel(model=cfg["model"], time=cfg["time"], polygon=SHAPES)

plot_healpix_cells(
    result, cfg["var"], store.nside,
    title=f"{cfg['model']} — {cfg['var']} — {cfg['label']} — polygon ({REGION})\n"
          "(native HEALPix pixels, no interpolation)",
    outfile=f"vesteralen_polygon_healpix_{args.freq}.png",
    shapes=SHAPES,
)
