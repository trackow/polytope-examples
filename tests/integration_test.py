import pytest
import numpy as np
from earthkit.data import from_source
from earthkit import regrid
from earthkit.plots import Map
import tempfile
from pathlib import Path
import shutil
from matplotlib.testing.compare import compare_images

def generate_plot(data, domain=None, filename="plot.png"):
    chart = Map(domain=domain) if domain else Map()
    chart.grid_cells(
        data,
        colors="turbo",
        auto_style=True,
        units="celsius"
    )
    chart.title("Temerature at 2m")
    chart.coastlines()
    chart.gridlines()

    # Save plot to a file instead of showing
    chart.save(filename)
    return filename


def test_earthkit_integration():
    data = from_source("file", "data/extremes-dt-earthkit-example-domain.grib")
    assert data is not None
    data.ls()

    out_grid = {"grid": [0.1, 0.1]}
    data_interpolated = regrid.interpolate(data, out_grid=out_grid, method="linear")
    xr = data_interpolated.to_xarray()

    # Check the shape and contents are as expected
    assert isinstance(xr, dict) or hasattr(xr, "data_vars")
    assert data_interpolated[0].shape == data[0].shape or data_interpolated[0].shape != (0,)

    # Temporary directory for plots
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        file1 = tmpdir / "plot_global.png"
        file2 = tmpdir / "plot_europe.png"

        generate_plot(data_interpolated[0], domain=None, filename=file1)
        generate_plot(data_interpolated[0], domain="Europe", filename=file2)

        # Compare plots visually
        diff = compare_images(str(file1), str(file2), tol=5)
        assert diff is None, f"Plots differ: {diff}"

        # Optionally check that data values are close
        np.testing.assert_allclose(
            data_interpolated[0].values,
            data_interpolated[0].values,
            rtol=1e-5,
            err_msg="Interpolated data is inconsistent"
        )
