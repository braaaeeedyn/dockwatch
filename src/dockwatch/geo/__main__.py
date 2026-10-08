"""Build web/geo/{sf,eastbay,sj}.json from the Census cartographic boundary file. Run: python tasks.py geo

Downloads cb_2023_us_county_500k.zip into data/geo/ once (git-ignored), reads the Bay Area counties with pyshp
(uv group `geo`) and writes one compact JSON document per region view.
"""

import argparse
import json
import logging
import urllib.request
from pathlib import Path

from dockwatch.geo.build import COUNTIES, VIEWS, build_view

log = logging.getLogger("dockwatch.geo")

URL = "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_county_500k.zip"
USER_AGENT = "dockwatch/0.1 (+https://github.com/braaaeeedyn/dockwatch)"


def download(url: str, dest: Path) -> Path:
    if dest.exists():
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    log.info("downloading %s", url)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    tmp = dest.with_suffix(".part")
    with urllib.request.urlopen(request, timeout=120) as response, tmp.open("wb") as f:
        f.write(response.read())
    tmp.replace(dest)
    return dest


def read_counties(path: Path) -> list[list[list[tuple[float, float]]]]:
    """Polygons (as lists of lon/lat rings) of the Bay Area counties."""
    import shapefile  # pyshp, uv group "geo"; imported here so host tests don't need it

    shapes = []
    with shapefile.Reader(str(path)) as reader:
        for record in reader.iterShapeRecords():
            if record.record["GEOID"] not in COUNTIES:
                continue
            shape = record.shape
            bounds = list(shape.parts) + [len(shape.points)]
            rings = [[(x, y) for x, y in shape.points[a:b]] for a, b in zip(bounds, bounds[1:], strict=False)]
            shapes.append(rings)
    return shapes


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("data/geo/cb_2023_us_county_500k.zip"))
    parser.add_argument("--out", type=Path, default=Path("web/geo"))
    args = parser.parse_args()

    shapes = read_counties(download(URL, args.source))
    log.info("read %d county shapes", len(shapes))
    args.out.mkdir(parents=True, exist_ok=True)
    for view in VIEWS:
        doc = build_view(view, shapes)
        path = args.out / f"{view}.json"
        path.write_text(json.dumps(doc, separators=(",", ":")), encoding="utf-8")
        log.info("wrote %s: %d land shapes, %d bytes", path, len(doc["land"]), path.stat().st_size)


if __name__ == "__main__":
    main()
