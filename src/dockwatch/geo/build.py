"""Map shapes for the live station map: pure functions (no shapefile reader here, so host tests need nothing extra).

Each region view gets its own projection into a 0 0 1000 1000 view box (DESIGN.md §6). The projection is a simple
equirectangular one scaled by cos(latitude) at the view's centre, which is accurate to well under a pixel over a
city-sized area. The browser projects station positions with the same parameters (web/js/map/project.js), so new
stations never need a rebuild.
"""

import math
from collections.abc import Iterable, Sequence

Point = tuple[float, float]
Ring = list[Point]

VIEWBOX = 1000.0
SOURCE = (
    "US Census Bureau TIGER cartographic boundary files (cb_2023_us_county_500k, clipped to shoreline), public domain"
)
# Bay Area counties (state 06): San Francisco, San Mateo, Alameda, Contra Costa, Santa Clara, Marin.
COUNTIES = ("06075", "06081", "06001", "06013", "06085", "06041")

# Fixed lon/lat bounds per view (west, south, east, north), chosen so every station of the view fits with padding.
VIEWS: dict[str, dict] = {
    "sf": {
        "name": "San Francisco",
        "bounds": (-122.528, 37.683, -122.360, 37.815),
        "landmarks": [
            ("Ferry Building", -122.3937, 37.7955),
            ("Golden Gate Park", -122.4862, 37.7694),
            ("Dolores Park", -122.4269, 37.7596),
            ("Oracle Park", -122.3893, 37.7786),
        ],
    },
    "eastbay": {
        "name": "East Bay (Oakland, Emeryville, Berkeley)",
        "bounds": (-122.327, 37.765, -122.185, 37.895),
        "landmarks": [
            ("Lake Merritt", -122.2590, 37.8020),
            ("UC Berkeley", -122.2585, 37.8719),
            ("Jack London Square", -122.2776, 37.7946),
            ("Emeryville", -122.2852, 37.8313),
        ],
    },
    "sj": {
        "name": "San José",
        "bounds": (-121.932, 37.302, -121.851, 37.378),
        "landmarks": [
            ("Diridon Station", -121.9026, 37.3297),
            ("San José State", -121.8811, 37.3352),
            ("Japantown", -121.8949, 37.3489),
        ],
    },
}


def projection_for(bounds: Sequence[float], size: float = VIEWBOX) -> dict:
    """Projection parameters that fit lon/lat `bounds` (west, south, east, north) centred in a size x size box."""
    west, south, east, north = bounds
    lon0 = (west + east) / 2
    lat0 = (south + north) / 2
    k = math.cos(math.radians(lat0))
    scale = size / max((east - west) * k, north - south)
    return {
        "type": "equirectangular",
        "lon0": lon0,
        "lat0": lat0,
        "k": k,
        "scale": scale,
        "cx": size / 2,
        "cy": size / 2,
    }


def project(lon: float, lat: float, p: dict) -> Point:
    """lon/lat -> view-box x/y (y grows downwards). Mirrored exactly by web/js/map/project.js."""
    x = p["cx"] + (lon - p["lon0"]) * p["k"] * p["scale"]
    y = p["cy"] - (lat - p["lat0"]) * p["scale"]
    return x, y


def _perpendicular_distance(pt: Point, a: Point, b: Point) -> float:
    (x, y), (x1, y1), (x2, y2) = pt, a, b
    dx, dy = x2 - x1, y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(x - x1, y - y1)
    return abs(dy * x - dx * y + x2 * y1 - y2 * x1) / math.hypot(dx, dy)


def simplify(points: Sequence[Point], tolerance: float) -> list[Point]:
    """Douglas–Peucker: drop points closer than `tolerance` to the simplified line. Endpoints are always kept."""
    if len(points) < 3:
        return list(points)
    keep = [False] * len(points)
    keep[0] = keep[-1] = True
    stack = [(0, len(points) - 1)]
    while stack:
        start, end = stack.pop()
        best, index = 0.0, -1
        for i in range(start + 1, end):
            d = _perpendicular_distance(points[i], points[start], points[end])
            if d > best:
                best, index = d, i
        if index != -1 and best > tolerance:
            keep[index] = True
            stack.append((start, index))
            stack.append((index, end))
    return [pt for pt, k in zip(points, keep, strict=True) if k]


def clip(ring: Sequence[Point], xmin: float, ymin: float, xmax: float, ymax: float) -> Ring:
    """Clip a closed polygon ring to a rectangle (Sutherland–Hodgman). Returns [] when nothing is left."""

    def edge(points: Ring, inside, cross) -> Ring:
        out: Ring = []
        for i, cur in enumerate(points):
            prev = points[i - 1]
            if inside(cur):
                if not inside(prev):
                    out.append(cross(prev, cur))
                out.append(cur)
            elif inside(prev):
                out.append(cross(prev, cur))
        return out

    def at_x(x):
        return lambda a, b: (x, a[1] + (b[1] - a[1]) * (x - a[0]) / (b[0] - a[0]))

    def at_y(y):
        return lambda a, b: (a[0] + (b[0] - a[0]) * (y - a[1]) / (b[1] - a[1]), y)

    pts: Ring = list(ring)
    if len(pts) > 1 and pts[0] == pts[-1]:
        pts = pts[:-1]
    for inside, cross in (
        (lambda p: p[0] >= xmin, at_x(xmin)),
        (lambda p: p[0] <= xmax, at_x(xmax)),
        (lambda p: p[1] >= ymin, at_y(ymin)),
        (lambda p: p[1] <= ymax, at_y(ymax)),
    ):
        if not pts:
            return []
        pts = edge(pts, inside, cross)
    return pts


def ring_to_path(ring: Sequence[Point]) -> str:
    """'M x y L x y … Z' with one decimal (0.1 of a view-box unit is far below a screen pixel)."""
    head, *rest = ring
    parts = [f"M{head[0]:.1f} {head[1]:.1f}"] + [f"L{x:.1f} {y:.1f}" for x, y in rest]
    return "".join(parts) + "Z"


# Land is kept this far outside the square view box: the map card is 16:10 on desktop and 4:5 on phones (DESIGN.md
# §4) and the SVG is drawn with preserveAspectRatio "meet", so up to 300 units show beside (or 125 above/below) the box.
LAND_MARGIN = 320.0


def build_view(
    view: str, shapes: Iterable[Sequence[Ring]], tolerance: float = 0.8, margin: float = LAND_MARGIN
) -> dict:
    """One region's map document from lon/lat polygons (each shape = its rings). Land paths are projected, clipped
    to the view box plus `margin`, and simplified; rings that end up smaller than a triangle are dropped."""
    spec = VIEWS[view]
    proj = projection_for(spec["bounds"])
    land: list[str] = []
    for rings in shapes:
        subpaths = []
        for ring in rings:
            projected = [project(lon, lat, proj) for lon, lat in ring]
            clipped = clip(projected, -margin, -margin, VIEWBOX + margin, VIEWBOX + margin)
            if len(clipped) < 3:
                continue
            simple = simplify(clipped + [clipped[0]], tolerance)[:-1]
            if len(simple) >= 3:
                subpaths.append(ring_to_path(simple))
        if subpaths:
            land.append("".join(subpaths))
    landmarks = []
    for name, lon, lat in spec["landmarks"]:
        x, y = project(lon, lat, proj)
        landmarks.append({"name": name, "x": round(x, 1), "y": round(y, 1)})
    return {
        "view": view,
        "name": spec["name"],
        "source": SOURCE,
        "projection": {**proj, "bounds": list(spec["bounds"])},
        "viewBox": f"0 0 {VIEWBOX:.0f} {VIEWBOX:.0f}",
        "land": land,
        "landmarks": landmarks,
    }
