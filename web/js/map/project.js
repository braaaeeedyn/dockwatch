// lon/lat -> view-box x/y with a region's projection params from web/geo/<view>.json.
// Mirrors dockwatch.geo.build.project exactly, so stations are placed in the browser and new stations need no rebuild.

export function project(lon, lat, p) {
  return [p.cx + (lon - p.lon0) * p.k * p.scale, p.cy - (lat - p.lat0) * p.scale];
}
