// Insights format 2 (web/data/insights/*.json, written by marts_daily): rows hold a `station` index into
// `stations.rows`; per-file constants sit in `window`. Pure functions from the three files to what the page shows.

/** A format-2 file as row objects by column name, with station_id / name / region_id from the station lookup. */
export function decode(doc) {
  const stations = doc.stations.rows.map((r) => Object.fromEntries(doc.stations.columns.map((c, i) => [c, r[i]])));
  return doc.rows.map((r) => {
    const row = Object.fromEntries(doc.columns.map((c, i) => [c, r[i]]));
    return { ...row, ...stations[row.station] };
  });
}

/** Heatmap buckets (DESIGN §2): % of time empty, upper edges of the first four of five seq-empty steps. */
export const BUCKETS = [5, 15, 30, 50];
export const BUCKET_LABELS = ["0–5 %", "5–15 %", "15–30 %", "30–50 %", "> 50 %"];

/** 1..5: the seq-empty step for a percentage. */
export function bucket(percent) {
  const i = BUCKETS.findIndex((edge) => percent < edge);
  return (i === -1 ? BUCKETS.length : i) + 1;
}

function mean(values) {
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
}

/** Per station from the trips file: name, the 24 hourly shares (null where missing), mean share empty, trips/day. */
export function stationProfiles(trips) {
  const byId = new Map();
  for (const r of trips) {
    let s = byId.get(r.station_id);
    if (!s) {
      s = { id: r.station_id, name: r.name, hours: Array(24).fill(null), tripsPerDay: 0 };
      byId.set(r.station_id, s);
    }
    s.hours[r.hour_of_day] = r.share_empty;
    s.tripsPerDay += r.trips_started_per_day;
  }
  return [...byId.values()].map((s) => ({ ...s, shareEmpty: mean(s.hours.filter((v) => v !== null)) }));
}

/** The KPI tiles (plan D2): time empty / full over the daily file, worst hour and trips per day from the trips file. */
export function kpis(daily, trips) {
  const coverageMinutes = daily.reduce((sum, r) => sum + r.coverage_hours, 0) * 60;
  const share = (key) => (coverageMinutes ? daily.reduce((sum, r) => sum + r[key], 0) / coverageMinutes : 0);
  const byHour = Array.from({ length: 24 }, () => []);
  for (const r of trips) byHour[r.hour_of_day].push(r.share_empty);
  const hourly = byHour.map(mean);
  const worstHour = hourly.indexOf(Math.max(...hourly));
  return {
    timeEmpty: share("minutes_empty"),
    timeFull: share("minutes_full"),
    worstHour,
    worstHourShare: hourly[worstHour],
    tripsPerDay: trips.reduce((sum, r) => sum + r.trips_started_per_day, 0),
  };
}

/** The `count` stations with the highest mean share empty (ties: name), for the heatmap. */
export function heatmapRows(profiles, count = 25) {
  return [...profiles].sort((a, b) => b.shareEmpty - a.shareEmpty || a.name.localeCompare(b.name)).slice(0, count);
}

/** Rebalancing rows with need_rank 1..maxRank, in rank order. */
export function rebalancingRows(need, maxRank = 20) {
  return need.filter((r) => r.need_rank <= maxRank).sort((a, b) => a.need_rank - b.need_rank || a.name.localeCompare(b.name));
}

/** Busiest vs quietest quarter of stations by trips per day: their mean % of time empty (for the takeaway). */
export function quarters(profiles) {
  const sorted = [...profiles].sort((a, b) => b.tripsPerDay - a.tripsPerDay);
  const q = Math.max(1, Math.floor(sorted.length / 4));
  return {
    busiest: mean(sorted.slice(0, q).map((s) => s.shareEmpty)),
    quietest: mean(sorted.slice(-q).map((s) => s.shareEmpty)),
  };
}

/** "8–9 AM", "11 AM–12 PM" for a Pacific hour of day. */
export function hourRange(hour) {
  const label = (h) => [(h % 12) || 12, h % 24 < 12 ? "AM" : "PM"];
  const [a, am] = label(hour);
  const [b, bm] = label(hour + 1);
  return am === bm ? `${a}–${b} ${am}` : `${a} ${am}–${b} ${bm}`;
}
