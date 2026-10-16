// F4 Insights page: KPI tiles, the % of time empty heatmap, the rebalancing table, the trips vs availability scatter,
// and the not-published / error / stalled-read states. Data: one real format-2 export (dag-test marts_daily) in
// tests/web/fixtures/insights, served by prepare(); expected values are recomputed here from those files.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { describePage, loadFixture, prepare } from "./helpers.js";

const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const DAILY = loadFixture("insights/station_availability_daily.json");
const NEED = loadFixture("insights/rebalancing_need.json");
const TRIPS = loadFixture("insights/trips_vs_availability.json");

const oneDecimal = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const number = new Intl.NumberFormat("en-US");
const monthDay = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" });
const pct = (share) => `${oneDecimal.format(share * 100)} %`;
const day = (iso) => monthDay.format(new Date(`${iso}T12:00:00Z`));
const mean = (values) => values.reduce((a, b) => a + b, 0) / values.length;

/** Format-2 rows as objects, with the station's id and name from the lookup. */
function rows(doc) {
  return doc.rows.map((r) => {
    const row = Object.fromEntries(doc.columns.map((c, i) => [c, r[i]]));
    const [id, name] = doc.stations.rows[row.station];
    return { ...row, id, name };
  });
}

function hourRange(hour) {
  const label = (h) => [h % 12 || 12, h % 24 < 12 ? "AM" : "PM"];
  const [a, am] = label(hour);
  const [b, bm] = label(hour + 1);
  return am === bm ? `${a}–${b} ${am}` : `${a} ${am}–${b} ${bm}`;
}

const daily = rows(DAILY);
const trips = rows(TRIPS);
const need = rows(NEED);
const stations = new Map();
for (const r of trips) {
  const s = stations.get(r.id) ?? { name: r.name, hours: Array(24).fill(null), trips: 0 };
  s.hours[r.hour_of_day] = r.share_empty;
  s.trips += r.trips_started_per_day;
  stations.set(r.id, s);
}
for (const s of stations.values()) s.share = mean(s.hours.filter((v) => v !== null));
const byHour = Array.from({ length: 24 }, (_, h) => mean(trips.filter((r) => r.hour_of_day === h).map((r) => r.share_empty)));
const coverageMinutes = daily.reduce((sum, r) => sum + r.coverage_hours, 0) * 60;

async function open(page, path = "/insights.html") {
  await page.goto(path);
  try {
    await expect(page.locator('[data-chart="scatter"]')).toHaveClass(/is-ready/);
  } catch (error) {
    error.message += `\n\nWhat the page did:\n${describePage(page)}`;
    throw error;
  }
}

test("insights KPI tiles show time empty, time full, the worst hour and trips per day from the insights files", async ({ page }) => {
  await prepare(page);
  await open(page);
  const tiles = page.locator(".kpis");
  await expect(tiles).not.toHaveAttribute("aria-busy", "true");
  const value = (key) => page.locator(`[data-kpi="${key}"] [data-kpi-value]`);
  const empty = daily.reduce((sum, r) => sum + r.minutes_empty, 0) / coverageMinutes;
  const full = daily.reduce((sum, r) => sum + r.minutes_full, 0) / coverageMinutes;
  const worst = byHour.indexOf(Math.max(...byHour));
  await expect(value("empty")).toHaveText(pct(empty));
  await expect(value("full")).toHaveText(pct(full));
  await expect(value("hour")).toHaveText(hourRange(worst));
  await expect(page.locator('[data-kpi="hour"] [data-kpi-sub]')).toHaveText(`${pct(byHour[worst])} of the time empty, on average`);
  await expect(value("trips")).toHaveText(number.format(Math.round(trips.reduce((s, r) => s + r.trips_started_per_day, 0))));
  const dates = DAILY.window.service_dates;
  await expect(page.locator('[data-kpi="empty"] [data-kpi-sub]')).toHaveText(
    `of station time, ${day(dates[0])} – ${day(dates[dates.length - 1])}`,
  );
  await expect(page.locator("[data-kpi-caption]")).toContainText("marts.mart_station_availability_daily");

  // ⓘ reveals the definition, and hides it again.
  const info = page.getByRole("button", { name: "What “Time empty” means" });
  await info.click();
  await expect(info).toHaveAttribute("aria-expanded", "true");
  await expect(page.locator("#kpi-def-empty")).toBeVisible();
  await info.click();
  await expect(page.locator("#kpi-def-empty")).toBeHidden();
});

test("insights heatmap shows % of time empty by station and hour with a labelled scale", async ({ page }) => {
  await prepare(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await open(page);
  const card = page.locator('[data-chart="heatmap"]');
  await expect(card.getByRole("heading", { level: 2 })).toHaveText("% of time empty by station and hour");
  await expect(card.locator(".scale li")).toHaveText(["0–5 %", "5–15 %", "15–30 %", "30–50 %", "> 50 %"]);
  await expect(card.getByRole("list", { name: "Scale: % of time empty" })).toBeVisible();
  await expect(card.locator("thead th")).toHaveCount(25); // Station + hours 0-23

  const top = [...stations.values()].sort((a, b) => b.share - a.share || a.name.localeCompare(b.name)).slice(0, 25);
  const body = card.locator("tbody tr");
  await expect(body).toHaveCount(25);
  await expect(card.locator("tbody th")).toHaveText(top.map((s) => s.name));
  const bucket = (p) => (p < 5 ? 1 : p < 15 ? 2 : p < 30 ? 3 : p < 50 ? 4 : 5);
  const first = body.first().locator("td");
  await expect(first).toHaveCount(24);
  await expect(first).toHaveText(top[0].hours.map((v) => (v === null ? "–" : String(Math.round(v * 100)))));
  for (let h = 0; h < 24; h += 1) {
    if (top[0].hours[h] !== null) await expect(first.nth(h)).toHaveAttribute("data-bucket", String(bucket(top[0].hours[h] * 100)));
  }
  await expect(card.locator("[data-takeaway]")).toContainText(`${top[0].name} is empty most often: ${pct(top[0].share)}`);
  await expect(card.locator("[data-caption]")).toContainText("Source: marts.mart_trips_vs_availability");

  // The table scrolls inside its card (a focusable region), the page itself never scrolls sideways.
  const wrap = card.getByRole("region", { name: "% of time empty by station and hour" });
  await expect(wrap).toHaveAttribute("tabindex", "0");
  expect(await wrap.evaluate((el) => el.scrollWidth > el.clientWidth)).toBe(true);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);
});

test("rebalancing table lists the stations that needed the most rebalancing in the last 7 days", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="rebalancing"]');
  await expect(card.getByRole("heading", { level: 2 })).toHaveText("Stations that needed the most rebalancing");
  const want = need.filter((r) => r.need_rank <= 20).sort((a, b) => a.need_rank - b.need_rank || a.name.localeCompare(b.name));
  await expect(card.locator("tbody tr")).toHaveCount(want.length);
  await expect(card.locator("tbody th")).toHaveText(want.map((r) => r.name));
  const firstRow = card.locator("tbody tr").first().locator("td");
  await expect(firstRow.nth(0)).toHaveText("1");
  await expect(firstRow.nth(1)).toHaveText(oneDecimal.format(want[0].hours_empty));
  await expect(firstRow.nth(2)).toHaveText(oneDecimal.format(want[0].hours_full));
  await expect(firstRow.nth(3)).toHaveText(String(want[0].episodes));
  await expect(card.locator("thead th")).toHaveText(["Station", "Rank", "Hours empty", "Hours full", "Episodes", "Longest empty"]);
  await expect(card.locator("[data-takeaway]")).toContainText(`${want[0].name} needed the most`);
  const { window_first_day: a, window_last_day: b } = NEED.window;
  expect((Date.parse(b) - Date.parse(a)) / 86_400_000 + 1).toBe(7); // the mart's need window
  await expect(card.locator("[data-caption]")).toContainText(`Source: marts.mart_rebalancing_need, ${day(a)} – ${day(b)}`);
});

test("trips vs availability chart plots one point per station with a takeaway and a source caption", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="scatter"]');
  await expect(card.getByRole("heading", { level: 2 })).toHaveText("Trips vs availability");
  await expect(card.locator("svg .chart__points circle")).toHaveCount(stations.size);
  const busiest = [...stations.values()].sort((x, y) => y.trips - x.trips);
  const q = Math.max(1, Math.floor(busiest.length / 4));
  const takeaway =
    `The busiest quarter of stations is empty ${pct(mean(busiest.slice(0, q).map((s) => s.share)))} of the time, ` +
    `the quietest quarter ${pct(mean(busiest.slice(-q).map((s) => s.share)))}.`;
  await expect(card.locator("[data-takeaway]")).toHaveText(takeaway);
  const chart = card.getByRole("img");
  await expect(chart).toHaveAttribute("aria-label", new RegExp(`^Scatter plot of ${number.format(stations.size)} stations`));
  await expect(chart).toHaveAttribute("aria-label", new RegExp(takeaway.replace(/[.%()]/g, "\\$&")));
  await expect(card.locator("[data-caption]")).toContainText("Source: marts.mart_trips_vs_availability");
  await expect(card.locator(".chart__axis-label")).toHaveText(["Trips started per day", "% of time empty"]);
});

test("insights page says when the insights are not published yet", async ({ page }) => {
  await prepare(page);
  await page.route("**/data/insights/*.json", (route) => route.fulfill({ status: 404, body: "" }));
  await page.goto("/insights.html");
  const card = page.locator("[data-insights-unpublished]");
  await expect(card).toBeVisible();
  await expect(card).toContainText("The insights are not published yet.");
  await expect(page.getByRole("link", { name: "See live station status" })).toHaveCount(1);
  await expect(card.getByRole("link", { name: "See live station status" })).toHaveAttribute("href", "index.html");
  await expect(page.locator("[data-insights-content]")).toBeHidden();
  await expect(page.locator("[data-insights-error]")).toBeHidden();
  await expect(page.locator("[aria-busy]")).toHaveCount(0);
});

test("insights page shows an error with Retry when a file cannot be read", async ({ page }) => {
  await prepare(page);
  const broken = (route) => route.fulfill({ status: 500, body: "" });
  await page.route("**/data/insights/rebalancing_need.json", broken);
  await page.goto("/insights.html");
  const message = page.locator("[data-insights-error]");
  await expect(message).toBeVisible();
  await expect(message).toContainText("The insights couldn’t be loaded. Check your connection, then try again.");
  await expect(page.locator("[data-insights-content]")).toBeHidden();
  await expect(page.locator("[data-insights-unpublished]")).toBeHidden();

  // The file is back: Retry shows the page.
  await page.unroute("**/data/insights/rebalancing_need.json", broken);
  await message.getByRole("button", { name: "Retry" }).click();
  await expect(page.locator('[data-chart="rebalancing"] tbody tr').first()).toBeVisible();
  await expect(message).toBeHidden();
});

test("insights page does not stay loading when a file never answers", async ({ page }) => {
  await prepare(page);
  await page.route("**/data/insights/trips_vs_availability.json", () => {}); // requested, never answered
  const asked = page.waitForRequest("**/data/insights/trips_vs_availability.json");
  await page.goto("/insights.html");
  await asked;
  await expect(page.locator(".kpis")).toHaveAttribute("aria-busy", "true");
  await expect(page.locator('[data-kpi="empty"] [data-kpi-sub]')).toHaveText("Loading…");

  // The read gives up after 10 s: the error message with Retry, nothing left busy.
  await page.clock.fastForward(10_000);
  const message = page.locator("[data-insights-error]");
  await expect(message).toBeVisible();
  await expect(message.getByRole("button", { name: "Retry" })).toBeVisible();
  await expect(page.locator("[aria-busy]")).toHaveCount(0);
});

test("axe finds no violations on the insights page with data in light and dark", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await open(page);
      const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
      const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
      expect(summary, `insights at ${width} px, ${colorScheme}`).toEqual([]);
    }
  }
});
