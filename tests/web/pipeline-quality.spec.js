// Loop L on the Pipeline page: the Great Expectations card (results of the checkpoint inside marts_daily's quality
// gate, from health.json quality.expectations), the latency axis in durations with labels clear of the axis title, and
// the latency tile's duration sub-line. Data: the real health.json fixture through prepare() and a fake clock; the
// variants (no expectations, a failed check, very large and very small latency) are built here from that fixture.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { formatDuration } from "../../web/js/util/time.js";
import { describePage, loadFixture, prepare } from "./helpers.js";

const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const HEALTH = loadFixture("health.json");
const GX = HEALTH.quality.expectations;
const RESULTS = { ok: "Passed", warn: "Warning", fail: "Failed" };
const number = new Intl.NumberFormat("en-US");
const oneDecimal = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const TICK = /^\d+(\.\d+)? (s|min|h)$/;
const P50 = HEALTH.minutely.columns.indexOf("latency_p50_s");
const P95 = HEALTH.minutely.columns.indexOf("latency_p95_s");
const MAX_P95 = Math.max(...HEALTH.minutely.rows.map((r) => r[P95] ?? 0));

/** A copy of the fixture with `change(doc)` applied. */
function variant(change) {
  const doc = structuredClone(HEALTH);
  change(doc);
  return doc;
}

/** The fixture with every latency scaled so the largest p95 becomes `top` seconds. */
function latencyUpTo(top) {
  return variant((doc) => {
    const k = top / MAX_P95;
    for (const r of doc.minutely.rows) {
      for (const i of [P50, P95]) r[i] = r[i] === null ? null : r[i] * k;
    }
    doc.tiles.latency_p95_s = doc.tiles.latency_p95_s === null ? null : doc.tiles.latency_p95_s * k;
  });
}

async function open(page, doc = HEALTH) {
  await page.route("**/data/health.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(doc) }),
  );
  await page.goto("/pipeline.html");
  try {
    await expect(page.locator('[data-chart="compaction"]')).toHaveClass(/is-ready/);
  } catch (error) {
    error.message += `\n\nWhat the page did:\n${describePage(page)}`;
    throw error;
  }
  await page.evaluate(() => document.fonts.ready); // the chart redraws once the fonts it measures have loaded
}

test("pipeline expectations card lists every Great Expectations result with its status", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="expectations"]');
  await expect(card.locator(".chart-card__title")).toHaveText("Great Expectations on silver");
  await expect(card.locator("thead th")).toHaveText(["Check", "Column", "Result", "Found"]);
  const rows = card.locator("tbody tr");
  await expect(rows).toHaveCount(GX.results.length);
  for (const [i, r] of GX.results.entries()) {
    const row = rows.nth(i);
    await expect(row.locator("th")).toHaveText(r.label);
    await expect(row.locator("td").nth(0)).toHaveText(r.column ? r.column.replaceAll(",", ", ") : "whole batch");
    await expect(row.locator(".status-badge")).toHaveText(RESULTS[r.status]);
    await expect(row.locator(".status-badge")).toHaveAttribute("data-status", r.status);
    const found = row.locator("td").nth(2);
    if (r.unexpected_count !== null) await expect(found).toHaveText(`${number.format(r.unexpected_count)} unexpected rows`);
    else if (r.column === "age_s") await expect(found).toHaveText(`${formatDuration(r.observed * 1000)} old`);
    else if (r.observed !== null) await expect(found).toHaveText(`${number.format(r.observed)} rows`);
  }
  const warns = GX.results.some((r) => r.column === "age_s" && r.status === "warn");
  await expect(card.locator("[data-takeaway]")).toHaveText(
    `${GX.passed} of ${GX.evaluated} checks passed${warns ? "; freshness is a warning: Spark runs on demand" : ""}.`,
  );
  await expect(card.locator("[data-caption]")).toContainText(
    `Source: Great Expectations ${GX.gx_version}, suite silver_station_status, newest 24 h of silver ` +
      `(${number.format(GX.batch.rows)} rows), checked by marts_daily at`,
  );
  await expect(card.locator("[data-expectations-none]")).toBeHidden();
  // The dbt card next to it is unchanged: its rows are still the dbt tests.
  await expect(page.locator('[data-chart="quality"] tbody tr')).toHaveCount(HEALTH.quality.tests.length);

  const broken = variant((doc) => {
    const r = doc.quality.expectations.results.find((x) => x.column === "station_id,snapshot_ts");
    Object.assign(r, { success: false, status: "fail", unexpected_count: 2 });
    Object.assign(doc.quality.expectations, { passed: GX.passed - 1, failed: 1, success: false });
  });
  await open(page, broken);
  const row = rows.filter({ hasText: "One row per station per snapshot" });
  await expect(row.locator(".status-badge")).toHaveText("Failed");
  await expect(row.locator("td").nth(2)).toHaveText("2 unexpected rows");
  await expect(card.locator("[data-takeaway]")).toContainText("1 blocking check failed, so the insights were not refreshed");
});

test("pipeline expectations card says when no expectations have run yet", async ({ page }) => {
  await prepare(page);
  for (const doc of [variant((d) => (d.quality.expectations = null)), variant((d) => delete d.quality.expectations)]) {
    await open(page, doc);
    const card = page.locator('[data-chart="expectations"]');
    await expect(card.locator("[data-expectations-none]")).toBeVisible();
    await expect(card.locator("[data-expectations-none]")).toHaveText("No Great Expectations results yet.");
    await expect(card.locator("[data-expectations-none]")).toHaveClass(/card-soft/);
    await expect(card.locator("[data-expectations-table]")).toBeHidden();
    await expect(card.locator("[data-takeaway]")).toBeHidden();
    await expect(card.locator(".status-badge")).toHaveCount(0);
    await expect(page.locator('[data-chart="quality"] tbody tr')).toHaveCount(HEALTH.quality.tests.length);
  }
});

test("pipeline latency axis reads in durations and its labels never overlap the axis title", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  const variants = [["fixture", HEALTH], ["over 100,000 s", latencyUpTo(120_000)], ["under 120 s", latencyUpTo(90)]];
  for (const [name, doc] of variants) {
    for (const width of [320, 390, 768, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await open(page, doc);
      const where = `${name} at ${width} px`;
      const svg = page.locator('[data-chart="latency"] svg');
      const got = await svg.evaluate((el) => {
        const box = (t) => {
          const r = t.getBoundingClientRect();
          return { text: t.textContent, left: r.left, right: r.right, width: r.width };
        };
        return {
          svg: box(el),
          title: box(el.querySelector(".chart__axis-label")),
          ticks: [...el.querySelectorAll('.chart__tick[text-anchor="end"]')].map(box),
        };
      });
      expect(got.title.text, where).toBe("Latency");
      expect(got.ticks.length, where).toBeGreaterThanOrEqual(3);
      for (const tick of got.ticks) {
        expect(tick.text, where).toMatch(TICK);
        expect(tick.width, `${where}: "${tick.text}" is drawn`).toBeGreaterThan(0);
        expect(tick.left, `${where}: "${tick.text}" overlaps the axis title`).toBeGreaterThanOrEqual(got.title.right);
      }
      expect(new Set(got.ticks.map((t) => t.text.split(" ")[1])).size, `${where}: one unit`).toBe(1);
      expect(got.title.left, `${where}: title inside the chart`).toBeGreaterThanOrEqual(got.svg.left);
    }
  }
  // The takeaway speaks in durations too.
  const big = latencyUpTo(120_000);
  const processed = big.minutely.columns.indexOf("processed");
  const worst = Math.max(...big.minutely.rows.filter((r) => r[processed] > 0).map((r) => r[P95] ?? 0));
  await open(page, big);
  await expect(page.locator('[data-chart="latency"] [data-takeaway]')).toContainText(
    `The slowest minute's p95 was ${formatDuration(worst * 1000)}, over the 5 min SLA`,
  );
});

test("pipeline latency tile adds the p95 as a readable duration", async ({ page }) => {
  await prepare(page);
  const tile = page.locator('[data-kpi="latency"]');
  const cases = [HEALTH, latencyUpTo(90), variant((d) => (d.tiles.latency_p95_s = null))];
  for (const doc of cases) {
    await open(page, doc);
    const p95 = doc.tiles.latency_p95_s;
    await expect(tile.locator("[data-kpi-value]")).toHaveText(p95 === null ? "—" : `${oneDecimal.format(p95)} s`);
    await expect(tile.locator("[data-kpi-sub]")).toHaveText(
      `${p95 === null ? "" : `${formatDuration(p95 * 1000)} · `}fetch to silver, worst minute of the last active hour`,
    );
  }
});

test("axe finds no violations in the expectations card in light and dark", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      for (const [name, doc] of [["results", HEALTH], ["none yet", variant((d) => (d.quality.expectations = null))]]) {
        await open(page, doc);
        const results = await new AxeBuilder({ page }).include('[data-chart="expectations"]').withTags(AXE_TAGS).analyze();
        const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
        expect(summary, `expectations card (${name}) at ${width} px, ${colorScheme}`).toEqual([]);
      }
    }
  }
});
