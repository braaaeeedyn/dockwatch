// F4b Pipeline page: the status banner, the four tiles, the latency and events charts, table freshness, data quality,
// compaction, and the not-published / stalled-read states. Data: one real health.json (dag-test pipeline_health) in
// tests/web/fixtures/health.json, served by prepare(); expected values are recomputed here from that file.
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { describePage, loadFixture, prepare } from "./helpers.js";

const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const HEALTH = loadFixture("health.json");
const WORDS = { ok: "Healthy", warn: "Degraded", fail: "Failing" };
const RESULTS = { ok: "Passed", warn: "Warning", fail: "Failed" };
const number = new Intl.NumberFormat("en-US");
const oneDecimal = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const rows = (block) => block.rows.map((r) => Object.fromEntries(block.columns.map((c, i) => [c, r[i]])));

async function open(page, path = "/pipeline.html") {
  await page.goto(path);
  try {
    await expect(page.locator('[data-chart="compaction"]')).toHaveClass(/is-ready/);
  } catch (error) {
    error.message += `\n\nWhat the page did:\n${describePage(page)}`;
    throw error;
  }
}

test("pipeline status banner shows the overall status with an icon and a word", async ({ page }) => {
  await prepare(page);
  await open(page);
  const banner = page.locator("[data-status-banner]");
  const overall = HEALTH.status.overall;
  const badge = banner.locator("[data-status-overall] .status-badge");
  await expect(badge).toHaveText(WORDS[overall]);
  await expect(badge).toHaveAttribute("data-status", overall);
  await expect(badge.locator("svg")).toHaveCount(1);
  const items = banner.locator("[data-status-components] li");
  await expect(items).toHaveCount(3);
  for (const [i, c] of HEALTH.status.components.entries()) {
    await expect(items.nth(i).locator(".status-badge")).toHaveText(WORDS[c.status]);
    await expect(items.nth(i)).toContainText(c.detail);
  }
  await expect(banner.locator("[data-status-caption]")).toContainText("Health as of");
});

test("pipeline tiles show p95 latency, events per minute, consumer lag and freshness from health.json", async ({ page }) => {
  await prepare(page);
  await open(page);
  const t = HEALTH.tiles;
  const value = (key) => page.locator(`[data-kpi="${key}"] [data-kpi-value]`);
  await expect(value("latency")).toHaveText(t.latency_p95_s === null ? "—" : `${oneDecimal.format(t.latency_p95_s)} s`);
  await expect(value("events")).toHaveText(t.events_per_min === null ? "—" : oneDecimal.format(t.events_per_min));
  await expect(value("lag")).toHaveText(number.format(t.lag_records));
  await expect(value("freshness")).not.toHaveText("Loading…");
  await expect(page.locator('[data-kpi="lag"] .stat-tile__label')).toHaveText("Consumer lag");
  const info = page.getByRole("button", { name: "What “Consumer lag” means" });
  await info.click();
  await expect(page.locator("#kpi-def-lag")).toBeVisible();
});

test("pipeline latency chart draws p50 and p95 with the 5 min SLA line", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="latency"]');
  await expect(card.locator("path.chart__line--primary")).toHaveCount(1);
  await expect(card.locator("path.chart__line--secondary")).toHaveCount(1);
  await expect(card.locator("line.chart__threshold")).toHaveCount(1);
  await expect(card.locator(".chart__threshold-label")).toHaveText("5 min SLA");
  await expect(card.locator(".chart-legend li")).toHaveText(["p50", "p95", "5 min SLA"]);
  const minutes = rows(HEALTH.minutely);
  const active = minutes.filter((r) => r.processed > 0).length;
  if (active) {
    const d = await card.locator("path.chart__line--secondary").getAttribute("d");
    expect(d.startsWith("M")).toBe(true); // a drawn series, with gaps where nothing was processed
  }
  await expect(card.getByRole("img")).toHaveAttribute("aria-label", /p50 and p95/);
  await expect(card.locator("[data-caption]")).toContainText("Source: lake.metrics.pipeline_minutely");
  await expect(page.locator('[data-chart="events"] path.chart__line--primary')).toHaveCount(1);
});

test("pipeline freshness table lists every lake table with its last commit and status", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="freshness"]');
  const tables = rows(HEALTH.tables);
  await expect(card.locator("tbody tr")).toHaveCount(tables.length);
  await expect(card.locator("tbody th")).toHaveText(tables.map((r) => r.table));
  await expect(card.locator("tbody tr").first().locator(".status-badge")).toHaveText(WORDS[tables[0].status]);
  await expect(card.locator("thead th")).toHaveText(["Table", "Last commit", "Age", "Files", "Status"]);
  await expect(card.locator("tbody tr").first().locator("td").nth(2)).toHaveText(number.format(tables[0].files));
});

test("pipeline data-quality table lists the dbt tests and the quality gate", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="quality"]');
  const tests = HEALTH.quality.tests;
  await expect(card.locator("tbody tr")).toHaveCount(tests.length);
  await expect(card.locator("tbody th").first()).toHaveText(tests[0].name);
  const gate = HEALTH.quality.gate;
  await expect(card.locator("[data-gate]")).toContainText(
    `Quality gate: ${number.format(gate.tests_passed)} of ${number.format(gate.tests)} tests passed`,
  );
  await expect(card.locator("[data-gate] .status-badge")).toHaveText(gate.passed ? RESULTS.ok : RESULTS.fail);
});

test("pipeline compaction chart shows files before and after the latest maintenance", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="compaction"]');
  const runs = rows(HEALTH.compaction);
  await expect(card.locator(".bars__row")).toHaveCount(runs.length);
  const measured = runs.findIndex((r) => r.files_before !== null);
  const row = card.locator(".bars__row").nth(measured);
  await expect(row.locator(".bars__name")).toHaveText(runs[measured].table);
  await expect(row.locator(".bars__bar--before")).toHaveText(`${number.format(runs[measured].files_before)} before`);
  await expect(row.locator(".bars__bar--after")).toHaveText(`${number.format(runs[measured].files_after)} after`);
  await expect(card.locator("[data-caption]")).toContainText("lake.metrics.maintenance_runs");
});

test("pipeline page says when health.json is not published yet", async ({ page }) => {
  await prepare(page);
  await page.route("**/data/health.json", (route) => route.fulfill({ status: 404, body: "" }));
  await page.goto("/pipeline.html");
  const card = page.locator("[data-pipeline-unpublished]");
  await expect(card).toBeVisible();
  await expect(card).toContainText("Pipeline health is not published yet.");
  await expect(page.getByRole("link", { name: "See live station status" })).toHaveCount(1);
  await expect(page.locator("[data-pipeline-content]")).toBeHidden();
  await expect(page.locator("[aria-busy]")).toHaveCount(0);
});

test("pipeline page does not stay loading when health.json never answers", async ({ page }) => {
  let hang = true;
  await prepare(page);
  await page.route("**/data/health.json", (route) => (hang ? undefined : route.fallback()));
  const asked = page.waitForRequest("**/data/health.json");
  await page.goto("/pipeline.html");
  await asked;
  await expect(page.locator(".kpis")).toHaveAttribute("aria-busy", "true");
  await page.clock.fastForward(10_000);
  const message = page.locator("[data-pipeline-error]");
  await expect(message).toBeVisible();
  await expect(message).toContainText("Pipeline health couldn’t be loaded.");
  await expect(page.locator("[aria-busy]")).toHaveCount(0);
  hang = false;
  await message.getByRole("button", { name: "Retry" }).click();
  await expect(page.locator('[data-chart="compaction"]')).toHaveClass(/is-ready/);
  await expect(message).toBeHidden();
});

test("axe finds no violations on the pipeline page with data in light and dark", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await open(page);
      const results = await new AxeBuilder({ page }).withTags(AXE_TAGS).analyze();
      const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
      expect(summary, `pipeline at ${width} px, ${colorScheme}`).toEqual([]);
    }
  }
});
