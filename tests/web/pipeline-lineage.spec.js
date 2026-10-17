// Loop M on the Pipeline page: the static Lineage card (a Marquez screenshot of the batch graph, web/img/lineage-batch.png,
// and the same graph as a table, its text alternative). The card needs no data, but it sits in the page's content, so
// it shows and hides with the rest; health.json is the real fixture through prepare().
import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { describePage, prepare } from "./helpers.js";

const AXE_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];
const MARTS = [
  "lake.marts.mart_station_availability_daily",
  "lake.marts.mart_rebalancing_need",
  "lake.marts.mart_trips_vs_availability",
];
const GATE = "data/dbt/quality_gate.json";
const GX = "data/quality/expectations.json";
const RUN_RESULTS = "data/dbt/target/run_results.json";
// What each batch job reads and writes, by its OpenLineage name (.loop/plan.md D5).
const JOBS = [
  ["trip_history_monthly.download", ["s3://baywheels-data/tripdata"], ["data/trips/zips"]],
  ["trip_history_monthly.load", ["data/trips/zips"], ["lake.bronze.trips", "lake.metrics.trip_loads"]],
  ["marts_daily.stations", ["s3://dockwatch/raw/gbfs/station_information"], ["lake.bronze.station_information"]],
  [
    "marts_daily.dbt_build",
    ["lake.silver.station_status", "lake.silver.availability_5m", "lake.silver.station_episodes", "lake.bronze.trips",
      "lake.bronze.station_information"],
    [...MARTS, RUN_RESULTS],
  ],
  ["marts_daily.quality_gate", [...MARTS, "lake.silver.station_status", "lake.silver.availability_5m", RUN_RESULTS], [GATE, GX]],
  ["marts_daily.export", [...MARTS, GATE], ["web/data/insights"]],
  ["pipeline_health.minutely", ["lake.silver.station_status"], ["lake.metrics.pipeline_minutely"]],
  [
    "pipeline_health.export",
    ["lake.metrics.pipeline_minutely", "lake.silver.station_status", "lake.metrics.maintenance_runs", GATE, GX, RUN_RESULTS],
    ["web/data/health.json"],
  ],
];

async function open(page) {
  await page.goto("/pipeline.html");
  try {
    await expect(page.locator('[data-chart="compaction"]')).toHaveClass(/is-ready/);
  } catch (error) {
    error.message += `\n\nWhat the page did:\n${describePage(page)}`;
    throw error;
  }
}

test("pipeline lineage card shows the Marquez screenshot with a text alternative", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="lineage"]');
  await expect(card).toBeVisible();
  expect(await card.getAttribute("aria-busy"), "static: never waits for health.json").toBeNull();
  await expect(card.locator(".chart-card__title")).toHaveText("Lineage");
  await expect(card).toHaveAttribute("aria-labelledby", "lineage-title");
  const img = card.locator("figure img");
  await expect(img).toHaveCount(1);
  await expect(img).toHaveAttribute("src", "img/lineage-batch.png");
  await expect(img).toHaveAttribute("loading", "lazy");
  const alt = await img.getAttribute("alt");
  expect(alt.length).toBeGreaterThan(80);
  for (const words of ["Marquez", "trip_history_monthly.load", "marts_daily.dbt_build", "lake.silver.station_status",
    "marts_daily.export", "table below"]) {
    expect(alt, `alt mentions ${words}`).toContain(words);
  }
  await img.scrollIntoViewIfNeeded();
  await expect.poll(() => img.evaluate((el) => el.complete && el.naturalWidth)).toBeGreaterThan(0);
  const size = await img.evaluate((el) => ({
    natural: [el.naturalWidth, el.naturalHeight],
    attrs: [Number(el.getAttribute("width")), Number(el.getAttribute("height"))],
  }));
  expect(size.attrs, "width / height are the PNG's real size (no layout shift)").toEqual(size.natural);
  const caption = card.locator("figure figcaption");
  await expect(caption).toHaveClass(/caption/);
  await expect(caption).toContainText("OpenLineage");
  await expect(caption).toContainText("Marquez 0.51.1");
  await expect(caption).toContainText("newest run of each DAG");
  // The text alternative is the table right under the figure.
  await expect(card.locator("figure + .table-wrap table tbody tr")).toHaveCount(JOBS.length);

  // It lives in the page's content: when health.json is not published, it hides with the rest.
  await page.route("**/data/health.json", (route) => route.fulfill({ status: 404, body: "" }));
  await page.goto("/pipeline.html");
  await expect(page.locator("[data-pipeline-unpublished]")).toBeVisible();
  await expect(card).toBeHidden();
});

test("pipeline lineage table lists every batch job with what it reads and writes", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="lineage"]');
  const region = card.locator(".table-wrap");
  await expect(region).toHaveAttribute("role", "region");
  await expect(region).toHaveAttribute("tabindex", "0");
  await expect(card.locator("thead th")).toHaveText(["Job", "Reads", "Writes"]);
  const rows = card.locator("tbody tr");
  await expect(rows).toHaveCount(JOBS.length);
  for (const [i, [job, reads, writes]] of JOBS.entries()) {
    const row = rows.nth(i);
    await expect(row.locator("th")).toHaveAttribute("scope", "row");
    await expect(row.locator("th code")).toHaveText(job);
    await expect(row.locator("td")).toHaveText([reads.join(", "), writes.join(", ")]);
  }
  // The quality gate reads silver (the Great Expectations batch) besides the marts.
  const gate = rows.filter({ has: page.locator("th", { hasText: "marts_daily.quality_gate" }) });
  await expect(gate.locator("td").first()).toContainText("lake.silver.station_status");
});

test("pipeline lineage card fits a 320 px screen without sideways page scroll", async ({ page }) => {
  await prepare(page);
  for (const width of [320, 390]) {
    await page.setViewportSize({ width, height: 800 });
    await open(page);
    const card = page.locator('[data-chart="lineage"]');
    await card.scrollIntoViewIfNeeded();
    const got = await page.evaluate(() => {
      const card = document.querySelector('[data-chart="lineage"]');
      const img = card.querySelector("img").getBoundingClientRect();
      const wrap = card.querySelector(".table-wrap").getBoundingClientRect();
      const box = card.getBoundingClientRect();
      return {
        scroll: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        card: [box.left, box.right],
        img: [img.left, img.right, img.width, img.height],
        wrap: [wrap.left, wrap.right],
      };
    });
    expect(got.scroll, `${width} px: horizontal page scroll`).toBeLessThanOrEqual(0);
    expect(got.img[0], `${width} px: image inside the card`).toBeGreaterThanOrEqual(got.card[0]);
    expect(got.img[1], `${width} px: image inside the card`).toBeLessThanOrEqual(got.card[1]);
    const aspect = Math.abs(got.img[3] - (got.img[2] * 1000) / 1600); // within its 1 px border
    expect(aspect, `${width} px: the image keeps its aspect`).toBeLessThanOrEqual(2);
    expect(got.wrap[1], `${width} px: the table scrolls inside its own box`).toBeLessThanOrEqual(got.card[1]);
  }
});

test("axe finds no violations in the lineage card in light and dark", async ({ page }) => {
  test.setTimeout(120_000);
  await prepare(page);
  for (const colorScheme of ["light", "dark"]) {
    await page.emulateMedia({ colorScheme, reducedMotion: "reduce" });
    for (const width of [390, 1440]) {
      await page.setViewportSize({ width, height: 900 });
      await open(page);
      await page.locator('[data-chart="lineage"] img').scrollIntoViewIfNeeded();
      const results = await new AxeBuilder({ page }).include('[data-chart="lineage"]').withTags(AXE_TAGS).analyze();
      const summary = results.violations.map((v) => `${v.id}: ${v.nodes.map((n) => n.target).join(", ")}`);
      expect(summary, `lineage card at ${width} px, ${colorScheme}`).toEqual([]);
    }
  }
});
