// Loop N on the Pipeline page: the Lineage card shows the full graph (Kafka through the Spark listener's jobs and dbt to
// the marts), its Spark half is a list after the batch table, and the table's job names stay whole at every width.
// health.json is the real fixture through prepare().
import { expect, test } from "@playwright/test";
import { describePage, prepare } from "./helpers.js";

// What each replayed Spark job reads and writes, by its OpenLineage name (.loop/plan.md D6).
const STREAM = [
  ["dockwatch_status_stream.write_to_data_source_v2.lake_bronze_station_status", ["gbfs.station_status"], "lake.bronze.station_status"],
  ["dockwatch_status_stream.write_to_data_source_v2.lake_silver_station_status", ["gbfs.station_status"], "lake.silver.station_status"],
  ["dockwatch_status_stream.write_to_data_source_v2.lake_silver_availability_5m", ["gbfs.station_status"], "lake.silver.availability_5m"],
  ["dockwatch_status_stream.replace_data.lake_silver_station_episodes", ["lake.silver.station_episodes"], "lake.silver.station_episodes"],
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

test("pipeline lineage image shows the full graph from gbfs.station_status through Spark to the marts", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="lineage"]');
  const img = card.locator("figure img");
  await expect(img).toHaveAttribute("src", "img/lineage-batch.png");
  const alt = await img.getAttribute("alt");
  for (const words of ["gbfs.station_status", "Spark", "lake.silver.station_status", "marts_daily.dbt_build",
    "trip_history_monthly.load", "marts_daily.export", "table below"]) {
    expect(alt, `alt mentions ${words}`).toContain(words);
  }
  const caption = card.locator("figure figcaption");
  await expect(caption).toContainText("Airflow and the Spark listener");
  await expect(caption).toContainText("Marquez 0.51.1");
  await expect(card.locator(".chart-card__takeaway")).toContainText("Spark");
  await img.scrollIntoViewIfNeeded();
  await expect.poll(() => img.evaluate((el) => el.complete && el.naturalWidth)).toBe(1600);
  expect(await img.evaluate((el) => el.naturalHeight)).toBe(1000);
});

test("pipeline lineage stream list names each Spark job with what it reads and writes", async ({ page }) => {
  await prepare(page);
  await open(page);
  const card = page.locator('[data-chart="lineage"]');
  await expect(card.locator(".table-wrap ~ ul.lineage__stream")).toHaveCount(1);
  await expect(card.locator(".table-wrap ~ .lineage__lead")).toContainText("Spark listener");
  const items = card.locator("ul.lineage__stream > li");
  await expect(items).toHaveCount(STREAM.length);
  for (const [i, [job, reads, writes]] of STREAM.entries()) {
    const item = items.nth(i);
    await expect(item.locator("code")).toHaveText(job);
    const rest = (await item.textContent()).replace(job, "");
    const named = rest.match(/lake\.[a-z_]+\.[a-z0-9_]+|gbfs\.[a-z_]+/g);
    expect(new Set(named), `${job} names exactly what it reads and writes`).toEqual(new Set([...reads, writes]));
  }
  // The batch table is unchanged by the Spark half: still the eight Airflow jobs.
  await expect(card.locator("tbody tr")).toHaveCount(8);
});

test("pipeline lineage job names stay on one line at every width", async ({ page }) => {
  await prepare(page);
  for (const width of [320, 390, 768, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    await open(page);
    await page.locator('[data-chart="lineage"]').scrollIntoViewIfNeeded();
    const got = await page.evaluate(() => {
      const card = document.querySelector('[data-chart="lineage"]');
      const box = card.getBoundingClientRect();
      return {
        scroll: document.documentElement.scrollWidth - document.documentElement.clientWidth,
        lines: [...card.querySelectorAll(".lineage__table th code")].map((el) => [el.textContent, el.getClientRects().length]),
        stream: [...card.querySelectorAll(".lineage__stream code")].map((el) => el.getBoundingClientRect().right - box.right),
      };
    });
    expect(got.lines.length, `${width} px: every batch job`).toBe(8);
    for (const [job, n] of got.lines) {
      expect(n, `${width} px: ${job} on one line`).toBe(1);
    }
    expect(got.scroll, `${width} px: horizontal page scroll`).toBeLessThanOrEqual(0);
    for (const over of got.stream) {
      expect(over, `${width} px: a Spark job name wraps inside the card`).toBeLessThanOrEqual(0);
    }
  }
});
