// Shared helpers for the web tests. UI tests never read the real web/data/*.json (gitignored, changes every
// minute): every test routes live.json / alerts.json to the fixtures and controls time with page.clock.
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "fixtures");

export const PAGES = [
  { path: "/index.html", name: "Live", skip: "Skip to map", target: "map" },
  { path: "/insights.html", name: "Insights", skip: "Skip to content", target: "content" },
  { path: "/pipeline.html", name: "Pipeline", skip: "Skip to content", target: "content" },
  { path: "/about.html", name: "About", skip: "Skip to content", target: "content" },
];

export function loadFixture(name) {
  return JSON.parse(readFileSync(join(FIXTURES, name), "utf-8"));
}

export const LIVE = loadFixture("live.json");
export const GENERATED_AT_MS = Date.parse(LIVE.generated_at);

/**
 * Route data/live.json and data/alerts.json to the fixtures (optionally a modified live.json) and fake the clock
 * so the page thinks `ageMs` has passed since the fixture's generated_at.
 */
export async function prepare(page, { ageMs = 30_000, live = LIVE } = {}) {
  await page.route("**/data/live.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(live) }),
  );
  await page.route("**/data/alerts.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", path: join(FIXTURES, "alerts.json") }),
  );
  await page.clock.install({ time: Date.parse(live.generated_at) + ageMs });
}
