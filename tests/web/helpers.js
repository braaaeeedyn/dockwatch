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

export const REPLAY_FIXTURE = join(FIXTURES, "replay.json");

/**
 * Route data/live.json and data/alerts.json to the fixtures (optionally a modified live.json) and fake the clock
 * so the page thinks `ageMs` has passed since the fixture's generated_at.
 * data/replay.json is never read from web/data/: by default it is "not published" (404); `replay: true` serves
 * tests/web/fixtures/replay.json, and `replay: <number>` answers with that HTTP status.
 */
export async function prepare(page, { ageMs = 30_000, live = LIVE, replay = false } = {}) {
  watchPage(page);
  await page.route("**/data/live.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(live) }),
  );
  await page.route("**/data/alerts.json", (route) =>
    route.fulfill({ status: 200, contentType: "application/json", path: join(FIXTURES, "alerts.json") }),
  );
  await page.route("**/data/replay.json", (route) =>
    replay === true
      ? route.fulfill({ status: 200, contentType: "application/json", path: REPLAY_FIXTURE })
      : route.fulfill({ status: typeof replay === "number" ? replay : 404, body: "" }),
  );
  await page.clock.install({ time: Date.parse(live.generated_at) + ageMs });
}

const watched = new WeakMap();

/**
 * Record what the page's network and scripts did since its last navigation started: failed requests, requests still
 * pending, page errors, console errors, and whether data/live.json was requested and answered. Idempotent per page.
 * Read it with describePage(page) when an assertion fails, so the failure names its cause.
 */
export function watchPage(page) {
  if (watched.has(page)) return;
  const log = { url: "", failed: [], errors: [], pending: new Set(), liveRequested: 0, liveAnswered: 0 };
  watched.set(page, log);
  const isLive = (request) => request.url().includes("/data/live.json");
  page.on("request", (request) => {
    if (request.isNavigationRequest() && request.frame() === page.mainFrame()) {
      Object.assign(log, { url: request.url(), failed: [], errors: [], liveRequested: 0, liveAnswered: 0 });
      log.pending.clear();
    }
    log.pending.add(request);
    if (isLive(request)) log.liveRequested += 1;
  });
  page.on("requestfinished", (request) => {
    log.pending.delete(request);
    if (isLive(request)) log.liveAnswered += 1;
  });
  page.on("requestfailed", (request) => {
    log.pending.delete(request);
    log.failed.push(`${request.url()}: ${request.failure()?.errorText ?? "failed"}`);
  });
  page.on("pageerror", (error) => log.errors.push(`pageerror: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") log.errors.push(`console error: ${message.text()}`);
  });
}

/** What watchPage() saw since the last navigation, as a few lines for an error message. */
export function describePage(page) {
  const log = watched.get(page);
  if (!log) return "(page not watched)";
  const pending = [...log.pending].map((request) => request.url());
  return [
    `navigation: ${log.url || "none"}`,
    `data/live.json: requested ${log.liveRequested}x, answered ${log.liveAnswered}x`,
    `failed requests: ${log.failed.length ? log.failed.join("; ") : "none"}`,
    `pending requests: ${pending.length ? pending.join("; ") : "none"}`,
    `errors: ${log.errors.length ? log.errors.join("; ") : "none"}`,
  ].join("\n");
}
