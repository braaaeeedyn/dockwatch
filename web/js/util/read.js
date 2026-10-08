// Read a JSON file with a read timeout. A read that hasn't answered (headers and body) after `timeoutMs` is aborted and
// throws, so a stalled connection ends in the caller's error message instead of "Loading…" forever.
// setTimeout, not AbortSignal.timeout(): the tests' fake clock (page.clock) drives it.
// freshness.js keeps its own copy of this pattern for live.json (proven by its own test).

/** Fetch and parse `url`; throws on HTTP errors, bad JSON, or after `timeoutMs` without a full answer. */
export async function readJson(url, { timeoutMs, cache = "no-store" } = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { cache, signal: controller.signal });
    if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
    return await response.json();
  } finally {
    clearTimeout(timer); // after the body is read: the timeout covers the whole read
  }
}
