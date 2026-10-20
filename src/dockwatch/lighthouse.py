"""Lighthouse on the Live page, run locally (F5, Loop P). Stdlib only, plus dockwatch.batch.guard.

  python -m dockwatch.lighthouse          (python tasks.py lighthouse)

Serves a copy of web/ whose data/ is tests/web/fixtures/ with tests/web/serve.py (no compression, no cache headers,
like S3 static hosting) and runs the pinned Lighthouse CLI (tools/lighthouse, installed with `npm ci --ignore-scripts`
from its lockfile) with Playwright's Chromium (chromium.executablePath()), RUNS times in each configuration: mobile
(Lighthouse's default) and desktop (--preset=desktop), on paused data (the fixtures byte-exact) and on live data (the
fixtures' timestamps moved to each run's start). Reports go to data/lighthouse/<id>/, the record to
data/metrics/lighthouse-<id>.json. Exit 0 only when every run reported and scored accessibility 100.
Refused beside Trino, a Spark JVM, Kafka Connect or Marquez: Chromium takes ~0.9 GB of host memory.
"""

import hashlib
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

from dockwatch.batch import guard as batch_guard

ROOT = Path(__file__).resolve().parents[2]
LIGHTHOUSE_DIR = ROOT / "tools" / "lighthouse"
CLI = LIGHTHOUSE_DIR / "node_modules" / "lighthouse" / "cli" / "index.js"
WEB = ROOT / "web"
FIXTURES = ROOT / "tests" / "web" / "fixtures"
SERVE = ROOT / "tests" / "web" / "serve.py"
METRICS_DIR = ROOT / "data" / "metrics"
REPORTS_DIR = ROOT / "data" / "lighthouse"
RUNS = 5
PORT = 5180
URL = f"http://127.0.0.1:{PORT}/index.html"
CONFIGS = ("mobile-live", "mobile-paused", "desktop-live", "desktop-paused")
BLOCKERS = ("trino", "status-stream", "cdc-apply", "connect")  # plus any marquez-* service
LIVE_KEYS = {"live.json": ("generated_at", "data_as_of"), "alerts.json": ("generated_at",)}
MEDIAN_KEYS = ("performance", "fcp_ms", "lcp_ms", "tbt_ms", "cls", "si_ms")
RUN_TIMEOUT_S = 180


def refusal() -> str | None:
    """None if Lighthouse may start now, else why not (it names the running services)."""
    busy = sorted(s for s in batch_guard.running_services() if s in BLOCKERS or s.startswith("marquez-"))
    if not busy:
        return None
    return (
        f"lighthouse: {' and '.join(busy)} running; stop it first. Lighthouse's Chromium (~0.9 GB) never runs beside "
        "Trino, a Spark JVM, Kafka Connect or Marquez on this host."
    )


def build_site(dest: Path) -> Path:
    """dest/site: web/ without web/data (the exporter rewrites it), and tests/web/fixtures/ as data/."""
    site = dest / "site"
    shutil.copytree(WEB, site, ignore=lambda d, names: ["data"] if Path(d) == WEB else [])
    shutil.copytree(FIXTURES, site / "data")
    return site


def make_live(site: Path, now: datetime) -> str:
    """Move live.json's generated_at / data_as_of and alerts.json's generated_at to `now`; nothing else changes."""
    stamp = now.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    for name, keys in LIVE_KEYS.items():
        f = site / "data" / name
        doc = json.loads(f.read_text(encoding="utf-8"))
        doc.update(dict.fromkeys(keys, stamp))
        f.write_text(json.dumps(doc, separators=(",", ":")), encoding="utf-8")  # compact, like the fixtures' bytes
    return stamp


def lighthouse_cmd(node: str, config: str, url: str, out: Path) -> list[str]:
    """The pinned local CLI; Lighthouse's default throttling and form factor (mobile), --preset=desktop for desktop."""
    cmd = [
        node,
        str(CLI),
        url,
        "--output=json",
        f"--output-path={out}",
        "--chrome-flags=--headless=new",
        "--only-categories=performance,accessibility,best-practices,seo",
        "--no-enable-error-reporting",
        "--quiet",
    ]
    return [*cmd, "--preset=desktop"] if config.startswith("desktop") else cmd


def scores(lhr: dict | None) -> dict | None:
    """One report's numbers (scores x 100, times in ms, CLS to 3 places); None for a missing or failed run."""
    if not lhr or lhr.get("runtimeError"):
        return None
    try:
        a, c = lhr["audits"], lhr["categories"]
        return {
            "performance": round(c["performance"]["score"] * 100),
            "accessibility": round(c["accessibility"]["score"] * 100),
            "best_practices": round(c["best-practices"]["score"] * 100),
            "seo": round(c["seo"]["score"] * 100),
            "fcp_ms": round(a["first-contentful-paint"]["numericValue"]),
            "lcp_ms": round(a["largest-contentful-paint"]["numericValue"]),
            "tbt_ms": round(a["total-blocking-time"]["numericValue"]),
            "cls": round(a["cumulative-layout-shift"]["numericValue"], 3),
            "si_ms": round(a["speed-index"]["numericValue"]),
        }
    except (KeyError, TypeError):  # a category or metric Lighthouse could not score is null
        return None


def summarize(reports: dict[str, list[dict | None]]) -> dict:
    """Each configuration's good runs, their medians and accessibility minimum; ok = RUNS good runs everywhere, each
    with accessibility 100."""
    configs, ok = {}, True
    for cfg in CONFIGS:
        runs = [r for r in map(scores, reports.get(cfg, [])) if r]
        ok = ok and len(runs) == RUNS and all(r["accessibility"] == 100 for r in runs)
        configs[cfg] = {
            "runs": runs,
            "median": {k: statistics.median(r[k] for r in runs) for k in MEDIAN_KEYS} if runs else {},
            "accessibility_min": min((r["accessibility"] for r in runs), default=None),
        }
    return {"configs": configs, "ok": ok}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run_lighthouse(cmd: list[str], env: dict) -> str:
    """Run one Lighthouse CLI; on a timeout kill its whole tree (Chromium is node's child). Returns its stderr tail."""
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, errors="replace")
    try:
        _, err = proc.communicate(timeout=RUN_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True, check=False)
        proc.kill()
        _, err = proc.communicate()
        err = f"timed out after {RUN_TIMEOUT_S} s\n{err}"
    return "" if proc.returncode == 0 else err[-2000:]


def playwright_chromium(node: str) -> tuple[str, str]:
    """(chromium.executablePath() of the repo's Playwright, its browserVersion from playwright-core's browsers.json);
    ("", "") when Playwright is not installed (npm ci at the repo root)."""
    chrome = subprocess.run(
        [node, "-e", "console.log(require('@playwright/test').chromium.executablePath())"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    browsers = ROOT / "node_modules" / "playwright-core" / "browsers.json"
    if not chrome or not browsers.is_file():
        return "", ""
    doc = json.loads(browsers.read_text(encoding="utf-8"))
    return chrome, next((b["browserVersion"] for b in doc["browsers"] if b["name"] == "chromium"), "")


def wait_for_port(seconds: float = 15) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", PORT)) == 0:
                return True
        time.sleep(0.2)
    return False


def main() -> int:
    reason = refusal()
    if reason:
        print(reason)
        return 1
    node = shutil.which("node")
    if not node:
        print("lighthouse: node not found (install Node 20, then npm ci)")
        return 1
    pinned = json.loads((LIGHTHOUSE_DIR / "package.json").read_text(encoding="utf-8"))["devDependencies"]["lighthouse"]
    installed = CLI.parents[1] / "package.json"
    if not installed.is_file() or json.loads(installed.read_text(encoding="utf-8")).get("version") != pinned:
        # node's own npm-cli.js, not npm.cmd (that goes through cmd.exe), like lineage-shot runs node directly
        npm = Path(node).parent / "node_modules" / "npm" / "bin" / "npm-cli.js"
        cmd = [node, str(npm), "ci", "--prefix", str(LIGHTHOUSE_DIR), "--ignore-scripts", "--no-audit", "--no-fund"]
        print("+", " ".join(cmd), flush=True)
        if subprocess.run(cmd, check=False).returncode:
            print("lighthouse: npm ci failed (tools/lighthouse/package-lock.json)")
            return 1
    chrome, chrome_version = playwright_chromium(node)
    if not chrome or not Path(chrome).is_file():
        print(
            f"lighthouse: Playwright's Chromium is missing ({chrome or 'no path'}); run npm ci, then npx playwright "
            "install chromium"
        )
        return 1
    with socket.socket() as s:
        if s.connect_ex(("127.0.0.1", PORT)) == 0:
            print(f"lighthouse: port {PORT} is in use; stop whatever listens there")
            return 1

    started = datetime.now(UTC)
    run_id = started.strftime("%Y%m%dT%H%M%SZ")
    out_dir = REPORTS_DIR / run_id
    out_dir.mkdir(parents=True)
    paused_at = json.loads((FIXTURES / "live.json").read_text(encoding="utf-8"))["generated_at"]
    env = {**os.environ, "CHROME_PATH": chrome}
    reports: dict[str, list[dict | None]] = {c: [] for c in CONFIGS}
    meta: dict[str, list[dict]] = {c: [] for c in CONFIGS}
    with tempfile.TemporaryDirectory() as tmp:
        site = build_site(Path(tmp))
        srv = subprocess.Popen(
            [sys.executable, str(SERVE), str(PORT), str(site)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try:
            if not wait_for_port():
                print(f"lighthouse: serve.py did not listen on {PORT}")
                return 1
            for cfg in CONFIGS:
                for i in range(1, RUNS + 1):
                    for name in LIVE_KEYS:  # paused = the fixtures byte-exact, restored before every run
                        shutil.copyfile(FIXTURES / name, site / "data" / name)
                    gen = make_live(site, datetime.now(UTC)) if cfg.endswith("-live") else paused_at
                    out = out_dir / f"{cfg}-{i}.json"
                    err = run_lighthouse(lighthouse_cmd(node, cfg, URL, out), env)
                    lhr = json.loads(out.read_text(encoding="utf-8")) if out.is_file() else None
                    reports[cfg].append(lhr)
                    meta[cfg].append({"report": out.name, "generated_at": gen})
                    row = scores(lhr)
                    if row:
                        print(
                            f"lighthouse: {cfg} {i}/{RUNS}: performance {row['performance']}, accessibility "
                            f"{row['accessibility']}, LCP {row['lcp_ms']} ms, CLS {row['cls']}",
                            flush=True,
                        )
                    else:
                        print(f"lighthouse: {cfg} {i}/{RUNS}: no usable report\n{err}", flush=True)
        finally:
            srv.terminate()
            srv.wait(10)

    s = summarize(reports)
    for cfg in CONFIGS:  # each good run's row with the report it came from and the generated_at it was served
        good = [m for m, lhr in zip(meta[cfg], reports[cfg], strict=True) if scores(lhr)]
        s["configs"][cfg]["runs"] = [m | r for m, r in zip(good, s["configs"][cfg]["runs"], strict=True)]
    first = next((x for c in CONFIGS for x in reports[c] if x), {})
    record = {
        "id": run_id,
        "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "lighthouse": pinned,
        "axe_core": ((first.get("environment") or {}).get("credits") or {}).get("axe-core"),
        "chromium": chrome,
        "chromium_version": chrome_version,
        "url": URL,
        "fixtures": {n: sha256(FIXTURES / n) for n in LIVE_KEYS},
        "reports_dir": out_dir.relative_to(ROOT).as_posix(),
        "runs_per_config": RUNS,
        **s,
    }
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    (METRICS_DIR / f"lighthouse-{run_id}.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    medians = {c: s["configs"][c]["median"].get("performance") for c in CONFIGS}
    a11y = {c: s["configs"][c]["accessibility_min"] for c in CONFIGS}
    print(json.dumps({"id": run_id, "ok": s["ok"], "performance_median": medians, "accessibility_min": a11y}))
    return 0 if s["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
