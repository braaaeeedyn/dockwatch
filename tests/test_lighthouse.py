"""Loop P: dockwatch.lighthouse (python tasks.py lighthouse) without a browser, network or Docker: the served site, the
live state, the summary, the pinned CLI with Playwright's Chromium (subprocesses faked) and the refusal beside a JVM or
Marquez (running_services faked)."""

import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from dockwatch import lighthouse
from dockwatch.batch import guard as batch_guard

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "web" / "fixtures"
NOW = datetime(2026, 10, 20, 7, 0, 0, tzinfo=UTC)
STAMP = "2026-10-20T07:00:00Z"
BLOCKING = ("trino", "status-stream", "cdc-apply", "connect", "marquez-db", "marquez-api", "marquez-web")
STREAM_STACK = {"redpanda", "s3", "iceberg-rest", "ops-db", "console", "airflow-db"}


def tree(root: Path, skip: str | None = None) -> dict[str, str]:
    """relative path -> sha256 of every file under root (optionally without the top-level dir `skip`)."""
    return {
        p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file() and p.relative_to(root).parts[0] != skip
    }


def lhr(perf: float, a11y: float = 1.0, lcp: float = 2500.4, cls: float = 0.0081) -> dict:
    audits = {
        "first-contentful-paint": 1800.6,
        "largest-contentful-paint": lcp,
        "total-blocking-time": 0,
        "cumulative-layout-shift": cls,
        "speed-index": 2100.2,
    }
    cats = {"performance": perf, "accessibility": a11y, "best-practices": 0.96, "seo": 1.0}
    return {
        "categories": {k: {"score": v} for k, v in cats.items()},
        "audits": {k: {"numericValue": v} for k, v in audits.items()},
        "environment": {"credits": {"axe-core": "4.14.0"}},
    }


def test_site_is_web_with_the_frozen_fixtures_as_data(tmp_path):
    site = lighthouse.build_site(tmp_path)
    assert site == tmp_path / "site"
    assert tree(site / "data") == tree(FIXTURES)  # never web/data, which the exporter rewrites
    assert tree(site, skip="data") == tree(ROOT / "web", skip="data")
    assert (site / "index.html").is_file()
    assert (site / "data" / "insights" / "manifest.json").is_file()


def test_live_state_moves_only_the_fixture_timestamps_to_now(tmp_path):
    site = lighthouse.build_site(tmp_path)
    pacific = NOW.astimezone(timezone(timedelta(hours=-7)))
    assert lighthouse.make_live(site, pacific) == STAMP
    for name, keys in (("live.json", {"generated_at", "data_as_of"}), ("alerts.json", {"generated_at"})):
        old = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
        new = json.loads((site / "data" / name).read_text(encoding="utf-8"))
        assert {k for k in old | new if old.get(k) != new.get(k)} == keys
        assert all(new[k] == STAMP for k in keys)
        # every other byte is the fixture's: the old stamps put back give the fixture file exactly
        back = json.dumps(new | {k: old[k] for k in keys}, separators=(",", ":"))
        assert back.encode() == (FIXTURES / name).read_bytes()
    rest = {k: v for k, v in tree(site / "data").items() if k not in ("live.json", "alerts.json")}
    assert rest == {k: v for k, v in tree(FIXTURES).items() if k not in ("live.json", "alerts.json")}


def test_summary_takes_each_configurations_median_and_fails_below_accessibility_100():
    perf = [0.9, 0.8, 0.95, 0.7, 0.85]
    lcp = [2300.2, 4100.0, 2500.7, 4400.0, 2900.4]
    reports = {cfg: [lhr(p, lcp=v) for p, v in zip(perf, lcp, strict=True)] for cfg in lighthouse.CONFIGS}
    s = lighthouse.summarize(reports)
    assert s["ok"] is True
    for cfg in lighthouse.CONFIGS:
        c = s["configs"][cfg]
        assert len(c["runs"]) == 5
        assert c["accessibility_min"] == 100
        assert c["median"] == {
            "performance": 85,
            "fcp_ms": 1801,
            "lcp_ms": 2900,
            "tbt_ms": 0,
            "cls": 0.008,
            "si_ms": 2100,
        }
        assert c["runs"][0] == {
            "performance": 90,
            "accessibility": 100,
            "best_practices": 96,
            "seo": 100,
            "fcp_ms": 1801,
            "lcp_ms": 2300,
            "tbt_ms": 0,
            "cls": 0.008,
            "si_ms": 2100,
        }

    low = {
        **reports,
        "desktop-paused": [*reports["desktop-paused"][:2], lhr(0.95, a11y=0.98), *reports["desktop-paused"][3:]],
    }
    s = lighthouse.summarize(low)
    assert s["ok"] is False
    assert s["configs"]["desktop-paused"]["accessibility_min"] == 98
    assert s["configs"]["desktop-paused"]["median"]["performance"] == 85

    # a missing report, a runtimeError and an unscored category are not runs: 4 of 5 is not ok
    for bad in (None, lhr(0.9) | {"runtimeError": {"code": "NO_FCP"}}, lhr(None)):
        s = lighthouse.summarize({**reports, "mobile-live": [*reports["mobile-live"][:4], bad]})
        assert s["ok"] is False
        assert len(s["configs"]["mobile-live"]["runs"]) == 4


def test_lighthouse_runs_the_pinned_local_cli_with_playwrights_chromium_never_npx(tmp_path, monkeypatch):
    pkg = json.loads((ROOT / "tools/lighthouse/package.json").read_text(encoding="utf-8"))
    assert pkg == {"private": True, "devDependencies": {"lighthouse": "12.8.2"}}
    packages = json.loads((ROOT / "tools/lighthouse/package-lock.json").read_text(encoding="utf-8"))["packages"]
    assert packages[""]["devDependencies"] == {"lighthouse": "12.8.2"}
    assert packages["node_modules/lighthouse"]["version"] == "12.8.2"
    assert "node_modules/axe-core" in packages
    for key, v in packages.items():
        assert not key or (
            v["resolved"].startswith("https://registry.npmjs.org/") and v["integrity"].startswith("sha512-")
        )
    for cfg in lighthouse.CONFIGS:
        cmd = lighthouse.lighthouse_cmd("node", cfg, lighthouse.URL, tmp_path / "r.json")
        assert cmd[0] == "node"
        assert Path(cmd[1]) == ROOT / "tools/lighthouse/node_modules/lighthouse/cli/index.js"
        assert lighthouse.URL in cmd
        assert f"--output-path={tmp_path / 'r.json'}" in cmd
        assert "--output=json" in cmd
        assert "--chrome-flags=--headless=new" in cmd
        assert "--no-enable-error-reporting" in cmd
        assert ("--preset=desktop" in cmd) == cfg.startswith("desktop")
        assert not any("throttl" in a or "form-factor" in a or "npx" in a for a in cmd)

    # main() end to end with every subprocess faked: npm ci from the lockfile, serve.py, 20 runs with CHROME_PATH
    node = str(tmp_path / "nodejs" / "node.exe")
    chrome = tmp_path / "ms-playwright" / "chrome.exe"
    chrome.parent.mkdir()
    chrome.write_bytes(b"")
    runs, served, lighthouse_runs = [], {}, []

    def fake_run(cmd, **kw):
        runs.append([str(a) for a in cmd])
        return subprocess.CompletedProcess(cmd, 0, "", "")

    class FakePopen:
        pid, returncode = 4242, 0

        def __init__(self, cmd, env=None, **kw):
            cmd = [str(a) for a in cmd]
            runs.append(cmd)
            if cmd[1].endswith("serve.py"):
                served["site"] = Path(cmd[3])
                return
            out = Path(next(a for a in cmd if a.startswith("--output-path=")).split("=", 1)[1])
            live = json.loads((served["site"] / "data" / "live.json").read_text(encoding="utf-8"))
            lighthouse_runs.append((cmd, env["CHROME_PATH"], live["generated_at"]))
            out.write_text(json.dumps(lhr(0.9 if "--preset=desktop" in cmd else 0.8)), encoding="utf-8")

        def communicate(self, timeout=None):
            return "", ""

        terminate = kill = wait = lambda self, *a: None

    monkeypatch.setattr(batch_guard, "running_services", lambda: set(STREAM_STACK))
    monkeypatch.setattr(lighthouse.shutil, "which", lambda name: node if name == "node" else None)
    monkeypatch.setattr(lighthouse, "CLI", tmp_path / "lh" / "node_modules" / "lighthouse" / "cli" / "index.js")
    monkeypatch.setattr(lighthouse, "playwright_chromium", lambda n: (str(chrome), "156.0.8078.4"))
    monkeypatch.setattr(lighthouse, "wait_for_port", lambda: True)
    monkeypatch.setattr(lighthouse, "ROOT", tmp_path)
    monkeypatch.setattr(lighthouse, "REPORTS_DIR", tmp_path / "data" / "lighthouse")
    monkeypatch.setattr(lighthouse, "METRICS_DIR", tmp_path / "data" / "metrics")
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(subprocess, "Popen", FakePopen)
    assert lighthouse.main() == 0

    npm = str(Path(node).parent / "node_modules" / "npm" / "bin" / "npm-cli.js")
    want = [node, npm, "ci", "--prefix", str(lighthouse.LIGHTHOUSE_DIR), "--ignore-scripts", "--no-audit", "--no-fund"]
    assert runs[0] == want  # the CLI was missing: installed from the lockfile, scripts off
    assert not any("npx" in a or "@latest" in a or "lighthouse@" in a for cmd in runs for a in cmd)
    assert len(lighthouse_runs) == 20
    assert all(c == str(chrome) for _, c, _ in lighthouse_runs)
    assert all(cmd[:2] == [node, str(lighthouse.CLI)] for cmd, _, _ in lighthouse_runs)
    paused_at = json.loads((FIXTURES / "live.json").read_text(encoding="utf-8"))["generated_at"]
    order = [cfg for cfg in lighthouse.CONFIGS for _ in range(5)]
    for cfg, (cmd, _, gen) in zip(order, lighthouse_runs, strict=True):
        assert ("--preset=desktop" in cmd) == cfg.startswith("desktop")
        assert (gen == paused_at) == cfg.endswith("-paused")

    (path,) = (tmp_path / "data" / "metrics").glob("lighthouse-*.json")
    rec = json.loads(path.read_text(encoding="utf-8"))
    assert path.name == f"lighthouse-{rec['id']}.json"
    assert rec["reports_dir"] == f"data/lighthouse/{rec['id']}"
    versions = (rec["lighthouse"], rec["axe_core"], rec["chromium"], rec["chromium_version"])
    assert versions == ("12.8.2", "4.14.0", str(chrome), "156.0.8078.4")
    assert rec["ok"] is True
    assert rec["runs_per_config"] == 5
    assert rec["url"] == "http://127.0.0.1:5180/index.html"
    assert rec["fixtures"]["live.json"] == hashlib.sha256((FIXTURES / "live.json").read_bytes()).hexdigest()
    assert rec["configs"]["mobile-live"]["median"]["performance"] == 80
    assert rec["configs"]["desktop-paused"]["runs"][0]["report"] == "desktop-paused-1.json"
    assert rec["configs"]["desktop-paused"]["runs"][0]["generated_at"] == paused_at


def test_lighthouse_refuses_beside_trino_spark_connect_and_marquez(monkeypatch, capsys):
    def boom(*a, **k):
        raise AssertionError("a subprocess or node lookup ran before the refusal")

    monkeypatch.setattr(subprocess, "run", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)
    monkeypatch.setattr(lighthouse.shutil, "which", boom)
    for service in BLOCKING:
        monkeypatch.setattr(batch_guard, "running_services", lambda s=service: STREAM_STACK | {s})
        assert lighthouse.main() == 1
        out = capsys.readouterr().out
        assert service in out
        assert "stop it first" in out
    monkeypatch.setattr(batch_guard, "running_services", lambda: set(STREAM_STACK))
    assert lighthouse.refusal() is None
