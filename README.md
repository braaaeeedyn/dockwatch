# DockWatch

[![CI](https://github.com/braaaeeedyn/dockwatch/actions/workflows/ci.yml/badge.svg)](https://github.com/braaaeeedyn/dockwatch/actions/workflows/ci.yml)

A real-time bike-share lakehouse on public Bay Wheels data: live GBFS station status streams through Kafka, is
processed by Spark Structured Streaming into Apache Iceberg tables, is joined with trip history and a CDC-replicated
operations database, and is published as tested, lineage-tracked marts, alerts and a live station map.

> **Status:** early build (M2 streaming done; site shell F1 and live station map F2 done; M3 CDC next). See [`docs/CURRENT_STATE.md`](docs/CURRENT_STATE.md) for exactly what works today.

DockWatch is an independent portfolio project. It is **not affiliated with Lyft or Bay Wheels**.

## Docs
| File | What it is |
|---|---|
| [`docs/DOCKWATCH_PLAN.md`](docs/DOCKWATCH_PLAN.md) | The project plan: what and why. |
| [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) | Ordered milestones and tasks: how. |
| [`docs/DESIGN.md`](docs/DESIGN.md) | Design system for the site (v1.0, frozen during frontend work). |
| [`docs/CURRENT_STATE.md`](docs/CURRENT_STATE.md) | What the app is right now and what is being worked on. |
| [`docs/DEVLOG.md`](docs/DEVLOG.md) | Append-only history of everything done, tried and removed. |

## Run it (so far)
Needs Python 3.12, [uv](https://docs.astral.sh/uv/) and Docker.

```bash
uv sync                              # install
python tasks.py test                 # unit tests (no Docker, no network)
cp .env.example .env                 # local settings
python tasks.py up stream            # Redpanda, SeaweedFS (S3 API), Iceberg REST catalog, Postgres
python tasks.py setup                # create Kafka topics + local bucket
python tasks.py produce              # poll Bay Wheels GBFS -> Kafka + raw archive in local S3
python tasks.py export               # live.json / alerts.json for the site, every 60 s
python tasks.py geo                  # (once) Census map shapes -> web/geo/*.json (committed)
python tasks.py replay-export        # "Replay a day": web/data/replay.json from the raw archive (no Spark; --date, --step)
python tasks.py tf-fmt-check         # terraform fmt -check (Docker; tf-fmt rewrites, tf-validate validates)
```

Spark (Kafka -> Iceberg, episodes, alerts) runs **on demand only** (one Spark JVM at a time; never restarted
automatically):

```bash
python tasks.py catchup              # process the Kafka backlog (availableNow), then exit
python tasks.py verify-lake          # consistency checks; exits 1 on duplicates, gaps, lag or no recent commits
python tasks.py stream               # or: keep the job running, one micro-batch per minute ...
python tasks.py stream-stop          # ... and stop it (always before catchup / inspect / sql / verify-lake)
python tasks.py inspect              # the same report as verify-lake, without failing
```

Redpanda Console: http://localhost:8088 · local S3 endpoint: http://localhost:8333

The site in `web/` is static (no build step). Serve it and run its tests (needs Node 20):

```bash
python -m http.server 5179 --bind 127.0.0.1 --directory web   # http://127.0.0.1:5179
npm ci                               # Playwright 1.64.0 + axe (dev only)
npx playwright test                  # shell + live map: layout, accessibility, behaviour (uses tests/web/fixtures)
```

**CI** (`.github/workflows/ci.yml`, on pushes to `main` and on pull requests) runs ruff, `ruff format --check` and
pytest, the Spark transform tests in the Spark image, `terraform fmt -check` + `validate`, the full Playwright suite
and a repeat job for the alerts and shell specs. Playwright runs with `--retries=0`, so a flaky test shows up red.

## Data and licence
- **Bay Wheels GBFS feeds** and **trip history**: Bay Wheels data is provided by Lyft Bikes and Scooters, LLC
  ("Bay Wheels") under the Bay Wheels License Agreement (https://www.lyft.com/bikes/bay-wheels/data-license-agreement).
  DockWatch uses it for a non-commercial portfolio project and does not use Lyft or Bay Wheels branding.
- **Map shapes:** US Census Bureau TIGER cartographic boundary files (`cb_2023_us_county_500k`, public domain),
  simplified into `web/geo/` by `python tasks.py geo`.
- **Fonts:** IBM Plex Sans and IBM Plex Mono, SIL Open Font License 1.1 (`web/fonts/OFL.txt`).
- **Icons:** Lucide v1.52.0, ISC (`web/licenses/lucide-LICENSE.txt`).
