# DockWatch

A real-time bike-share lakehouse on public Bay Wheels data: live GBFS station status streams through Kafka, is
processed by Spark Structured Streaming into Apache Iceberg tables, is joined with trip history and a CDC-replicated
operations database, and is published as tested, lineage-tracked marts, alerts and a live station map.

> **Status:** early build (M1, ingest). See [`docs/CURRENT_STATE.md`](docs/CURRENT_STATE.md) for exactly what works today.

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
```

Redpanda Console: http://localhost:8088 · local S3 endpoint: http://localhost:8333

## Data and licence
- **Bay Wheels GBFS feeds** and **trip history**: Bay Wheels data is provided by Lyft Bikes and Scooters, LLC
  ("Bay Wheels") under the Bay Wheels License Agreement (https://www.lyft.com/bikes/bay-wheels/data-license-agreement).
  DockWatch uses it for a non-commercial portfolio project and does not use Lyft or Bay Wheels branding.
- **Map shapes** (planned): US Census Bureau TIGER/Line cartographic boundary files (public domain).
