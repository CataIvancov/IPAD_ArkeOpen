# IPAD VPS — Repository & Deployment Assessment

Deployment-readiness assessment of the IPAD forks, focused on running **Arches** (development) and **ArkeOpen/ArkeoGIS** (public portal) on an Ubuntu VPS.

## Repo ecosystem

| Repo | Role | VPS readiness |
| --- | --- | --- |
| `CataIvancov/IPAD_ArkeOpen` | ArkeOpen + IPAD data pipeline | Partial — closest to a deployable IPAD stack |
| `CataIvancov/arches_ipad` | Arches core platform fork | Not deployable alone (platform source, not a project) |
| `CataIvancov/arches-via-docker` | Docker-based Arches deployment | Not used here — VPS requires **native** Arches, no Docker |
| `CataIvancov/ipad_researchspace` | ResearchSpace fork | Separate platform, out of scope for this deploy |

## ArkeOpen (`IPAD_ArkeOpen`)

**What it is:** ArkeOpen customized for Indonesian prehistoric archaeology, with a substantial data-import/cleaning pipeline under `arke-platform/`.

**Architecture:**

```
React web-app + React web-admin
        | GraphQL
Hasura (graphql-engine)
        |
PostgreSQL 14+ / PostGIS (+ ltree)
        |
nginx reverse proxy (+ SSL when a domain exists)
```

**What already works toward a VPS deploy:**

- Docker Compose for PostgreSQL/PostGIS + Hasura (`arke-platform/server/docker-compose.yaml`)
- Pinned images: `postgis/postgis:16-3.4`, `hasura/graphql-engine:v2.44.0`
- `env.development` and `env.production` templates
- Rich Python/Node data pipeline (Indonesian sites, chronologies, Google Drive, Airtable)
- IIIF deployment note (`arke-platform/docs/hetzner-iiif-deployment.md`) for illustration serving (Cantaloupe + nginx)

**Blockers to resolve before production:**

1. **Broken submodules** — `arkeopen-upstream` and `ipad_researchspace` are gitlinks with no resolvable remote (no `.gitmodules`). A fresh clone does **not** get a working upstream app. The real ArkeOpen source exists in Google Drive and on GitLab (`gitlab.huma-num.fr/arkeogis/arkeopen`); it must be vendored in or wired as a proper submodule.
2. **Branch choice** — `main` vs `data-cleaning` (data-cleaning was ~253 commits ahead, ~300 files changed, unmerged as PR #1). Decide which is the production source.
3. **Empty databases** — schema exists but core tables (`site`, `charac`, `chronology`, `map_source`, `database`) are empty without a data-only SQL dump import.
4. **Production Hasura settings** — `env.production` still carries `HASURA_GRAPHQL_DEV_MODE` and `HASURA_GRAPHQL_ENABLE_CONSOLE`; these must be locked down on the VPS.
5. **Unmerged Dependabot security PRs** — vite, postcss, path-to-regexp, picomatch.
6. **Repo hygiene** — large PDFs committed to git (bad for VPS clones); media should move to Drive.
7. **ArkeoGIS dependency** — ArkeOpen's own install docs state ArkeoGIS is required for admin and data transfer. ArkeOpen is an ArkeoGIS ecosystem component, not a self-contained platform.

**Sizing:** 4–8 GB RAM, 40–80 GB disk. Stack: Docker (PostGIS + Hasura only), nginx, Node, Python 3.

## Arches (`arches_ipad`)

**What it is:** A fork of `archesproject/arches` — the Arches **platform source**, not an IPAD application instance.

- Default branch `dev/8.2.x`: ~118 commits behind upstream; the only custom change is a `docker-compose.override.yml` forcing `linux/amd64` for Mac Apple Silicon + Rosetta. **That override is for local Mac dev and must be removed on a Linux VPS.**
- `stable/8.1.3` exists and is the recommended production/development base.

**Critical clarification:** `arches_ipad` alone is not a deployable inventory. Arches needs:

1. The core platform (official `arches==8.1.3`)
2. A **separate Django project** (`arches-admin startproject ipad`) with resource models, graphs, settings
3. PostgreSQL 14+ / PostGIS, Elasticsearch 8, nginx, Celery
4. 8–16 GB RAM for production asset builds

Because the previous custom Arches development was lost, the plan is a **clean, stripped-down official 8.1.3** instance. You recover the platform, not the lost resource models — those get rebuilt later from the IPAD CSVs/thesauri.

See [`arches-native-ubuntu.md`](arches-native-ubuntu.md) for the no-Docker install plan.

## Priority fixes before any VPS deploy

1. Restore/vendor `arkeopen-upstream` (from GitLab or Drive) into a proper git tree — exclude `node_modules`/`dist`.
2. Reconcile `main` vs `data-cleaning` and tag a production branch.
3. Merge the Dependabot security PRs on `IPAD_ArkeOpen`.
4. Prepare a data-only PostgreSQL dump of populated `arkeopen`/`arkeogis` tables for the import step.
5. For Arches: deploy clean official **8.1.3** natively (no Docker); keep the Mac `docker-compose.override.yml` off the VPS.
6. Move large PDFs/images out of git and onto Google Drive.
