# Media & bulky files on Google Drive, not the VPS

Development-specific and bulky files (images, PDFs, SQL dumps, fixtures) are kept in **Google Drive**, not on the VPS. The VPS runs only the applications and their database indexes.

## What stays on Drive

- Source images / site illustrations (masters)
- Reference PDFs (currently oversized in git — move them out)
- PostgreSQL data-only dumps used to seed `arkeopen` / `arkeogis`
- Large import fixtures for the Python data pipeline

## What lives on the VPS

- Application code (Arches project, ArkeOpen web-app/web-admin/server)
- Databases and their indexes (PostgreSQL/PostGIS, Elasticsearch, Hasura metadata)
- Small runtime config
- Optional: a small local IIIF/image cache if illustration serving is enabled later

## Why

- Keeps `git clone` on the VPS fast and small (no 35 MB+ PDFs in the tree).
- Keeps disk usage predictable; bulky media does not compete with database storage.
- Centralizes originals in Drive where they are already curated.

## Workflow

1. Keep originals in the shared Drive `arke-platform` structure.
2. On the VPS, pull only the specific dump/fixture needed for an import step, run the pipeline, then remove the local copy.
3. For public illustration serving, sync only the derived/optimized images the IIIF layer needs (see `arke-platform/docs/hetzner-iiif-deployment.md`), not the full master set.

## git hygiene

- Add media/dump patterns to `.gitignore` (`*.pdf`, `*.sql`, image dirs) so they never re-enter the repo.
- Treat Drive as the source of truth for media; the repo references paths/URLs, not the binaries.
