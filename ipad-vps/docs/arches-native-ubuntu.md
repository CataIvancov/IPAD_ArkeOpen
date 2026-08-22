# Arches (native, no Docker) on Ubuntu — development instance

Plan for a clean, stripped-down **official Arches 8.1.3** development instance installed **natively** on Ubuntu, with a dedicated database `arches_ipad`. This is intentionally the official platform, not the `arches_ipad` fork — the previous custom Arches development was lost, so this recovers the platform only. Resource models are rebuilt later from the IPAD CSVs/thesauri.

> This is a plan. Exact commands should be confirmed against the official Arches docs for 8.1.3 at install time (`arches.readthedocs.io`), since dependency versions change between releases.

## Services (all native, no Docker)

- **PostgreSQL 14+** with **PostGIS 3** — database `arches_ipad`
- **Elasticsearch 8** — Arches search index (separate from ArkeOpen; not shared)
- **Python** (Arches 8.1.3 supported version) + virtualenv
- **Node/npm** — frontend asset build
- **nginx** — reverse proxy (+ Certbot only once a domain exists)
- **Celery** + a process supervisor (systemd units) for background jobs

## Outline

1. **System packages**

   Install PostgreSQL + PostGIS, Elasticsearch 8, Python + venv tooling, Node, nginx, and build essentials via `apt`. Arches ships an Ubuntu setup helper (`arches/install/ubuntu_setup.sh` in the stable branch) that documents the expected packages — use it as a reference, not blindly.

2. **Database**

   Create a dedicated role and the `arches_ipad` database, enable PostGIS, and keep it isolated from the ArkeOpen databases (see [`shared-host.md`](shared-host.md)).

3. **Arches core + project**

   ```bash
   python3 -m venv ~/envs/arches
   source ~/envs/arches/bin/activate
   pip install "arches==8.1.3"
   arches-admin startproject ipad
   # configure settings (DB name arches_ipad, Elasticsearch host, ALLOWED_HOSTS)
   ```

4. **Build + migrate**

   Install frontend deps and build assets, run migrations, load default graphs, and create the admin user per the official 8.1.3 instructions.

5. **Run**

   - Development: `python manage.py runserver 0.0.0.0:8000` (reachable at `http://VPS_IP:8000`)
   - Longer-lived: gunicorn/uwsgi behind nginx, Celery worker + beat as systemd units

## VPS notes

- **No Docker** for this stack — everything runs as native services/systemd units.
- Remove the fork's `docker-compose.override.yml` (Mac Apple Silicon + Rosetta only) from any VPS path.
- **RAM:** 8–16 GB. The npm production asset build alone can need ~8 GB; give the box headroom, especially if Elasticsearch runs on the same host.
- **Domain:** not required for development. Use the IP with a firewall (SSH + your IP) or an SSH tunnel. Add a domain before exposing publicly so Let's Encrypt, `ALLOWED_HOSTS`, CSRF and secure cookies work cleanly.
- **Media:** uploads/fixtures/large files belong on Google Drive; keep only what the running app and indexes need on disk (see [`google-drive-storage.md`](google-drive-storage.md)).
