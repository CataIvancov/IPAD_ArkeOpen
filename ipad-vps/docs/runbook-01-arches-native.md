# Runbook 01 — Arches 8.1.3 native install (no Docker)

Clean, stripped-down **official Arches 8.1.3** development instance, installed natively on Ubuntu 24.04. Dedicated database `arches_ipad`. No Docker.

> Version-sensitive: before running, open the official 8.1.3 install guide (`arches.readthedocs.io`, "Installing" for 8.1) and the branch helper `arches/install/ubuntu_setup.sh` in `archesproject/arches` at tag/branch `stable/8.1.3`. Confirm the exact Python version, the frontend build command, and the Elasticsearch major version Arches 8.1 expects. The steps below are the standard shape; reconcile any command that differs in the official doc.

## 1.1 PostgreSQL + PostGIS (host-native)

Ubuntu 24.04 ships PostgreSQL 16 (Arches 8.1 supports PG 14+):

```bash
sudo apt -y install postgresql postgresql-contrib postgresql-16-postgis-3 postgis
sudo systemctl enable --now postgresql
psql --version
```

Create the role and database:

```bash
sudo -u postgres psql <<'SQL'
CREATE ROLE arches WITH LOGIN PASSWORD 'CHANGE_ME_STRONG';
ALTER ROLE arches CREATEDB;
CREATE DATABASE arches_ipad OWNER arches;
\c arches_ipad
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
SQL
```

Pick a strong password and keep it out of git (you'll put it in the Arches project settings, which stays on the server).

## 1.2 Elasticsearch 8 (host-native, local only)

Install from Elastic's apt repo:

```bash
curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list
sudo apt update && sudo apt -y install elasticsearch
```

For a local development node, bind to localhost and disable the security layer (dev only — never expose ES publicly):

```bash
sudo tee -a /etc/elasticsearch/elasticsearch.yml >/dev/null <<'YML'
network.host: 127.0.0.1
discovery.type: single-node
xpack.security.enabled: false
YML
```

**Cap the heap (important on an 8 GB box).** By default Elasticsearch sizes its heap from total RAM and can grab several GB. Pin it to 1 GB for this dev box:

```bash
sudo tee /etc/elasticsearch/jvm.options.d/heap.options >/dev/null <<'OPTS'
-Xms1g
-Xmx1g
OPTS
sudo systemctl enable --now elasticsearch
sleep 20
curl -s http://127.0.0.1:9200 | head -20
```

> Confirm the ES major version Arches 8.1.3 expects. If it wants Elasticsearch 8.x, the repo above is correct; if the official doc pins a specific 8.minor, install that.

## 1.3 Python environment + Arches core

Ubuntu 24.04 has Python 3.12:

```bash
sudo apt -y install python3-venv python3-dev libgdal-dev gdal-bin
cd /opt/ipad
python3 -m venv envs-arches
source envs-arches/bin/activate
pip install --upgrade pip wheel
pip install "arches==8.1.3"
```

> If `pip install arches==8.1.3` fails on a native dependency (commonly GDAL), match the GDAL Python binding to the system GDAL version reported by `gdal-config --version`, then retry. Paste the error here if it happens.

## 1.4 Create the IPAD project

```bash
cd /opt/ipad
arches-admin startproject ipad
cd ipad
```

Edit the project settings (`ipad/settings.py` or `settings_local.py`) so:

- `DATABASES['default']`: name `arches_ipad`, user `arches`, password from 1.1, host `127.0.0.1`
- `ELASTICSEARCH_HOSTS` / connection points at `http://127.0.0.1:9200`
- `ALLOWED_HOSTS = ['103.197.188.213', 'localhost', '127.0.0.1']`
- `DEBUG = True` for now (development only)

## 1.5 Initialize database + frontend

```bash
# from /opt/ipad/ipad, venv active
python manage.py setup_db          # creates schema + loads defaults (confirm name in 8.1 docs)
python manage.py createsuperuser
```

Frontend assets (Arches 8 builds the UI with a Node toolchain). **On 8 GB, free up memory first** — stop the ArkeOpen Docker stack during this build (`cd /opt/ipad/arkeopen-repo/arke-platform/server && docker compose down`), then build:

```bash
npm install
# The exact build command changed across Arches versions. In 8.1 it is typically one of:
#   python manage.py build_development_frontend
#   npm run build_development
# Use whichever the official 8.1.3 doc specifies.
```

## 1.6 Run it (development)

```bash
python manage.py runserver 0.0.0.0:8000
```

Open `http://103.197.188.213:8000` and log in with the superuser.

**Hello-world check:** create one Resource Model (or load a default graph) and add a single resource instance. That proves DB + ES + UI are wired.

## 1.7 Notes

- **No Docker here.** Do not copy the fork's `docker-compose.override.yml` (Mac Apple Silicon/Rosetta only) onto this box.
- For a longer-lived setup, front `runserver` with gunicorn + nginx and run Celery worker/beat as systemd units. That's a later hardening step once the dev instance is verified.
- Keep `arches_ipad` isolated from ArkeOpen's databases (see [`shared-host.md`](shared-host.md)).

Next: [`runbook-02-arkeopen.md`](runbook-02-arkeopen.md).
