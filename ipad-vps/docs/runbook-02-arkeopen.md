# Runbook 02 — ArkeOpen / ArkeoGIS (Docker for PostGIS + Hasura only)

Bring up the ArkeOpen public portal. Databases `arkeopen` + `arkeogis` run in a **containerized PostGIS**; Hasura runs in Docker too. The React frontends and nginx run on the host. Arches from Runbook 01 is untouched and uses a **separate** host PostgreSQL.

> **Source of truth = your Google Drive `arkeopen-upstream`.** The Drive copy carries essential IPAD-specific development modifications for your ArkeOpen instance. The clean GitLab tree (`gitlab.huma-num.fr/arkeogis/arkeopen`) is only a **reference base** for comparison/updates — deploying it directly would lose your customizations. In `IPAD_ArkeOpen`, `arkeopen-upstream` is a broken gitlink (no `.gitmodules`), so it does not carry your code either. The repo's own `arke-platform/server/docker-compose.yaml` is self-contained for the DB + Hasura layer, so data services can come up before the frontends are in place.
>
> Known extras observed in the Drive tree (from the earlier assessment; confirm against the actual folder): a GeoServer service in the compose file, GeoServer log-helper npm scripts, and an `import:ipad-sites:arkeopen` script. Your modified compose may therefore differ from the repo's.

## 2.1 Install Docker Engine (for the DB + Hasura layer only)

```bash
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt update
sudo apt -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"
newgrp docker
docker --version && docker compose version
```

## 2.2 Get the ArkeOpen source (your modified Drive tree)

Clone the repo, then bring in **your modified `arkeopen-upstream` from Google Drive** (not the clean GitLab tree). The `arke-platform/` scripts expect the app at `../arkeopen-upstream`, so it sits next to the checkout.

```bash
cd /opt/ipad
git clone https://github.com/CataIvancov/IPAD_ArkeOpen.git arkeopen-repo
```

Choose **one** way to place your modified `arkeopen-upstream` at `/opt/ipad/arkeopen-upstream`:

**Option A — vendor it into GitHub first (recommended, durable).** On your Mac (which can read the Drive folder): copy the Drive `arkeopen-upstream` into a clone of `IPAD_ArkeOpen`, delete `node_modules/` and `dist/`, remove the broken submodule gitlink, commit, and push. Then the VPS just clones your modified source from GitHub and the broken submodule is fixed for good:

```bash
# on the VPS, once it is pushed to GitHub:
cd /opt/ipad
git clone https://github.com/CataIvancov/IPAD_ArkeOpen.git arkeopen-repo
ln -s arkeopen-repo/arkeopen-upstream arkeopen-upstream   # or clone a repo that has it at root
```

**Option B — copy straight from Drive to the VPS with rclone.** Configure a Google Drive remote once, then pull the folder, excluding build artifacts:

```bash
sudo -v ; curl https://rclone.org/install.sh | sudo bash
rclone config          # create a remote named "gdrive" (Google Drive; authorize in a browser)
rclone copy "gdrive:arke-platform/arkeopen-upstream" /opt/ipad/arkeopen-upstream \
  --exclude "node_modules/**" --exclude "dist/**" --progress
```

**Reference base (optional, for diffing/updates only — do NOT deploy this):**

```bash
git clone https://gitlab.huma-num.fr/arkeogis/arkeopen.git /opt/ipad/arkeopen-clean
# compare your mods vs upstream, e.g.:
# diff -ruq /opt/ipad/arkeopen-clean /opt/ipad/arkeopen-upstream | grep -v node_modules
```

Whichever option you use, the frontends and server come from **your** `arkeopen-upstream`, so your IPAD modifications are preserved.

## 2.3 Bring up PostGIS + Hasura

**If your modified `arkeopen-upstream/server` has its own `docker-compose` (e.g. with GeoServer), use that one instead** — it reflects your instance. Otherwise fall back to the repo's self-contained compose (pinned `postgis/postgis:16-3.4` + `hasura/graphql-engine:v2.44.0`) shown below:

```bash
cd /opt/ipad/arkeopen-repo/arke-platform/server
cp env.development .env
# Edit .env: set a strong POSTGRES_PASSWORD and HASURA_GRAPHQL_ADMIN_SECRET.
# For a server, set HASURA_GRAPHQL_ENABLE_CONSOLE=false and HASURA_GRAPHQL_DEV_MODE=false.
docker network create arkeo   # one-time; ignore "already exists"
docker compose --env-file .env -f docker-compose.yaml up -d
docker compose --env-file .env -f docker-compose.yaml ps
```

Create the two databases and enable PostGIS in each:

```bash
# container name is ${COMPOSE_PROJECT_NAME}-postgres (arke-platform-postgres by default)
docker exec -it arke-platform-postgres psql -U postgres <<'SQL'
CREATE DATABASE arkeopen;
CREATE DATABASE arkeogis;
\c arkeopen
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS ltree;
\c arkeogis
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS ltree;
SQL
```

Check Hasura:

```bash
curl -s http://127.0.0.1:40022/healthz && echo OK
```

(Hasura is bound to `127.0.0.1:40022` per `env.development`; keep it localhost-only and reach it through nginx.)

## 2.4 Import data (from Google Drive dumps)

The schemas exist but core tables (`site`, `charac`, `chronology`, `map_source`, `database`) are empty without a data-only dump. Pull the dump you keep on Drive, load it, then remove the local copy:

```bash
# after copying arkeopen-data.sql / arkeogis-data.sql onto the box (temporarily):
docker exec -i arke-platform-postgres psql -U postgres arkeopen < arkeopen-data.sql
docker exec -i arke-platform-postgres psql -U postgres arkeogis < arkeogis-data.sql
rm -f arkeopen-data.sql arkeogis-data.sql
```

If the dump has circular FK warnings, add near the top of the SQL file:

```sql
set session_replication_role = replica;
```

Verify (uses the repo's checker):

```bash
cd /opt/ipad/arkeopen-repo/arke-platform
# adjust the container name in scripts/check-upstream-data.sh if it differs
sh ./scripts/check-upstream-data.sh
```

## 2.5 Frontends

Build the ArkeOpen web-app and web-admin from the upstream tree:

```bash
cd /opt/ipad/arkeopen-upstream/web-app
npm install
npm run build:arkeopen     # or the upstream build-prod script
cd /opt/ipad/arkeopen-upstream/web-admin
npm install
npm run build              # upstream build-prod.sh, per its README
```

> The upstream frontends read a GraphQL endpoint from env (e.g. `REACT_APP_GRAPHQL_URI`). For IP-only dev, point it at the nginx path that proxies Hasura; set a real hostname later.

## 2.6 nginx reverse proxy

Serve the built frontends as static files and proxy `/v1/graphql` to Hasura. Example vhost (`/etc/nginx/sites-available/arkeopen`):

```nginx
server {
    listen 80;
    server_name 103.197.188.213;

    root /opt/ipad/arkeopen-upstream/web-app/dist;
    index index.html;

    location / {
        try_files $uri /index.html;
    }

    location /v1/graphql {
        proxy_pass http://127.0.0.1:40022/v1/graphql;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/arkeopen /etc/nginx/sites-enabled/arkeopen
sudo nginx -t && sudo systemctl reload nginx
```

Open `http://103.197.188.213/` — the ArkeOpen map/search UI should load once data is imported.

**Hello-world check:** with data imported, run a search or open a site on the map. Rendering real records proves DB + Hasura + frontend are wired end to end.

## 2.7 Notes

- **ArkeoGIS dependency:** ArkeOpen's admin/data-transfer flow expects ArkeoGIS. The `arkeogis` database above is the start of that; full ArkeoGIS admin is a separate step if you need editorial workflows.
- **Docker only here.** Arches (Runbook 01) stays native; its host PostgreSQL and this container PostGIS are independent — separate databases by design.
- **Media on Drive:** keep images/PDFs/dumps on Google Drive; only pull what an import needs, then delete it (see [`google-drive-storage.md`](google-drive-storage.md)).
- **Before public exposure:** add a domain, switch Hasura console/dev-mode off, and add HTTPS (Certbot) with proper `server_name`.
