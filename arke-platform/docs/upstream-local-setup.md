# Upstream Local Setup

This workspace now uses the real upstream ArkeOpen applications from:

- `/Users/cataivancov/IdeaProjects/arkeopen-upstream/web-app`
- `/Users/cataivancov/IdeaProjects/arkeopen-upstream/web-admin`
- `/Users/cataivancov/IdeaProjects/arkeopen-upstream/server`

## Current local state

Hasura is reachable on `http://localhost:40022/v1/graphql` and both databases exist:

- `arkeopen`
- `arkeogis`

The schemas are migrated, but the core product tables are empty in both databases:

- `charac`
- `chronology`
- `map_source`
- `database`
- `site`

You can verify that at any time with:

```bash
npm run doctor:upstream-data
```

## What is missing

The real UI needs content, not just schema. The upstream install flow expects:

1. PostgreSQL and Hasura running
2. `arkeopen` and `arkeogis` databases created
3. Hasura metadata applied with `ako_` and `akg_` root prefixes
4. A data-only import or restore into both databases

Without step 4, the app can start but it cannot render the map/search UI because the startup
queries for chronologies and characteristics return empty arrays.

## Restore path

The upstream documentation expects a data-only SQL dump from an existing environment.

Examples:

```bash
docker exec -i arkeopen-prod-postgres pg_dump -Upostgres --data-only arkeopen > /tmp/arkeopen-data.sql
docker exec -i arkeo4prod-postgres pg_dump -Upostgres --data-only arkeogis > /tmp/arkeogis-data.sql
```

Then restore into the local container:

```bash
docker exec -i arkeopenlocal-postgres psql -Upostgres arkeopen < /tmp/arkeopen-data.sql
docker exec -i arkeopenlocal-postgres psql -Upostgres arkeogis < /tmp/arkeogis-data.sql
```

If the dump contains circular foreign key warnings, add this line near the top of the SQL file:

```sql
set session_replication_role = replica;
```

## Runtime commands

From `/Users/cataivancov/IdeaProjects/arke-platform`:

```bash
npm run server:up
npm run dev:web-app:arkeopen
npm run dev:web-app:arkeogis
npm run dev:web-admin:arkeogis
```

## Limitation

No real dataset dump is present in either repository, so the final import step still requires a
source SQL dump from an existing ArkeOpen or ArkeoGIS environment.
