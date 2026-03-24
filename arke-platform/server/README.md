# Server

Starter infrastructure for ArkeOpen and ArkeoGIS.

## Stack

- PostgreSQL
- PostGIS
- Hasura GraphQL Engine
- optional Node service in `arkeoserver/`

## First run

```bash
cp env.development .env
docker network create arkeo
docker compose --env-file .env up -d
```

## Hasura metadata

Copy the database config before applying metadata:

```bash
cp hasura/metadata/databases/databases.dist.yaml hasura/metadata/databases/databases.yaml
```

