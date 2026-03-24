# Arke Platform

Starter monorepo for developing the ArkeOpen and ArkeoGIS platforms in one workspace.

## Layout

```text
.
|-- docs/                  Project notes
|-- server/                PostgreSQL + PostGIS + Hasura + service stubs
|-- web-app/               Public application
`-- web-admin/             Admin/backoffice application
```

## App targets

The frontend packages expose separate modes for the two products:

- `arkeopen`: public open-data interface
- `arkeogis`: research/admin-oriented interface

At build time, Vite injects:

- `__ARKEOPEN__`
- `__ARKEOGIS__`

## Quick start

1. Copy `server/env.development` to `server/.env`.
2. Create the Docker network once:
   `docker network create arkeo`
3. Start PostgreSQL + Hasura:
   `npm run server:up`
4. Install frontend dependencies:
   `npm install`
5. Run one target:
   `npm run dev:web-app:arkeopen`

## Current status

This repository is a scaffold aligned to upstream structure, not a full import of the upstream applications yet.

## Local upstream wiring

The root scripts now delegate to the real upstream applications located at:

- `../arkeopen-upstream/web-app`
- `../arkeopen-upstream/web-admin`
- `../arkeopen-upstream/server`

Useful commands:

- `npm run server:up`
- `npm run dev:web-app:arkeopen`
- `npm run dev:web-app:arkeogis`
- `npm run dev:web-admin:arkeogis`
- `npm run doctor:upstream-data`

The full local setup and the current data-import gap are documented in
`docs/upstream-local-setup.md`.
