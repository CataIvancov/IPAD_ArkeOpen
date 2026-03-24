# Architecture Notes

This monorepo follows the current upstream direction:

- one repository
- two application targets
- shared infrastructure

## Components

- `server/`
  - PostgreSQL + PostGIS
  - Hasura metadata and migrations
  - optional backend services for exports, auth, or sync
- `web-app/`
  - public-facing UI for ArkeOpen and ArkeoGIS viewer features
- `web-admin/`
  - admin and editorial tools

## Product split

Use compile-time flags to keep one codebase with product-specific behavior:

```js
if (__ARKEOPEN__) {
  // public-only behavior
}

if (__ARKEOGIS__) {
  // research/admin-only behavior
}
```

