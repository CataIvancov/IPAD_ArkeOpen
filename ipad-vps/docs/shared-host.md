# One VPS, two stacks — what they can and cannot share

The IPAD box runs **Arches** (development) and **ArkeOpen/ArkeoGIS** (public portal). They are complementary, not one system.

## They cannot share

- **A single database with shared tables.** Arches (Django schema) and ArkeOpen (Hasura-managed schema) have completely different data models.
- **Search/query engines.** Arches uses **Elasticsearch 8**; ArkeOpen uses **Hasura** over PostgreSQL. Neither replaces the other.
- **Application code.** Different frameworks (Django vs React + Hasura).

## They can share

- **Host OS and ops:** Ubuntu, nginx, SSH, firewall, Node runtime.
- **nginx as a single reverse proxy** routing to each app (different server names / ports).
- **Optionally one PostgreSQL/PostGIS server** holding **three separate databases**:
  - `arches_ipad` (Arches, native)
  - `arkeopen` (ArkeOpen public)
  - `arkeogis` (ArkeoGIS admin/data)

  Separate databases keep the systems isolated even on a shared server.

> Constraint for this project: Arches uses a **host-native** PostgreSQL, and ArkeOpen uses a **containerized** PostGIS (Docker for PostGIS + Hasura only). So in practice the databases are separate by design — Arches on the host cluster, ArkeOpen in its container. Consolidating onto one PostgreSQL cluster is optional and only worthwhile if you want a single backup/maintenance surface.

## Data exchange between the two

There is **no live sync**. If site inventories need to move between Arches and ArkeOpen/ArkeoGIS, do it explicitly:

- CSV export/import, or
- shared IIIF image URLs, or
- a purpose-built one-way bridge script.

## Ports (development, IP-only)

- Arches: `http://VPS_IP:8000` (or nginx :80)
- ArkeOpen web-app: `http://VPS_IP` via nginx; Hasura/GraphQL bound to localhost behind nginx
- Keep the firewall tight (SSH + your IP, or SSH tunnel) until a domain + HTTPS is added.

## Sizing

- Arches wants **8–16 GB RAM**; ArkeOpen is lighter (**4–8 GB**).
- One shared box: plan for **16 GB**. Two smaller VPS is cleaner and isolates load.
