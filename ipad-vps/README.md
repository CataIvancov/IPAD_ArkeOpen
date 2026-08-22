# IPAD VPS

One Ubuntu VPS project (working name **IPAD**) that hosts two **separate, complementary** systems:

1. **Arches** — official, stripped-down heritage inventory, used for **development**. Installed **natively** (no Docker).
2. **ArkeOpen / ArkeoGIS** — public FAIR map/search portal (React + Hasura + PostgreSQL/PostGIS).

These are **two stacks**, not one product. They do **not** share a database or a codebase. They can share host-level infrastructure (Ubuntu, nginx, SSH, firewall, Node) and, optionally, a single PostgreSQL/PostGIS server holding three separate databases.

> Source: this documentation was assembled from the "ipad-vps" assessment chat and adapted to the constraints below. Nothing here has been installed on a VPS yet.

## Target constraints

- **OS:** Ubuntu (24.04 LTS recommended)
- **Arches:** official **8.1.3** stable, native install, **no Docker**, dedicated development database `arches_ipad`
- **ArkeOpen/ArkeoGIS:** databases `arkeopen` + `arkeogis`; Docker used **only** for PostGIS + Hasura
- **Databases stay separate** — Arches never shares tables with ArkeOpen
- **Media (images, PDFs, SQL dumps) live on Google Drive**, not on the VPS
- **Domain name:** not required for development (IP + firewall/SSH tunnel is fine); required only for public HTTPS

## Documents

| File | Purpose |
| --- | --- |
| [`docs/vps-assessment.md`](docs/vps-assessment.md) | Repo/branch readiness assessment for ArkeOpen and Arches |
| [`docs/arches-native-ubuntu.md`](docs/arches-native-ubuntu.md) | Native (no-Docker) Arches 8.1.3 install plan on Ubuntu |
| [`docs/shared-host.md`](docs/shared-host.md) | What the two stacks can and cannot share on one box |
| [`docs/google-drive-storage.md`](docs/google-drive-storage.md) | Keeping bulky media on Google Drive instead of the VPS |

## Sizing at a glance

| | Arches (development) | ArkeOpen / ArkeoGIS |
| --- | --- | --- |
| OS | Ubuntu 24.04 native | Ubuntu 24.04 |
| Docker | **None** | Only PostGIS + Hasura |
| Database | Host PostgreSQL → `arches_ipad` | Container PostGIS → `arkeopen` + `arkeogis` |
| Version | Official **8.1.3** | Current ArkeOpen (GitLab) tree |
| RAM | 8–16 GB | 4–8 GB |

One box for both: plan for **16 GB RAM**. Two smaller VPS is cleaner if the budget allows.
