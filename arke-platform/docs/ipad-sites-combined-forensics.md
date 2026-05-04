# `ipad-sites-combined.csv` Forensics

Current baseline:

- `arke-platform/data/ipad-sites-combined.csv` is intentionally a 21-column semicolon-delimited file with no `WEB_IMAGES` column.
- The file is branch-local to `data-cleaning`; it does not exist on `main`.
- The May 4, 2026 repair keeps valid multi-line site groups and fixes only conflicting `SITE_SOURCE_ID` reuse.

Key branch timeline:

- `6ab98c7` on 2026-04-23 16:44:37 +0700 introduced the first repeated `SITE_SOURCE_ID`, `IPAD_177` (`Leang Bulu Bettue`).
- `6723227` on 2026-04-23 18:09:11 +0700 introduced the repeated `IPAD_191` group (`Leang Rakkoe`), which is a valid multi-line site after site-level normalization checks.
- `9a391f3` on 2026-04-24 10:10:37 +0700 changed the schema from 22 columns to 21 by removing `WEB_IMAGES`.
- `20e64bb` on 2026-04-28 20:02:57 +0700 introduced the `IPAD_863` collision between `Yomokho 1` and `Mamorikotey`.
- `f6334f8` on 2026-04-30 22:08:53 +0700 introduced the large collision block where later rows reused IDs already assigned in the `IPAD_343`-`IPAD_368` range.
- `29f0d4a` on 2026-05-03 12:32:37 +0700 and `57d05fb` on 2026-05-03 12:42:56 +0700 touched the file tail during a transient artifact. The repaired CSV has no markdown/code-fence residue.

Repair rules applied on 2026-05-04:

- Keep repeated IDs only when the repeated rows describe the same site and all site-level fields match.
- Normalize `IPAD_177` as a valid shared-ID site group by making its site-level fields consistent.
- Preserve `IPAD_191` as a valid shared-ID site group.
- Split true collisions by assigning new IDs to the later conflicting rows, including the `Mamorikotey` row and the late appended block near the end of the file.

Relevant tooling status:

- `scripts/validate-ipad-sites-combined.py` is the canonical validator for the 21-column combined file.
- `scripts/merge-csvs.py` now writes the 21-column combined schema and accepts repeated IDs only when their site-level fields are identical.
- `scripts/import-airtable-site-only-v4.py` now treats `WEB_IMAGES` as optional so it can consume 21-column rows safely.
- `rebuild_csv.py` remains scoped to the 22-column drive-site export that still includes `WEB_IMAGES`.
