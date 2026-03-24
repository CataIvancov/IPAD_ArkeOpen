#!/bin/sh

set -eu

CONTAINER="${CONTAINER:-arkeopenlocal-postgres}"
REF="${REF:-master}"
BASE_URL="https://gitlab.huma-num.fr/api/v4/projects/2112/repository/files"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

fetch() {
  path="$1"
  out="$2"
  encoded="$(printf '%s' "$path" | sed 's/\//%2F/g')"
  curl -ks "${BASE_URL}/${encoded}/raw?ref=${REF}" > "$out"
}

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Container $CONTAINER is not running."
  exit 1
fi

for db in arkeopen arkeogis; do
  count="$(docker exec "$CONTAINER" psql -U postgres -d "$db" -Atc "SELECT count(*) FROM public.charac;")"
  if [ "${FORCE:-0}" != "1" ] && [ "$count" != "0" ]; then
    echo "$db already contains charac rows. Re-run with FORCE=1 if you want to import anyway."
    exit 1
  fi
done

fetch "datas/sql/20_license.sql" "$TMPDIR/20_license.sql"
fetch "datas/sql/40_caracs.sql" "$TMPDIR/40_caracs.sql"
fetch "datas/sql/40_chronologies.sql" "$TMPDIR/40_chronologies.sql"

awk '
  BEGIN { in_copy = 0 }
  /^COPY charac \(id, parent_id, "order", author_user_id, created_at, updated_at\) FROM stdin;$/ {
    print "COPY charac (id, parent_id, \"order\", author_user_id, created_at, updated_at, ark_id, pactols_id, aat_id) FROM stdin;"
    in_copy = 1
    next
  }
  in_copy && /^\\\.$/ {
    in_copy = 0
    print
    next
  }
  in_copy {
    print $0 "\t\t\t"
    next
  }
  { print }
' "$TMPDIR/40_caracs.sql" > "$TMPDIR/40_caracs.normalized.sql"

awk '
  BEGIN { in_copy = 0 }
  /^COPY charac_root \(root_charac_id, admin_group_id\) FROM stdin;$/ {
    print "COPY charac_root (root_charac_id, admin_group_id, cached_langs) FROM stdin;"
    in_copy = 1
    next
  }
  in_copy && /^\\\.$/ {
    in_copy = 0
    print
    next
  }
  in_copy {
    print $0 "\t"
    next
  }
  { print }
' "$TMPDIR/40_caracs.normalized.sql" > "$TMPDIR/40_caracs.ready.sql"

awk '
  BEGIN { in_copy = 0 }
  /^COPY chronology \(id, parent_id, start_date, end_date, color, created_at, updated_at\) FROM stdin;$/ {
    print "COPY chronology (id, parent_id, start_date, end_date, color, created_at, updated_at, id_ark_periodo, id_ark_pactols) FROM stdin;"
    in_copy = 1
    next
  }
  in_copy && /^\\\.$/ {
    in_copy = 0
    print
    next
  }
  in_copy {
    print $0 "\t\t"
    next
  }
  { print }
' "$TMPDIR/40_chronologies.sql" > "$TMPDIR/40_chronologies.normalized.sql"

awk '
  BEGIN { in_copy = 0 }
  /^COPY chronology_root \(root_chronology_id, admin_group_id, author_user_id, credits, active, geom\) FROM stdin;$/ {
    print "COPY chronology_root (root_chronology_id, admin_group_id, author_user_id, credits, active, geom, cached_langs, opened, editor, editor_uri, deposit_uri, chronology_collection_id) FROM stdin;"
    in_copy = 1
    next
  }
  in_copy && /^\\\.$/ {
    in_copy = 0
    print
    next
  }
  in_copy {
    print $0 "\t\tf\t\t\t\t0"
    next
  }
  { print }
' "$TMPDIR/40_chronologies.normalized.sql" > "$TMPDIR/40_chronologies.ready.sql"

cat > "$TMPDIR/bootstrap.sql" <<'SQL'
INSERT INTO public.lang (isocode, active)
VALUES
  ('fr', true),
  ('en', true),
  ('es', true),
  ('de', true),
  ('D', true)
ON CONFLICT (isocode) DO NOTHING;

INSERT INTO public.photo (id, photo)
VALUES (0, '')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.country (geonameid, iso_code, geom)
VALUES (0, 'XX', NULL)
ON CONFLICT (geonameid) DO NOTHING;

INSERT INTO public.city (geonameid, country_geonameid, geom, geom_centroid)
VALUES (
  0,
  0,
  NULL,
  ST_GeogFromText('SRID=4326;POINT(0 0)')
)
ON CONFLICT (geonameid) DO NOTHING;

INSERT INTO public."group" (id, type)
VALUES
  (15, 'chronology'),
  (20, 'charac'),
  (21, 'charac'),
  (22, 'charac'),
  (23, 'charac'),
  (24, 'chronology'),
  (25, 'chronology'),
  (26, 'chronology'),
  (27, 'chronology')
ON CONFLICT (id) DO NOTHING;

INSERT INTO public."user" (
  id,
  username,
  firstname,
  lastname,
  email,
  password,
  description,
  active,
  first_lang_isocode,
  second_lang_isocode,
  city_geonameid,
  photo_id,
  orcid
)
VALUES
  (61, 'legacy61', 'Legacy', 'User 61', 'legacy61@example.invalid', '', '', true, 'fr', 'en', 0, 0, NULL),
  (62, 'legacy62', 'Legacy', 'User 62', 'legacy62@example.invalid', '', '', true, 'fr', 'en', 0, 0, NULL),
  (63, 'legacy63', 'Legacy', 'User 63', 'legacy63@example.invalid', '', '', true, 'fr', 'en', 0, 0, NULL),
  (74, 'legacy74', 'Legacy', 'User 74', 'legacy74@example.invalid', '', '', true, 'fr', 'en', 0, 0, NULL)
ON CONFLICT (id) DO NOTHING;

SELECT setval('photo_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.photo), 1), true);
SELECT setval('group_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public."group"), 1), true);
SELECT setval('user_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public."user"), 1), true);
SQL

for db in arkeopen arkeogis; do
  echo "Importing legacy public seeds into $db..."
  {
    printf 'BEGIN;\n'
    printf "SET session_replication_role = replica;\n"
    cat "$TMPDIR/bootstrap.sql"
    cat "$TMPDIR/20_license.sql"
    cat "$TMPDIR/40_caracs.ready.sql"
    cat "$TMPDIR/40_chronologies.ready.sql"
    printf "SET session_replication_role = origin;\n"
    printf "SELECT setval('charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.charac), 1), true);\n"
    printf "SELECT setval('chronology_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.chronology), 1), true);\n"
    printf 'COMMIT;\n'
  } | docker exec -i "$CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d "$db" >/dev/null
done

for db in arkeopen arkeogis; do
  echo "[$db]"
  docker exec "$CONTAINER" psql -U postgres -d "$db" -Atc \
    "SELECT 'lang', count(*) FROM public.lang
     UNION ALL
     SELECT 'license', count(*) FROM public.license
     UNION ALL
     SELECT 'charac', count(*) FROM public.charac
     UNION ALL
     SELECT 'charac_root', count(*) FROM public.charac_root
     UNION ALL
     SELECT 'charac_tr', count(*) FROM public.charac_tr
     UNION ALL
     SELECT 'chronology', count(*) FROM public.chronology
     UNION ALL
     SELECT 'chronology_root', count(*) FROM public.chronology_root
     UNION ALL
     SELECT 'chronology_tr', count(*) FROM public.chronology_tr
     UNION ALL
     SELECT 'database', count(*) FROM public.database
     UNION ALL
     SELECT 'site', count(*) FROM public.site;"
done
