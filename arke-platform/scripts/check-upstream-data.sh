#!/bin/sh

set -eu

CONTAINER="arkeopenlocal-postgres"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "Container $CONTAINER is not running."
  exit 1
fi

for db in arkeopen arkeogis; do
  echo "[$db]"
  docker exec "$CONTAINER" psql -U postgres -d "$db" -Atc \
    "SELECT 'charac', count(*) FROM public.charac
     UNION ALL
     SELECT 'chronology', count(*) FROM public.chronology
     UNION ALL
     SELECT 'map_source', count(*) FROM public.map_source
     UNION ALL
     SELECT 'database', count(*) FROM public.database
     UNION ALL
     SELECT 'site', count(*) FROM public.site;"
done
