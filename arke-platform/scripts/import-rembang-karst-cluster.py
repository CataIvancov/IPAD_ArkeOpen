#!/usr/bin/env python3

from __future__ import annotations

import csv
import pathlib
import subprocess
from collections import OrderedDict

CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
INPUT_CSV = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/rembang-karst-cluster.csv")

COLLECTION_NAME = "Rembang Karst Cluster"
DATASET_NAME = COLLECTION_NAME
DATASET_DESCRIPTION = (
    "Thematic dataset of archaeological caves and rockshelters in the Rembang karst region "
    "(Central Java, Indonesia), compiled from local surveys and synthesis tables."
)
DATASET_BIBLIOGRAPHY = "Compiled from drive text extraction and local reports (Rembang karst cluster)."
DATASET_SOURCE_DESCRIPTION = "Local CSV import (Rembang karst cluster)"
DATASET_SOURCE_RELATION = ""
DATASET_SUBJECT = "rembang; karst; central java; caves; rockshelters; indonesia"
DATASET_COPYRIGHT = "Local working copy"
DATASET_REUSE = "Local evaluation import only."
DATASET_GEOGRAPHICAL_LIMIT = "Rembang karst, Central Java, Indonesia"
DATASET_CONTEXT_DESCRIPTION = "Thematic subset of karst-related sites in the Rembang region"
LICENSE_FALLBACK_NAME = "CC-BY-NC-ND-4.0"
COUNTRY_ID = 0
ROOT_CHRONOLOGY_ID = 970000
CITY_BASE = 982000
UNDETERMINED_LEFT = -2147483648
UNDETERMINED_RIGHT = 2147483647

KNOWLEDGE_TYPE_MAP = {
    "not documented": "not_documented",
    "not documented ": "not_documented",
    "documented": "literature",
    "literature": "literature",
}

OCCUPATION_MAP = {
    "not specified": "not_documented",
    "not documented": "not_documented",
    "unique": "single",
    "continuous": "continuous",
    "multiple": "multiple",
}


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def query_value(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout.strip()


def normalize_ascii(value):
    replacements = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a", "ä": "a",
        "î": "i", "ï": "i",
        "ô": "o", "ö": "o",
        "ù": "u", "û": "u", "ü": "u",
        "ç": "c", "œ": "oe", "’": "'", "É": "E", "À": "A", "Ç": "C",
    }
    out = value
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    return out


def load_rows():
    with INPUT_CSV.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        rows = []
        for row in reader:
            if None in row:
                row.pop(None, None)
            rows.append(row)
        return rows


def parse_period(value, side):
    value = (value or "").strip()
    if not value or value == "Undefined":
        return UNDETERMINED_LEFT if side == "left" else UNDETERMINED_RIGHT
    return int(value)


def normalize_bool(value):
    return (value or "").strip().lower() in {"oui", "yes", "true"}


def load_charac_paths(db):
    sql = (
        "WITH RECURSIVE tree AS ("
        "SELECT c.id, c.parent_id, ct.name, ct.name::text AS path "
        "FROM public.charac c "
        "JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode = 'en' "
        "WHERE c.parent_id = 0 "
        "UNION ALL "
        "SELECT c.id, c.parent_id, ct.name, (tree.path || ' > ' || ct.name) "
        "FROM public.charac c "
        "JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode = 'en' "
        "JOIN tree ON c.parent_id = tree.id"
        ") "
        "SELECT id, path FROM tree ORDER BY path;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-AtF", "|", "-c", sql]).stdout
    mapping = {}
    for line in result.splitlines():
        if line:
            charac_id, path = line.split("|", 1)
            mapping[path] = int(charac_id)
    return mapping


def build_charac_path(row):
    parts = []
    for key in ("MAIN_CHARAC", "CHARAC_LVL1", "CHARAC_LVL2", "CHARAC_LVL3", "CHARAC_LVL4"):
        value = (row.get(key) or "").strip()
        if value:
            parts.append(value)
    return " > ".join(parts)


def build_dataset(rows, charac_paths):
    city_map = OrderedDict()
    bbox_lons = [float(row["LONGITUDE"]) for row in rows if row.get("LONGITUDE")]
    bbox_lats = [float(row["LATITUDE"]) for row in rows if row.get("LATITUDE")]
    if bbox_lons and bbox_lats:
        min_lon, max_lon = min(bbox_lons), max(bbox_lons)
        min_lat, max_lat = min(bbox_lats), max(bbox_lats)
    else:
        min_lon = max_lon = min_lat = max_lat = 0.0

    sites = []
    for row in rows:
        locality = (row.get("LOCALISATION") or row.get("SITE_NAME") or "").strip()
        if locality not in city_map:
            longitude = float(row["LONGITUDE"]) if row.get("LONGITUDE") else 0.0
            latitude = float(row["LATITUDE"]) if row.get("LATITUDE") else 0.0
            city_map[locality] = {
                "geonameid": CITY_BASE + len(city_map) + 1,
                "name": locality,
                "name_ascii": normalize_ascii(locality),
                "longitude": longitude,
                "latitude": latitude,
            }

        charac_path = build_charac_path(row)
        if not charac_path:
            raise RuntimeError(f"Missing charac path for site {row.get('SITE_NAME')}")
        if charac_path not in charac_paths:
            raise RuntimeError(f"Missing charac path in local thesaurus: {charac_path}")

        start = parse_period(row.get("STARTING_PERIOD"), "left")
        end = parse_period(row.get("ENDING_PERIOD"), "right")
        knowledge_type = KNOWLEDGE_TYPE_MAP.get((row.get("STATE_OF_KNOWLEDGE") or "").strip().lower(), "not_documented")
        occupation = OCCUPATION_MAP.get((row.get("OCCUPATION") or "").strip().lower(), "not_documented")
        lon = float(row["LONGITUDE"]) if row.get("LONGITUDE") else None
        lat = float(row["LATITUDE"]) if row.get("LATITUDE") else None

        site = {
            "id": row.get("SITE_SOURCE_ID") or "",
            "code": row.get("SITE_SOURCE_ID") or "",
            "name": (row.get("SITE_NAME") or "").strip(),
            "city_name": locality,
            "city_geonameid": city_map[locality]["geonameid"],
            "longitude": lon,
            "latitude": lat,
            "altitude": float(row["ALTITUDE"]) if row.get("ALTITUDE") else 0.0,
            "centroid": normalize_bool(row.get("CITY_CENTROID")),
            "occupation": occupation,
            "start_date1": start,
            "start_date2": start if start != UNDETERMINED_LEFT else UNDETERMINED_RIGHT,
            "end_date1": end if end != UNDETERMINED_RIGHT else UNDETERMINED_LEFT,
            "end_date2": end,
            "description": (row.get("COMMENTS") or "").strip(),
            "knowledge_type": knowledge_type,
            "charac_id": charac_paths[charac_path],
            "exceptional": normalize_bool(row.get("CHARAC_EXP")),
            "bibliography": (row.get("BIBLIOGRAPHY") or "").strip(),
            "comment": (row.get("COMMENTS") or "").strip(),
            "web_images": (row.get("WEB_IMAGES") or "").strip(),
        }
        sites.append(site)

    finite_starts = [site["start_date1"] for site in sites if site["start_date1"] != UNDETERMINED_LEFT]
    finite_ends = [site["end_date2"] for site in sites if site["end_date2"] != UNDETERMINED_RIGHT]

    return {
        "bbox": (min_lon, min_lat, max_lon, max_lat),
        "cities": list(city_map.values()),
        "sites": sites,
        "start_date": min(finite_starts) if finite_starts else -2578050,
        "end_date": max(finite_ends) if finite_ends else 1950,
    }


def ensure_support_rows(db):
    next_license_id = int(query_value(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM public.license;"))
    statements = [
        "BEGIN;",
        "CREATE EXTENSION IF NOT EXISTS unaccent;",
        "SET session_replication_role = replica;",
        "INSERT INTO public.country (geonameid, iso_code, geom, created_at, updated_at) "
        f"VALUES ({COUNTRY_ID}, NULL, NULL, now(), now()) ON CONFLICT (geonameid) DO NOTHING;",
        "INSERT INTO public.license (id, name, url) "
        f"SELECT {next_license_id}, {sql_literal(LICENSE_FALLBACK_NAME)}, '' "
        "WHERE NOT EXISTS (SELECT 1 FROM public.license WHERE name = "
        f"{sql_literal(LICENSE_FALLBACK_NAME)});",
        "SELECT setval('license_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.license), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ]
    sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], stdin="\n".join(statements) + "\n")


def get_ids(db):
    database_id = query_value(db, f"SELECT id FROM public.database WHERE name = {sql_literal(DATASET_NAME)} LIMIT 1;")
    if database_id:
        database_id = int(database_id)
    else:
        database_id = int(query_value(db, "SELECT COALESCE(MAX(id),0)+1 FROM public.database;"))
    owner_user_id = int(query_value(db, "SELECT id FROM \"user\" WHERE username='IPAD_admin' ORDER BY id LIMIT 1;"))
    collection_id = query_value(db, f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;")
    if collection_id:
        collection_id = int(collection_id)
    else:
        query_value(db, f"INSERT INTO database_collection(name) VALUES ({sql_literal(COLLECTION_NAME)});")
        collection_id = int(query_value(db, f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;"))
    license_id = int(query_value(db, f"SELECT id FROM public.license WHERE name = {sql_literal(LICENSE_FALLBACK_NAME)} LIMIT 1;") or 1)
    return database_id, owner_user_id, collection_id, license_id


def build_sql(dataset_id, owner_user_id, collection_id, license_id, dataset):
    min_lon, min_lat, max_lon, max_lat = dataset["bbox"]
    bbox_wkt = f"POLYGON(({min_lon} {min_lat},{min_lon} {max_lat},{max_lon} {max_lat},{max_lon} {min_lat},{min_lon} {min_lat}))"

    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
        f"DELETE FROM public.database__authors WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database__country WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_context WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_tr WHERE database_id = {dataset_id};",
    ]

    site_ids = ", ".join(sql_literal(site["id"]) for site in dataset["sites"] if site["id"])
    if site_ids:
        statements.extend([
            f"DELETE FROM public.site_range__charac_tr WHERE site_range__charac_id IN (SELECT id FROM public.site_range__charac WHERE site_range_id IN (SELECT id FROM public.site_range WHERE site_id IN ({site_ids})));",
            f"DELETE FROM public.site_range__charac WHERE site_range_id IN (SELECT id FROM public.site_range WHERE site_id IN ({site_ids}));",
            f"DELETE FROM public.site_range WHERE site_id IN ({site_ids});",
            f"DELETE FROM public.site_tr WHERE site_id IN ({site_ids});",
            f"DELETE FROM public.site WHERE id IN ({site_ids});",
        ])

    statements.extend([
        "INSERT INTO public.database "
        "(id, name, scale_resolution, geographical_extent, type, owner, editor, editor_url, contributor, default_language, state, "
        "license_id, published, soft_deleted, geographical_extent_geom, start_date, end_date, declared_creation_date, public, created_at, updated_at, root_chronology_id, illustrations, database_collection_id) "
        f"VALUES ({dataset_id}, {sql_literal(DATASET_NAME)}, 'region', 'region', 'inventory', {owner_user_id}, "
        f"{sql_literal('IPAD import')}, {sql_literal('')}, {sql_literal('IPAD')}, 'en', 'finished', "
        f"{license_id}, true, false, ST_GeogFromText({sql_literal(bbox_wkt)}), {dataset['start_date']}, {dataset['end_date']}, "
        f"{sql_literal('2026-04-01T00:00:00+00:00')}, true, now(), now(), {ROOT_CHRONOLOGY_ID}, '', {collection_id}) "
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now(), database_collection_id = EXCLUDED.database_collection_id;",
        "INSERT INTO public.database_tr "
        "(database_id, lang_isocode, description, geographical_limit, bibliography, context_description, source_description, source_relation, copyright, subject, re_use) "
        f"VALUES ({dataset_id}, 'en', {sql_literal(DATASET_DESCRIPTION)}, {sql_literal(DATASET_GEOGRAPHICAL_LIMIT)}, {sql_literal(DATASET_BIBLIOGRAPHY)}, "
        f"{sql_literal(DATASET_CONTEXT_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_RELATION)}, "
        f"{sql_literal(DATASET_COPYRIGHT)}, {sql_literal(DATASET_SUBJECT)}, {sql_literal(DATASET_REUSE)}) "
        "ON CONFLICT (database_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description;",
        f"INSERT INTO public.database__authors (database_id, user_id) VALUES ({dataset_id}, {owner_user_id}) ON CONFLICT (database_id, user_id) DO NOTHING;",
        f"INSERT INTO public.database_context (id, database_id, context) VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM public.database_context), {dataset_id}, 'academic-work');",
        f"INSERT INTO public.database__country (database_id, country_geonameid) VALUES ({dataset_id}, {COUNTRY_ID}) ON CONFLICT (database_id, country_geonameid) DO NOTHING;",
    ])

    for city in dataset["cities"]:
        statements.extend([
            "INSERT INTO public.city (geonameid, country_geonameid, geom, geom_centroid, created_at, updated_at) "
            f"VALUES ({city['geonameid']}, {COUNTRY_ID}, NULL, ST_GeogFromText('POINT({city['longitude']} {city['latitude']})'), now(), now()) "
            "ON CONFLICT (geonameid) DO UPDATE SET country_geonameid = EXCLUDED.country_geonameid, geom_centroid = EXCLUDED.geom_centroid, updated_at = now();",
            "INSERT INTO public.city_tr (city_geonameid, lang_isocode, name, name_ascii) "
            f"VALUES ({city['geonameid']}, 'en', {sql_literal(city['name'])}, {sql_literal(city['name_ascii'])}) "
            "ON CONFLICT (city_geonameid, lang_isocode) DO UPDATE SET name = EXCLUDED.name, name_ascii = EXCLUDED.name_ascii;",
        ])

    range_id = int(query_value("arkeopen", "SELECT COALESCE(MAX(id),0)+1 FROM public.site_range;"))
    src_id = int(query_value("arkeopen", "SELECT COALESCE(MAX(id),0)+1 FROM public.site_range__charac;"))

    for site in dataset["sites"]:
        lon = site["longitude"]
        lat = site["latitude"]
        geom = f"ST_GeogFromText('POINT({lon} {lat})')" if lon is not None and lat is not None else "NULL"
        geom3d = (
            f"ST_Force3DZ(ST_GeomFromText('POINT({lon} {lat})', 4326), {site['altitude']})::geography"
            if lon is not None and lat is not None
            else "NULL"
        )
        statements.extend([
            "INSERT INTO public.site "
            "(id, code, name, city_name, city_geonameid, geom, geom_3d, centroid, occupation, database_id, created_at, updated_at, altitude, start_date1, start_date2, end_date1, end_date2) "
            f"VALUES ({sql_literal(site['id'])}, {sql_literal(site['code'])}, {sql_literal(site['name'])}, {sql_literal(site['city_name'])}, "
            f"{site['city_geonameid']}, {geom}, {geom3d}, "
            f"{'true' if site['centroid'] else 'false'}, '{site['occupation']}', {dataset_id}, now(), now(), {site['altitude']}, "
            f"{site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}) "
            "ON CONFLICT (id) DO UPDATE SET updated_at = now();",
            f"INSERT INTO public.site_tr (site_id, lang_isocode, description) VALUES ({sql_literal(site['id'])}, 'en', {sql_literal(site['description'])}) ON CONFLICT (site_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description;",
            f"INSERT INTO public.site_range (id, site_id, start_date1, start_date2, end_date1, end_date2, created_at, updated_at) VALUES ({range_id}, {sql_literal(site['id'])}, {site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}, now(), now());",
            f"INSERT INTO public.site_range__charac (id, site_range_id, charac_id, exceptional, knowledge_type, web_images) VALUES ({src_id}, {range_id}, {site['charac_id']}, {'true' if site['exceptional'] else 'false'}, '{site['knowledge_type']}', {sql_literal(site['web_images'])});",
            f"INSERT INTO public.site_range__charac_tr (site_range__charac_id, lang_isocode, comment, bibliography) VALUES ({src_id}, 'en', {sql_literal(site['comment'])}, {sql_literal(site['bibliography'])});",
        ])
        range_id += 1
        src_id += 1

    statements.extend([
        "SELECT setval('database_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.database), 1), true);",
        "SELECT setval('database_context_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.database_context), 1), true);",
        "SELECT setval('site_range_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range), 1), true);",
        "SELECT setval('site_range__charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range__charac), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ])

    return "\n".join(statements) + "\n"


def main():
    rows = load_rows()
    for db in DATABASES:
        ensure_support_rows(db)
        charac_paths = load_charac_paths(db)
        dataset = build_dataset(rows, charac_paths)
        dataset_id, owner_user_id, collection_id, license_id = get_ids(db)
        sql = build_sql(dataset_id, owner_user_id, collection_id, license_id, dataset)
        sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], stdin=sql)
        print(f"[rembang-karst:{db}] sites={len(dataset['sites'])} dataset_id={dataset_id}")


if __name__ == "__main__":
    main()
