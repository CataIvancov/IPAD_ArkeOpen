#!/usr/bin/env python3

import csv
import io
import subprocess
from collections import OrderedDict


CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
CSV_URL = "https://api.nakala.fr/data/10.34847/nkl.332auar2/c9cdc810b08ed899106246779044d40f93593a76"
DATASET_NAME = "Inscriptions protohistoriques d'Enserune (Gaule du sud)"
DATASET_DESCRIPTION = (
    "Jeu de donnees Nakala consacre aux inscriptions protohistoriques d'Enserune. "
    "La notice source indique que ces donnees sont desormais desuetes et renvoie "
    "a la publication Ruiz-Darasse 2024 en attendant un nouveau depot."
)
DATASET_BIBLIOGRAPHY = (
    "MOHAMMED BENKHALID; Coline Ruiz-Darasse. Inscriptions protohistoriques d'Enserune "
    "(Gaule du sud). DOI 10.34847/nkl.332auar2."
)
DATASET_SOURCE_DESCRIPTION = "Import CSV Nakala Inscriptions protohistoriques d'Enserune"
DATASET_SOURCE_RELATION = "https://nakala.fr/10.34847/nkl.332auar2"
DATASET_SUBJECT = "inscription ecriture; Age du fer; Gaule"
DATASET_COPYRIGHT = "CC-BY-NC-SA-4.0"
DATASET_REUSE = "Consultation et reutilisation soumises a la licence CC-BY-NC-SA-4.0."
DATASET_GEOGRAPHICAL_LIMIT = "Enserune, Gaule du sud, France"
DATASET_CONTEXT_DESCRIPTION = "Corpus d'inscriptions protohistoriques"
LICENSE_NAME = "CC-BY-NC-SA-4.0"
LICENSE_URL = "https://spdx.org/licenses/CC-BY-NC-SA-4.0.html#licenseText"
COUNTRY_ID = 0
OWNER_USER_ID = 61
ROOT_CHRONOLOGY_ID = 37
CITY_BASE = 960000

KNOWLEDGE_TYPE_MAP = {
    "Fouillé": "dig",
    "Non renseigné": "not_documented",
    "Prospecté pédestre": "prospected_pedestrian",
}
OCCUPATION_MAP = {
    "Multiple": "multiple",
}


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def query_value(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout.strip()


def fetch_rows():
    text = sh(["curl", "-Ls", CSV_URL]).stdout
    return list(csv.DictReader(io.StringIO(text), delimiter=";"))


def parse_period(value):
    value = (value or "").strip()
    if not value or value.lower() == "indéterminé":
        return 0, 0
    if ":" in value:
        start, end = value.split(":", 1)
        return int(start), int(end)
    parsed = int(value)
    return parsed, parsed


def normalize_bool(value):
    return (value or "").strip().lower() in {"oui", "yes", "true"}


def normalize_ascii(value):
    replacements = {
        "é": "e",
        "è": "e",
        "ê": "e",
        "ë": "e",
        "à": "a",
        "â": "a",
        "ä": "a",
        "î": "i",
        "ï": "i",
        "ô": "o",
        "ö": "o",
        "ù": "u",
        "û": "u",
        "ü": "u",
        "ç": "c",
        "œ": "oe",
        "’": "'",
        "É": "E",
        "À": "A",
        "Ç": "C",
    }
    out = value
    for src, dst in replacements.items():
        out = out.replace(src, dst)
    return out


def load_charac_paths(db):
    sql = (
        "WITH RECURSIVE tree AS ("
        "SELECT c.id, c.parent_id, ct.name, ct.name::text AS path "
        "FROM public.charac c "
        "JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode = 'fr' "
        "WHERE c.parent_id = 0 "
        "UNION ALL "
        "SELECT c.id, c.parent_id, ct.name, (tree.path || ' > ' || ct.name) "
        "FROM public.charac c "
        "JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode = 'fr' "
        "JOIN tree ON c.parent_id = tree.id"
        ") "
        "SELECT id, path FROM tree ORDER BY path;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-AtF", "|", "-c", sql]).stdout
    mapping = {}
    for line in result.splitlines():
        if not line:
            continue
        charac_id, path = line.split("|", 1)
        mapping[path] = int(charac_id)
    return mapping


def build_dataset(rows, charac_paths):
    city_map = OrderedDict()
    min_lon = min(float(row["LONGITUDE"]) for row in rows)
    min_lat = min(float(row["LATITUDE"]) for row in rows)
    max_lon = max(float(row["LONGITUDE"]) for row in rows)
    max_lat = max(float(row["LATITUDE"]) for row in rows)

    for row in rows:
        locality = row["MAIN_CITY_NAME"].strip()
        if locality not in city_map:
            city_map[locality] = {
                "geonameid": CITY_BASE + len(city_map) + 1,
                "name": locality,
                "name_ascii": normalize_ascii(locality),
                "longitude": float(row["LONGITUDE"]),
                "latitude": float(row["LATITUDE"]),
            }

    sites = []
    for row in rows:
        charac_path = " > ".join(
            part.strip()
            for part in [
                row["CARAC_NAME"],
                row["CARAC_LVL1"],
                row["CARAC_LVL2"],
                row["CARAC_LVL3"],
                row["CARAC_LVL4"],
            ]
            if part.strip()
        )
        if charac_path not in charac_paths:
            raise RuntimeError(f"Missing charac path in local thesaurus: {charac_path}")

        start1, start2 = parse_period(row["STARTING_PERIOD"])
        end1, end2 = parse_period(row["ENDING_PERIOD"])
        altitude = float(row["ALTITUDE"]) if row["ALTITUDE"].strip() else 0.0
        sites.append(
            {
                "id": row["SITE_SOURCE_ID"].strip(),
                "code": row["SITE_SOURCE_ID"].strip(),
                "name": row["SITE_NAME"].strip(),
                "city_name": row["MAIN_CITY_NAME"].strip(),
                "city_geonameid": city_map[row["MAIN_CITY_NAME"].strip()]["geonameid"],
                "longitude": float(row["LONGITUDE"]),
                "latitude": float(row["LATITUDE"]),
                "altitude": altitude,
                "centroid": normalize_bool(row["CITY_CENTROID"]),
                "occupation": OCCUPATION_MAP.get(row["OCCUPATION"].strip(), "not_documented"),
                "start_date1": start1,
                "start_date2": start2,
                "end_date1": end1,
                "end_date2": end2,
                "description": row["COMMENTS"].strip(),
                "knowledge_type": KNOWLEDGE_TYPE_MAP.get(row["STATE_OF_KNOWLEDGE"].strip(), "not_documented"),
                "charac_id": charac_paths[charac_path],
                "exceptional": normalize_bool(row["CARAC_EXP"]),
                "bibliography": row["BIBLIOGRAPHY"].strip(),
                "comment": row["COMMENTS"].strip(),
                "web_images": "",
            }
        )

    dated_sites = [site for site in sites if site["start_date1"] != 0]
    end_sites = [site for site in sites if site["end_date2"] != 0]
    return {
        "bbox": (min_lon, min_lat, max_lon, max_lat),
        "cities": list(city_map.values()),
        "sites": sites,
        "start_date": min(site["start_date1"] for site in dated_sites) if dated_sites else 0,
        "end_date": max(site["end_date2"] for site in end_sites) if end_sites else 0,
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
        f"SELECT {next_license_id}, {sql_literal(LICENSE_NAME)}, {sql_literal(LICENSE_URL)} "
        "WHERE NOT EXISTS (SELECT 1 FROM public.license WHERE name = "
        f"{sql_literal(LICENSE_NAME)} OR url = {sql_literal(LICENSE_URL)});",
        "SELECT setval('license_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.license), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ]
    sh(
        ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
        stdin="\n".join(statements) + "\n",
    )


def get_ids(db):
    database_id = query_value(db, f"SELECT id FROM public.database WHERE name = {sql_literal(DATASET_NAME)} LIMIT 1;")
    if database_id:
        database_id = int(database_id)
    else:
        database_id = int(query_value(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM public.database;"))
    license_id = int(
        query_value(
            db,
            f"SELECT id FROM public.license WHERE name = {sql_literal(LICENSE_NAME)} OR url = {sql_literal(LICENSE_URL)} ORDER BY id LIMIT 1;",
        )
    )
    next_range_id = int(query_value(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM public.site_range;"))
    next_src_id = int(query_value(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM public.site_range__charac;"))
    return database_id, license_id, next_range_id, next_src_id


def build_sql(dataset_id, license_id, next_range_id, next_src_id, dataset):
    min_lon, min_lat, max_lon, max_lat = dataset["bbox"]
    bbox_wkt = (
        f"POLYGON(({min_lon} {min_lat},{min_lon} {max_lat},{max_lon} {max_lat},"
        f"{max_lon} {min_lat},{min_lon} {min_lat}))"
    )
    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
        f"DELETE FROM public.database__authors WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database__country WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_context WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_tr WHERE database_id = {dataset_id};",
    ]

    site_ids = ", ".join(sql_literal(site["id"]) for site in dataset["sites"])
    if site_ids:
        statements.extend(
            [
                f"DELETE FROM public.site_range__charac_tr WHERE site_range__charac_id IN (SELECT id FROM public.site_range__charac WHERE site_range_id IN (SELECT id FROM public.site_range WHERE site_id IN ({site_ids})));",
                f"DELETE FROM public.site_range__charac WHERE site_range_id IN (SELECT id FROM public.site_range WHERE site_id IN ({site_ids}));",
                f"DELETE FROM public.site_range WHERE site_id IN ({site_ids});",
                f"DELETE FROM public.site_tr WHERE site_id IN ({site_ids});",
                f"DELETE FROM public.site WHERE id IN ({site_ids});",
            ]
        )

    statements.extend(
        [
            "INSERT INTO public.database "
            "(id, name, scale_resolution, geographical_extent, type, owner, editor, editor_url, contributor, default_language, state, "
            "license_id, published, soft_deleted, geographical_extent_geom, start_date, end_date, declared_creation_date, public, created_at, updated_at, root_chronology_id, illustrations, database_collection_id) "
            f"VALUES ({dataset_id}, {sql_literal(DATASET_NAME)}, 'site', 'region', 'research', {OWNER_USER_ID}, "
            f"{sql_literal('Nakala')}, {sql_literal('https://nakala.fr/')}, {sql_literal('MOHAMMED BENKHALID; Coline Ruiz-Darasse')}, 'fr', 'finished', "
            f"{license_id}, true, false, ST_GeogFromText({sql_literal(bbox_wkt)}), {dataset['start_date']}, {dataset['end_date']}, "
            f"{sql_literal('2020-01-01T00:00:00+00:00')}, true, now(), now(), {ROOT_CHRONOLOGY_ID}, '', 0) "
            "ON CONFLICT (id) DO UPDATE SET "
            "name = EXCLUDED.name, scale_resolution = EXCLUDED.scale_resolution, geographical_extent = EXCLUDED.geographical_extent, "
            "type = EXCLUDED.type, owner = EXCLUDED.owner, editor = EXCLUDED.editor, editor_url = EXCLUDED.editor_url, contributor = EXCLUDED.contributor, "
            "default_language = EXCLUDED.default_language, state = EXCLUDED.state, license_id = EXCLUDED.license_id, published = EXCLUDED.published, "
            "soft_deleted = EXCLUDED.soft_deleted, geographical_extent_geom = EXCLUDED.geographical_extent_geom, start_date = EXCLUDED.start_date, "
            "end_date = EXCLUDED.end_date, declared_creation_date = EXCLUDED.declared_creation_date, public = EXCLUDED.public, updated_at = now(), "
            "root_chronology_id = EXCLUDED.root_chronology_id, illustrations = EXCLUDED.illustrations, database_collection_id = EXCLUDED.database_collection_id;",
            "INSERT INTO public.database_tr "
            "(database_id, lang_isocode, description, geographical_limit, bibliography, context_description, source_description, source_relation, copyright, subject, re_use) "
            f"VALUES ({dataset_id}, 'fr', {sql_literal(DATASET_DESCRIPTION)}, {sql_literal(DATASET_GEOGRAPHICAL_LIMIT)}, {sql_literal(DATASET_BIBLIOGRAPHY)}, "
            f"{sql_literal(DATASET_CONTEXT_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_RELATION)}, "
            f"{sql_literal(DATASET_COPYRIGHT)}, {sql_literal(DATASET_SUBJECT)}, {sql_literal(DATASET_REUSE)}) "
            "ON CONFLICT (database_id, lang_isocode) DO UPDATE SET "
            "description = EXCLUDED.description, geographical_limit = EXCLUDED.geographical_limit, bibliography = EXCLUDED.bibliography, "
            "context_description = EXCLUDED.context_description, source_description = EXCLUDED.source_description, source_relation = EXCLUDED.source_relation, "
            "copyright = EXCLUDED.copyright, subject = EXCLUDED.subject, re_use = EXCLUDED.re_use;",
            "INSERT INTO public.database__authors (database_id, user_id) "
            f"VALUES ({dataset_id}, {OWNER_USER_ID}) ON CONFLICT (database_id, user_id) DO NOTHING;",
            "INSERT INTO public.database_context (id, database_id, context) "
            f"VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM public.database_context), {dataset_id}, 'academic-work');",
            "INSERT INTO public.database__country (database_id, country_geonameid) "
            f"VALUES ({dataset_id}, {COUNTRY_ID}) ON CONFLICT (database_id, country_geonameid) DO NOTHING;",
        ]
    )

    for city in dataset["cities"]:
        statements.extend(
            [
                "INSERT INTO public.city (geonameid, country_geonameid, geom, geom_centroid, created_at, updated_at) "
                f"VALUES ({city['geonameid']}, {COUNTRY_ID}, NULL, ST_GeogFromText('POINT({city['longitude']} {city['latitude']})'), now(), now()) "
                "ON CONFLICT (geonameid) DO UPDATE SET country_geonameid = EXCLUDED.country_geonameid, geom_centroid = EXCLUDED.geom_centroid, updated_at = now();",
                "INSERT INTO public.city_tr (city_geonameid, lang_isocode, name, name_ascii) "
                f"VALUES ({city['geonameid']}, 'fr', {sql_literal(city['name'])}, {sql_literal(city['name_ascii'])}) "
                "ON CONFLICT (city_geonameid, lang_isocode) DO UPDATE SET name = EXCLUDED.name, name_ascii = EXCLUDED.name_ascii;",
            ]
        )

    range_id = next_range_id
    src_id = next_src_id
    for site in dataset["sites"]:
        statements.extend(
            [
                "INSERT INTO public.site "
                "(id, code, name, city_name, city_geonameid, geom, geom_3d, centroid, occupation, database_id, created_at, updated_at, altitude, "
                "start_date1, start_date2, end_date1, end_date2) "
                f"VALUES ({sql_literal(site['id'])}, {sql_literal(site['code'])}, {sql_literal(site['name'])}, {sql_literal(site['city_name'])}, "
                f"{site['city_geonameid']}, ST_GeogFromText('POINT({site['longitude']} {site['latitude']})'), "
                f"ST_Force3DZ(ST_GeomFromText('POINT({site['longitude']} {site['latitude']})', 4326), {site['altitude']})::geography, "
                f"{'true' if site['centroid'] else 'false'}, '{site['occupation']}', {dataset_id}, now(), now(), {site['altitude']}, "
                f"{site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}) "
                "ON CONFLICT (id) DO UPDATE SET "
                "code = EXCLUDED.code, name = EXCLUDED.name, city_name = EXCLUDED.city_name, city_geonameid = EXCLUDED.city_geonameid, "
                "geom = EXCLUDED.geom, geom_3d = EXCLUDED.geom_3d, centroid = EXCLUDED.centroid, occupation = EXCLUDED.occupation, "
                "database_id = EXCLUDED.database_id, updated_at = now(), altitude = EXCLUDED.altitude, start_date1 = EXCLUDED.start_date1, "
                "start_date2 = EXCLUDED.start_date2, end_date1 = EXCLUDED.end_date1, end_date2 = EXCLUDED.end_date2;",
                "INSERT INTO public.site_tr (site_id, lang_isocode, description) "
                f"VALUES ({sql_literal(site['id'])}, 'fr', {sql_literal(site['description'])}) "
                "ON CONFLICT (site_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description;",
                "INSERT INTO public.site_range (id, site_id, start_date1, start_date2, end_date1, end_date2, created_at, updated_at) "
                f"VALUES ({range_id}, {sql_literal(site['id'])}, {site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}, now(), now());",
                "INSERT INTO public.site_range__charac (id, site_range_id, charac_id, exceptional, knowledge_type, web_images) "
                f"VALUES ({src_id}, {range_id}, {site['charac_id']}, {'true' if site['exceptional'] else 'false'}, '{site['knowledge_type']}', {sql_literal(site['web_images'])});",
                "INSERT INTO public.site_range__charac_tr (site_range__charac_id, lang_isocode, comment, bibliography) "
                f"VALUES ({src_id}, 'fr', {sql_literal(site['comment'])}, {sql_literal(site['bibliography'])});",
            ]
        )
        range_id += 1
        src_id += 1

    statements.extend(
        [
            "SELECT setval('database_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.database), 1), true);",
            "SELECT setval('database_context_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.database_context), 1), true);",
            "SELECT setval('site_range_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range), 1), true);",
            "SELECT setval('site_range__charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range__charac), 1), true);",
            "SET session_replication_role = origin;",
            "COMMIT;",
        ]
    )
    return "\n".join(statements) + "\n"


def verify(db, dataset_id):
    summary_query = (
        "SELECT d.id, d.name, count(s.id), min(s.start_date1), max(s.end_date2) "
        "FROM public.database d "
        "LEFT JOIN public.site s ON s.database_id = d.id "
        f"WHERE d.id = {dataset_id} GROUP BY d.id, d.name;"
    )
    locality_query = (
        "SELECT count(DISTINCT city_name), count(DISTINCT name) "
        "FROM public.site "
        f"WHERE database_id = {dataset_id};"
    )
    print(f"[enserune:{db}]")
    print(sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", summary_query]).stdout, end="")
    print(sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", locality_query]).stdout, end="")


def main():
    rows = fetch_rows()
    for db in DATABASES:
        ensure_support_rows(db)
        charac_paths = load_charac_paths(db)
        dataset = build_dataset(rows, charac_paths)
        database_id, license_id, next_range_id, next_src_id = get_ids(db)
        sql = build_sql(database_id, license_id, next_range_id, next_src_id, dataset)
        sh(
            ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
            stdin=sql,
        )
        verify(db, database_id)


if __name__ == "__main__":
    main()
