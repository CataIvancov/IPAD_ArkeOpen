#!/usr/bin/env python3

from __future__ import annotations

import subprocess
from collections import OrderedDict

CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
SOURCE_DATASET_NAME = "Airtable archaeology sites (site-only v4)"

COLLECTION_NAME = "Maros-Pangkep Karst"
DATASET_NAME = "Maros-Pangkep Karst"
DATASET_DESCRIPTION = (
    "Thematic dataset of sites in the Maros–Pangkep karst region (South Sulawesi, Indonesia), "
    "filtered from the Airtable site-only import using locality and keyword matches."
)
DATASET_BIBLIOGRAPHY = "Filtered from Airtable archaeology sites (site-only v4) dataset."
DATASET_SOURCE_DESCRIPTION = "Subset of Airtable site-only import (Maros–Pangkep karst thematic extraction)"
DATASET_SOURCE_RELATION = ""
DATASET_SUBJECT = "maros; pangkep; maros-pangkep; karst; sulawesi; toalean"
DATASET_COPYRIGHT = "Local working copy"
DATASET_REUSE = "Local evaluation import only."
DATASET_GEOGRAPHICAL_LIMIT = "Maros–Pangkep karst, South Sulawesi, Indonesia"
DATASET_CONTEXT_DESCRIPTION = "Thematic subset of karst-related sites in Maros–Pangkep"
LICENSE_FALLBACK_NAME = "CC-BY-NC-ND-4.0"
COUNTRY_ID = 0
ROOT_CHRONOLOGY_ID = 970000

KEYWORDS = [
    "maros",
    "pangkep",
    "maros-pangkep",
    "maros pangkep",
    "bantimurung",
    "rammang",
    "rammang-rammang",
    "toalean",
    "toalian",
    "toala",
]


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def query_value(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout.strip()


def query(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout

def query_tsv(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-AtF", "\t", "-c", sql]).stdout

def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def get_owner_id(db):
    value = query_value(db, "SELECT id FROM \"user\" WHERE username='IPAD_admin' ORDER BY id LIMIT 1;")
    if not value:
        raise RuntimeError("IPAD_admin user not found in database: " + db)
    return int(value)


def get_collection_id(db):
    value = query_value(db, f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;")
    if value:
        return int(value)
    query_value(db, f"INSERT INTO database_collection(name) VALUES ({sql_literal(COLLECTION_NAME)});")
    return int(query_value(db, f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;"))


def get_dataset_id(db):
    value = query_value(db, f"SELECT id FROM database WHERE name={sql_literal(DATASET_NAME)} LIMIT 1;")
    if value:
        return int(value)
    return int(query_value(db, "SELECT COALESCE(MAX(id),0)+1 FROM database;"))


def get_license_id(db):
    value = query_value(db, f"SELECT id FROM license WHERE name={sql_literal(LICENSE_FALLBACK_NAME)} LIMIT 1;")
    if value:
        return int(value)
    return int(query_value(db, "SELECT id FROM license ORDER BY id LIMIT 1;"))


def select_sites(db, source_db_id):
    patterns = [f"%{kw}%" for kw in KEYWORDS]
    where_parts = []
    for field in ["s.name", "s.city_name", "st.description", "src_t.comment", "src_t.bibliography"]:
        part = "(" + " OR ".join([f"{field} ILIKE {sql_literal(p)}" for p in patterns]) + ")"
        where_parts.append(part)
    where_clause = " OR ".join(where_parts)

    sql = (
        "SELECT DISTINCT s.id "
        "FROM site s "
        "LEFT JOIN site_tr st ON st.site_id=s.id AND st.lang_isocode='en' "
        "LEFT JOIN site_range sr ON sr.site_id=s.id "
        "LEFT JOIN site_range__charac src ON src.site_range_id=sr.id "
        "LEFT JOIN site_range__charac_tr src_t ON src_t.site_range__charac_id=src.id AND src_t.lang_isocode='en' "
        f"WHERE s.database_id = {source_db_id} AND ({where_clause});"
    )
    return [line.strip() for line in query(db, sql).splitlines() if line.strip()]


def load_sites_payload(db, site_ids):
    site_map = OrderedDict()
    if not site_ids:
        return site_map
    id_list = ",".join(sql_literal(sid) for sid in site_ids)

    site_sql = (
        "SELECT "
        "REPLACE(REPLACE(s.id, E'\\t', ' '), '|', ' '), "
        "REPLACE(REPLACE(s.code, E'\\t', ' '), '|', ' '), "
        "REPLACE(REPLACE(s.name, E'\\t', ' '), '|', ' '), "
        "REPLACE(REPLACE(s.city_name, E'\\t', ' '), '|', ' '), "
        "s.city_geonameid, "
        "ST_X(s.geom::geometry), ST_Y(s.geom::geometry), s.altitude, s.centroid, s.occupation, "
        "s.start_date1, s.start_date2, s.end_date1, s.end_date2 "
        "FROM site s "
        f"WHERE s.id IN ({id_list});"
    )
    for line in query_tsv(db, site_sql).splitlines():
        parts = line.split("|") if "|" in line else line.split("\t")
        if len(parts) < 14:
            continue
        parts = parts[:14]
        (sid, code, name, city_name, city_geonameid, lon, lat, altitude, centroid,
         occupation, start1, start2, end1, end2) = parts
        site_map[sid] = {
            "id": sid,
            "code": code,
            "name": name,
            "city_name": city_name,
            "city_geonameid": city_geonameid,
            "longitude": float(lon) if lon else 0.0,
            "latitude": float(lat) if lat else 0.0,
            "altitude": float(altitude) if altitude else 0.0,
            "centroid": centroid == "t",
            "occupation": occupation,
            "start_date1": int(start1),
            "start_date2": int(start2),
            "end_date1": int(end1),
            "end_date2": int(end2),
            "ranges": [],
            "trs": [],
        }

    # site_tr
    tr_sql = (
        "SELECT "
        "REPLACE(REPLACE(site_id, E'\\t', ' '), '|', ' '), "
        "lang_isocode, "
        "REPLACE(REPLACE(description, E'\\t', ' '), '|', ' ') "
        "FROM site_tr "
        f"WHERE site_id IN ({id_list});"
    )
    for line in query_tsv(db, tr_sql).splitlines():
        parts = line.split("|") if "|" in line else line.split("\t")
        if len(parts) < 3:
            continue
        sid, lang, desc = parts[0], parts[1], parts[2]
        if sid in site_map:
            site_map[sid]["trs"].append({"lang": lang, "description": desc})

    # site_range + charac
    range_sql = (
        "SELECT sr.id, "
        "REPLACE(REPLACE(sr.site_id, E'\\t', ' '), '|', ' '), "
        "sr.start_date1, sr.start_date2, sr.end_date1, sr.end_date2, "
        "src.id, src.charac_id, src.exceptional, src.knowledge_type, src.web_images "
        "FROM site_range sr "
        "LEFT JOIN site_range__charac src ON src.site_range_id = sr.id "
        f"WHERE sr.site_id IN ({id_list}) ORDER BY sr.id;"
    )
    for line in query_tsv(db, range_sql).splitlines():
        parts = line.split("|") if "|" in line else line.split("\t")
        if len(parts) < 11:
            continue
        (range_id, sid, s1, s2, e1, e2, src_id, charac_id, exceptional, knowledge_type, web_images) = parts
        if sid not in site_map:
            continue
        site_map[sid]["ranges"].append({
            "range_id": int(range_id),
            "start_date1": int(s1),
            "start_date2": int(s2),
            "end_date1": int(e1),
            "end_date2": int(e2),
            "src_id": int(src_id) if src_id else None,
            "charac_id": int(charac_id) if charac_id else None,
            "exceptional": exceptional == "t",
            "knowledge_type": knowledge_type or "not_documented",
            "web_images": web_images or "",
            "trs": [],
        })

    # site_range__charac_tr
    src_ids = [str(r["src_id"]) for s in site_map.values() for r in s["ranges"] if r["src_id"]]
    if src_ids:
        src_id_list = ",".join(src_ids)
        src_tr_sql = (
            "SELECT site_range__charac_id, lang_isocode, "
            "REPLACE(REPLACE(comment, E'\\t', ' '), '|', ' '), "
            "REPLACE(REPLACE(bibliography, E'\\t', ' '), '|', ' ') "
            "FROM site_range__charac_tr "
            f"WHERE site_range__charac_id IN ({src_id_list});"
        )
        src_map = {}
        for s in site_map.values():
            for r in s["ranges"]:
                if r["src_id"]:
                    src_map[r["src_id"]] = r
        for line in query(db, src_tr_sql).splitlines():
            parts = line.split("|") if "|" in line else line.split("\t")
            if len(parts) < 4:
                continue
            src_id, lang, comment, bibli = parts[0], parts[1], parts[2], parts[3]
            src_id = int(src_id)
            if src_id in src_map:
                src_map[src_id]["trs"].append({"lang": lang, "comment": comment, "bibliography": bibli})

    return site_map


def build_sql(db, owner_user_id, collection_id, dataset_id, license_id, site_map):
    statements = ["BEGIN;", "SET session_replication_role = replica;"]

    # Delete previous dataset if exists
    statements.extend([
        f"DELETE FROM public.database__authors WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database__country WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_context WHERE database_id = {dataset_id};",
        f"DELETE FROM public.database_tr WHERE database_id = {dataset_id};",
    ])

    # delete previous sites with prefix
    statements.extend([
        f"DELETE FROM public.site_range__charac_tr WHERE site_range__charac_id IN (SELECT id FROM public.site_range__charac WHERE site_range_id IN (SELECT id FROM public.site_range WHERE site_id LIKE 'maros-%'));",
        f"DELETE FROM public.site_range__charac WHERE site_range_id IN (SELECT id FROM public.site_range WHERE site_id LIKE 'maros-%');",
        f"DELETE FROM public.site_range WHERE site_id LIKE 'maros-%';",
        f"DELETE FROM public.site_tr WHERE site_id LIKE 'maros-%';",
        f"DELETE FROM public.site WHERE id LIKE 'maros-%';",
    ])

    # dataset
    statements.append(
        "INSERT INTO public.database "
        "(id, name, scale_resolution, geographical_extent, type, owner, editor, editor_url, contributor, default_language, state, "
        "license_id, published, soft_deleted, geographical_extent_geom, start_date, end_date, declared_creation_date, public, created_at, updated_at, root_chronology_id, illustrations, database_collection_id) "
        f"VALUES ({dataset_id}, {sql_literal(DATASET_NAME)}, 'region', 'region', 'inventory', {owner_user_id}, "
        f"{sql_literal('IPAD import')}, {sql_literal('')}, {sql_literal('IPAD')}, 'en', 'finished', "
        f"{license_id}, true, false, ST_GeogFromText({sql_literal('POLYGON((0 0,0 0,0 0,0 0,0 0))')}), 0, 0, "
        f"{sql_literal('2026-03-30T00:00:00+00:00')}, true, now(), now(), {ROOT_CHRONOLOGY_ID}, '', {collection_id}) "
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now(), database_collection_id = EXCLUDED.database_collection_id;"
    )

    statements.append(
        "INSERT INTO public.database_tr "
        "(database_id, lang_isocode, description, geographical_limit, bibliography, context_description, source_description, source_relation, copyright, subject, re_use) "
        f"VALUES ({dataset_id}, 'en', {sql_literal(DATASET_DESCRIPTION)}, {sql_literal(DATASET_GEOGRAPHICAL_LIMIT)}, {sql_literal(DATASET_BIBLIOGRAPHY)}, "
        f"{sql_literal(DATASET_CONTEXT_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_RELATION)}, "
        f"{sql_literal(DATASET_COPYRIGHT)}, {sql_literal(DATASET_SUBJECT)}, {sql_literal(DATASET_REUSE)}) "
        "ON CONFLICT (database_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description, bibliography = EXCLUDED.bibliography;"
    )

    statements.append(
        f"INSERT INTO public.database__authors (database_id, user_id) VALUES ({dataset_id}, {owner_user_id}) ON CONFLICT (database_id, user_id) DO NOTHING;"
    )
    statements.append(
        f"INSERT INTO public.database_context (id, database_id, context) VALUES ((SELECT COALESCE(MAX(id),0)+1 FROM public.database_context), {dataset_id}, 'academic-work');"
    )
    statements.append(
        f"INSERT INTO public.database__country (database_id, country_geonameid) VALUES ({dataset_id}, {COUNTRY_ID}) ON CONFLICT (database_id, country_geonameid) DO NOTHING;"
    )

    # allocate ids
    next_range_id = int(query_value(db, "SELECT COALESCE(MAX(id),0)+1 FROM public.site_range;"))
    next_src_id = int(query_value(db, "SELECT COALESCE(MAX(id),0)+1 FROM public.site_range__charac;"))

    min_lon = min_lat = None
    max_lon = max_lat = None
    start_dates = []
    end_dates = []

    for site in site_map.values():
        new_id = f"maros-{site['id']}"
        lon = site["longitude"]
        lat = site["latitude"]
        if min_lon is None:
            min_lon = max_lon = lon
            min_lat = max_lat = lat
        else:
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)
        start_dates.append(site["start_date1"])
        end_dates.append(site["end_date2"])

        statements.append(
            "INSERT INTO public.site "
            "(id, code, name, city_name, city_geonameid, geom, geom_3d, centroid, occupation, database_id, created_at, updated_at, altitude, start_date1, start_date2, end_date1, end_date2) "
            f"VALUES ({sql_literal(new_id)}, {sql_literal(site['code'])}, {sql_literal(site['name'])}, {sql_literal(site['city_name'])}, "
            f"{site['city_geonameid']}, ST_GeogFromText('POINT({lon} {lat})'), "
            f"ST_Force3DZ(ST_GeomFromText('POINT({lon} {lat})', 4326), {site['altitude']})::geography, "
            f"{'true' if site['centroid'] else 'false'}, '{site['occupation']}', {dataset_id}, now(), now(), {site['altitude']}, "
            f"{site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}) "
            "ON CONFLICT (id) DO UPDATE SET updated_at = now();"
        )

        for tr in site["trs"]:
            statements.append(
                "INSERT INTO public.site_tr (site_id, lang_isocode, description) "
                f"VALUES ({sql_literal(new_id)}, {sql_literal(tr['lang'])}, {sql_literal(tr['description'])}) "
                "ON CONFLICT (site_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description;"
            )

        for r in site["ranges"]:
            range_id = next_range_id
            next_range_id += 1
            statements.append(
                "INSERT INTO public.site_range (id, site_id, start_date1, start_date2, end_date1, end_date2, created_at, updated_at) "
                f"VALUES ({range_id}, {sql_literal(new_id)}, {r['start_date1']}, {r['start_date2']}, {r['end_date1']}, {r['end_date2']}, now(), now());"
            )
            if r["src_id"]:
                src_id = next_src_id
                next_src_id += 1
                charac_id = r["charac_id"] or 0
                statements.append(
                    "INSERT INTO public.site_range__charac (id, site_range_id, charac_id, exceptional, knowledge_type, web_images) "
                    f"VALUES ({src_id}, {range_id}, {charac_id}, {'true' if r['exceptional'] else 'false'}, '{r['knowledge_type']}', {sql_literal(r['web_images'])});"
                )
                for tr in r["trs"]:
                    statements.append(
                        "INSERT INTO public.site_range__charac_tr (site_range__charac_id, lang_isocode, comment, bibliography) "
                        f"VALUES ({src_id}, {sql_literal(tr['lang'])}, {sql_literal(tr['comment'])}, {sql_literal(tr['bibliography'])});"
                    )

    # update dataset bbox/dates
    if min_lon is None:
        min_lon = min_lat = max_lon = max_lat = 0.0
    bbox_wkt = f"POLYGON(({min_lon} {min_lat},{min_lon} {max_lat},{max_lon} {max_lat},{max_lon} {min_lat},{min_lon} {min_lat}))"
    start_date = min(start_dates) if start_dates else 0
    end_date = max(end_dates) if end_dates else 0

    statements.append(
        "UPDATE public.database SET geographical_extent_geom = ST_GeogFromText(" + sql_literal(bbox_wkt) + "), "
        f"start_date = {start_date}, end_date = {end_date}, updated_at = now() WHERE id = {dataset_id};"
    )

    statements.extend([
        "SELECT setval('site_range_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range), 1), true);",
        "SELECT setval('site_range__charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.site_range__charac), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ])

    return "\n".join(statements) + "\n"


def main():
    for db in DATABASES:
        source_db_id = query_value(db, f"SELECT id FROM database WHERE name={sql_literal(SOURCE_DATASET_NAME)} LIMIT 1;")
        if not source_db_id:
            raise RuntimeError(f"Source dataset not found in {db}: {SOURCE_DATASET_NAME}")
        source_db_id = int(source_db_id)
        owner_user_id = get_owner_id(db)
        collection_id = get_collection_id(db)
        dataset_id = get_dataset_id(db)
        license_id = get_license_id(db)
        site_ids = select_sites(db, source_db_id)
        site_map = load_sites_payload(db, site_ids)
        sql = build_sql(db, owner_user_id, collection_id, dataset_id, license_id, site_map)
        sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], stdin=sql)
        print(f"[maros-pangkep:{db}] sites={len(site_map)} dataset_id={dataset_id}")


if __name__ == "__main__":
    main()
