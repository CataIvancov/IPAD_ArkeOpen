#!/usr/bin/env python3

from __future__ import annotations

import csv
import subprocess
from collections import OrderedDict

CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
SOURCE_DATASET_NAME = "Teluk Wondama - Papua: Rumberpon and Roon islands"

COLLECTION_NAME = "Teluk Wondama Collection"
DATASET_NAME = "Teluk Wondama - Papua: Rumberpon and Roon islands"
DATASET_DESCRIPTION = (
    "Archaeological sites in Teluk Wondama Regency, Papua, Indonesia, "
    "including sites on Rumberpon and Roon islands with rock art, burials, and ceremonial features."
)
DATASET_BIBLIOGRAPHY = "Mas'ud, Z. (2019). Situs dan peninggalan arkeologi Kabupaten Teluk Wondama Pulau Romberpon dan Pulau Roon."
DATASET_SOURCE_DESCRIPTION = "Extracted from Wondama-Bukuuuu(1).pdf with coordinates and descriptions"
DATASET_SOURCE_RELATION = ""
DATASET_SUBJECT = "teluk wondama; papua; rumberpon; roon; rock art; burial; ceremonial"
DATASET_COPYRIGHT = "Local working copy"
DATASET_REUSE = "Local evaluation import only."
DATASET_GEOGRAPHICAL_LIMIT = "Teluk Wondama Regency, Papua, Indonesia"
DATASET_CONTEXT_DESCRIPTION = "Archaeological survey of sites in Teluk Wondama including rock art, burial niches, and ceremonial sites"
LICENSE_FALLBACK_NAME = "CC-BY-NC-ND-4.0"
COUNTRY_ID = 0
ROOT_CHRONOLOGY_ID = 970000

CSV_FILE = "../data/teluk-wondama-rumberpon-roon.csv"

def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)

def query_value(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout.strip()

def query(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout

def sql_literal(value):
    if value is None or value == "":
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

def read_csv():
    site_map = OrderedDict()
    with open(CSV_FILE, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            site_id = row['SITE_SOURCE_ID']
            site_map[site_id] = {
                "id": site_id,
                "name": row['SITE_NAME'],
                "localisation": row['LOCALISATION'],
                "longitude": float(row['LONGITUDE']) if row['LONGITUDE'] else None,
                "latitude": float(row['LATITUDE']) if row['LATITUDE'] else None,
                "altitude": int(row['ALTITUDE']) if row['ALTITUDE'] else None,
                "state_of_knowledge": row['STATE_OF_KNOWLEDGE'],
                "occupation": row['OCCUPATION'],
                "start_date1": int(row['STARTING_PERIOD']) if row['STARTING_PERIOD'] else None,
                "end_date1": int(row['ENDING_PERIOD']) if row['ENDING_PERIOD'] else None,
                "main_charac": row['MAIN_CHARAC'],
                "charac_lvl1": row['CHARAC_LVL1'],
                "bibliography": row['BIBLIOGRAPHY'],
                "comments": row['COMMENTS'],
                "ranges": [],
                "trs": [],
            }
    return site_map

def main():
    site_map = read_csv()
    for db in DATABASES:
        print(f"Importing to {db}...")
        owner_user_id = get_owner_id(db)
        collection_id = get_collection_id(db)
        dataset_id = get_dataset_id(db)
        license_id = get_license_id(db)

        statements = []

        # delete old
        statements.append(
            f"DELETE FROM public.site WHERE id LIKE 'wondama-%';",
        )

        # dataset
        statements.append(
            "INSERT INTO public.database "
            "(id, name, scale_resolution, geographical_extent, type, owner, editor, editor_url, contributor, default_language, state, "
            "license_id, published, soft_deleted, geographical_extent_geom, start_date, end_date, declared_creation_date, public, created_at, updated_at, root_chronology_id, illustrations, database_collection_id) "
            f"VALUES ({dataset_id}, {sql_literal(DATASET_NAME)}, 'region', 'region', 'inventory', {owner_user_id}, "
            f"{sql_literal('IPAD import')}, {sql_literal('')}, {sql_literal('IPAD')}, 'en', 'finished', "
            f"{license_id}, true, false, ST_GeogFromText({sql_literal('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')}), 0, 0, "
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
            new_id = f"wondama-{site['id']}"
            lon = site["longitude"]
            lat = site["latitude"]
            if lon is not None and lat is not None:
                if min_lon is None:
                    min_lon = max_lon = lon
                    min_lat = max_lat = lat
                else:
                    min_lon = min(min_lon, lon)
                    max_lon = max(max_lon, lon)
                    min_lat = min(min_lat, lat)
                    max_lat = max(max_lat, lat)
            if site["start_date1"] is not None:
                start_dates.append(site["start_date1"])
            if site["end_date1"] is not None:
                end_dates.append(site["end_date1"])

            # site
            geom = f"ST_GeogFromText('POINT({lon} {lat})')" if lon is not None and lat is not None else "NULL"
            statements.append(
                "INSERT INTO public.site "
                "(id, database_id, name, city_name, centroid, geom, altitude, created_at, updated_at) "
                f"VALUES ({sql_literal(new_id)}, {dataset_id}, {sql_literal(site['name'])}, {sql_literal(site['localisation'])}, "
                f"{geom}, {geom}, {site['altitude'] or 'NULL'}, now(), now()) "
                "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, geom = EXCLUDED.geom, updated_at = now();"
            )

            # site_tr
            if site["comments"]:
                statements.append(
                    "INSERT INTO public.site_tr "
                    "(site_id, lang_isocode, description) "
                    f"VALUES ({sql_literal(new_id)}, 'en', {sql_literal(site['comments'])}) "
                    "ON CONFLICT (site_id, lang_isocode) DO UPDATE SET description = EXCLUDED.description;"
                )

            # site_range
            if site["start_date1"] is not None or site["end_date1"] is not None:
                range_id = next_range_id
                next_range_id += 1
                statements.append(
                    "INSERT INTO public.site_range "
                    "(id, site_id, start_date1, start_date2, end_date1, end_date2, created_at, updated_at) "
                    f"VALUES ({range_id}, {sql_literal(new_id)}, {site['start_date1'] or 0}, 0, {site['end_date1'] or 0}, 0, now(), now()) "
                    "ON CONFLICT (id) DO UPDATE SET start_date1 = EXCLUDED.start_date1, end_date1 = EXCLUDED.end_date1, updated_at = now();"
                )

                # site_range__charac
                src_id = next_src_id
                next_src_id += 1
                knowledge_type = site["state_of_knowledge"].lower().replace(" ", "_") if site["state_of_knowledge"] else "not_documented"
                statements.append(
                    "INSERT INTO public.site_range__charac "
                    "(id, site_range_id, charac_id, exceptional, knowledge_type, created_at, updated_at) "
                    f"VALUES ({src_id}, {range_id}, 940013, false, {sql_literal(knowledge_type)}, now(), now()) "
                    "ON CONFLICT (id) DO UPDATE SET knowledge_type = EXCLUDED.knowledge_type, updated_at = now();"
                )

        # update dataset geom
        if min_lon is not None:
            geom_wkt = f"POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
            statements.append(
                f"UPDATE public.database SET geographical_extent_geom = ST_GeogFromText({sql_literal(geom_wkt)}), "
                f"start_date = {min(start_dates) if start_dates else 0}, end_date = {max(end_dates) if end_dates else 0} WHERE id = {dataset_id};"
            )

        # execute
        sql_script = "\n".join(statements)
        sh(["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres", "-d", db], stdin=sql_script)
        print(f"Imported {len(site_map)} sites to {db}")

if __name__ == "__main__":
    main()