#!/usr/bin/env python3

from __future__ import annotations

import subprocess
from collections import OrderedDict

CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")

COLLECTION_NAME = "Prehistoric occupation in Meratus Mountains (South Kalimantan)"
DATASET_NAME = "Prehistoric occupation in Meratus mountains, South Kalimantan / Hunian prasejarah di pegunungan Meratus, Kalimantan Selatan"
DATASET_DESCRIPTION = (
    "Thematic dataset grouping prehistoric occupation sites from the Meratus Mountains (South Kalimantan), "
    "based on the Batu Cave publication and associated settlement-cave survey table."
)
DATASET_BIBLIOGRAPHY = (
    "Fajari, N., & Wibisono, W. (2020). Batu Cave: Prehistoric occupation of Meratus Mountains, South Kalimantan. "
    "Berkala Arkeologi, 40, 179-194. https://doi.org/10.30883/jba.v40i2.518"
)
DATASET_SOURCE_DESCRIPTION = "Extracted from Batu Cave: Prehistoric occupation of Meratus Mountains, South Kalimantan (2020)."
DATASET_SOURCE_RELATION = ""
DATASET_SUBJECT = "meratus; south kalimantan; caves; rockshelters; prehistoric occupation"
DATASET_COPYRIGHT = "Local working copy"
DATASET_REUSE = "Local evaluation import only."
DATASET_GEOGRAPHICAL_LIMIT = "Meratus Mountains, South Kalimantan, Indonesia"
DATASET_CONTEXT_DESCRIPTION = "Thematic dataset of Meratus prehistoric occupation sites"
LICENSE_FALLBACK_NAME = "CC-BY-NC-ND-4.0"
ROOT_CHRONOLOGY_ID = 970000
COUNTRY_ID = 0

UNDETERMINED_LEFT = -2147483648
UNDETERMINED_RIGHT = 2147483647

# Use the new Archaeological Sites thesaurus root.
CHARAC_CAVE = "Archaeological Sites > Cave / Gua / Liang / Ceruk"
CHARAC_UNKNOWN = "Archaeological Sites > Open-air site"

SITES = [
    {"id": "meratus-batu-cave", "name": "Batu Cave", "type": "cave", "calbp": (6065, 5650), "lat": -2.477611, "lon": 116.075187},
    {"id": "meratus-hasan-basri-cave", "name": "Hasan Basri Cave", "type": "cave"},
    {"id": "meratus-gunung-else-cave", "name": "Gunung Else Cave", "type": "cave"},
    {"id": "meratus-gunung-bambu-else-rockshelter", "name": "Gunung Bambu Else Rockshelter", "type": "cave"},
    {"id": "meratus-rasidi-1-cave", "name": "Rasidi 1 Cave", "type": "cave"},
    {"id": "meratus-gunung-beringin-rockshelter", "name": "Gunung Beringin Rockshelter", "type": "cave"},
    {"id": "meratus-gunung-liang-udud-cave", "name": "Gunung Liang Udud Cave", "type": "cave"},
    {"id": "meratus-takasima-2-cave", "name": "Takasima 2 Cave", "type": "cave"},
    {"id": "meratus-bali-rockshelter", "name": "Bali Rockshelter", "type": "cave"},
    {"id": "meratus-isur-cave", "name": "Isur Cave", "type": "cave"},
    {"id": "meratus-kebun-sawit", "name": "Kebun Sawit (Palm Oil Plantation)", "type": "unknown"},
    {
        "id": "meratus-liang-bangkai-1",
        "name": "Liang Bangkai 1",
        "type": "cave",
        "calbp": (5920, 6045),
        "lat": -3.20124608232546,
        "lon": 115.79630683995282,
        "comment": (
            "Bangkai Hill has around 12 caves and rockshelters. The prehistoric culture that developed in the caves on "
            "Bangkai Hill was supported by at least two human groups based on two burials: the first a mixed Mongoloid "
            "and Australomelanesoid population, the second Mongoloid. These populations developed the same stone tool "
            "technology, namely blade-flake technology. The dominant use of black pigments in the rock art images is a "
            "unique and distinctive phenomenon, different from the same culture in Sangkulirang-Mangkalihat (East Kalimantan)."
        ),
    },
    {
        "id": "meratus-liang-bangkai-10",
        "name": "Liang Bangkai 10",
        "type": "cave",
        "calbp": (6424, 6573),
        "lat": -3.20124608232546,
        "lon": 115.79630683995282,
        "comment": (
            "Bangkai Hill has around 12 caves and rockshelters. The prehistoric culture that developed in the caves on "
            "Bangkai Hill was supported by at least two human groups based on two burials: the first a mixed Mongoloid "
            "and Australomelanesoid population, the second Mongoloid. These populations developed the same stone tool "
            "technology, namely blade-flake technology. The dominant use of black pigments in the rock art images is a "
            "unique and distinctive phenomenon, different from the same culture in Sangkulirang-Mangkalihat (East Kalimantan)."
        ),
    },
    {
        "id": "meratus-payung-cave",
        "name": "Payung Cave",
        "type": "cave",
        "calbp": (3082, 3408),
        "lat": -3.269083,
        "lon": 115.78175,
        "comment": (
            "Mining at Gua Payung removed nearly one metre of upper deposit. By 2012, about 88% of the cave floor "
            "had been removed. Four test pits were dug in the least disturbed areas. Gua Payung was occupied mainly "
            "during Neolithic times (circa 1000 BC), much later than Gua Babi, which produced only plain pottery in "
            "its upper layers. The Gua Payung pottery is closely paralleled in the Mangkalihat Peninsula, more than "
            "500 km to the north. The first millennium BC dating is significant and likely applies to pottery with "
            "some red-slipping, punctate/dentate stamping and incision, though contexts are somewhat disturbed. The "
            "lithics assemblage at the base of Test Pit 4 may indicate mid-Holocene occupation, but further excavation "
            "is needed to confirm additional traces."
        ),
    },
    {
        "id": "meratus-liang-ulin-2",
        "name": "Liang Ulin 2",
        "type": "cave",
        "calbp": (11247, 11560),
        "lat": -3.311444,
        "lon": 115.743778,
        "comment": (
            "The Ulin Hill complex has three rockshelters; among them, Liang Ulin 2 is the most archaeologically rich "
            "in terms of artifacts. Tools consist of core and flake, while debitage includes proximal flake, flake "
            "shatter, and nonflake."
        ),
    },
    {
        "id": "meratus-cililin-cave",
        "name": "Cililin Cave",
        "type": "cave",
        "lat": -2.8416405198833137,
        "lon": 115.61430019061646,
        "comment": (
            "Cililin Cave 1 is located in Bangkalan Dayak Village, Kelumpang Hulu District. A 2018 survey of "
            "Cililin Cave revealed archaeological features in the form of three iron smelting furnaces on the cave floor."
        ),
    },
    {
        "id": "meratus-jauharlin-1-cave",
        "name": "Jauharlin 1 Cave",
        "type": "cave",
        "lat": -2.856778,
        "lon": 115.59975,
        "comment": (
            "Findings include stone artifacts, pottery fragments, bone fragments, mollusk shells, and rock art features "
            "on the cave ceiling. The Jauharlin 1 rock art consists of figurative and non-figurative designs. The "
            "figurative design depicts a human in a complete composition with head, neck, body, both hands, and feet "
            "drawn as simple lines. Non-figurative designs include boats, geometric shapes, and motifs resembling fishhooks."
        ),
    },
]


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def query_value(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout.strip()


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
    value = query_value(db, f"SELECT id FROM database_collection WHERE name={sql_literal(COLLECTION_NAME)} LIMIT 1;")
    return int(value)


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


def calbp_to_year(calbp):
    return 1950 - calbp


def calbp_range_to_years(range_tuple):
    younger, older = range_tuple
    # ensure older is the larger calBP
    oldest = max(younger, older)
    youngest = min(younger, older)
    start = calbp_to_year(oldest)
    end = calbp_to_year(youngest)
    return start, end


def build_dataset(charac_paths):
    city_map = OrderedDict()
    sites = []
    for site in SITES:
        charac_path = CHARAC_CAVE if site["type"] == "cave" else CHARAC_UNKNOWN
        if charac_path not in charac_paths:
            raise RuntimeError(f"Missing charac path in local thesaurus: {charac_path}")
        if "calbp" in site:
            start, end = calbp_range_to_years(site["calbp"])
        else:
            start, end = UNDETERMINED_LEFT, UNDETERMINED_RIGHT

        locality = "South Kalimantan"
        if locality not in city_map:
            city_map[locality] = {
                "geonameid": 980000 + len(city_map) + 1,
                "name": locality,
                "name_ascii": locality,
                "longitude": 0.0,
                "latitude": 0.0,
            }
        latitude = site.get("lat", 0.0)
        longitude = site.get("lon", 0.0)
        sites.append({
            "id": site["id"],
            "code": site["id"],
            "name": site["name"],
            "city_name": locality,
            "city_geonameid": city_map[locality]["geonameid"],
            "longitude": longitude,
            "latitude": latitude,
            "altitude": 0.0,
            "centroid": False,
            "occupation": "not_documented",
            "start_date1": start,
            "start_date2": start if start != UNDETERMINED_LEFT else UNDETERMINED_RIGHT,
            "end_date1": end if end != UNDETERMINED_RIGHT else UNDETERMINED_LEFT,
            "end_date2": end,
            "description": "",
            "knowledge_type": "not_documented",
            "charac_id": charac_paths[charac_path],
            "exceptional": False,
            "bibliography": DATASET_BIBLIOGRAPHY,
            "comment": site.get("comment", ""),
            "web_images": "",
        })

    finite_starts = [site["start_date1"] for site in sites if site["start_date1"] != UNDETERMINED_LEFT]
    finite_ends = [site["end_date2"] for site in sites if site["end_date2"] != UNDETERMINED_RIGHT]

    return {
        "bbox": (0.0, 0.0, 0.0, 0.0),
        "cities": list(city_map.values()),
        "sites": sites,
        "start_date": min(finite_starts) if finite_starts else -2578050,
        "end_date": max(finite_ends) if finite_ends else 1950,
    }


def build_sql(dataset_id, license_id, owner_user_id, collection_id, dataset):
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

    site_ids = ", ".join(sql_literal(site["id"]) for site in dataset["sites"])
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
        f"{sql_literal('2026-03-29T00:00:00+00:00')}, true, now(), now(), {ROOT_CHRONOLOGY_ID}, '', {collection_id}) "
        "ON CONFLICT (id) DO UPDATE SET "
        "name = EXCLUDED.name, scale_resolution = EXCLUDED.scale_resolution, geographical_extent = EXCLUDED.geographical_extent, "
        "type = EXCLUDED.type, owner = EXCLUDED.owner, editor = EXCLUDED.editor, editor_url = EXCLUDED.editor_url, contributor = EXCLUDED.contributor, "
        "default_language = EXCLUDED.default_language, state = EXCLUDED.state, license_id = EXCLUDED.license_id, published = EXCLUDED.published, "
        "soft_deleted = EXCLUDED.soft_deleted, geographical_extent_geom = EXCLUDED.geographical_extent_geom, start_date = EXCLUDED.start_date, "
        "end_date = EXCLUDED.end_date, declared_creation_date = EXCLUDED.declared_creation_date, public = EXCLUDED.public, updated_at = now(), "
        "root_chronology_id = EXCLUDED.root_chronology_id, illustrations = EXCLUDED.illustrations, database_collection_id = EXCLUDED.database_collection_id;",
        "INSERT INTO public.database_tr "
        "(database_id, lang_isocode, description, geographical_limit, bibliography, context_description, source_description, source_relation, copyright, subject, re_use) "
        f"VALUES ({dataset_id}, 'en', {sql_literal(DATASET_DESCRIPTION)}, {sql_literal(DATASET_GEOGRAPHICAL_LIMIT)}, {sql_literal(DATASET_BIBLIOGRAPHY)}, "
        f"{sql_literal(DATASET_CONTEXT_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_DESCRIPTION)}, {sql_literal(DATASET_SOURCE_RELATION)}, "
        f"{sql_literal(DATASET_COPYRIGHT)}, {sql_literal(DATASET_SUBJECT)}, {sql_literal(DATASET_REUSE)}) "
        "ON CONFLICT (database_id, lang_isocode) DO UPDATE SET "
        "description = EXCLUDED.description, geographical_limit = EXCLUDED.geographical_limit, bibliography = EXCLUDED.bibliography, "
        "context_description = EXCLUDED.context_description, source_description = EXCLUDED.source_description, source_relation = EXCLUDED.source_relation, "
        "copyright = EXCLUDED.copyright, subject = EXCLUDED.subject, re_use = EXCLUDED.re_use;",
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

    range_id = int(query_value(DB, "SELECT COALESCE(MAX(id),0)+1 FROM public.site_range;"))
    src_id = int(query_value(DB, "SELECT COALESCE(MAX(id),0)+1 FROM public.site_range__charac;"))

    for site in dataset["sites"]:
        statements.extend([
            "INSERT INTO public.site "
            "(id, code, name, city_name, city_geonameid, geom, geom_3d, centroid, occupation, database_id, created_at, updated_at, altitude, start_date1, start_date2, end_date1, end_date2) "
            f"VALUES ({sql_literal(site['id'])}, {sql_literal(site['code'])}, {sql_literal(site['name'])}, {sql_literal(site['city_name'])}, "
            f"{site['city_geonameid']}, ST_GeogFromText('POINT({site['longitude']} {site['latitude']})'), "
            f"ST_Force3DZ(ST_GeomFromText('POINT({site['longitude']} {site['latitude']})', 4326), {site['altitude']})::geography, "
            f"{'true' if site['centroid'] else 'false'}, '{site['occupation']}', {dataset_id}, now(), now(), {site['altitude']}, "
            f"{site['start_date1']}, {site['start_date2']}, {site['end_date1']}, {site['end_date2']}) "
            "ON CONFLICT (id) DO UPDATE SET "
            "code = EXCLUDED.code, name = EXCLUDED.name, city_name = EXCLUDED.city_name, city_geonameid = EXCLUDED.city_geonameid, "
            "geom = EXCLUDED.geom, geom_3d = EXCLUDED.geom_3d, centroid = EXCLUDED.centroid, occupation = EXCLUDED.occupation, database_id = EXCLUDED.database_id, "
            "updated_at = now(), altitude = EXCLUDED.altitude, start_date1 = EXCLUDED.start_date1, start_date2 = EXCLUDED.start_date2, "
            "end_date1 = EXCLUDED.end_date1, end_date2 = EXCLUDED.end_date2;",
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


for DB in DATABASES:
    owner_user_id = get_owner_id(DB)
    collection_id = get_collection_id(DB)
    dataset_id = get_dataset_id(DB)
    license_id = get_license_id(DB)
    charac_paths = load_charac_paths(DB)
    dataset = build_dataset(charac_paths)
    sql = build_sql(dataset_id, license_id, owner_user_id, collection_id, dataset)
    sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", DB], stdin=sql)
    print(f"[meratus-prehistoric-occupation:{DB}] dataset_id={dataset_id} collection_id={collection_id} owner={owner_user_id}")
