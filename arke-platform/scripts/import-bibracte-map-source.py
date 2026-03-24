#!/usr/bin/env python3

import hashlib
import json
import re
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path


CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
ZIP_URL = "https://api.nakala.fr/data/10.34847/nkl.5ae3og8y/91cc5f75fb0dd8f57f13b6b13e20cbd0035ebfbf"
DOI_URL = "https://nakala.fr/10.34847/nkl.5ae3og8y"
IDENTIFIER = "10.34847/nkl.5ae3og8y"
FILENAME = "Bibracte_Interventions_2010_2018_poly.zip"
LAYER_FILENAME = "Interventions_Fouilles_2010_18 multi.shp"
TITLE_FR = "Carte des interventions archeologiques 2010-2018 a Bibracte"
ATTRIBUTION_FR = "Sebastien Durost; Arnaud Meunier; Matthieu Thivet"
CITATION_FR = "Durost, Meunier, Thivet. Carte des interventions archeologiques 2010-2018 a Bibracte. DOI 10.34847/nkl.5ae3og8y."
DESCRIPTION_FR = "Carte au format shapefile des interventions archeologiques effectuees a Bibracte entre 2010 et 2018, convertie en GeoJSON EPSG:4326 pour ArkeOpen."
GEOGRAPHICAL_COVERING_FR = "Bibracte"
LICENSE_NAME = "Etalab-2.0"
LICENSE_URL = "https://spdx.org/licenses/etalab-2.0.html#licenseText"
COLLECTION_ID = 0
COLLECTION_NAME = "ArkeOpen Cartographies"
CREATOR_USER_ID = 61
START_DATE = 2010
END_DATE = 2018
DECLARED_CREATION_DATE = "2022-01-01T00:00:00+00:00"
EDITOR = "Nakala"
EDITOR_URI = "https://nakala.fr/"
TYPE = "geojson"
WORLD_BBOX_WKT = None


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def query_value(db, sql):
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql])
    return result.stdout.strip()


def prepare_geojson():
    workdir = Path(tempfile.mkdtemp(prefix="bibracte-map-source-"))
    try:
        zip_path = workdir / "bibracte.zip"
        shp_dir = workdir / "unz"
        shp_dir.mkdir()
        sh(["curl", "-Ls", "-o", str(zip_path), ZIP_URL])
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(shp_dir)

        md5sum = hashlib.md5(zip_path.read_bytes()).hexdigest()

        container_dir = "/tmp/bibracte_map_source"
        sh(["docker", "exec", CONTAINER, "rm", "-rf", container_dir])
        sh(["docker", "cp", f"{shp_dir}/.", f"{CONTAINER}:{container_dir}"])
        sh(
            [
                "docker",
                "exec",
                CONTAINER,
                "ogr2ogr",
                "-f",
                "GeoJSON",
                "-t_srs",
                "EPSG:4326",
                f"{container_dir}/out.geojson",
                f"{container_dir}/{LAYER_FILENAME}",
            ]
        )
        geojson = sh(["docker", "exec", CONTAINER, "cat", f"{container_dir}/out.geojson"]).stdout
        info = sh(["docker", "exec", CONTAINER, "ogrinfo", "-al", "-so", f"{container_dir}/out.geojson"]).stdout
        sh(["docker", "exec", CONTAINER, "rm", "-rf", container_dir])

        minx = miny = maxx = maxy = None
        for line in info.splitlines():
            if line.startswith("Extent:"):
                values = [float(value) for value in re.findall(r"-?\d+\.\d+", line)]
                if len(values) != 4:
                    raise RuntimeError(f"Unexpected extent format: {line}")
                minx, miny, maxx, maxy = values
                break
        if None in (minx, miny, maxx, maxy):
            raise RuntimeError("Failed to extract extent from ogrinfo output")

        bbox_wkt = (
            f"POLYGON(({minx} {miny},{minx} {maxy},{maxx} {maxy},"
            f"{maxx} {miny},{minx} {miny}))"
        )
        # Normalize the GeoJSON to compact UTF-8 JSON before storing.
        geojson = json.dumps(json.loads(geojson), ensure_ascii=True, separators=(",", ":"))
        return geojson, md5sum, bbox_wkt
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def ensure_support_rows(db):
    next_license_id = int(query_value(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM public.license;"))
    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
        "INSERT INTO public.map_source_collection (id, name) "
        f"VALUES ({COLLECTION_ID}, {sql_literal(COLLECTION_NAME)}) "
        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;",
        "SELECT setval('map_source_collection_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.map_source_collection), 1), true);",
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
    map_source_id = query_value(
        db,
        f"SELECT id FROM public.map_source WHERE deposit_uri = {sql_literal(DOI_URL)} LIMIT 1;",
    )
    if map_source_id:
        source_id = int(map_source_id)
    else:
        source_id = int(query_value(db, "SELECT COALESCE(MAX(id), 0) + 1 FROM public.map_source;"))

    license_id = int(
        query_value(
            db,
            f"SELECT id FROM public.license WHERE name = {sql_literal(LICENSE_NAME)} OR url = {sql_literal(LICENSE_URL)} ORDER BY id LIMIT 1;",
        )
    )
    return source_id, license_id


def apply_map_source(db, source_id, license_id, geojson, bbox_wkt):
    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
        "INSERT INTO public.map_source "
        "(id, creator_user_id, filename, md5sum, geojson, start_date, end_date, geographical_extent_geom, "
        "published, license, license_id, declared_creation_date, created_at, updated_at, opened, editor, editor_uri, deposit_uri, map_source_collection_id, identifier, url, type) "
        f"VALUES ({source_id}, {CREATOR_USER_ID}, {sql_literal(FILENAME)}, {sql_literal(md5sum)}, {sql_literal(geojson)}, "
        f"{START_DATE}, {END_DATE}, ST_GeogFromText({sql_literal(bbox_wkt)}), true, {sql_literal(LICENSE_NAME)}, {license_id}, "
        f"{sql_literal(DECLARED_CREATION_DATE)}, now(), now(), true, {sql_literal(EDITOR)}, {sql_literal(EDITOR_URI)}, "
        f"{sql_literal(DOI_URL)}, {COLLECTION_ID}, {sql_literal(IDENTIFIER)}, {sql_literal(ZIP_URL)}, {sql_literal(TYPE)}) "
        "ON CONFLICT (id) DO UPDATE SET "
        "creator_user_id = EXCLUDED.creator_user_id, "
        "filename = EXCLUDED.filename, "
        "md5sum = EXCLUDED.md5sum, "
        "geojson = EXCLUDED.geojson, "
        "start_date = EXCLUDED.start_date, "
        "end_date = EXCLUDED.end_date, "
        "geographical_extent_geom = EXCLUDED.geographical_extent_geom, "
        "published = EXCLUDED.published, "
        "license = EXCLUDED.license, "
        "license_id = EXCLUDED.license_id, "
        "declared_creation_date = EXCLUDED.declared_creation_date, "
        "updated_at = now(), "
        "opened = EXCLUDED.opened, "
        "editor = EXCLUDED.editor, "
        "editor_uri = EXCLUDED.editor_uri, "
        "deposit_uri = EXCLUDED.deposit_uri, "
        "map_source_collection_id = EXCLUDED.map_source_collection_id, "
        "identifier = EXCLUDED.identifier, "
        "url = EXCLUDED.url, "
        "type = EXCLUDED.type;",
        "INSERT INTO public.map_source_tr "
        "(map_source_id, lang_isocode, name, attribution, citation, description, geographical_covering) "
        f"VALUES ({source_id}, 'fr', {sql_literal(TITLE_FR)}, {sql_literal(ATTRIBUTION_FR)}, {sql_literal(CITATION_FR)}, {sql_literal(DESCRIPTION_FR)}, {sql_literal(GEOGRAPHICAL_COVERING_FR)}) "
        "ON CONFLICT (map_source_id, lang_isocode) DO UPDATE SET "
        "name = EXCLUDED.name, attribution = EXCLUDED.attribution, citation = EXCLUDED.citation, "
        "description = EXCLUDED.description, geographical_covering = EXCLUDED.geographical_covering;",
        "INSERT INTO public.map_source__authors (map_source_id, user_id) "
        f"VALUES ({source_id}, {CREATOR_USER_ID}) "
        "ON CONFLICT (map_source_id, user_id) DO NOTHING;",
        "SELECT setval('shapefile_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.map_source), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ]
    sh(
        ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
        stdin="\n".join(statements) + "\n",
    )


def verify(db, source_id):
    query = (
        "SELECT ms.id, ms.type, ms.start_date, ms.end_date, ms.deposit_uri, mst.lang_isocode, mst.name "
        "FROM public.map_source ms "
        "LEFT JOIN public.map_source_tr mst ON mst.map_source_id = ms.id "
        f"WHERE ms.id = {source_id} ORDER BY mst.lang_isocode;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", query])
    print(f"[bibracte-map-source:{db}]")
    print(result.stdout, end="")


def main():
    global md5sum
    geojson, md5sum, bbox_wkt = prepare_geojson()
    for db in DATABASES:
        ensure_support_rows(db)
        source_id, license_id = get_ids(db)
        apply_map_source(db, source_id, license_id, geojson, bbox_wkt)
        verify(db, source_id)


if __name__ == "__main__":
    main()
