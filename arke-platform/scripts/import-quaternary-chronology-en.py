#!/usr/bin/env python3

import csv
import io
import subprocess


CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
CSV_URL = "https://api.nakala.fr/data/10.34847/nkl.84b1sxn3/576bcb244a04f06131dbb068589a2cceff43c7be"
ROOT_NAME = "The Quaternary Period Around the World, International Commission on Stratigraphy (2020)"
ROOT_DESCRIPTION = (
    "Imported from the English Nakala chronology dataset "
    "10.34847/nkl.84b1sxn3 using file 576bcb244a04f06131dbb068589a2cceff43c7be."
)
ROOT_CREDITS = "Philippe JULLIEN; International Commission on Stratigraphy; Nakala DOI 10.34847/nkl.84b1sxn3"
ROOT_DEPOSIT_URI = "https://nakala.fr/10.34847/nkl.84b1sxn3"
ROOT_EDITOR = "International Commission on Stratigraphy"
ROOT_EDITOR_URI = "https://stratigraphy.org/"
ROOT_GEOGRAPHICAL_COVERING = "World"
WORLD_POLYGON = "POLYGON((-179.9 -85,-179.9 85,179.9 85,179.9 -85,-179.9 -85))"
COLORS = {
    0: "4fbcf3",
    1: "32ad5d",
    2: "8cb042",
    3: "f39306",
    4: "f84c48",
}


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def fetch_rows():
    text = sh(["curl", "-Ls", CSV_URL]).stdout
    return list(csv.DictReader(io.StringIO(text), delimiter=";"))


def next_root_id(db):
    result = sh(
        ["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", "SELECT COALESCE(MAX(id), 0) + 1 FROM chronology;"]
    )
    return int(result.stdout.strip())


def build_nodes(rows, root_id):
    nodes = {
        root_id: {
            "id": root_id,
            "parent_id": 0,
            "start_date": min(int(row["START_LVL1"]) for row in rows),
            "end_date": max(int(row["STOP_LVL1"]) for row in rows),
            "color": COLORS[0],
            "id_ark_periodo": "",
            "id_ark_pactols": "",
            "name": ROOT_NAME,
            "description": ROOT_DESCRIPTION,
        }
    }
    path_to_id = {}

    for row in rows:
        levels = [
            ("PERIOD_NAME_LVL1", "START_LVL1", "STOP_LVL1"),
            ("PERIOD_NAME_LVL2", "START_LVL2", "STOP_LVL2"),
            ("PERIOD_NAME_LVL3", "START_LVL3", "STOP_LVL3"),
            ("PERIOD_NAME_LVL4", "START_LVL4", "STOP_LVL4"),
        ]
        parent_id = root_id
        current_path = []
        for depth, (name_key, start_key, stop_key) in enumerate(levels, start=1):
            name = (row.get(name_key) or "").strip()
            if not name:
                continue

            current_path.append(name)
            path = tuple(current_path)
            if path in path_to_id:
                parent_id = path_to_id[path]
                continue

            if depth == len(current_path) and depth == sum(1 for key, _, _ in levels if (row.get(key) or "").strip()):
                node_id = int(row["IdArkeogis"])
                ark_periodo = (row.get("IdArkPeriodo") or "").strip()
                ark_pactols = (row.get("IdArkPactols") or "").strip()
                description = (row.get("Description") or "").strip()
            else:
                node_id = int(row["IdArkeogis"]) if depth == 1 else None
                ark_periodo = ""
                ark_pactols = ""
                description = ""

            if node_id is None:
                raise RuntimeError(f"Missing chronology ID for path: {' > '.join(path)}")

            nodes[node_id] = {
                "id": node_id,
                "parent_id": parent_id,
                "start_date": int(row[start_key]),
                "end_date": int(row[stop_key]),
                "color": COLORS.get(depth, COLORS[max(COLORS)]),
                "id_ark_periodo": ark_periodo,
                "id_ark_pactols": ark_pactols,
                "name": name,
                "description": description,
            }
            path_to_id[path] = node_id
            parent_id = node_id

    return nodes


def build_sql(root_id, nodes):
    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
    ]

    for node_id in sorted(nodes):
        node = nodes[node_id]
        statements.append(
            "INSERT INTO public.chronology "
            "(id, parent_id, start_date, end_date, color, created_at, updated_at, id_ark_periodo, id_ark_pactols) "
            f"VALUES ({node['id']}, {node['parent_id']}, {node['start_date']}, {node['end_date']}, "
            f"{sql_literal(node['color'])}, now(), now(), {sql_literal(node['id_ark_periodo'])}, {sql_literal(node['id_ark_pactols'])}) "
            "ON CONFLICT (id) DO UPDATE SET "
            "parent_id = EXCLUDED.parent_id, "
            "start_date = EXCLUDED.start_date, "
            "end_date = EXCLUDED.end_date, "
            "color = EXCLUDED.color, "
            "id_ark_periodo = EXCLUDED.id_ark_periodo, "
            "id_ark_pactols = EXCLUDED.id_ark_pactols, "
            "updated_at = now();"
        )
        statements.append(
            "INSERT INTO public.chronology_tr (lang_isocode, chronology_id, name, description) "
            f"VALUES ('en', {node['id']}, {sql_literal(node['name'])}, {sql_literal(node['description'])}) "
            "ON CONFLICT (chronology_id, lang_isocode) DO UPDATE SET "
            "name = EXCLUDED.name, description = EXCLUDED.description;"
        )

    statements.extend(
        [
            "INSERT INTO public.chronology_root "
            "(root_chronology_id, admin_group_id, author_user_id, credits, active, geom, cached_langs, opened, editor, editor_uri, deposit_uri, chronology_collection_id) "
            f"VALUES ({root_id}, 0, 0, {sql_literal(ROOT_CREDITS)}, true, ST_GeogFromText({sql_literal(WORLD_POLYGON)}), 'en', false, "
            f"{sql_literal(ROOT_EDITOR)}, {sql_literal(ROOT_EDITOR_URI)}, {sql_literal(ROOT_DEPOSIT_URI)}, 0) "
            "ON CONFLICT (root_chronology_id) DO UPDATE SET "
            "credits = EXCLUDED.credits, "
            "active = EXCLUDED.active, "
            "geom = EXCLUDED.geom, "
            "cached_langs = EXCLUDED.cached_langs, "
            "opened = EXCLUDED.opened, "
            "editor = EXCLUDED.editor, "
            "editor_uri = EXCLUDED.editor_uri, "
            "deposit_uri = EXCLUDED.deposit_uri, "
            "chronology_collection_id = EXCLUDED.chronology_collection_id;",
            "INSERT INTO public.chronology_root_tr (lang_isocode, root_chronology_id, geographical_covering) "
            f"VALUES ('en', {root_id}, {sql_literal(ROOT_GEOGRAPHICAL_COVERING)}) "
            "ON CONFLICT (lang_isocode, root_chronology_id) DO UPDATE SET "
            "geographical_covering = EXCLUDED.geographical_covering;",
            "SELECT setval('chronology_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.chronology), 1), true);",
            "SET session_replication_role = origin;",
            "COMMIT;",
        ]
    )

    return "\n".join(statements) + "\n"


def verify(db, root_id):
    query = (
        "WITH RECURSIVE tree AS ("
        f"SELECT id, parent_id, start_date, end_date FROM chronology WHERE id = {root_id} "
        "UNION ALL "
        "SELECT c.id, c.parent_id, c.start_date, c.end_date FROM chronology c JOIN tree t ON c.parent_id = t.id"
        ") "
        "SELECT t.id, t.parent_id, t.start_date, t.end_date, ct.lang_isocode, ct.name "
        "FROM tree t "
        "LEFT JOIN chronology_tr ct ON ct.chronology_id = t.id AND ct.lang_isocode = 'en' "
        "ORDER BY t.id;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", query])
    print(f"[quaternary-en:{db}]")
    print(result.stdout, end="")


def main():
    rows = fetch_rows()
    for db in DATABASES:
        root_id = next_root_id(db)
        nodes = build_nodes(rows, root_id)
        sql = build_sql(root_id, nodes)
        sh(
            ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
            stdin=sql,
        )
        verify(db, root_id)


if __name__ == "__main__":
    main()
