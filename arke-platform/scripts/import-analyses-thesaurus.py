#!/usr/bin/env python3

from __future__ import annotations

import csv
import io
import subprocess
from collections import defaultdict
from pathlib import Path

CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")

ROOT_NAME_FR = "Analyses"
ROOT_NAME_EN = "Analysis"
CACHED_LANGS = "fr,en"
ADMIN_GROUP_ID = 20
ROOT_ID = 253

DATA_FR = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/analyses-thesaurus-fr.csv")


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def query(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def get_owner_id(db):
    value = query(db, "SELECT id FROM \"user\" WHERE username='IPAD_admin' ORDER BY id LIMIT 1;").strip()
    if not value:
        raise RuntimeError("IPAD_admin user not found in database: " + db)
    return int(value)


def read_csv(path: Path):
    text = path.read_text(encoding="utf-8")
    return list(csv.DictReader(io.StringIO(text), delimiter=";"))


def load_nodes(rows):
    nodes = {}
    for index, row in enumerate(rows, start=1):
        path = [
            (row.get("CARAC_NAME") or row.get("MAIN_CHARAC") or "").strip(),
            (row.get("CARAC_LVL1") or row.get("CHARAC_LVL1") or "").strip(),
            (row.get("CARAC_LVL2") or row.get("CHARAC_LVL2") or "").strip(),
            (row.get("CARAC_LVL3") or row.get("CHARAC_LVL3") or "").strip(),
            (row.get("CARAC_LVL4") or row.get("CHARAC_LVL4") or "").strip(),
        ]
        path = [p for p in path if p]

        if not path:
            continue

        for depth in range(1, len(path) + 1):
            key = tuple(path[:depth])
            node = nodes.setdefault(key, {"names": {}, "order": index, "id": None, "ark_id": "", "pactols_id": "", "aat_id": ""})
            if index < node["order"]:
                node["order"] = index
            node["names"]["fr"] = path[depth - 1]
            node["names"]["en"] = path[depth - 1]  # Default EN to FR for now

        # Set ID and external IDs if present at the deepest level
        raw_id = (row.get("IDArkeoGIS") or "").strip()
        full_key = tuple(path)
        if full_key in nodes:
            if raw_id:
                nodes[full_key]["id"] = int(raw_id)
            nodes[full_key]["ark_id"] = (row.get("IdArk") or "").strip()
            nodes[full_key]["pactols_id"] = (row.get("IdPactols") or "").strip()
            nodes[full_key]["aat_id"] = (row.get("IdAat") or "").strip()

    return nodes


def build_sql(db, owner_user_id, nodes):
    root_key = (ROOT_NAME_FR,)
    statements = ["BEGIN;", "SET session_replication_role = replica;"]

    # Check if root exists
    root_exists = query(
        db,
        f"SELECT id FROM charac WHERE id = {ROOT_ID};"
    ).strip()

    if root_exists:
        # Delete existing subtree (except root)
        statements.extend([
            "WITH RECURSIVE tree AS ("
            f"SELECT id FROM charac WHERE id = {ROOT_ID} "
            "UNION ALL "
            "SELECT c.id FROM charac c JOIN tree t ON c.parent_id = t.id"
            ") "
            f"DELETE FROM charac_tr WHERE charac_id IN (SELECT id FROM tree WHERE id <> {ROOT_ID});",
            "WITH RECURSIVE tree AS ("
            f"SELECT id FROM charac WHERE id = {ROOT_ID} "
            "UNION ALL "
            "SELECT c.id FROM charac c JOIN tree t ON c.parent_id = t.id"
            ") "
            f"DELETE FROM charac WHERE id IN (SELECT id FROM tree WHERE id <> {ROOT_ID});",
        ])
    else:
        # Create root
        statements.append(
            f"INSERT INTO charac (id, parent_id, \"order\", author_user_id, ark_id, pactols_id, aat_id) "
            f"VALUES ({ROOT_ID}, 0, 0, {owner_user_id}, '', '', '');"
        )

    # Update root translations
    statements.append(
        "INSERT INTO charac_tr (charac_id, lang_isocode, name, description) "
        f"VALUES ({ROOT_ID}, 'fr', {sql_literal(ROOT_NAME_FR)}, '') "
        "ON CONFLICT (charac_id, lang_isocode) DO UPDATE SET name = EXCLUDED.name;"
    )
    statements.append(
        "INSERT INTO charac_tr (charac_id, lang_isocode, name, description) "
        f"VALUES ({ROOT_ID}, 'en', {sql_literal(ROOT_NAME_EN)}, '') "
        "ON CONFLICT (charac_id, lang_isocode) DO UPDATE SET name = EXCLUDED.name;"
    )

    statements.append(
        "INSERT INTO charac_root (root_charac_id, admin_group_id, cached_langs) "
        f"VALUES ({ROOT_ID}, {ADMIN_GROUP_ID}, {sql_literal(CACHED_LANGS)}) "
        "ON CONFLICT (root_charac_id) DO UPDATE SET admin_group_id = EXCLUDED.admin_group_id, cached_langs = EXCLUDED.cached_langs;"
    )

    nodes = dict(nodes)
    nodes[root_key] = {"names": {"fr": ROOT_NAME_FR, "en": ROOT_NAME_EN}, "order": 0, "id": ROOT_ID, "ark_id": "", "pactols_id": "", "aat_id": ""}
    id_map = {root_key: ROOT_ID}

    # Get next ID
    next_id = int(query(db, "SELECT COALESCE(MAX(id),0)+1 FROM charac;"))
    if next_id <= ROOT_ID:
        next_id = ROOT_ID + 1

    def sort_key(item):
        path, meta = item
        return (len(path), meta["order"], path)

    # Assign IDs to nodes that don't have them
    for path, meta in sorted(nodes.items(), key=sort_key):
        if path == root_key:
            continue
        if path in id_map:
            continue
        if meta["id"] is not None:
            id_map[path] = meta["id"]
        else:
            id_map[path] = next_id
            next_id += 1

    order_by_parent = defaultdict(int)
    for path, meta in sorted(nodes.items(), key=sort_key):
        if path == root_key:
            continue
        parent_path = path[:-1]
        parent_id = id_map[parent_path]
        order_by_parent[parent_id] += 1
        node_id = id_map[path]
        order_value = order_by_parent[parent_id]

        statements.append(
            "INSERT INTO charac (id, parent_id, \"order\", author_user_id, ark_id, pactols_id, aat_id) "
            f"VALUES ({node_id}, {parent_id}, {order_value}, {owner_user_id}, "
            f"{sql_literal(meta['ark_id'])}, {sql_literal(meta['pactols_id'])}, {sql_literal(meta['aat_id'])}) "
            "ON CONFLICT (id) DO UPDATE SET parent_id = EXCLUDED.parent_id, \"order\" = EXCLUDED.\"order\", "
            "ark_id = EXCLUDED.ark_id, pactols_id = EXCLUDED.pactols_id, aat_id = EXCLUDED.aat_id;"
        )
        for lang in ("fr", "en"):
            name = meta["names"].get(lang, meta["names"].get("fr", ""))
            statements.append(
                "INSERT INTO charac_tr (charac_id, lang_isocode, name, description) "
                f"VALUES ({node_id}, '{lang}', {sql_literal(name)}, '') "
                "ON CONFLICT (charac_id, lang_isocode) DO UPDATE SET name = EXCLUDED.name;"
            )

    statements.extend([
        "SELECT setval('charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM charac), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ])

    return "\n".join(statements) + "\n"


def main():
    rows = read_csv(DATA_FR)
    nodes = load_nodes(rows)

    for db in DATABASES:
        owner_user_id = get_owner_id(db)
        sql = build_sql(db, owner_user_id, nodes)
        sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], stdin=sql)
        print(f"[analyses-thesaurus:{db}] imported root='{ROOT_NAME_FR}' (id={ROOT_ID})")


if __name__ == "__main__":
    main()
