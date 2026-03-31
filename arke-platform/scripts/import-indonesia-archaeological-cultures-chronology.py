#!/usr/bin/env python3

from __future__ import annotations

import csv
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "data" / "indonesia-archaeological-cultures-chronology-en.csv"
CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
ROOT_EDITOR = "Local draft for Indonesia chronology personalization"
ROOT_EDITOR_URI = "https://drive.google.com/drive/folders/1Gvb4TRuzuiu8s1jMd-5NY4dPmGdxXdEI"
ROOT_DEPOSIT_URI = "https://drive.google.com/drive/folders/1Gvb4TRuzuiu8s1jMd-5NY4dPmGdxXdEI"
ROOT_CREDITS = (
    "Regional Indonesian techno-cultural complexes are defined by lithic traditions, "
    "subsistence patterns, and regional interaction spheres, and often cut across conventional "
    "period boundaries. Peter S. Bellwood. First islanders: prehistory and human migration in "
    "Island Southeast Asia. 2017. Hoboken (NJ): Wiley Blackwell."
)
ROOT_GEOGRAPHICAL_COVERING = "Indonesia"
INDONESIA_POLYGON = "POLYGON((94.5 -11.5,94.5 6.5,141.5 6.5,141.5 -11.5,94.5 -11.5))"


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def load_rows():
    with CSV_PATH.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def build_sql(rows):
    root_id = int(rows[0]["id"])
    statements = [
        "BEGIN;",
        "SET session_replication_role = replica;",
    ]

    for row in rows:
        node_id = int(row["id"])
        parent_id = int(row["parent_id"])
        statements.append(
            "INSERT INTO public.chronology "
            "(id, parent_id, start_date, end_date, color, created_at, updated_at, id_ark_periodo, id_ark_pactols) "
            f"VALUES ({node_id}, {parent_id}, {row['start_date']}, {row['end_date']}, {sql_literal(row['color'])}, now(), now(), '', '') "
            "ON CONFLICT (id) DO UPDATE SET "
            "parent_id = EXCLUDED.parent_id, "
            "start_date = EXCLUDED.start_date, "
            "end_date = EXCLUDED.end_date, "
            "color = EXCLUDED.color, "
            "updated_at = now();"
        )
        statements.append(
            "INSERT INTO public.chronology_tr (lang_isocode, chronology_id, name, description) "
            f"VALUES ('en', {node_id}, {sql_literal(row['name'])}, {sql_literal(row['description'])}) "
            "ON CONFLICT (chronology_id, lang_isocode) DO UPDATE SET "
            "name = EXCLUDED.name, description = EXCLUDED.description;"
        )

    statements.extend(
        [
            "INSERT INTO public.chronology_root "
            "(root_chronology_id, admin_group_id, author_user_id, credits, active, geom, cached_langs, opened, editor, editor_uri, deposit_uri, chronology_collection_id) "
            f"VALUES ({root_id}, 0, 0, {sql_literal(ROOT_CREDITS)}, true, ST_GeogFromText({sql_literal(INDONESIA_POLYGON)}), 'en,id', false, "
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
        "SELECT t.id, t.parent_id, t.start_date, t.end_date, ct.name "
        "FROM tree t "
        "LEFT JOIN chronology_tr ct ON ct.chronology_id = t.id AND ct.lang_isocode = 'en' "
        "ORDER BY t.id;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", query])
    print(f"[indonesia-cultures-chronology:{db}]")
    print(result.stdout, end="")


def main():
    rows = load_rows()
    root_id = int(rows[0]["id"])
    sql = build_sql(rows)
    for db in DATABASES:
        sh(
            ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
            stdin=sql,
        )
        verify(db, root_id)


if __name__ == "__main__":
    main()
