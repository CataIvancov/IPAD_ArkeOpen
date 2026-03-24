#!/usr/bin/env python3

import csv
import io
import json
import subprocess
import sys
from collections import defaultdict


CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")

DATASETS = {
    "mobilier": {
        "root_id": 1,
        "synthetic_base": 920000,
        "root_names": {
            "fr": "Mobilier",
            "en": "Furniture",
            "de": "Funde",
            "es": "Mobiliario",
        },
        "files": {
            "fr": "https://api.nakala.fr/data/10.34847/nkl.510a974o/f04593de9c895d0f7b17882dbc535dd4f14dd4df",
            "en": "https://api.nakala.fr/data/10.34847/nkl.510a974o/3d39a1f6ef769d408d79c28870b76d9b7e0d3cad",
            "de": "https://api.nakala.fr/data/10.34847/nkl.510a974o/6d61018c75ba643f4ea893e85d1a99241195bd05",
            "es": "https://api.nakala.fr/data/10.34847/nkl.510a974o/5078f9dc49792b633b445d0d9e43af4ed58652b8",
        },
    },
    "paysage": {
        "root_id": 251,
        "synthetic_base": 930000,
        "root_names": {
            "fr": "Paysage",
        },
        "files": {
            "fr": "https://api.nakala.fr/data/10.34847/nkl.58147mf7/6fed12f01a33fb1334519311e71a46b388381991",
        },
    },
    "production": {
        "root_id": 360,
        "synthetic_base": 910000,
        "root_names": {"fr": "Production"},
        "files": {
            "fr": "https://api.nakala.fr/data/10.34847/nkl.3f9brl3c/48ffaf3981f327df3cfeaab1ffe4641fbd4266e0",
        },
    },
    "immobilier": {
        "root_id": 523,
        "synthetic_base": 940000,
        "root_names": {
            "fr": "Immobilier",
            "en": "Realestate",
            "de": "Befunde",
            "es": "Inmobiliario",
        },
        "files": {
            "fr": "https://api.nakala.fr/data/10.34847/nkl.a7654qj6/03452061b346d8b942abcaf11d7fd720bb97cf94",
            "en": "https://api.nakala.fr/data/10.34847/nkl.a7654qj6/cb6a5d285003b3ca35c03c14cfdfa30e2375a7de",
            "de": "https://api.nakala.fr/data/10.34847/nkl.a7654qj6/0715959fddfa699e48439db98fdf588abb625b0c",
            "es": "https://api.nakala.fr/data/10.34847/nkl.a7654qj6/1087bd59a7ecd2627b9e52195e8e98e52deb2bda",
        },
    },
    "analyses": {
        "root_id": 253,
        "synthetic_base": 900000,
        "root_names": {
            "fr": "Analyses",
            "en": "Analysis",
            "de": "Analysen",
            "es": "Análisis",
        },
        "files": {
            "fr": "https://api.nakala.fr/data/10.34847/nkl.390bddzv/72c89b4b79b0acc542089a075a4630a9ea3cb97d",
            "en": "https://api.nakala.fr/data/10.34847/nkl.390bddzv/c1c229ec238aed82791e955f8f059eb746f63514",
            "de": "https://api.nakala.fr/data/10.34847/nkl.390bddzv/469e0cdf96488c4e534db15a51bca26db4623cd9",
            "es": "https://api.nakala.fr/data/10.34847/nkl.390bddzv/152c36a82605e47e045254451fc19ef23929c954",
        },
    },
}


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def fetch(url):
    return sh(["curl", "-Ls", url]).stdout


def ensure_container_running():
    result = sh(["docker", "ps", "--format", "{{.Names}}"])
    names = {line.strip() for line in result.stdout.splitlines()}
    if CONTAINER not in names:
        raise RuntimeError(f"Container {CONTAINER} is not running.")


def parse_csv(text):
    rows = list(csv.DictReader(io.StringIO(text), delimiter=";"))
    normalized = []
    for index, row in enumerate(rows, start=1):
        raw_path = [
            (row.get("CARAC_NAME") or row.get("MAIN_CHARAC") or "").strip(),
            (row.get("CARAC_LVL1") or row.get("CHARAC_LVL1") or "").strip(),
            (row.get("CARAC_LVL2") or row.get("CHARAC_LVL2") or "").strip(),
            (row.get("CARAC_LVL3") or row.get("CHARAC_LVL3") or "").strip(),
            (row.get("CARAC_LVL4") or row.get("CHARAC_LVL4") or "").strip(),
        ]
        path = tuple(part for part in raw_path if part)
        raw_id = (row.get("IDArkeoGIS") or "").strip()
        normalized.append(
            {
                "id": int(raw_id) if raw_id else None,
                "path": path,
                "name": path[-1],
                "ark_id": (row.get("IdArk") or "").strip(),
                "pactols_id": (row.get("IdPactols") or "").strip(),
                "aat_id": (row.get("IdAat") or "").strip(),
                "index": index,
            }
        )
    return normalized


def load_dataset(dataset_key):
    dataset = DATASETS[dataset_key]
    by_lang = {lang: parse_csv(fetch(url)) for lang, url in dataset["files"].items()}

    fr_rows = by_lang["fr"]
    canonical = {}
    for idx, fr_row in enumerate(fr_rows):
        for depth in range(1, len(fr_row["path"]) + 1):
            fr_path = fr_row["path"][:depth]
            entry = canonical.setdefault(
                fr_path,
                {
                    "id": None,
                    "path": fr_path,
                    "index": fr_row["index"],
                    "ark_id": "",
                    "pactols_id": "",
                    "aat_id": "",
                    "names": {},
                },
            )

            if fr_row["index"] < entry["index"]:
                entry["index"] = fr_row["index"]

            if depth == len(fr_row["path"]):
                if entry["id"] is None and fr_row["id"] is not None:
                    entry["id"] = fr_row["id"]
                if not entry["ark_id"] and fr_row["ark_id"]:
                    entry["ark_id"] = fr_row["ark_id"]
                if not entry["pactols_id"] and fr_row["pactols_id"]:
                    entry["pactols_id"] = fr_row["pactols_id"]
                if not entry["aat_id"] and fr_row["aat_id"]:
                    entry["aat_id"] = fr_row["aat_id"]

            for lang, rows in by_lang.items():
                if idx >= len(rows):
                    continue
                lang_row = rows[idx]
                if len(lang_row["path"]) < depth:
                    continue
                entry["names"][lang] = lang_row["path"][depth - 1]

    return dataset, canonical


def build_nodes(dataset, canonical):
    root_id = dataset["root_id"]
    synthetic_id = dataset["synthetic_base"]
    path_to_id = {}
    nodes = {
        root_id: {
            "id": root_id,
            "parent_id": None,
            "order": 0,
            "ark_id": "",
            "pactols_id": "",
            "aat_id": "",
            "names": dataset["root_names"],
        }
    }

    child_orders = defaultdict(int)
    for path, entry in sorted(canonical.items(), key=lambda item: (len(item[0]), item[1]["index"])):
        if len(path) == 1:
            path_to_id[path] = root_id
            continue

        parent_path = path[:-1]
        parent_id = path_to_id.get(parent_path)
        if parent_id is None:
            raise RuntimeError(f"Missing parent path: {' > '.join(path)}")

        node_id = entry["id"]
        if node_id is None:
            while synthetic_id in nodes:
                synthetic_id += 1
            node_id = synthetic_id
            synthetic_id += 1

        child_orders[parent_id] += 1
        nodes[node_id] = {
            "id": node_id,
            "parent_id": parent_id,
            "order": child_orders[parent_id],
            "ark_id": entry["ark_id"],
            "pactols_id": entry["pactols_id"],
            "aat_id": entry["aat_id"],
            "names": entry["names"],
        }
        path_to_id[path] = node_id

    return nodes


def apply_nodes(nodes):
    for db in DATABASES:
        root_id = nodes[min(nodes)]
        root_meta = sh(
            [
                "docker",
                "exec",
                CONTAINER,
                "psql",
                "-U",
                "postgres",
                "-d",
                db,
                "-AtF",
                "|",
                "-c",
                f'SELECT parent_id, "order" FROM public.charac WHERE id = {root_id["id"]};',
            ]
        ).stdout.strip()
        if root_meta:
            parent_id, order = root_meta.split("|")
            nodes[root_id["id"]]["parent_id"] = int(parent_id)
            nodes[root_id["id"]]["order"] = int(order)
        else:
            nodes[root_id["id"]]["parent_id"] = 0
            nodes[root_id["id"]]["order"] = 0

        statements = ["BEGIN;", "SET session_replication_role = replica;"]
        statements.extend(
            [
                "WITH RECURSIVE tree AS ("
                f"SELECT id FROM public.charac WHERE id = {root_id['id']} "
                "UNION ALL "
                "SELECT c.id FROM public.charac c JOIN tree t ON c.parent_id = t.id"
                ") "
                "DELETE FROM public.charac_tr WHERE charac_id IN (SELECT id FROM tree WHERE id <> "
                f"{root_id['id']});",
                "WITH RECURSIVE tree AS ("
                f"SELECT id FROM public.charac WHERE id = {root_id['id']} "
                "UNION ALL "
                "SELECT c.id FROM public.charac c JOIN tree t ON c.parent_id = t.id"
                ") "
                "DELETE FROM public.charac WHERE id IN (SELECT id FROM tree WHERE id <> "
                f"{root_id['id']});",
            ]
        )

        for node_id in sorted(nodes):
            node = nodes[node_id]
            statements.append(
                "INSERT INTO public.charac "
                '(id, parent_id, "order", author_user_id, ark_id, pactols_id, aat_id, created_at, updated_at) '
                f"VALUES ({node['id']}, {node['parent_id']}, {node['order']}, 0, "
                f"{sql_literal(node['ark_id'])}, {sql_literal(node['pactols_id'])}, {sql_literal(node['aat_id'])}, now(), now()) "
                "ON CONFLICT (id) DO UPDATE SET "
                "parent_id = EXCLUDED.parent_id, "
                '"order" = EXCLUDED."order", '
                "ark_id = EXCLUDED.ark_id, "
                "pactols_id = EXCLUDED.pactols_id, "
                "aat_id = EXCLUDED.aat_id, "
                "updated_at = now();"
            )
            for lang, name in node["names"].items():
                statements.append(
                    "INSERT INTO public.charac_tr (lang_isocode, charac_id, name, description) "
                    f"VALUES ({sql_literal(lang)}, {node['id']}, {sql_literal(name)}, '') "
                    "ON CONFLICT (charac_id, lang_isocode) DO UPDATE SET "
                    "name = EXCLUDED.name, description = EXCLUDED.description;"
                )

        statements.extend(
            [
                "SET session_replication_role = origin;",
                "SELECT setval('charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.charac), 1), true);",
                "COMMIT;",
            ]
        )
        sql = "\n".join(statements) + "\n"
        sh(
            ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
            stdin=sql,
        )


def verify(dataset_key, dataset):
    root_id = dataset["root_id"]
    query = (
        "WITH RECURSIVE tree AS ("
        f"SELECT id, parent_id FROM public.charac WHERE id = {root_id} "
        "UNION ALL "
        "SELECT c.id, c.parent_id FROM public.charac c JOIN tree t ON c.parent_id = t.id"
        ") "
        "SELECT c.id, c.parent_id, c.ark_id, c.pactols_id, c.aat_id, ct.lang_isocode, ct.name "
        "FROM tree t "
        "JOIN public.charac c ON c.id = t.id "
        "LEFT JOIN public.charac_tr ct ON ct.charac_id = c.id AND ct.lang_isocode IN ('fr','en','de','es') "
        "ORDER BY c.id, ct.lang_isocode;"
    )
    for db in DATABASES:
        print(f"[{dataset_key}:{db}]")
        result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", query])
        sys.stdout.write(result.stdout)


def main():
    ensure_container_running()
    valid = ", ".join(sorted(DATASETS))
    if len(sys.argv) < 2:
        raise SystemExit(f"Usage: {sys.argv[0]} <{valid}|all>")

    dataset_key = sys.argv[1]
    if dataset_key == "all":
        order = ["mobilier", "paysage", "production", "immobilier", "analyses"]
    elif dataset_key in DATASETS:
        order = [dataset_key]
    else:
        raise SystemExit(f"Usage: {sys.argv[0]} <{valid}|all>")

    for key in order:
        dataset, canonical = load_dataset(key)
        nodes = build_nodes(dataset, canonical)
        apply_nodes(nodes)
        verify(key, dataset)


if __name__ == "__main__":
    main()
