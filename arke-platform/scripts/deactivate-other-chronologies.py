#!/usr/bin/env python3

"""
Deactivate all chronologies except the Indonesia Prehistory and Protohistory chronology.
This keeps only our custom geological epochs chronology visible in the UI.
"""

import subprocess
import sys


CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")
# The Indonesia chronology root ID from our CSV
INDONESIA_ROOT_ID = 970000


def sh(cmd, *, stdin=None):
    result = subprocess.run(cmd, input=stdin, text=True, check=False, capture_output=True)
    if result.returncode != 0:
        print(f"Command failed: {' '.join(cmd)}", file=sys.stderr)
        print(f"stderr: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result


def build_sql():
    statements = [
        "BEGIN;",
        # Deactivate all chronologies first
        "UPDATE public.chronology_root SET active = false;",
        # Activate only the Indonesia chronology
        f"UPDATE public.chronology_root SET active = true WHERE root_chronology_id = {INDONESIA_ROOT_ID};",
        "COMMIT;",
    ]
    return "\n".join(statements) + "\n"


def verify(db):
    query = (
        "SELECT root_chronology_id, active, editor, deposit_uri "
        "FROM public.chronology_root "
        "ORDER BY root_chronology_id;"
    )
    result = sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", query])
    print(f"[chronology-status:{db}]")
    print(result.stdout, end="")


def main():
    sql = build_sql()
    for db in DATABASES:
        sh(
            ["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db],
            stdin=sql,
        )
        verify(db)
    print("\n✓ Only Indonesia Prehistory and Protohistory chronology is now active.")


if __name__ == "__main__":
    main()
