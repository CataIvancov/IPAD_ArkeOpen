#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import pathlib
import re
import sys

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DEFAULT_TOKEN = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-token.json")
DEFAULT_OUTPUT_DIR = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-pdfs")
DEFAULT_INVENTORY = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-inventory.csv")
PDF_MIME = "application/pdf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download all PDFs listed in a Google Drive inventory CSV.")
    parser.add_argument("--inventory", type=pathlib.Path, default=DEFAULT_INVENTORY)
    parser.add_argument("--token", type=pathlib.Path, default=DEFAULT_TOKEN)
    parser.add_argument("--output-dir", type=pathlib.Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit", type=int, default=0, help="Optional limit for testing; 0 means no limit")
    return parser.parse_args()


def load_credentials(token_path: pathlib.Path) -> Credentials:
    if not token_path.exists():
        raise FileNotFoundError(f"Google Drive OAuth token not found: {token_path}")
    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)
    if creds.valid:
        return creds
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json(), encoding="utf-8")
        return creds
    raise RuntimeError("Stored Google Drive token is invalid and cannot be refreshed.")


def safe_rel_path(path_value: str) -> pathlib.Path:
    path_value = path_value.strip().replace("\\", "/")
    path_value = re.sub(r"^\./+", "", path_value)
    parts = [part for part in path_value.split("/") if part and part not in {".", ".."}]
    return pathlib.Path(*parts)


def iter_pdf_rows(inventory_path: pathlib.Path):
    with inventory_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("is_pdf") == "yes" and row.get("mime_type") == PDF_MIME:
                yield row


def download_file(service, file_id: str, destination: pathlib.Path, resource_key: str = "") -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    kwargs = {"fileId": file_id, "supportsAllDrives": True}
    if resource_key:
        kwargs["resourceKey"] = resource_key
    request = service.files().get_media(**kwargs)
    temp_destination = destination.with_name(f"{destination.name}.part")
    try:
        with temp_destination.open("wb") as handle:
            downloader = MediaIoBaseDownload(handle, request, chunksize=1024 * 1024 * 8)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        temp_destination.replace(destination)
    except Exception:
        if temp_destination.exists():
            temp_destination.unlink()
        raise


def main() -> None:
    args = parse_args()
    creds = load_credentials(args.token)
    service = build("drive", "v3", credentials=creds)

    rows = list(iter_pdf_rows(args.inventory))
    if args.limit > 0:
        rows = rows[: args.limit]

    downloaded = 0
    skipped = 0
    failures = []

    for index, row in enumerate(rows, start=1):
        rel_path = safe_rel_path(row["path"])
        destination = args.output_dir / rel_path
        expected_size = int(row.get("size") or 0)

        if destination.exists() and expected_size and destination.stat().st_size == expected_size:
            skipped += 1
            print(f"[{index}/{len(rows)}] skip {rel_path}", flush=True)
            continue

        try:
            print(f"[{index}/{len(rows)}] download {rel_path}", flush=True)
            download_file(service, row["target_id"], destination, row.get("resource_key", ""))
            downloaded += 1
        except Exception as exc:  # pragma: no cover - network/runtime path
            failures.append((row["path"], str(exc)))
            print(f"[{index}/{len(rows)}] failed {rel_path}: {exc}", file=sys.stderr, flush=True)

    print(f"Downloaded: {downloaded}", flush=True)
    print(f"Skipped: {skipped}", flush=True)
    print(f"Failures: {len(failures)}", flush=True)
    if failures:
        fail_path = args.output_dir.parent / "google-drive-download-failures.csv"
        with fail_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["path", "error"])
            writer.writerows(failures)
        print(f"Wrote failure log to {fail_path}", flush=True)


if __name__ == "__main__":
    main()
