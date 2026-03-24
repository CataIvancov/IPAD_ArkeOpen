#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import pathlib
import re
import sys
from collections import deque
from typing import Iterable

try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
except ImportError as exc:  # pragma: no cover - runtime dependency check
    print(
        "Missing Google Drive dependencies. Install them with:\n"
        "python3 -m pip install -r requirements-google-drive.txt",
        file=sys.stderr,
    )
    raise SystemExit(1) from exc


SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DEFAULT_CREDENTIALS = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-oauth-client.json")
DEFAULT_TOKEN = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-token.json")
DEFAULT_OUTPUT = pathlib.Path("/Users/cataivancov/IdeaProjects/arke-platform/data/google-drive-inventory.csv")
FOLDER_MIME = "application/vnd.google-apps.folder"
PDF_MIME = "application/pdf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inventory a Google Drive folder recursively into CSV.")
    parser.add_argument("folder", help="Google Drive folder id or share URL")
    parser.add_argument("--credentials", type=pathlib.Path, default=DEFAULT_CREDENTIALS)
    parser.add_argument("--token", type=pathlib.Path, default=DEFAULT_TOKEN)
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def extract_folder_id(value: str) -> str:
    text = value.strip()
    match = re.search(r"/folders/([^/?#]+)", text)
    if match:
        return match.group(1)
    if re.fullmatch(r"[-_A-Za-z0-9]{10,}", text):
        return text
    raise ValueError(f"Could not extract a folder id from: {value}")


def load_credentials(credentials_path: pathlib.Path, token_path: pathlib.Path) -> Credentials:
    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if creds and creds.valid:
        return creds

    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        token_path.write_text(creds.to_json(), encoding="utf-8")
        return creds

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"OAuth client file not found: {credentials_path}\n"
            "Create a Google Cloud OAuth Desktop client and save the JSON there."
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), SCOPES)
    creds = flow.run_local_server(port=0)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def iter_children(service, folder_id: str) -> Iterable[dict]:
    query = f"'{folder_id}' in parents and trashed = false"
    page_token = None
    while True:
        response = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, mimeType, size, modifiedTime, webViewLink, resourceKey, shortcutDetails)",
                orderBy="folder,name_natural",
                pageToken=page_token,
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                pageSize=1000,
            )
            .execute()
        )
        for item in response.get("files", []):
            yield item
        page_token = response.get("nextPageToken")
        if not page_token:
            break


def build_inventory(service, root_folder_id: str) -> list[dict[str, str]]:
    rows = []
    queue = deque([(root_folder_id, "")])

    while queue:
        folder_id, folder_path = queue.popleft()
        for item in iter_children(service, folder_id):
            mime_type = item.get("mimeType", "")
            name = item.get("name", "")
            current_path = f"{folder_path}/{name}" if folder_path else name
            target_id = item.get("shortcutDetails", {}).get("targetId", item["id"])
            target_mime = item.get("shortcutDetails", {}).get("targetMimeType", mime_type)
            is_folder = target_mime == FOLDER_MIME
            is_pdf = target_mime == PDF_MIME or name.lower().endswith(".pdf")

            rows.append(
                {
                    "path": current_path,
                    "name": name,
                    "id": item["id"],
                    "target_id": target_id,
                    "mime_type": target_mime,
                    "resource_key": item.get("resourceKey", ""),
                    "is_folder": "yes" if is_folder else "no",
                    "is_pdf": "yes" if is_pdf else "no",
                    "size": item.get("size", ""),
                    "modified_time": item.get("modifiedTime", ""),
                    "web_view_link": item.get("webViewLink", ""),
                }
            )

            if is_folder:
                queue.append((target_id, current_path))

    return rows


def write_csv(rows: list[dict[str, str]], output_path: pathlib.Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "path",
        "name",
        "id",
        "target_id",
        "mime_type",
        "resource_key",
        "is_folder",
        "is_pdf",
        "size",
        "modified_time",
        "web_view_link",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    folder_id = extract_folder_id(args.folder)
    creds = load_credentials(args.credentials, args.token)
    service = build("drive", "v3", credentials=creds)
    rows = build_inventory(service, folder_id)
    write_csv(rows, args.output)

    pdf_count = sum(1 for row in rows if row["is_pdf"] == "yes")
    folder_count = sum(1 for row in rows if row["is_folder"] == "yes")
    print(f"Wrote {len(rows)} rows to {args.output}")
    print(f"Folders: {folder_count}")
    print(f"PDFs: {pdf_count}")


if __name__ == "__main__":
    main()
