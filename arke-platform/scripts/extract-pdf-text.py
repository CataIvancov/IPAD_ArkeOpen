#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from pypdf import PdfReader


ROOT = Path(__file__).resolve().parents[1]
PDF_ROOT = ROOT / "data" / "google-drive-pdfs"
TEXT_ROOT = ROOT / "data" / "google-drive-text"
INVENTORY_PATH = ROOT / "data" / "google-drive-text-inventory.csv"


WHITESPACE_RE = re.compile(r"\s+")


@dataclass
class ExtractionResult:
    relative_pdf_path: str
    relative_text_path: str
    status: str
    pages: int
    extracted_pages: int
    text_chars: int
    sha256: str
    error: str


def normalize_text(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value).strip()


def sha256_for_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_pdf_text(pdf_path: Path) -> tuple[list[str], int]:
    reader = PdfReader(str(pdf_path))
    page_texts: list[str] = []
    extracted_pages = 0
    for page in reader.pages:
        text = page.extract_text() or ""
        normalized = normalize_text(text)
        if normalized:
            extracted_pages += 1
        page_texts.append(normalized)
    return page_texts, extracted_pages


def write_text_payload(text_path: Path, pdf_path: Path, page_texts: list[str], sha256: str) -> int:
    text_path.parent.mkdir(parents=True, exist_ok=True)
    joined = "\n\n".join(page_texts).strip()
    payload = {
        "source_pdf": str(pdf_path.relative_to(ROOT)),
        "sha256": sha256,
        "pages": len(page_texts),
        "text_chars": len(joined),
        "page_texts": page_texts,
    }
    text_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(joined)


def existing_inventory() -> dict[str, dict[str, str]]:
    if not INVENTORY_PATH.exists():
        return {}
    with INVENTORY_PATH.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return {row["relative_pdf_path"]: row for row in reader}


def iter_pdf_paths(limit: int | None) -> list[Path]:
    pdf_paths = sorted(PDF_ROOT.rglob("*.pdf"))
    if limit is not None:
        return pdf_paths[:limit]
    return pdf_paths


def target_text_path(pdf_path: Path) -> Path:
    rel = pdf_path.relative_to(PDF_ROOT)
    return TEXT_ROOT / rel.with_suffix(".json")


def extract_one(pdf_path: Path, force: bool, inventory_rows: dict[str, dict[str, str]]) -> ExtractionResult:
    rel_pdf = str(pdf_path.relative_to(ROOT))
    text_path = target_text_path(pdf_path)
    rel_text = str(text_path.relative_to(ROOT))

    current_sha = sha256_for_file(pdf_path)
    existing = inventory_rows.get(rel_pdf)
    if not force and existing and existing.get("status") == "ok" and existing.get("sha256") == current_sha and text_path.exists():
        return ExtractionResult(
            relative_pdf_path=rel_pdf,
            relative_text_path=rel_text,
            status="cached",
            pages=int(existing.get("pages", "0") or 0),
            extracted_pages=int(existing.get("extracted_pages", "0") or 0),
            text_chars=int(existing.get("text_chars", "0") or 0),
            sha256=current_sha,
            error="",
        )

    try:
        page_texts, extracted_pages = extract_pdf_text(pdf_path)
        text_chars = write_text_payload(text_path, pdf_path, page_texts, current_sha)
        return ExtractionResult(
            relative_pdf_path=rel_pdf,
            relative_text_path=rel_text,
            status="ok",
            pages=len(page_texts),
            extracted_pages=extracted_pages,
            text_chars=text_chars,
            sha256=current_sha,
            error="",
        )
    except Exception as exc:
        return ExtractionResult(
            relative_pdf_path=rel_pdf,
            relative_text_path=rel_text,
            status="error",
            pages=0,
            extracted_pages=0,
            text_chars=0,
            sha256=current_sha,
            error=str(exc),
        )


def write_inventory(results: list[ExtractionResult]) -> None:
    INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with INVENTORY_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "relative_pdf_path",
                "relative_text_path",
                "status",
                "pages",
                "extracted_pages",
                "text_chars",
                "sha256",
                "error",
            ],
        )
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "relative_pdf_path": result.relative_pdf_path,
                    "relative_text_path": result.relative_text_path,
                    "status": result.status,
                    "pages": result.pages,
                    "extracted_pages": result.extracted_pages,
                    "text_chars": result.text_chars,
                    "sha256": result.sha256,
                    "error": result.error,
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract text from downloaded Google Drive PDFs.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N PDFs.")
    parser.add_argument("--force", action="store_true", help="Re-extract even if cached.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not PDF_ROOT.exists():
        print(f"PDF root not found: {PDF_ROOT}", file=sys.stderr)
        return 1

    inventory_rows = existing_inventory()
    pdf_paths = iter_pdf_paths(args.limit)
    if not pdf_paths:
        print("No PDFs found.")
        return 0

    results: list[ExtractionResult] = []
    ok_count = 0
    cached_count = 0
    error_count = 0

    for pdf_path in pdf_paths:
        result = extract_one(pdf_path, args.force, inventory_rows)
        results.append(result)
        if result.status == "ok":
            ok_count += 1
        elif result.status == "cached":
            cached_count += 1
        else:
            error_count += 1
        print(f"{result.status:6} {result.relative_pdf_path}")

    write_inventory(results)
    print(
        f"Processed {len(results)} PDFs: ok={ok_count}, cached={cached_count}, error={error_count}. "
        f"Inventory: {INVENTORY_PATH}"
    )
    return 0 if error_count == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
