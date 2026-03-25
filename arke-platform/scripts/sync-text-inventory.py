#!/usr/bin/env python3
"""
Synchronize text inventory with processed OCR JSON files.

Scans data/google-drive-text/ for all JSON files and ensures they are
recorded in google-drive-text-inventory.csv with proper metadata.

Usage:
    python3 scripts/sync-text-inventory.py
"""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
TEXT_ROOT = ROOT / "data" / "google-drive-text"
INVENTORY_FILE = ROOT / "data" / "google-drive-text-inventory.csv"
INVENTORY_BACKUP = ROOT / "data" / "google-drive-text-inventory.backup.csv"


def load_existing_inventory() -> Dict[str, dict]:
    """Load existing inventory into memory keyed by filename."""
    inventory = {}
    if INVENTORY_FILE.exists():
        with open(INVENTORY_FILE, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("filename"):
                    inventory[row["filename"]] = row
    return inventory


def get_json_metadata(json_path: Path) -> Tuple[str, int, int, float, str]:
    """Extract metadata from OCR JSON file.

    Returns: (filename, pages, text_chars, ocr_confidence, source_pdf)
    """
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
        return (
            json_path.stem,
            data.get("pages", 0),
            data.get("text_chars", 0),
            data.get("ocr_confidence", 0.0),
            data.get("source_pdf", ""),
        )
    except Exception as e:
        print(f"  ERROR reading {json_path.name}: {e}")
        return (json_path.stem, 0, 0, 0.0, "")


def sync_inventory() -> int:
    """Synchronize inventory with JSON files.

    Returns number of new/updated entries.
    """
    print(f"Loading existing inventory from {INVENTORY_FILE.name}...")
    existing = load_existing_inventory()
    print(f"  Found {len(existing)} existing entries")

    print(f"\nScanning {TEXT_ROOT}...")
    json_files = sorted(TEXT_ROOT.glob("*.json"))
    print(f"  Found {len(json_files)} JSON files")

    # Build new inventory
    new_inventory = []
    updated_count = 0

    for json_path in json_files:
        filename, pages, text_chars, confidence, source_pdf = get_json_metadata(json_path)

        # Check if this is new or updated
        if filename not in existing:
            updated_count += 1
            status = "NEW"
        else:
            existing_entry = existing[filename]
            # Check if text_chars differs (indicates reprocessing)
            if str(text_chars) != str(existing_entry.get("text_chars", 0)):
                updated_count += 1
                status = "UPDATED"
            else:
                status = "unchanged"

        row = {
            "filename": filename,
            "pdf_path": source_pdf,
            "pages": str(pages),
            "text_chars": str(text_chars),
            "ocr_confidence": f"{confidence:.2f}",
            "date_processed": datetime.now().isoformat(),
        }

        if status != "unchanged":
            print(f"  {status}: {filename} ({text_chars} chars, {pages} pages)")

        new_inventory.append(row)

    # Backup original
    if INVENTORY_FILE.exists():
        INVENTORY_FILE.rename(INVENTORY_BACKUP)
        print(f"\nBacked up original to {INVENTORY_BACKUP.name}")

    # Write new inventory
    print(f"\nWriting synchronized inventory ({len(new_inventory)} entries)...")
    with open(INVENTORY_FILE, "w", encoding="utf-8", newline="") as f:
        fieldnames = ["filename", "pdf_path", "pages", "text_chars", "ocr_confidence", "date_processed"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(new_inventory)

    print(f"  Saved to {INVENTORY_FILE.name}")

    # Statistics
    zero_char_count = sum(1 for row in new_inventory if row["text_chars"] == "0")
    print(f"\nInventory Statistics:")
    print(f"  Total entries: {len(new_inventory)}")
    print(f"  New/Updated: {updated_count}")
    print(f"  Failed (0 chars): {zero_char_count}")
    print(f"  Successful: {len(new_inventory) - zero_char_count}")

    return updated_count


if __name__ == "__main__":
    updated = sync_inventory()
    if updated > 0:
        print(f"\n✓ Inventory synced successfully ({updated} changes)")
    else:
        print("\n✓ Inventory already up-to-date")
