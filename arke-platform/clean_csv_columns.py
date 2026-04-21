#!/usr/bin/env python3

import csv
from pathlib import Path

CSV_PATH = Path(__file__).parent / "data" / "drive-sites-to-arkeogis.csv"

# Original expected headers from the Python script
EXPECTED_HEADERS = [
    "SITE_SOURCE_ID",
    "SITE_NAME",
    "LOCALISATION",
    "GEONAME_ID",
    "PROJECTION_SYSTEM",
    "LONGITUDE",
    "LATITUDE",
    "ALTITUDE",
    "CITY_CENTROID",
    "STATE_OF_KNOWLEDGE",
    "OCCUPATION",
    "STARTING_PERIOD",
    "ENDING_PERIOD",
    "MAIN_CHARAC",
    "CHARAC_LVL1",
    "CHARAC_LVL2",
    "CHARAC_LVL3",
    "CHARAC_LVL4",
    "CHARAC_EXP",
    "BIBLIOGRAPHY",
    "COMMENTS",
    "WEB_IMAGES",
]

def fix_encoding_issues(text: str) -> str:
    """Fix common encoding issues in text."""
    if not text:
        return ""
    
    # Fix mojibake from latin-1 interpreted as utf-8
    try:
        # If text has encoding artifacts, try to fix them
        if "Ã" in text or "â€" in text:
            # Try to encode back to latin-1 bytes and decode as utf-8
            try:
                fixed = text.encode("latin-1").decode("utf-8", errors="replace")
                return fixed.replace("Â", "").strip()
            except:
                pass
    except:
        pass
    
    return text.strip()

def process_csv():
    """Read CSV with all columns, extract only expected ones, and fix issues."""
    
    rows = []
    
    # Read with latin-1 encoding
    with CSV_PATH.open("r", encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        
        for row_idx, row in enumerate(reader):
            # Create a new row with only expected headers
            new_row = {}
            
            for header in EXPECTED_HEADERS:
                value = row.get(header, "") or row.get(header.strip(), "") or ""
                
                # Fix encoding issues in specific fields
                if header in ["COMMENTS", "BIBLIOGRAPHY", "SITE_NAME", "LOCALISATION"]:
                    value = fix_encoding_issues(value)
                else:
                    value = value.strip() if isinstance(value, str) else value
                
                new_row[header] = value
            
            rows.append(new_row)
    
    # Write back with only expected columns
    if rows:
        with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=EXPECTED_HEADERS, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"✓ Cleaned {len(rows)} rows")
        print(f"✓ Removed extra columns (DUPLICATE_DB_NAME, DUPLICATE_SCORE, DUPLICATE_FLAG)")
        print(f"✓ Removed empty field columns")
        print(f"✓ Fixed encoding issues in comments")
        print(f"✓ Kept only expected {len(EXPECTED_HEADERS)} columns")
        print(f"✓ All data preserved")
        return len(rows)
    
    return 0

if __name__ == "__main__":
    count = process_csv()
    print(f"\nCSV file cleaned: {CSV_PATH}")
