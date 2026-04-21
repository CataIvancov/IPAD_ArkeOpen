#!/usr/bin/env python3

import csv
import re
from pathlib import Path

CSV_PATH = Path(__file__).parent / "data" / "drive-sites-to-arkeogis.csv"

def clean_mojibake(text: str) -> str:
    """Remove mojibake and corrupted characters."""
    if not text:
        return ""
    
    # Remove malformed character sequences
    # Ã\x91 appears to be a corrupted dash or special character
    text = re.sub(r'Ã\x91', '—', text)
    text = re.sub(r'Ã', '', text)
    text = re.sub(r'â€', '', text)
    text = re.sub(r'[^\x20-\x7E\n\r\t.,;:!?()&\'-]', '', text)  # Keep printable ASCII
    
    # Clean up excessive whitespace/newlines
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def process_csv():
    """Clean mojibake from CSV."""
    
    rows = []
    
    # Read with UTF-8
    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        
        for row in reader:
            # Clean mojibake from text fields
            for field in ["COMMENTS", "BIBLIOGRAPHY", "SITE_NAME", "LOCALISATION"]:
                if field in row:
                    row[field] = clean_mojibake(row[field])
            
            rows.append(row)
    
    # Write back
    if rows:
        fieldnames = rows[0].keys()
        with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"✓ Cleaned {len(rows)} rows")
        print(f"✓ Removed mojibake characters from comments")
        print(f"✓ Cleaned excessive whitespace")
        return len(rows)
    
    return 0

if __name__ == "__main__":
    count = process_csv()
    print(f"\nCSV cleaned: {CSV_PATH}")
