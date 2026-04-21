#!/usr/bin/env python3

import csv
import re
from pathlib import Path

CSV_PATH = Path(__file__).parent / "data" / "drive-sites-to-arkeogis.csv"

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

def custom_csv_reader(filepath):
    """Custom CSV reader that handles broken field alignment."""
    rows = []
    
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
    
    # Skip header
    header_line = lines[0]
    
    # For data rows, we need to be smart about parsing
    # because some fields contain semicolons
    current_row = []
    for line in lines[1:]:
        line = line.rstrip('\n\r')
        
        # Try standard split first
        fields = line.split(';')
        
        # If we got roughly the right number of fields (within reason), use it
        if 20 <= len(fields) <= 25:
            rows.append(fields)
        else:
            # Line might be a continuation or broken somehow
            # Append to previous row's last field
            if rows:
                rows[-1][-1] += '\n' + line
    
    return rows

def process_csv():
    """Fix CSV by properly reconstructing rows and quoting fields with semicolons."""
    
    print("Reading CSV with custom parser...")
    raw_rows = custom_csv_reader(CSV_PATH)
    
    print(f"Found {len(raw_rows)} raw rows")
    
    # Clean rows and map to headers
    cleaned_rows = []
    for row_idx, raw_row in enumerate(raw_rows):
        # Trim spaces from all fields
        raw_row = [f.strip() if isinstance(f, str) else f for f in raw_row]
        
        # Try to align with expected headers
        clean_row = {}
        
        # Try to match fields - take first 22 or as many as we have
        for i, header in enumerate(EXPECTED_HEADERS):
            if i < len(raw_row):
                clean_row[header] = raw_row[i]
            else:
                clean_row[header] = ""
        
        cleaned_rows.append(clean_row)
    
    print(f"Cleaned {len(cleaned_rows)} rows")
    
    # Write with smart quoting
    if cleaned_rows:
        # Write with QUOTE_MINIMAL and escape semicolons in fields that have them
        with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
            # Manual write to have full control
            f.write(";".join(EXPECTED_HEADERS) + "\n")
            
            for row in cleaned_rows:
                values = []
                for header in EXPECTED_HEADERS:
                    val = row.get(header, "")
                    
                    # Quote field if it contains semicolon or newline
                    if ";" in val or "\n" in val or '"' in val:
                        # Escape quotes by doubling them
                        val = val.replace('"', '""')
                        val = f'"{val}"'
                    
                    values.append(val)
                
                f.write(";".join(values) + "\n")
        
        print(f"✓ Wrote {len(cleaned_rows)} rows")
        print(f"✓ Applied selective quoting for fields with special characters")
        return len(cleaned_rows)
    
    return 0

if __name__ == "__main__":
    count = process_csv()
    print(f"\n✓ CSV fixed: {CSV_PATH}")
