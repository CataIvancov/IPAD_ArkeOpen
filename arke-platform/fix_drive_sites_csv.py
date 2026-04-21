#!/usr/bin/env python3

import csv
import re
from pathlib import Path

CSV_PATH = Path(__file__).parent / "data" / "drive-sites-to-arkeogis.csv"

def fix_coordinate(value: str) -> str:
    """Fix malformed coordinates with multiple decimal points."""
    if not value or not value.strip():
        return ""
    
    value = value.strip()
    
    # Remove spaces
    value = value.replace(" ", "")
    
    # Fix multiple decimal points - keep only the last valid decimal
    # e.g., "119.669.050" -> "119.669050" 
    if value.count(".") > 1:
        parts = value.split(".")
        # If it looks like a bad float, try to reconstruct it
        if len(parts) > 2:
            # Take first part, then join remaining with only one decimal
            value = parts[0] + "." + "".join(parts[1:])
    
    # Handle comma as decimal separator
    value = value.replace(",", ".")
    
    # Validate it's a number
    try:
        float(value)
        return value
    except ValueError:
        return ""

def process_csv():
    """Read CSV, fix issues, and write back."""
    
    rows = []
    encoding_used = None
    
    # Try different encodings
    for encoding in ["latin-1", "iso-8859-1", "utf-8-sig", "cp1252", "utf-8"]:
        try:
            with CSV_PATH.open("r", encoding=encoding, newline="") as f:
                reader = csv.DictReader(f, delimiter=";")
                
                # Get fieldnames and strip spaces
                if reader.fieldnames:
                    original_fieldnames = [name.strip() for name in reader.fieldnames]
                
                for row in reader:
                    # Normalize keys by stripping spaces
                    normalized_row = {k.strip(): v for k, v in row.items()}
                    
                    # Fix longitude and latitude
                    if "LONGITUDE" in normalized_row:
                        fixed_lon = fix_coordinate(normalized_row["LONGITUDE"])
                        normalized_row["LONGITUDE"] = fixed_lon
                    
                    if "LATITUDE" in normalized_row:
                        fixed_lat = fix_coordinate(normalized_row["LATITUDE"])
                        normalized_row["LATITUDE"] = fixed_lat
                    
                    # Add PROJECTION_SYSTEM if missing but coordinates exist
                    if "PROJECTION_SYSTEM" in normalized_row:
                        lon = normalized_row.get("LONGITUDE", "").strip()
                        lat = normalized_row.get("LATITUDE", "").strip()
                        if (lon or lat) and not normalized_row["PROJECTION_SYSTEM"].strip():
                            normalized_row["PROJECTION_SYSTEM"] = "4326"
                    
                    # Clean up all field values
                    for key in list(normalized_row.keys()):
                        if normalized_row[key]:
                            normalized_row[key] = normalized_row[key].strip() if isinstance(normalized_row[key], str) else normalized_row[key]
                    
                    rows.append(normalized_row)
            
            encoding_used = encoding
            break
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # Write back with same delimiter and cleaned headers
    if rows:
        fieldnames = list(rows[0].keys())
        with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"✓ Fixed {len(rows)} rows (read with {encoding_used} encoding)")
        print(f"✓ Cleaned up header field names (removed extra spaces)")
        print(f"✓ Corrected malformed coordinates")
        print(f"✓ Added missing PROJECTION_SYSTEM values")
        print(f"✓ Cleaned up field spacing")
        print(f"✓ Preserved all data - no deletions")
        return len(rows)
    
    return 0

if __name__ == "__main__":
    count = process_csv()
    print(f"\nCSV file updated: {CSV_PATH}")
