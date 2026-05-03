#!/usr/bin/env python3

import csv
from pathlib import Path

# Translation mappings (English to Indonesian)
TRANSLATIONS = {
    "Production": "Produksi",
    "Agricultural": "Pertanian",
    "Granary": "Lumbung",
    "Silo": "Silo",
    "Ceramic": "Keramik",
    "Extraction": "Ekstraksi",
    "Workshop": "Bengkel",
    "Local": "Lokal",
    "Regional": "Regional",
    "Supraregional": "Supraregional",
    "TCA Workshop": "Bengkel TCA",
    "Metal": "Logam",
    "Iron": "Besi",
    "Silver": "Perak",
    "Bronze": "Perunggu",
    "Gold": "Emas",
    "Copper": "Tembaga",
    "Lead": "Timbal",
    "Others": "Lainnya",
    "Reduction": "Reduksi",
    "Forge": "Tempa",
    "Foundry": "Pengecoran",
    "Mint workshop": "Bengkel uang",
    "Stone": "Batu",
    "Millstone": "Batu giling",
    "Architecture": "Arsitektur",
    "Tools": "Alat",
    "Lime": "Kapur",
    "Coal": "Batubara",
    "Salt": "Garam",
    "Glass": "Kaca",
    "Not specified": "Tidak tercatat",
}

INPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/production-thesaurus-en.csv")
OUTPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/production-thesaurus-id.csv")

def translate_term(term):
    """Translate a single term, return original if not found."""
    term = term.strip()
    return TRANSLATIONS.get(term, term)

def translate_row(row):
    """Translate all text fields in a row."""
    translated = {}
    for key, value in row.items():
        if key in ["CARAC_NAME", "CARAC_LVL1", "CARAC_LVL2", "CARAC_LVL3", "CARAC_LVL4"]:
            translated[key] = translate_term(value) if value else ""
        else:
            translated[key] = value
    return translated

def main():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        fieldnames = reader.fieldnames
        
        translated_rows = []
        for row in reader:
            translated_rows.append(translate_row(row))
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(translated_rows)
    
    print(f"Translated {len(translated_rows)} rows to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
