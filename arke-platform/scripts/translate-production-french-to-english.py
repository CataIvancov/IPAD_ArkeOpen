#!/usr/bin/env python3

import csv
from pathlib import Path

# Translation mappings (French to English)
TRANSLATIONS = {
    "Production": "Production",
    "Agricole": "Agricultural",
    "Grenier": "Granary",
    "Silo": "Silo",
    "Céramique": "Ceramic",
    "Extraction": "Extraction",
    "Atelier": "Workshop",
    "Local": "Local",
    "Régional": "Regional",
    "Suprarégional": "Supraregional",
    "Atelier de TCA": "TCA Workshop",
    "Métal": "Metal",
    "Fer": "Iron",
    "Argent": "Silver",
    "Bronze": "Bronze",
    "Or": "Gold",
    "Cuivre": "Copper",
    "Plomb": "Lead",
    "Autres": "Others",
    "Réduction": "Reduction",
    "Forge": "Forge",
    "Fonderie": "Foundry",
    "Atelier monétaire": "Mint workshop",
    "Pierre": "Stone",
    "Meule": "Millstone",
    "Architecture": "Architecture",
    "Outils": "Tools",
    "Chaux": "Lime",
    "Charbon": "Coal",
    "Sel": "Salt",
    "Verre": "Glass",
    "Non renseigné": "Not specified",
}

INPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/production-thesaurus-fr.csv")
OUTPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/production-thesaurus-en.csv")

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
    
    print(f"Translated {len(translated_rows)} rows from French to English to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
