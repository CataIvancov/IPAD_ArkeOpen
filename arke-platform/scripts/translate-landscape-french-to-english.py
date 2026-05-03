#!/usr/bin/env python3

import csv
from pathlib import Path

# Translation mappings (French to English)
TRANSLATIONS = {
    "Paysage": "Landscape",
    "Diagnostic négatif": "Negative diagnosis",
    "Formation superficielle": "Surface formation",
    "Paléochenal daté": "Dated palaeochannel",
    "Non renseigné": "Not specified",
    "Extrasite": "Extrasite",
    "Intrasite": "Intrasite",
    "Paléosol": "Paleosol",
    "Petrologie": "Petrology",
    "Structure agraire": "Agrarian structure",
    "Indéterminé": "Undetermined",
    "Crêtes de labours_Ackerberg": "Plough ridges",
    "Champ bombé": "Raised field",
    "Enclos": "Enclosure",
    "Muret": "Low wall",
    "Murger_Steinrudel": "Stone mound",
    "Autres": "Others",
    "Calcaire": "Limestone",
    "Gneiss": "Gneiss",
    "Granite": "Granite",
    "Grès": "Sandstone",
    "Parcellaire fossile": "Fossil parcel",
    "Rideau de culture": "Cultivation curtain",
    "Terrasse agricole": "Agricultural terrace",
}

INPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/landscape-thesaurus-fr.csv")
OUTPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/landscape-thesaurus-en.csv")

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
