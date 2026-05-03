#!/usr/bin/env python3

import csv
from pathlib import Path

# Translation mappings (English to Indonesian)
TRANSLATIONS = {
    "Analyses": "Analisis",
    "Preparation": "Persiapan",
    "Thin section": "Irisan tipis",
    "Site sample": "Sampel situs",
    "Reference sample": "Sampel referensi",
    "Polished section": "Irisan poles",
    "Sampling": "Pengambilan sampel",
    "Laser ablation": "Ablasi laser",
    "Remote sensing": "Penginderaan jauh",
    "LIDAR": "LIDAR",
    "Observation": "Observasi",
    "SEM": "SEM",
    "Confocal microscope": "Mikroskop konfokal",
    "Optical microscope": "Mikroskop optik",
    "Dating": "Penanggalan",
    "Radiocarbon": "Radiokarbon",
    "Dendrochronology": "Dendrokronologi",
    "Oxygen isotope": "Isotop oksigen",
    "OSL": "OSL",
    "Paleomagnetism": "Paleomagnetisme",
    "Thermoluminescence": "Termoluminesensi",
    "Anthropology": "Antropologi",
    "Botany": "Botani",
    "Macroremains": "Makroremains",
    "Anthracology": "Antrakologi",
    "Carpology": "Karpologi",
    "Microremains": "Mikroremains",
    "Palynology": "Palinologi",
    "Phytoliths": "Fitolit",
    "Geology": "Geologi",
    "Granulometry": "Granulometri",
    "Pedology": "Pedologi",
    "Petrology": "Petrologi",
    "Phosphates": "Fosfat",
    "Sedimentology": "Sedimentologi",
    "Zoology": "Zoologi",
    "Entomology": "Entomologi",
    "Fauna": "Fauna",
    "Malacofauna": "Malakofauna",
    "Physical geography": "Geografi fisik",
    "Hydrology": "Hidrologi",
    "Physico-chemical": "Fisiko-kimia",
    "Cathodoluminescence": "Katodoluminesensi",
    "Diffraction X": "Difraksi X",
    "Fluorescence X": "Fluoresensi X",
    "ICP-MS": "ICP-MS",
    "MEB-EDS": "MEB-EDS",
    "MEB-WDS": "MEB-WDS",
    "Percolation of clays": "Perkolasi lempung",
    "Infrared spectroscopy": "Spektroskopi inframerah",
    "RAMAN spectroscopy": "Spektroskopi RAMAN",
    "Thermodifferential": "Termodiferensial",
    "Thermogravimetric": "Termogravimetrik",
}

INPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/analyses-thesaurus-en.csv")
OUTPUT_FILE = Path("/Users/cataivancov/IdeaProjects/arke-platform/data/analyses-thesaurus-id.csv")

def translate_term(term):
    """Translate a single term, return original if not found."""
    term = term.strip()
    return TRANSLATIONS.get(term, term)

def translate_row(row):
    """Translate all text fields in a row."""
    translated = {}
    for key, value in row.items():
        if key in ["MAIN_CHARAC", "CHARAC_LVL1", "CHARAC_LVL2", "CHARAC_LVL3", "CHARAC_LVL4"]:
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
