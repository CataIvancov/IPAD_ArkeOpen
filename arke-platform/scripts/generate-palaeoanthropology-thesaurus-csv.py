#!/usr/bin/env python3
"""Generate Palaeoanthropology thesaurus CSV files."""

import csv
from pathlib import Path

HEADER = [
    "IDArkeoGIS",
    "CARAC_NAME",
    "CARAC_LVL1",
    "CARAC_LVL2",
    "CARAC_LVL3",
    "CARAC_LVL4",
    "IdArk",
    "IdPactols",
    "IdAat",
]

ROOT = "Human remains"

SPECIES = [
    "Homo erectus",
    "Homo sapiens",
    "Homo floresiensis",
]

SKELETON_TYPES = [
    "Complete skeleton",
    "Partial skeleton",
]

CRANIAL_PARTS = [
    "Skull",
    "Cranium",
    "Calvarium",
    "Mandible",
    "Maxilla",
    "Zygomatic",
    "Frontal",
    "Parietal",
    "Temporal",
    "Occipital",
]

TEETH_TYPES = [
    "Incisor",
    "Canine",
    "Premolar",
    "Molar",
]

AXIAL_PARTS = [
    "Vertebrae",
    "Ribs",
    "Sternum",
]

UPPER_LIMB_PARTS = [
    "Clavicle",
    "Scapula",
    "Humerus",
    "Radius",
    "Ulna",
    "Hand bones",
]

LOWER_LIMB_PARTS = [
    "Pelvis",
    "Femur",
    "Patella",
    "Tibia",
    "Fibula",
    "Foot bones",
]


def make_row(lvl1: str, lvl2: str = "", lvl3: str = "", lvl4: str = ""):
    return ["", ROOT, lvl1, lvl2, lvl3, lvl4, "", "", ""]


def build_rows():
    rows = []

    for species in SPECIES:
        for skeleton_type in SKELETON_TYPES:
            rows.append(make_row(species, skeleton_type))

        for cranial_part in CRANIAL_PARTS:
            rows.append(make_row(species, "Cranial", cranial_part))
        for tooth_type in TEETH_TYPES:
            rows.append(make_row(species, "Cranial", "Teeth", tooth_type))

        for axial_part in AXIAL_PARTS:
            rows.append(make_row(species, "Postcranial", "Axial", axial_part))

        for upper_limb_part in UPPER_LIMB_PARTS:
            rows.append(make_row(species, "Postcranial", "Upper limb", upper_limb_part))

        for lower_limb_part in LOWER_LIMB_PARTS:
            rows.append(make_row(species, "Postcranial", "Lower limb", lower_limb_part))

    rows.append(make_row("Unknown", "Complete skeleton"))
    rows.append(make_row("Unknown", "Partial skeleton"))
    rows.append(make_row("Unknown", "Indeterminate"))

    return rows


def write_csv(path: Path, rows):
    with open(path, "w", newline="", encoding="utf-8") as file_obj:
        writer = csv.writer(file_obj, delimiter=";")
        writer.writerow(HEADER)
        writer.writerows(rows)


def main():
    data_dir = Path("/Users/cataivancov/IdeaProjects/arke-platform/data")
    rows = build_rows()

    for lang in ("en", "id"):
        output_path = data_dir / f"palaeoanthropology-thesaurus-{lang}.csv"
        write_csv(output_path, rows)
        print(f"Created {output_path} with {len(rows)} rows")


if __name__ == "__main__":
    main()
