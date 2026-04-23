#!/usr/bin/env python3
"""Generate portable-objects-thesaurus-en.csv and portable-objects-thesaurus-id.csv
from the Furniture hierarchy extracted from the database, replacing the root
with 'Portable Objects / Artefacts' / 'Artefak / Benda Bergerak'."""

import csv
import re
from pathlib import Path

# --- Same translation mappings as import-portable-objects-thesaurus.py ---

EXACT_TRANSLATIONS = {
    "Furniture": "Artefak / Benda Bergerak",
    "Vegetal - organic materials": "Bahan nabati - organik",
    "Glass": "Kaca",
    "Bone": "Tulang",
    "Others": "Lainnya",
    "Ceramic": "Keramik",
    "Metal": "Logam",
    "Stone": "Batu",
    "Architectural Terra-cotta": "Terakota arsitektural",
    "Animal bones": "Tulang hewan",
    "Human bones": "Tulang manusia",
    "Cockery": "Peralatan masak",
    "Container": "Wadah",
    "Funerary Urn": "Guci jenazah",
    "Miniature pot": "Pot mini",
    "Perfume burner": "Pembakar parfum",
    "Boundary Stone": "Batu batas",
    "Raw material": "Bahan baku",
    "Rock crystal": "Kristal batu",
    "Other material": "Bahan lain",
    "Undocumented": "Tidak terdokumentasi",
    "Unknown": "Tidak diketahui",
    "Equipment": "Peralatan",
    "Equipement": "Peralatan",
    "Watercraft": "Perahu",
    "Rowboat": "Perahu dayung",
    "Wickerwork": "Anyaman",
    "Building component": "Komponen bangunan",
    "Statuary": "Patung",
    "Statuette": "Patung kecil",
}

REPLACEMENTS = [
    (r"\bJewellery\b", "Perhiasan"),
    (r"\bTools\b", "Alat"),
    (r"\bWeaponry\b", "Persenjataan"),
    (r"\bSarcophagus\b", "Sarkofagus"),
    (r"\bStele\b", "Stela"),
    (r"\bInscription\b", "Prasasti"),
    (r"\bDecoration\b", "Dekorasi"),
    (r"\bTile\b", "Ubin"),
    (r"\bBrick\b", "Bata"),
    (r"\bPartition\b", "Sekat"),
    (r"\bHypocaust\b", "Hipokausta"),
    (r"\bStove pots\b", "Panci tungku"),
    (r"\bStove tiles\b", "Ubin tungku"),
    (r"\bLamp\b", "Lampu"),
    (r"\bGames\b", "Permainan"),
    (r"\bTextile\b", "Tekstil"),
    (r"\bTokens\b", "Token"),
    (r"\bWindow\b", "Jendela"),
    (r"\bCoins\b", "Koin"),
    (r"\bCounterfeit currency\b", "Uang palsu"),
    (r"\bPlated coin\b", "Koin berlapis"),
    (r"\bHarness\b", "Perlengkapan kuda"),
    (r"\bBit\b", "Kekang"),
    (r"\bBuckles\b", "Gesper"),
    (r"\bCart\b", "Kereta"),
    (r"\bHorse-boot\b", "Sepatu kuda"),
    (r"\bArmring\b", "Gelang lengan"),
    (r"\bAnklets\b", "Gelang kaki"),
    (r"\bBracelet\b", "Gelang"),
    (r"\bRing\b", "Cincin"),
    (r"\bBelt\b", "Ikat pinggang"),
    (r"\bEarring\b", "Anting"),
    (r"\bPendant\b", "Liontin"),
    (r"\bPin\b", "Peniti"),
    (r"\bIngots\b", "Batangan"),
    (r"\bSlag\b", "Terak"),
    (r"\bAxe\b", "Kapak"),
    (r"\bCold chisel\b", "Pahat dingin"),
    (r"\bHammer\b", "Palu"),
    (r"\bKnife\b", "Pisau"),
    (r"\bNails\b", "Paku"),
    (r"\bSchears\b", "Gunting"),
    (r"\bSickle\b", "Sabit"),
    (r"\bToiletry\b", "Perlengkapan mandi"),
    (r"\bWeights\b", "Beban"),
    (r"\bArrowhead\b", "Mata panah"),
    (r"\bBreastplate\b", "Pelindung dada"),
    (r"\bDagger\b", "Belati"),
    (r"\bHelmet\b", "Helm"),
    (r"\bSchield\b", "Perisai"),
    (r"\bSpear\b", "Tombak"),
    (r"\bSword\b", "Pedang"),
    (r"\bCoating\b", "Lapisan"),
    (r"\bMosaic\b", "Mozaik"),
    (r"\bBead barrel\b", "Manik berbentuk barel"),
    (r"\bBead bitronconical\b", "Manik bitronkonikal"),
    (r"\bBead blank barrel\b", "Bahan manik barel"),
    (r"\bBead blank bitronconical\b", "Bahan manik bitronkonikal"),
    (r"\bBead blank cylindrical\b", "Bahan manik silindris"),
    (r"\bBead blank discoid\b", "Bahan manik cakram"),
    (r"\bBead blank\b", "Bahan manik"),
    (r"\bBead\b", "Manik-manik"),
    (r"\bPendant lizard\b", "Liontin kadal"),
    (r"\bPendant other animal\b", "Liontin hewan lain"),
    (r"\bPendant undetermined shape\b", "Liontin bentuk tak tentu"),
    (r"\bBaslalt\b", "Basalt"),
    (r"\bFlint\b", "Batu api"),
    (r"\bLimestone\b", "Batu gamping"),
    (r"\bSandstone\b", "Batu pasir"),
    (r"\bRhyolith\b", "Riolit"),
    (r"\bGrinding\b", "Penggiling"),
    (r"\bPolisher\b", "Pemoles"),
    (r"\bScraper\b", "Pengikis"),
    (r"\bBottle\b", "Botol"),
    (r"\bCup\b", "Cangkir"),
    (r"\bGoblet\b", "Piala"),
    (r"\bFunnel\b", "Corong"),
    (r"\bJar\b", "Tempayan"),
    (r"\bLid\b", "Tutup"),
    (r"\bMortar\b", "Lesung"),
    (r"\bPlate\b", "Piring"),
    (r"\bSieve\b", "Saringan"),
    (r"\bUrn\b", "Guci"),
    (r"\bAmphora\b", "Amfora"),
    (r"\bPirogue\b", "Pirogue"),
    (r"\bVessel\b", "Kapal"),
    (r"\bLeather\b", "Kulit"),
    (r"\bRope\b", "Tali"),
    (r"\bFurniture\b", "Perabot"),
    (r"\bCoral\b", "Karang"),
    (r"\bShell\b", "Kerang"),
]


def translate_name(name: str) -> str:
    """Apply EXACT_TRANSLATIONS first, then REPLACEMENTS regexes."""
    if name in EXACT_TRANSLATIONS:
        return EXACT_TRANSLATIONS[name]
    result = name
    for pattern, replacement in REPLACEMENTS:
        result = re.sub(pattern, replacement, result)
    return result


def build_hierarchy(lines):
    nodes = {}
    for line in lines:
        if not line.strip():
            continue
        parts = line.strip().split('|')
        if len(parts) != 4:
            continue
        node_id, parent_id, order, name = parts
        node_id = int(node_id)
        parent_id = int(parent_id)
        nodes[node_id] = {
            'parent_id': parent_id,
            'order': int(order),
            'name': name,
        }
    return nodes


def get_path(nodes, node_id):
    path = []
    current_id = node_id
    while current_id in nodes:
        node = nodes[current_id]
        path.insert(0, node['name'])
        if node['parent_id'] == 0:
            break
        current_id = node['parent_id']
    return path


def main():
    input_file = Path("/tmp/furniture-thesaurus.txt")
    with open(input_file, 'r') as f:
        lines = f.readlines()

    nodes = build_hierarchy(lines)

    HEADER = ["IDArkeoGIS", "CARAC_NAME", "CARAC_LVL1", "CARAC_LVL2", "CARAC_LVL3", "CARAC_LVL4", "IdArk", "IdPactols", "IdAat"]

    en_rows = []
    id_rows = []

    for node_id, node in sorted(nodes.items(), key=lambda x: (x[1]['parent_id'], x[1]['order'])):
        # Skip the root node "Furniture" itself
        if node['parent_id'] == 0:
            continue

        path_en = get_path(nodes, node_id)
        # Replace root "Furniture" with "Portable Objects / Artefacts"
        if path_en and path_en[0] == "Furniture":
            path_en[0] = "Portable Objects / Artefacts"

        # Build Indonesian path
        path_id = [translate_name(n) for n in get_path(nodes, node_id)]
        if path_id and path_id[0] == "Artefak / Benda Bergerak":
            pass  # already replaced via EXACT_TRANSLATIONS
        elif path_id and path_id[0] == "Furniture":
            path_id[0] = "Artefak / Benda Bergerak"

        # Pad to 5 levels (CARAC_NAME + 4 LVLs)
        while len(path_en) < 5:
            path_en.append("")
        while len(path_id) < 5:
            path_id.append("")

        row_en = ["", path_en[0], path_en[1], path_en[2], path_en[3], path_en[4], "", "", ""]
        row_id = ["", path_id[0], path_id[1], path_id[2], path_id[3], path_id[4], "", "", ""]
        en_rows.append(row_en)
        id_rows.append(row_id)

    data_dir = Path("/Users/cataivancov/IdeaProjects/arke-platform/data")

    for lang, rows in [("en", en_rows), ("id", id_rows)]:
        out_path = data_dir / f"portable-objects-thesaurus-{lang}.csv"
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(HEADER)
            writer.writerows(rows)
        print(f"Created {out_path} with {len(rows)} rows")


if __name__ == "__main__":
    main()
