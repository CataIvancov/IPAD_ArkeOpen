#!/usr/bin/env python3

from __future__ import annotations

import subprocess

CONTAINER = "arkeopenlocal-postgres"
DATABASES = ("arkeopen", "arkeogis")

SOURCE_ROOT_EN = "Furniture"
NEW_ROOT_EN = "Portable Objects / Artefacts"
NEW_ROOT_ID = "Artefak / Benda Bergerak"
CACHED_LANGS = "en,id"
ADMIN_GROUP_ID = 20

EXACT_TRANSLATIONS = {
    "Furniture": NEW_ROOT_ID,
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
    (r"\\bJewellery\\b", "Perhiasan"),
    (r"\\bTools\\b", "Alat"),
    (r"\\bWeaponry\\b", "Persenjataan"),
    (r"\\bSarcophagus\\b", "Sarkofagus"),
    (r"\\bStele\\b", "Stela"),
    (r"\\bInscription\\b", "Prasasti"),
    (r"\\bDecoration\\b", "Dekorasi"),
    (r"\\bTile\\b", "Ubin"),
    (r"\\bBrick\\b", "Bata"),
    (r"\\bPartition\\b", "Sekat"),
    (r"\\bHypocaust\\b", "Hipokausta"),
    (r"\\bStove pots\\b", "Panci tungku"),
    (r"\\bStove tiles\\b", "Ubin tungku"),
    (r"\\bLamp\\b", "Lampu"),
    (r"\\bGames\\b", "Permainan"),
    (r"\\bTextile\\b", "Tekstil"),
    (r"\\bTokens\\b", "Token"),
    (r"\\bWindow\\b", "Jendela"),
    (r"\\bCoins\\b", "Koin"),
    (r"\\bCounterfeit currency\\b", "Uang palsu"),
    (r"\\bPlated coin\\b", "Koin berlapis"),
    (r"\\bHarness\\b", "Perlengkapan kuda"),
    (r"\\bBit\\b", "Kekang"),
    (r"\\bBuckles\\b", "Gesper"),
    (r"\\bCart\\b", "Kereta"),
    (r"\\bHorse-boot\\b", "Sepatu kuda"),
    (r"\\bArmring\\b", "Gelang lengan"),
    (r"\\bAnklets\\b", "Gelang kaki"),
    (r"\\bBracelet\\b", "Gelang"),
    (r"\\bRing\\b", "Cincin"),
    (r"\\bBelt\\b", "Ikat pinggang"),
    (r"\\bEarring\\b", "Anting"),
    (r"\\bPendant\\b", "Liontin"),
    (r"\\bPin\\b", "Peniti"),
    (r"\\bIngots\\b", "Batangan"),
    (r"\\bSlag\\b", "Terak"),
    (r"\\bAxe\\b", "Kapak"),
    (r"\\bCold chisel\\b", "Pahat dingin"),
    (r"\\bHammer\\b", "Palu"),
    (r"\\bKnife\\b", "Pisau"),
    (r"\\bNails\\b", "Paku"),
    (r"\\bSchears\\b", "Gunting"),
    (r"\\bSickle\\b", "Sabit"),
    (r"\\bToiletry\\b", "Perlengkapan mandi"),
    (r"\\bWeights\\b", "Beban"),
    (r"\\bArrowhead\\b", "Mata panah"),
    (r"\\bBreastplate\\b", "Pelindung dada"),
    (r"\\bDagger\\b", "Belati"),
    (r"\\bHelmet\\b", "Helm"),
    (r"\\bSchield\\b", "Perisai"),
    (r"\\bSpear\\b", "Tombak"),
    (r"\\bSword\\b", "Pedang"),
    (r"\\bCoating\\b", "Lapisan"),
    (r"\\bMosaic\\b", "Mozaik"),
    (r"\\bBead barrel\\b", "Manik berbentuk barel"),
    (r"\\bBead bitronconical\\b", "Manik bitronkonikal"),
    (r"\\bBead blank barrel\\b", "Bahan manik barel"),
    (r"\\bBead blank bitronconical\\b", "Bahan manik bitronkonikal"),
    (r"\\bBead blank cylindrical\\b", "Bahan manik silindris"),
    (r"\\bBead blank discoid\\b", "Bahan manik cakram"),
    (r"\\bBead blank\\b", "Bahan manik"),
    (r"\\bBead\\b", "Manik-manik"),
    (r"\\bPendant lizard\\b", "Liontin kadal"),
    (r"\\bPendant other animal\\b", "Liontin hewan lain"),
    (r"\\bPendant undetermined shape\\b", "Liontin bentuk tak tentu"),
    (r"\\bBaslalt\\b", "Basalt"),
    (r"\\bFlint\\b", "Batu api"),
    (r"\\bLimestone\\b", "Batu gamping"),
    (r"\\bSandstone\\b", "Batu pasir"),
    (r"\\bRhyolith\\b", "Riolit"),
    (r"\\bGrinding\\b", "Penggiling"),
    (r"\\bPolisher\\b", "Pemoles"),
    (r"\\bScraper\\b", "Pengikis"),
    (r"\\bBottle\\b", "Botol"),
    (r"\\bCup\\b", "Cangkir"),
    (r"\\bGoblet\\b", "Piala"),
    (r"\\bFunnel\\b", "Corong"),
    (r"\\bJar\\b", "Tempayan"),
    (r"\\bLid\\b", "Tutup"),
    (r"\\bMortar\\b", "Lesung"),
    (r"\\bPlate\\b", "Piring"),
    (r"\\bSieve\\b", "Saringan"),
    (r"\\bUrn\\b", "Guci"),
    (r"\\bAmphora\\b", "Amfora"),
    (r"\\bPirogue\\b", "Pirogue"),
    (r"\\bVessel\\b", "Kapal"),
    (r"\\bLeather\\b", "Kulit"),
    (r"\\bRope\\b", "Tali"),
    (r"\\bFurniture\\b", "Perabot"),
    (r"\\bCoral\\b", "Karang"),
    (r"\\bShell\\b", "Kerang"),
    (r"\\bMetal\\b", "Logam"),
    (r"\\bStone\\b", "Batu"),
    (r"\\bGlass\\b", "Kaca"),
    (r"\\bBone\\b", "Tulang"),
    (r"\\bCeramic\\b", "Keramik"),
]


def sh(cmd, *, stdin=None):
    return subprocess.run(cmd, input=stdin, text=True, check=True, capture_output=True)


def query(db, sql):
    return sh(["docker", "exec", CONTAINER, "psql", "-U", "postgres", "-d", db, "-Atc", sql]).stdout


def sql_literal(value):
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def ensure_lang(db):
    statements = [
        "INSERT INTO lang (isocode, active) SELECT 'id', true WHERE NOT EXISTS (SELECT 1 FROM lang WHERE isocode='id');",
        "INSERT INTO lang_tr (lang_isocode, lang_isocode_tr, name) SELECT 'id','en','Indonesian' WHERE NOT EXISTS (SELECT 1 FROM lang_tr WHERE lang_isocode='id' AND lang_isocode_tr='en');",
        "INSERT INTO lang_tr (lang_isocode, lang_isocode_tr, name) SELECT 'id','id','Bahasa Indonesia' WHERE NOT EXISTS (SELECT 1 FROM lang_tr WHERE lang_isocode='id' AND lang_isocode_tr='id');",
    ]
    sh(["docker", "exec", "-i", CONTAINER, "psql", "-U", "postgres", "-d", db], stdin="\n".join(statements))


def get_owner_id(db):
    value = query(db, "SELECT id FROM \"user\" WHERE username='IPAD_admin' ORDER BY id LIMIT 1;").strip()
    if not value:
        raise RuntimeError("IPAD_admin user not found in database: " + db)
    return int(value)


def build_sql(db, owner_user_id):
    # find source root
    source_root_id = query(
        db,
        "SELECT c.id FROM charac c JOIN charac_tr ct ON ct.charac_id=c.id "
        "WHERE c.parent_id=0 AND ct.lang_isocode='en' AND ct.name=" + sql_literal(SOURCE_ROOT_EN) + " LIMIT 1;",
    ).strip()
    if not source_root_id:
        raise RuntimeError(f"Source root not found: {SOURCE_ROOT_EN}")
    source_root_id = int(source_root_id)

    # if target already exists, drop its subtree first
    existing_root_id = query(
        db,
        "SELECT c.id FROM charac c JOIN charac_tr ct ON ct.charac_id=c.id "
        "WHERE c.parent_id=0 AND ct.lang_isocode='en' AND ct.name=" + sql_literal(NEW_ROOT_EN) + " LIMIT 1;",
    ).strip()

    statements = ["BEGIN;", "SET session_replication_role = replica;"]

    if existing_root_id:
        existing_root_id = int(existing_root_id)
        statements.extend([
            "WITH RECURSIVE tree AS ("
            f"SELECT id FROM charac WHERE id = {existing_root_id} "
            "UNION ALL "
            "SELECT c.id FROM charac c JOIN tree t ON c.parent_id = t.id"
            ") "
            "DELETE FROM charac_tr WHERE charac_id IN (SELECT id FROM tree);",
            "WITH RECURSIVE tree AS ("
            f"SELECT id FROM charac WHERE id = {existing_root_id} "
            "UNION ALL "
            "SELECT c.id FROM charac c JOIN tree t ON c.parent_id = t.id"
            ") "
            "DELETE FROM charac WHERE id IN (SELECT id FROM tree);",
        ])

    # allocate new ids
    next_id = int(query(db, "SELECT COALESCE(MAX(id),0)+1 FROM charac;"))

    # fetch source tree with order and names
    tree_sql = (
        "WITH RECURSIVE tree AS ("
        f"SELECT c.id, c.parent_id, c.\"order\", ct.name FROM charac c "
        "JOIN charac_tr ct ON ct.charac_id=c.id AND ct.lang_isocode='en' "
        f"WHERE c.id = {source_root_id} "
        "UNION ALL "
        "SELECT c.id, c.parent_id, c.\"order\", ct.name FROM charac c "
        "JOIN charac_tr ct ON ct.charac_id=c.id AND ct.lang_isocode='en' "
        "JOIN tree t ON c.parent_id = t.id"
        ") "
        "SELECT id, parent_id, \"order\", name FROM tree ORDER BY parent_id, \"order\";"
    )
    rows = [line.split('|') for line in query(db, tree_sql).splitlines() if line]

    # map old->new ids
    id_map = {}
    for old_id, _, _, _ in rows:
        id_map[int(old_id)] = next_id
        next_id += 1

    # create new root
    new_root_id = id_map[source_root_id]
    statements.append(
        "INSERT INTO charac (id, parent_id, \"order\", author_user_id, ark_id, pactols_id, aat_id) "
        f"VALUES ({new_root_id}, 0, 0, {owner_user_id}, '', '', '');"
    )

    def translate_to_id(name: str) -> str:
        if name in EXACT_TRANSLATIONS:
            return EXACT_TRANSLATIONS[name]
        translated = name
        for pattern, replacement in REPLACEMENTS:
            translated = __import__("re").sub(pattern, replacement, translated)
        return translated

    # insert children (preserve order, translate names)
    for old_id, parent_id, order_value, name in rows:
        old_id = int(old_id)
        parent_id = int(parent_id)
        if old_id == source_root_id:
            en_name = NEW_ROOT_EN
            id_name = NEW_ROOT_ID
            node_id = new_root_id
            parent_new = 0
            order_value = 0
        else:
            en_name = name
            id_name = translate_to_id(name)
            node_id = id_map[old_id]
            parent_new = id_map[parent_id]
        statements.append(
            "INSERT INTO charac (id, parent_id, \"order\", author_user_id, ark_id, pactols_id, aat_id) "
            f"VALUES ({node_id}, {parent_new}, {int(order_value)}, {owner_user_id}, '', '', '') "
            "ON CONFLICT (id) DO UPDATE SET parent_id = EXCLUDED.parent_id, \"order\" = EXCLUDED.\"order\";"
        )
        statements.append(
            "INSERT INTO charac_tr (charac_id, lang_isocode, name, description) "
            f"VALUES ({node_id}, 'en', {sql_literal(en_name)}, '') "
            "ON CONFLICT (charac_id, lang_isocode) DO UPDATE SET name = EXCLUDED.name;"
        )
        statements.append(
            "INSERT INTO charac_tr (charac_id, lang_isocode, name, description) "
            f"VALUES ({node_id}, 'id', {sql_literal(id_name)}, '') "
            "ON CONFLICT (charac_id, lang_isocode) DO UPDATE SET name = EXCLUDED.name;"
        )

    # add charac_root entry
    statements.append(
        "INSERT INTO charac_root (root_charac_id, admin_group_id, cached_langs) "
        f"VALUES ({new_root_id}, {ADMIN_GROUP_ID}, {sql_literal(CACHED_LANGS)}) "
        "ON CONFLICT (root_charac_id) DO UPDATE SET admin_group_id = EXCLUDED.admin_group_id, cached_langs = EXCLUDED.cached_langs;"
    )

    statements.extend([
        "SELECT setval('charac_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM charac), 1), true);",
        "SET session_replication_role = origin;",
        "COMMIT;",
    ])

    return "\n".join(statements) + "\n"


def main():
    for db in DATABASES:
        ensure_lang(db)
        owner_user_id = get_owner_id(db)
        sql = build_sql(db, owner_user_id)
        sh(["docker", "exec", "-i", CONTAINER, "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", db], stdin=sql)
        print(f"[portable-objects-thesaurus:{db}] imported root='{NEW_ROOT_EN}'")


if __name__ == "__main__":
    main()
