import csv, re

def norm(n):
    n = re.sub(r"^(Gua|Leang)\s+", "", n.strip(), flags=re.I)
    return re.sub(r"[^a-z0-9]", "", n.lower())

# Read existing
with open('/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-maros-pangkep.csv','r') as f:
    r = csv.DictReader(f, delimiter=';')
    flds = list(r.fieldnames)
    rows = [{k:v for k,v in row.items() if k} for row in r]

# Read image data
with open('/Users/cataivancov/IdeaProjects/arke-platform/data/img_data.csv','r') as f:
    img = list(csv.DictReader(f, delimiter=';'))

# Build lookup
lookup = {}
for row in rows:
    lookup.setdefault(norm(row['SITE_NAME']), []).append(row)
    c = row.get('COMMENTS','')
    m = re.search(r'Alternative_Name:\s*([^|]+)', c)
    if m: lookup.setdefault(norm(m.group(1)), []).append(row)

# Explicit mapping: normalized image name -> exact CSV site name to match
mp = {
    'allebireng': 'Gua Alle Bireng',
    'ambebacco': None,
    'balang': 'Leang Balangajia',
    'barajarang': 'Leang Bara Jarang',
    'baratedong1': 'Leang Bara Tedong',
    'baratedong2': 'Leang Barattedong',
    'barugayya': None,
    'batubatae': None,
    'batukarope': 'Leang Batu Karope',
    'batutianang': 'Leang Batu Tianang / Leang Barakka',
    'bembe': 'Leang Bembe',
    'bettuelopilopi': None,
    'bettuetompobalang': None,
    'boddong': 'Leang Boddong',
    'botto': 'Leang Botto',
    'bulubatue': 'Leang Bulubatua',
    'bulukamase': 'Leang Bulu Kamase',
    'bulusipong1': 'Leang Bulu Sipong I',
    'bulusipong2': 'Leang Bulu Sipong II',
    'bulusipong3': 'Leang Bulu Sipong III',
    'bulutungkee': 'Bulu Tungke\'e',
    'bungaeja1': None,
    'bungaeja2': None,
    'burung1': 'Leang Burung I, II',
    'burung2': 'Leang Burung I, II',
    'cabbu': 'Leang Cabbu',
    'canggoreng': 'Leang Canggoreng',
    'cempae': 'Leang Cempae',
    'ellepusae': 'Gua Alla Pusae',
    'jarie': 'Leang Jarie',
    'jing': 'Leang Jing',
    'kado': None,
    'karamaakkaraaka': 'Karama',
    'karrasa': 'Leang Karrasa',
    'lambatorang': 'Leang Lambattorang',
    'lompoa': 'Leang Lompoa',
    'mandauseng': 'Leang Mandauseng',
    'monroe': 'Monroe Cave',
    'pabbunojuku': 'Leang Pabbuno Juku',
    'paccepacce': 'Leang Paccepacce',
    'pajae': 'Leang Pajae',
    'palimukangpakalu': None,
    'pangia': None,
    'pannampu1': 'Leang Pannampu',
    'pannampu2': None,
    'pasaung': None,
    'pattae': 'Leang Pettae',
    'pellenge': 'Leang Pellenge',
    'pettakere': 'Leang Petta Kere',
    'pucu': 'Leang Pucu',
    'samongkeng1': 'Leang Samongkeng I',
    'samongkeng2': 'Leang Samongkeng II',
    'samongkeng3': 'Leang Samongkeng III',
    'samongkeng4': None,
    'sampeang': 'Leang Sampeang I',
    'saripa': None,
    'sengkae': None,
    'tampuang': 'Leang Tampuang',
    'tanre': 'Leang Tanre',
    'tengngae': 'Leang Tengngae',
    'timpuseng': 'Leang Timpuseng',
    'tinggiada': 'Leang Tinggi Ada',
    'ululeang': 'Leang Ulu Leang',
    'uluwae': 'Leang Uluwae',
    'waniuwae': 'Leang Wanuwae',
    'allamasigi': None,
    'barayya': None,
    'batanglamara': 'Gua Batang Lamara',
    'battabattae': None,
    'bawangleangnge': None,
    'bawie': 'Leang Bawię',
    'biringere1': 'Gua Biring Ere',
    'biringere2': None,
    'bubuka': 'Bubuka',
    'bujung': None,
    'bujungdare': 'Leang Bujung Dare',
    'bulubellang': None,
    'bulusumi': 'Leang Bulu Sumi',
    'buluribba': 'Gua Bulu Ribba',
    'buto': 'Gua Buto',
    'caddia': 'Gua Caddia',
    'cammingkana': 'Leang Cammingkana',
    'carawali': 'Gua Carawaii',
    'cinayya': None,
    'cumilantang': 'Leang Cumi Lantang',
    'garunggung': 'Leang Garunggung',
    'jempang': 'Gua Jempang',
    'kahu': None,
    'kajuara': 'Gua Kajuara',
    'kappara': None,
    'kassi': 'Leang Kassi',
    'lamperajang': 'Gua Lamperajang',
    'lasitae': 'Gua Lasitae',
    'leangnge1': None,
    'leangnge2': None,
    'lessang': 'Gua Lesang',
    'lompoa2': 'Leang Lompoa II',
    'macinna': 'Gua Maccina',
    'nippong': None,
    'pabujangbujangang': 'Gua Pabujangang',
    'pakkatallu': None,
    'pamelakkangtedong': 'Gua Pamelakang Tedong',
    'pappanaungang1': 'Leang Pappanaungang I',
    'pappanaungang2': None,
    'parewe': 'Leang Parewe',
    'pattennung': 'Gua Pattennung',
    'perataranmataairjenetaea': None,
    'pisingpising': None,
    'sakapao1': 'Leang Sakapao',
    'sakapao2': None,
    'saluka': 'Leang Saluka',
    'sapiria': 'Gua Sapiria',
}

used = set()
new = []
updated = 0
for row in img:
    n = norm(row['name'])
    csv_name = mp.get(n)
    if csv_name is None:
        new.append(row)
        continue
    matches = [r for r in rows if r['SITE_NAME'] == csv_name]
    if not matches:
        # fallback by normalized name
        matches = lookup.get(norm(csv_name), [])
    if matches:
        for m in matches:
            if not m.get('ALTITUDE'):
                m['ALTITUDE'] = row['alt']
                updated += 1
        used.add(csv_name)
    else:
        new.append(row)

print(f"Updated {updated} rows, {len(new)} new sites")

# Next ID
nums = [int(re.search(r'\d+', r['SITE_SOURCE_ID']).group()) for r in rows if re.search(r'\d+', r['SITE_SOURCE_ID'])]
nxt = max(nums) + 1

for row in new:
    sid = f"IPAD_{nxt:03d}"
    nxt += 1
    d = {k:'' for k in flds}
    d['SITE_SOURCE_ID'] = sid
    d['SITE_NAME'] = row['name']
    d['LOCALISATION'] = f"{row['kab']} | South Sulawesi"
    d['PROJECTION_SYSTEM'] = ''
    d['LONGITUDE'] = row['utmx']
    d['LATITUDE'] = row['utmy']
    d['ALTITUDE'] = row['alt']
    d['CITY_CENTROID'] = 'No'
    d['STATE_OF_KNOWLEDGE'] = 'Documented'
    d['OCCUPATION'] = 'Not specified'
    d['MAIN_CHARAC'] = 'Archaeological Sites'
    d['COMMENTS'] = f"UTM Zone 50M; UTM X={row['utmx']}; UTM Y={row['utmy']}; Altitude={row['alt']}m"
    rows.append(d)

with open('/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-maros-pangkep.csv','w',newline='') as f:
    w = csv.DictWriter(f, fieldnames=flds, delimiter=';')
    w.writeheader()
    for r in rows:
        w.writerow({k:v for k,v in r.items() if k in flds})

print("Saved")
