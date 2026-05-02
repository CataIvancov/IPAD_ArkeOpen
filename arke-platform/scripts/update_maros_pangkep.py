#!/usr/bin/env python3
import csv, re, os, json, sys

CSV = '/Users/cataivancov/IdeaProjects/arke-platform/data/ipad-sites-maros-pangkep.csv'

# image data: name, altitude, utm_x, utm_y, kabupaten
IMG = [
("Leang Alle Bireng","46","797538","9449867","Maros"),("Leang Ambe Pacco","88","796081","9448069","Maros"),
("Leang Balang","25","793818","9447605","Maros"),("Leang Bara Jarang","64","798567","9450044","Maros"),
("Leang Bara Tedong 1","38","797996","9449056","Maros"),("Leang Baratedong 2","40","798061","9449320","Maros"),
("Leang Barugayya","45","794587","9447400","Maros"),("Leang Batubatae","149","793641","9447768","Maros"),
("Leang Batu Karope","140","795207","9447362","Maros"),("Leang Batu Tianang","21","788971","9454945","Maros"),
("Leang Bembe","18","794759","9446727","Maros"),("Leang Bettue (Lopi-Lopi)","28","794395","9446725","Maros"),
("Leang Bettue (Tompobalang)","88","795833","9447600","Maros"),("Leang Boddong","99","793195","9447373","Maros"),
("Leang Botto","84","792696","9452118","Maros"),("Leang Bulu Batue","27","792718","9447852","Maros"),
("Leang Bulu Kamase","40","794706","9451234","Maros"),("Leang Bulu Sipong 1","88","789599","9458606","Maros"),
("Leang Bulu Sipong 2","88","789590","9458714","Maros"),("Leang Bulu Sipong 3","38","789855","9458737","Maros"),
("Leang Bulu Tungke'e","31","795281","9447724","Maros"),("Leang Bunga Eja 1","47","794143","9452134","Maros"),
("Leang Bunga Eja 2","45","794098","9451996","Maros"),("Leang Burung 1","29","795305","9446494","Maros"),
("Leang Burung 2","29","795212","9446403","Maros"),("Leang Cabbu","22","794093","9447058","Maros"),
("Leang Canggoreng","55","793145","9447105","Maros"),("Leang Cempae","64","789840","9458725","Maros"),
("Leang Elle Pusae","212","796231","9448179","Maros"),("Leang Jarie","30","797775","9443311","Maros"),
("Leang Jing","48","793752","9447665","Maros"),("Leang Kado'","47","798752","9442213","Maros"),
("Leang Karama (Akkarasaka)","65","790305","9455430","Maros"),("Leang Karrasa","176","800118","9441861","Maros"),
("Leang Lambatorang","128","795820","9449923","Maros"),("Leang Lompoa","22","794388","9446415","Maros"),
("Leang Mandauseng","70","793049","9452144","Maros"),("Leang Monroé","36","789686","9458812","Maros"),
("Leang Pabbuno Juku","30","799455","9451010","Maros"),("Leang Paccepacce","69","793054","9448018","Maros"),
("Leang Pajae","124","796042","9448528","Maros"),("Leang Pa'limukang (Pakalu)","31","794292","9447001","Maros"),
("Leang Pangia","30","795415","9446657","Maros"),("Leang Pannampu 1","36","794145","9451471","Maros"),
("Leang Pannampu 2","38","794178","9451438","Maros"),("Leang Pasaung","37","789629","9456356","Maros"),
("Leang Pattae","205","796672","9449021","Maros"),("Leang Pellenge","60","797862","9449981","Maros"),
("Leang Petta Kere","158","796781","9449092","Maros"),("Leang Pucu","40","796734","9450457","Maros"),
("Leang Samongkeng 1","227","794774","9449521","Maros"),("Leang Samongkeng 2","176","794731","9449474","Maros"),
("Leang Samongkeng 3","137","794610","9449532","Maros"),("Leang Samongkeng 4","123","794726","9449589","Maros"),
("Leang Sampeang","40","795759","9447204","Maros"),("Leang Saripa","53","799512","9442112","Maros"),
("Leang Sengka'e","94","793627","9447722","Maros"),("Leang Tampuang","50","798683","9442811","Maros"),
("Leang Tanre","32","793854","9447531","Maros"),("Leang Tengngae","52","794529","9450883","Maros"),
("Leang Timpuseng","25","795092","9446919","Maros"),("Leang Tinggi Ada","45","797143","9449032","Maros"),
("Leang Ulu Leang","60","795837","9447934","Maros"),("Leang Ulu Wae","65","796042","9448528","Maros"),
("Leang Waniuwae","50","797318","9450547","Maros"),("Leang Alla Masigi","331","797743","9459664","Pangkep"),
("Leang Barayya","28","780370","9468685","Pangkep"),("Leang Batanglamara","30","787791","9463780","Pangkep"),
("Leang Batta-Battae","330","798577","9457750","Pangkep"),("Leang Bawang Leangnge","17","779588","9468987","Pangkep"),
("Leang Bawie","90","788700","9463654","Pangkep"),("Leang Biring Ere 1","25","789619","9470732","Pangkep"),
("Leang Biring Ere 2","26","789633","9470772","Pangkep"),("Leang Bubuka","20","786385","9465481","Pangkep"),
("Leang Bujung","10","787649","9464241","Pangkep"),("Leang Bujung Dare","30","790125","9467447","Pangkep"),
("Leang Bulu Bellang","22","780211","9469535","Pangkep"),("Leang Bulu Sumi","25","793400","9456043","Pangkep"),
("Leang Buluribba","35","787870","9464382","Pangkep"),("Leang Buto","10","786562","9465440","Pangkep"),
("Leang Caddia","25","786488","9465414","Pangkep"),("Leang Cammingkana","20","787727","9464260","Pangkep"),
("Leang Carawali","110","787649","9464242","Pangkep"),("Leang Cinayya","16","779696","9468774","Pangkep"),
("Leang Cumilantang","32","788465","9465950","Pangkep"),("Leang Garunggung","30","789949","9466989","Pangkep"),
("Leang Jempang","15","787603","9464991","Pangkep"),("Leang Kahu","50","809056","9462688","Pangkep"),
("Leang Kajuara","35","787459","9464965","Pangkep"),("Leang Kappara","22","789685","9468382","Pangkep"),
("Leang Kassi","15","787220","9464970","Pangkep"),("Leang Lamperajang","9","788000","9462639","Pangkep"),
("Leang Lasitae","14","779981","9469475","Pangkep"),("Leang Leangnge 1","45","783769","9468679","Pangkep"),
("Leang Leangnge 2","28","783823","9468670","Pangkep"),("Leang Lessang","20","786362","9465546","Pangkep"),
("Leang Lompoa","20","786967","9465187","Pangkep"),("Leang Macinna","48","791465","9468743","Pangkep"),
("Leang Nippong","6","791261","9472065","Pangkep"),("Leang Pa'bujang-Bujangang","9","779757","9470101","Pangkep"),
("Leang Pakkatallu","45","802374","9466663","Pangkep"),("Leang Pamelakkang Tedong","14","780161","9469528","Pangkep"),
("Leang Pappanaungang 1","13","779758","9468456","Pangkep"),("Leang Pappanaungang 2","13","779732","9468517","Pangkep"),
("Leang Parewe","12","779353","9469547","Pangkep"),("Leang Pattennung","108","787559","9464990","Pangkep"),
("Perataran Mata Air Je'netaesa","20","788928","9464933","Pangkep"),("Leang Pising-Pising","108","793896","9475680","Pangkep"),
("Leang Sakapao 1","90","788687","9465111","Pangkep"),("Leang Sakapao 2","25","788698","9465076","Pangkep"),
("Leang Saluka","25","790193","9467517","Pangkep"),("Leang Sapiria","25","787827","9463717","Pangkep"),
]

def norm(n):
    n = re.sub(r"^(Gua|Leang)\s+", "", n.strip(), flags=re.I)
    return re.sub(r"[^a-z0-9]", "", n.lower())

# Read existing CSV
rows = []
with open(CSV, 'r', newline='', encoding='utf-8') as f:
    rdr = csv.DictReader(f, delimiter=';')
    flds = list(rdr.fieldnames)
    for row in rdr:
        rows.append({k: v for k, v in row.items() if k is not None})

# Build lookup
lookup = {}
for row in rows:
    lookup.setdefault(norm(row['SITE_NAME']), []).append(row)
    # alt names from comments
    c = row.get('COMMENTS','')
    m = re.search(r'Alternative_Name:\s*([^|]+)', c)
    if m: lookup.setdefault(norm(m.group(1)), []).append(row)

# Manual overrides for tricky matches
overrides = {
    norm("Leang Burung 1"): "Leang Burung I, II",
    norm("Leang Burung 2"): "Leang Burung I, II",
    norm("Leang Monroé"): "Monroe Cave",
    norm("Leang Karama (Akkarasaka)"): "Karama",
    norm("Leang Balang"): "Leang Balangajia",
    norm("Leang Bulu Bettue (Lopi-Lopi)"): "Leang Bulu Bettue",
    norm("Leang Bulu Bettue (Tompobalang)"): "Leang Bulu Bettue",
    norm("Leang Pappanaungang 1"): "Leang Pappanaungang I",
    norm("Leang Samongkeng 1"): "Leang Samongkeng I",
    norm("Leang Samongkeng 2"): "Leang Samongkeng II",
    norm("Leang Samongkeng 3"): "Leang Samongkeng III",
    norm("Leang Samongkeng 4"): "Leang Samongkeng III",
    norm("Leang Sampeang"): "Leang Sampeang I",
    norm("Leang Sakapao 1"): "Leang Sakapao",
    norm("Leang Sakapao 2"): "Leang Sakapao",
    norm("Leang Bulu Sipong 1"): "Leang Bulu Sipong I",
    norm("Leang Bulu Sipong 2"): "Leang Bulu Sipong II",
    norm("Leang Bulu Sipong 3"): "Leang Bulu Sipong III",
    norm("Leang Pannampu 1"): "Leang Pannampu",
    norm("Leang Pannampu 2"): "Leang Pannampu",
    norm("Leang Batu Tianang"): "Leang Batu Tianang / Leang Barakka",
    norm("Leang Baratedong 2"): "Leang Barattedong",
    norm("Leang Bara Tedong 1"): "Leang Bara Tedong",
    norm("Leang Batanglamara"): "Gua Batang Lamara",
    norm("Leang Batta-Battae"): "Leang Batubatae",
    norm("Leang Buluribba"): "Gua Bulu Ribba",
    norm("Leang Caddia"): "Gua Caddia",
    norm("Leang Carawali"): "Gua Carawaii",
    norm("Leang Jempang"): "Gua Jempang",
    norm("Leang Kajuara"): "Gua Kajuara",
    norm("Leang Lasitae"): "Gua Lasitae",
    norm("Leang Pamelakkang Tedong"): "Gua Pamelakang Tedong",
    norm("Leang Sapiria"): "Gua Sapiria",
    norm("Leang Pattennung"): "Gua Pattennung",
    norm("Leang Tenggae"): "Leang Tengae",
}

matched = set()
new_sites = []
updated = 0

for name, alt, utmx, utmy, kab in IMG:
    key = overrides.get(norm(name), name)
    nkey = norm(key)
    if nkey in lookup:
        for row in lookup[nkey]:
            if row['ALTITUDE'] in ('', None):
                row['ALTITUDE'] = alt
                updated += 1
            matched.add(name)
    else:
        new_sites.append((name, alt, utmx, utmy, kab))

print(f"Updated altitude for {updated} existing site record(s)")
print(f"New sites to add: {len(new_sites)}")
for s in new_sites:
    print("  NEW:", s)

# Determine next ID
nums = [int(re.search(r'\d+', r['SITE_SOURCE_ID']).group()) for r in rows if re.search(r'\d+', r['SITE_SOURCE_ID'])]
next_id = max(nums) + 1

# Add new rows
for name, alt, utmx, utmy, kab in new_sites:
    sid = f"IPAD_{next_id:03d}"
    next_id += 1
    new_row = {k: '' for k in flds}
    new_row['SITE_SOURCE_ID'] = sid
    new_row['SITE_NAME'] = name
    new_row['LOCALISATION'] = f"{kab} | South Sulawesi"
    new_row['PROJECTION_SYSTEM'] = ''
    new_row['LONGITUDE'] = utmx
    new_row['LATITUDE'] = utmy
    new_row['ALTITUDE'] = alt
    new_row['CITY_CENTROID'] = 'No'
    new_row['STATE_OF_KNOWLEDGE'] = 'Documented'
    new_row['OCCUPATION'] = 'Not specified'
    new_row['STARTING_PERIOD'] = ''
    new_row['ENDING_PERIOD'] = ''
    new_row['MAIN_CHARAC'] = 'Archaeological Sites'
    new_row['CHARAC_LVL1'] = ''
    new_row['CHARAC_LVL2'] = ''
    new_row['CHARAC_LVL3'] = ''
    new_row['CHARAC_LVL4'] = ''
    new_row['CHARAC_EXP'] = ''
    new_row['BIBLIOGRAPHY'] = ''
    new_row['COMMENTS'] = f"UTM Zone 50M; UTM X={utmx}; UTM Y={utmy}; Altitude={alt}m"
    rows.append(new_row)

# Write CSV back
with open(CSV, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=flds, delimiter=';', quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    for row in rows:
        clean = {k: v for k, v in row.items() if k in flds}
        w.writerow(clean)

print("Done. Saved to", CSV)
