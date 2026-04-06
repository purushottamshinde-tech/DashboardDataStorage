#!/usr/bin/env python3
"""
generate_projects_json.py
Run by GitHub Action after sync_data.yml to produce projects.json.gz
Pre-aggregates 825K DN dump rows → ~32K project-level rows (~1.2 MB gz vs 14.7 MB)

Metering is calculated per-project using the backend rate table formula:
  metering = NM_rate(city, phase) + GM_rate(city, phase) + DN_dump_metering_items
No manual monthly totals needed — fully automatic for any month.

Usage:  python3 generate_projects_json.py
Input:  data.csv.gz  (same directory)
Output: projects.json.gz (same directory)
"""
import gzip, csv, io, json, os, re
from collections import defaultdict
from datetime import datetime

# ── Configuration ────────────────────────────────────────────────────────────
CIVL_TO_ELEC   = {'CIVL-0012','CIVL-0013','CIVL-0014','CIVL-0015','CIVL-0016'}
METERING_REMAP = {'ACDB-2449-EATON'}
DONGLE_PFX     = {'DALO','DALA'}
COGS_CATS = {
    'Module','Inverter','Prefab MMS','Cables','I&C KIT','Conduit Pipe',
    'Earthing & LA','Junction Box','Tin Shed MMS','Safety','I&C Accessories',
    'Welded MMS','SS NBW','Electrical BoS','Data Logger','Metering','Welcome Kit and Board'
}

# ── Backend Metering Rate Tables ─────────────────────────────────────────────
# Formula: metering = NM(city, inv_phase) + GM(city, sanction_phase) + DN_dump_extras
# NM = Net Meter rate (keyed on Inverter Phase — detected from DN inverter item name)
# GM = Generation Meter rate (keyed on Sanction Phase = Phase Connection from data.csv)
# Tuple format: (single_phase, three_phase)
# Source: Backend rate matrix (GMB_GMP_GMI ERP Categorization)

NM_RATES = {
    # MH clusters — Net Meter included in discom, rate = 0
    'Pune':        (0, 0),
    'Nashik':      (0, 0),
    'Nagpur':      (0, 0),
    'Aurangabad':  (0, 0),
    'Jalgaon':     (0, 0),
    'Ahmednagar':  (0, 0),
    'Latur':       (0, 0),
    'Kolhapur':    (0, 0),
    'Mumbai':      (0, 0),
    'Amravati':    (0, 0),
    'Solapur':     (0, 0),
    # MP clusters
    'Bhopal':      (2841, 4617),
    'Indore':      (6800, 9050),
    'Jabalpur':    (9785, 14050),
    'Gwalior':     (2841, 4617),
    # South
    'Bengaluru':   (3250, 6376),
    'Hyderabad':   (0, 0),
    # Gujarat
    'Ahmedabad':   (0, 0),
    'Surat':       (0, 0),
    'Baroda':      (0, 0),
    # Rajasthan
    'Jaipur':      (3550, 6650),
    'Ajmer':       (3550, 6650),
    'Kota':        (3550, 6650),
    # UP / North
    'Lucknow':     (1350, 4350),
    'Kanpur':      (1350, 4350),
    'Varanasi':    (1350, 4350),
    'Noida':       (1350, 4350),
    'NCR':         (0, 0),
    # South (others)
    'Kochi':       (3250, 6376),
    'Chennai':     (2763, 5011),
    'Agra':        (1350, 4350),
    'Coimbatore':  (2763, 5011),
    # Additional mapped cities
    'Raipur':      (0, 0),
    'Mysuru':      (3250, 6376),
    'Warangal':    (0, 0),
    'Gurgaon':     (0, 0),
    'Delhi NCR':   (0, 0),
    'Ghaziabad':   (1350, 4350),
}

GM_RATES = {
    # MH clusters — Generation Meter procured by SSE
    'Pune':        (1260, 2620),
    'Nashik':      (1260, 2620),
    'Nagpur':      (1260, 2620),
    'Aurangabad':  (1260, 2620),
    'Jalgaon':     (1260, 2620),
    'Ahmednagar':  (1260, 2620),
    'Latur':       (1260, 2620),
    'Kolhapur':    (1260, 2620),
    'Mumbai':      (1260, 2620),
    'Amravati':    (1260, 2620),
    'Solapur':     (1260, 2620),
    # MP clusters — no Gen Meter
    'Bhopal':      (0, 0),
    'Indore':      (0, 0),
    'Jabalpur':    (0, 0),
    'Gwalior':     (0, 0),
    # South
    'Bengaluru':   (0, 0),
    'Hyderabad':   (0, 0),
    # Gujarat
    'Ahmedabad':   (0, 0),
    'Surat':       (0, 0),
    'Baroda':      (0, 0),
    # Rajasthan — Gen Meter applies
    'Jaipur':      (3050, 5650),
    'Ajmer':       (3050, 5650),
    'Kota':        (3050, 5650),
    # UP / North — no Gen Meter
    'Lucknow':     (0, 0),
    'Kanpur':      (0, 0),
    'Varanasi':    (0, 0),
    'Noida':       (0, 0),
    'NCR':         (0, 0),
    # South (others)
    'Kochi':       (0, 0),
    'Chennai':     (0, 0),
    'Agra':        (0, 0),
    'Coimbatore':  (0, 0),
    # Additional mapped cities
    'Raipur':      (0, 0),
    'Mysuru':      (0, 0),
    'Warangal':    (0, 0),
    'Gurgaon':     (0, 0),
    'Delhi NCR':   (0, 0),
    'Ghaziabad':   (0, 0),
}

def detect_inverter_phase(inv_item_name):
    """Detect inverter phase from DN item_name.
    NM rate uses Inverter Phase (not Sanction Phase).
    Returns 'Single Phase' or 'Three Phase'."""
    n = str(inv_item_name)
    # Skip batteries — they aren't the main inverter
    if 'Battery' in n and 'Hybrid' in n:
        return None  # signal to skip this item
    if '3 Phase' in n or '3-Phase' in n or 'Three Phase' in n:
        return 'Three Phase'
    if '1 Phase' in n or '1-Phase' in n or 'Single Phase' in n:
        return 'Single Phase'
    # Enphase microinverters are always single phase
    if 'ENPHASE' in n.upper() or 'Micro' in n.lower():
        return 'Single Phase'
    # Default: single phase
    return 'Single Phase'

def calc_metering_backend(city, inv_phase, sanction_phase):
    """Calculate backend metering = NM_rate(city, inv_phase) + GM_rate(city, sanction_phase).
    NM uses Inverter Phase (detected from DN item name).
    GM uses Sanction Phase (= Phase Connection from data.csv)."""
    # NM lookup — keyed on inverter phase
    nm_idx = 0 if (not inv_phase or 'single' in inv_phase.lower()) else 1
    nm = NM_RATES.get(city, (0, 0))
    # GM lookup — keyed on sanction/connection phase
    gm_idx = 0 if (not sanction_phase or 'single' in sanction_phase.lower()) else 1
    gm = GM_RATES.get(city, (0, 0))
    return nm[nm_idx] + gm[gm_idx]

def is_metering_dn_item(item_name):
    """Check if a DN dump item matches the metering SUMIFS patterns"""
    if 'Communication Modem' in item_name and 'Optical Cable' in item_name:
        return True
    if 'FRP Meter Box' in item_name:
        return True
    if 'Meter Box' in item_name and '400x300x150' in item_name and 'SPARK' in item_name:
        return True
    return False

# ── Cell Name → City/State Lookup ────────────────────────────────────────────
CELL_CITY_STATE = {
    'Aurangabad Expansion':{'c':'Aurangabad','s':'MH East'},
    'Bangalore Royal Challengers':{'c':'Bengaluru','s':'Karnataka'},
    'Bangalore Royal Challengers**':{'c':'Bengaluru','s':'Karnataka'},
    'Bangalore Royal Challengers 2':{'c':'Bengaluru','s':'Karnataka'},
    'Bengaluru Royal Challengers':{'c':'Bengaluru','s':'Karnataka'},
    'Bengaluru Royal Challengers 2':{'c':'Bengaluru','s':'Karnataka'},
    'Baroda Blasters':{'c':'Baroda','s':'Gujrat'},
    'Baroda Smashers':{'c':'Baroda','s':'Gujrat'},
    'Bhopal Strikers':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Bhopal Strikers 2':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Bhopal Strikers 3':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Bhopal Strikers 4':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Bhopal Strikers 5':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Bhopal Strikers 6':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Bhopal Strikers**':{'c':'Bhopal','s':'Madhya Pradesh'},
    'Delhi Dashers 2':{'c':'Gurgaon','s':'Delhi'},
    'Delhi Dashers 3':{'c':'Delhi NCR','s':'Delhi'},
    'Delhi Dashers 4':{'c':'Ghaziabad','s':'Delhi'},
    'Delhi Dashers 5':{'c':'Delhi NCR','s':'Delhi'},
    'Delhi Dashers 6':{'c':'Delhi NCR','s':'Delhi'},
    'Gujrat Gladiators':{'c':'Ahmedabad','s':'Gujrat'},
    'Gujrat Gladiators 2':{'c':'Ahmedabad','s':'Gujrat'},
    'Gujarat Gladiators':{'c':'Ahmedabad','s':'Gujrat'},
    'Gujarat Gladiators 2':{'c':'Ahmedabad','s':'Gujrat'},
    'Ahmedabad Gladiators':{'c':'Ahmedabad','s':'Gujrat'},
    'Ahmedabad Gladiators 2':{'c':'Ahmedabad','s':'Gujrat'},
    'Gwalior Groundbreakers':{'c':'Gwalior','s':'Madhya Pradesh'},
    'Gwalior Groundbreakers 2':{'c':'Gwalior','s':'Madhya Pradesh'},
    'Gwalior Groundbreakers 3':{'c':'Gwalior','s':'Madhya Pradesh'},
    'Gwalior Groundbreakers 4':{'c':'Gwalior','s':'Madhya Pradesh'},
    'Gwalior Groundbreakers 5':{'c':'Gwalior','s':'Madhya Pradesh'},
    'Speed Order Gwalior 5':{'c':'Gwalior','s':'Madhya Pradesh'},
    'Indore Immortals':{'c':'Indore','s':'Madhya Pradesh'},
    'Indore Immortals 2':{'c':'Indore','s':'Madhya Pradesh'},
    'Indore Immortals 3':{'c':'Indore','s':'Madhya Pradesh'},
    'Indore Immortals 4':{'c':'Indore','s':'Madhya Pradesh'},
    'Indore Immortals 5':{'c':'Indore','s':'Madhya Pradesh'},
    'Indore Immortals 6':{'c':'Indore','s':'Madhya Pradesh'},
    'Indore Immortals 7':{'c':'Indore','s':'Madhya Pradesh'},
    'Jabalpur Champions':{'c':'Jabalpur','s':'Madhya Pradesh'},
    'Jabalpur Champions 2':{'c':'Jabalpur','s':'Madhya Pradesh'},
    'Jabalpur Champions 3':{'c':'Jabalpur','s':'Madhya Pradesh'},
    'Jabalpur Champions 4':{'c':'Jabalpur','s':'Madhya Pradesh'},
    'Jabalpur Champions 5':{'c':'Jabalpur','s':'Madhya Pradesh'},
    'Jalgaon Expansion':{'c':'Jalgaon','s':'MH East'},
    'Jalgaon Expansion 2':{'c':'Jalgaon','s':'MH East'},
    'Kolhapur Kings':{'c':'Kolhapur','s':'MH West'},
    'Lucknow Lions':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Lucknow Lions 2':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Lucknow Lions 3':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Lucknow Lions 4':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Speed Order Lucknow 4':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Speed Order Lucknow 5':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Noida Knight Riders':{'c':'Noida','s':'Uttar Pradesh'},
    'Kanpur Tigers':{'c':'Kanpur','s':'Uttar Pradesh'},
    'Kanpur Tigers 2':{'c':'Kanpur','s':'Uttar Pradesh'},
    'Kanpur Tigers 3':{'c':'Kanpur','s':'Uttar Pradesh'},
    'Varanasi Warriors':{'c':'Varanasi','s':'Uttar Pradesh'},
    'Agra Knight Riders':{'c':'Agra','s':'Uttar Pradesh'},
    'Nagpur Daredevils':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 2':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 3':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 4':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 5':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 6':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 7':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 8':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 9':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 10':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 13':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 14':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils 15':{'c':'Nagpur','s':'MH East'},
    'Nagpur Daredevils Temp':{'c':'Nagpur','s':'MH East'},
    'Amravati Riders':{'c':'Amravati','s':'MH East'},
    'Amravati Riders 3':{'c':'Amravati','s':'MH East'},
    'Nashik Finishers':{'c':'Nashik','s':'MH West'},
    'Nashik Finishers 2':{'c':'Nashik','s':'MH West'},
    'Nashik Finishers 3':{'c':'Nashik','s':'MH West'},
    'Nashik Finishers 5':{'c':'Nashik','s':'MH West'},
    'Pune Squadrons':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 2':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 3':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 4':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 5':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 6':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 7':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 8':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 9':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 10':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 11':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 12':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 13':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 14':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 15':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 16':{'c':'Pune','s':'MH West'},
    'Pune Squadrons 17':{'c':'Pune','s':'MH West'},
    'Pune Squadrons Temp':{'c':'Pune','s':'MH West'},
    'Pune Squadrons**':{'c':'Pune','s':'MH West'},
    'Ahilyanagar Regiments':{'c':'Pune','s':'MH West'},
    'Speed Order Ahilyanagar 1':{'c':'Pune','s':'MH West'},
    'Speed Order Pune 11':{'c':'Pune','s':'MH West'},
    'Solapur Super Kings':{'c':'Solapur','s':'MH West'},
    'Surat Expansion':{'c':'Surat','s':'Gujrat'},
    'Surat Expansion 2':{'c':'Surat','s':'Gujrat'},
    'Jaipur Titans':{'c':'Jaipur','s':'Rajasthan'},
    'Speed Order Jaipur 2':{'c':'Jaipur','s':'Rajasthan'},
    'Kota Knights':{'c':'Kota','s':'Rajasthan'},
    'Ajmer Aces':{'c':'Ajmer','s':'Rajasthan'},
    'Ajmer Aces 2':{'c':'Ajmer','s':'Rajasthan'},
    'Telangana Tuskers':{'c':'Hyderabad','s':'Telangana'},
    'Telangana Tuskers 2':{'c':'Hyderabad','s':'Telangana'},
    'Telangana Tuskers 3':{'c':'Hyderabad','s':'Telangana'},
    'Hyderabad Tuskers':{'c':'Hyderabad','s':'Telangana'},
    'Hyderabad Tuskers 2':{'c':'Hyderabad','s':'Telangana'},
    'Hyderabad Tuskers 3':{'c':'Hyderabad','s':'Telangana'},
    'Warangal Waveriders':{'c':'Warangal','s':'Telangana'},
    'Kochi Crushers':{'c':'Kochi','s':'Kerala'},
    'Raipur Royals':{'c':'Raipur','s':'Chhattisgarh'},
    'Chennai Super Kings':{'c':'Chennai','s':'Tamil Nadu'},
    'Chennai Super Kings 2':{'c':'Chennai','s':'Tamil Nadu'},
    'Speed Order Chennai 3':{'c':'Chennai','s':'Tamil Nadu'},
    'Coimbatore Kovai Kings':{'c':'Coimbatore','s':'Tamil Nadu'},
    'Mysuru Mavericks':{'c':'Mysuru','s':'Karnataka'},
    'Speed Order Gurgaon':{'c':'Gurgaon','s':'Delhi'},
}

MON_MAP = {'jan':0,'feb':1,'mar':2,'apr':3,'may':4,'jun':5,'jul':6,'aug':7,'sep':8,'oct':9,'nov':10,'dec':11}

def parse_date(v):
    if not v: return None
    parts = v.strip().split('-')
    if len(parts) == 3:
        try:
            day = int(parts[0])
            mon = MON_MAP.get(parts[1].lower()[:3])
            yr  = int(parts[2]); yr = 2000+yr if yr < 100 else yr
            if mon is not None: return datetime(yr, mon+1, day)
        except: pass
    try: return datetime.strptime(v.strip(), '%Y-%m-%d')
    except: pass
    return None

def resolve_cat(item_code, raw_cat):
    pfx = item_code[:4].upper()
    if pfx in DONGLE_PFX:                 return 'EXCLUDE'
    if item_code in CIVL_TO_ELEC:         return 'Electrical BoS'
    if item_code in METERING_REMAP:       return 'Metering'
    if raw_cat == 'Fixtures and Tools':   return 'Welcome Kit and Board'
    if not raw_cat and item_code.startswith('INVS'): return 'Inverter'
    return raw_cat.strip()

CAT_KEY = {
    'Module':'mod','Inverter':'inv','Prefab MMS':'prf','Cables':'cab','I&C KIT':'ick',
    'Conduit Pipe':'con','Earthing & LA':'ear','Junction Box':'jbx','Tin Shed MMS':'tsh',
    'Safety':'saf','I&C Accessories':'ica','Welded MMS':'wel','SS NBW':'ssn',
    'Electrical BoS':'ebo','Data Logger':'dlg','Metering':'mtr','Welcome Kit and Board':'wkt',
}

# ── Build project map ─────────────────────────────────────────────────────────
print("Reading data.csv.gz...")
project_map = {}
dn_metering = defaultdict(float)   # DN dump metering items per project
unmapped_cells = defaultdict(int)   # Track unmapped cells for warning

with gzip.open('data.csv.gz', 'rt', encoding='utf-8', errors='replace') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        if i % 100000 == 0: print(f"  {i:,} rows processed...")
        sse = row['SSE ID'].strip()
        if not sse: continue

        try: rev = float(row['Final Revenue Excl. GST']) if row['Final Revenue Excl. GST'].strip() else 0
        except: rev = 0
        try: kw = float(row['Project Size (kW)']) if row['Project Size (kW)'].strip() else 0
        except: kw = 0
        try: amt = float(row['amount']) if row['amount'].strip() else 0
        except: amt = 0
        try: qty = float(row['qty']) if row['qty'].strip() else 0
        except: qty = 0

        raw_cat   = row['item_category'].strip()
        item_code = row['item_code'].strip()
        item_name = row['item_name'].strip()
        cat = resolve_cat(item_code, raw_cat)

        # Track DN dump metering items (from the Excel SUMIFS part of the formula)
        if is_metering_dn_item(item_name):
            dn_metering[sse] += amt

        if sse not in project_map:
            cell = row['Cell Name'].strip()
            cs   = CELL_CITY_STATE.get(cell)
            city = cs['c'] if cs else row['City'].strip()
            state= cs['s'] if cs else row['State'].strip()
            if cell and not cs and not city:
                unmapped_cells[cell] += 1
            d    = parse_date(row['Installation Completion Date'])
            offer= row['Offer Type'].strip().replace('GoodZero+','GoodZero')
            phase= row['Phase Connection'].strip()
            project_map[sse] = {
                'id':sse,'c':city,'s':state,'o':offer,'ph':phase,
                'kw':kw,'rev':round(rev,2),'dt':d.strftime('%Y-%m-%d') if d else '',
                'mod':0,'inv':0,'prf':0,'cab':0,'ick':0,'con':0,'ear':0,'jbx':0,
                'tsh':0,'saf':0,'ica':0,'wel':0,'ssn':0,'ebo':0,'dlg':0,'mtr':0,'wkt':0,
                'mt':'','mq':0,'it':'','iq':0,
            }

        if cat == 'EXCLUDE': continue
        k2 = CAT_KEY.get(cat)
        if k2: project_map[sse][k2] = round(project_map[sse][k2] + amt, 2)

        p = project_map[sse]
        if cat == 'Module' and item_name:
            if not p['mt']: p['mt'] = item_name; p['mq'] = qty
            elif p['mt'] == item_name: p['mq'] += qty
        if cat == 'Inverter' and item_name:
            if not p['it']:
                p['it'] = item_name; p['iq'] = qty
            # Detect inverter phase from DN item name (for NM rate lookup)
            # Skip batteries — they aren't the main inverter
            if '_inv_phase' not in p or not p['_inv_phase']:
                detected = detect_inverter_phase(item_name)
                if detected:  # None = battery, skip
                    p['_inv_phase'] = detected

print(f"Built {len(project_map):,} projects")

if unmapped_cells:
    print(f"\n⚠  WARNING: {len(unmapped_cells)} unmapped cell names (add to CELL_CITY_STATE):")
    for cell, cnt in sorted(unmapped_cells.items(), key=lambda x: -x[1]):
        print(f"    {cell}: {cnt} projects")

# ── Backend metering injection (formula-based, dual-phase) ────────────────────
# metering = NM_rate(city, inv_phase) + GM_rate(city, sanction_phase) + DN_dump_items
# NM uses Inverter Phase (detected from DN item name via detect_inverter_phase)
# GM uses Sanction Phase (= Phase Connection from data.csv, stored in p['ph'])

month_metering = defaultdict(float)
no_rate_cities = defaultdict(int)
phase_mismatch_count = 0

for sse, p in project_map.items():
    inv_phase = p.get('_inv_phase', p['ph'])  # fallback to Phase Connection if no inverter detected
    sanction_phase = p['ph']                   # Phase Connection = Sanction Phase (100% match)
    if inv_phase != sanction_phase:
        phase_mismatch_count += 1

    backend = calc_metering_backend(p['c'], inv_phase, sanction_phase)
    dn = dn_metering.get(sse, 0)
    total_mtr = backend + dn

    if total_mtr > 0:
        p['mtr'] = round(p['mtr'] + total_mtr, 2)

    if p['dt']:
        mkey = p['dt'][:7]
        month_metering[mkey] += total_mtr

    if p['c'] and p['c'] not in NM_RATES and backend == 0:
        no_rate_cities[p['c']] += 1

print()
print(f"  Phase mismatches (inv_phase ≠ sanction_phase): {phase_mismatch_count}")
for mkey in sorted(month_metering):
    if month_metering[mkey] > 0:
        count = sum(1 for p in project_map.values() if p['dt'].startswith(mkey))
        print(f"  Metering {mkey}: ₹{month_metering[mkey]:,.0f} → {count} projects")

if no_rate_cities:
    print(f"\n⚠  Cities not in rate table (0 metering):")
    for c, cnt in sorted(no_rate_cities.items(), key=lambda x: -x[1]):
        print(f"    {c}: {cnt} projects")

# ── Compute final COGS ────────────────────────────────────────────────────────
projects = []
for p in project_map.values():
    cogs = round(p['mod']+p['inv']+p['prf']+p['cab']+p['ick']+p['con']+p['ear']+
                 p['jbx']+p['tsh']+p['saf']+p['ica']+p['wel']+p['ssn']+p['ebo']+
                 p['dlg']+p['mtr']+p['wkt'], 2)
    projects.append({**p, 'cogs': cogs})

# ── Write output ──────────────────────────────────────────────────────────────
json_str = json.dumps(projects, separators=(',',':'))
with gzip.open('projects.json.gz', 'wt', encoding='utf-8', compresslevel=9) as f:
    f.write(json_str)

raw_mb = len(json_str)/1e6
gz_mb  = os.path.getsize('projects.json.gz')/1e6
print(f"\nOutput: {len(projects):,} projects | JSON {raw_mb:.1f} MB → gz {gz_mb:.2f} MB")

# ── Quick verification ─────────────────────────────────────────────────────────
print("\n── Verification ──")
for mo, label, actual_cogs, actual_rev, actual_mtr in [
    (1, 'Jan 26', 332173601, 576507216, 5926077),
    (2, 'Feb 26', 305964188, 532008767, 5755707),
    (3, 'Mar 26', None, None, 7909163),
]:
    ps = [p for p in projects if p['dt'].startswith(f'2026-0{mo}')]
    rev  = sum(p['rev']  for p in ps)
    cogs = sum(p['cogs'] for p in ps)
    mtr  = sum(p['mtr']  for p in ps)
    gm   = (rev-cogs)/rev*100 if rev else 0
    mtr_delta = mtr - actual_mtr if actual_mtr else 0
    mtr_pct   = mtr_delta / actual_mtr * 100 if actual_mtr else 0
    line = f"  {label}: {len(ps)} projects | Metering={mtr:,.0f}"
    if actual_mtr:
        line += f" (actual {actual_mtr:,.0f}, delta {mtr_delta:+,.0f} = {mtr_pct:+.2f}%)"
    if actual_cogs and actual_rev:
        ag = (actual_rev-actual_cogs)/actual_rev*100
        line += f" | COGS={cogs/1e7:.2f}Cr (actual {actual_cogs/1e7:.2f}Cr) | GM%={gm:.2f}% (actual {ag:.2f}%)"
    else:
        line += f" | COGS={cogs/1e7:.2f}Cr | GM%={gm:.2f}%"
    print(line)
