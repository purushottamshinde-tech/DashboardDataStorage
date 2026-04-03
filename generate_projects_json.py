#!/usr/bin/env python3
"""
generate_projects_json.py
Run by GitHub Action after sync_data.yml to produce projects.json.gz
Pre-aggregates 825K DN dump rows → ~32K project-level rows (~1.2 MB gz vs 14.7 MB)

Usage:  python3 generate_projects_json.py
Input:  data.csv.gz  (same directory)
Output: projects.json.gz (same directory)
"""
import gzip, csv, io, json, os, re
from collections import defaultdict
from datetime import datetime

# ── Configuration ────────────────────────────────────────────────────────────
# Per-month TOTAL metering target (Net Meter + Gen Meter + modem/FRP/meter-box items).
# The formula-based lookup (city+phase → fixed rate) gives the base; any residual gap
# vs. this target (= modem, FRP meter box, meter box 400x300 items not yet in DN dump)
# is distributed proportionally by project kW within the month.
# Add a new key each month-close once the GL figure is confirmed.
BACKEND_METER_BY_MONTH = {
    '2026-1': 5926077,   # Jan 26
    '2026-2': 5755707,   # Feb 26
    '2026-3': 7909163,   # Mar 26
    # Add: '2026-4': <amt>, etc. each month-close
}

# Net Meter cost lookup: city → (1-phase ₹, 3-phase ₹)
# Source: Backend sheet columns Q:S (Net Meter table)
NET_METER_RATE = {
    'Pune':        (0,     0),     'Nashik':      (0,     0),
    'Nagpur':      (0,     0),     'Aurangabad':  (0,     0),
    'Jalgaon':     (0,     0),     'Ahmednagar':  (0,     0),
    'Ahilyanagar': (0,     0),     'Latur':       (0,     0),
    'Kolhapur':    (0,     0),     'Mumbai':      (0,     0),
    'Amravati':    (0,     0),     'Solapur':     (0,     0),
    'Bhopal':      (2841,  4617),  'Indore':      (6800,  9050),
    'Jabalpur':    (9785,  14050), 'Gwalior':     (2841,  4617),
    'Bengaluru':   (3250,  6376),  'Hyderabad':   (0,     0),
    'Ahmedabad':   (0,     0),     'Surat':       (0,     0),
    'Baroda':      (0,     0),     'Jaipur':      (3550,  6650),
    'Ajmer':       (3550,  6650),  'Kota':        (3550,  6650),
    'Lucknow':     (1350,  4350),  'Kanpur':      (1350,  4350),
    'Varanasi':    (1350,  4350),  'Noida':       (1350,  4350),
    'NCR':         (0,     0),     'Kochi':       (3250,  6376),
    'Chennai':     (2763,  5011),  'Agra':        (1350,  4350),
    'Coimbatore':  (2763,  5011),
}

# Generation Meter cost lookup: city → (1-phase ₹, 3-phase ₹)
# Source: Backend sheet columns T:V (Generation Meter table)
GEN_METER_RATE = {
    'Pune':        (1260,  2620),  'Nashik':      (1260,  2620),
    'Nagpur':      (1260,  2620),  'Aurangabad':  (1260,  2620),
    'Jalgaon':     (1260,  2620),  'Ahmednagar':  (1260,  2620),
    'Ahilyanagar': (1260,  2620),  'Latur':       (1260,  2620),
    'Kolhapur':    (1260,  2620),  'Mumbai':      (1260,  2620),
    'Amravati':    (1260,  2620),  'Solapur':     (1260,  2620),
    'Bhopal':      (0,     0),     'Indore':      (0,     0),
    'Jabalpur':    (0,     0),     'Gwalior':     (0,     0),
    'Bengaluru':   (0,     0),     'Hyderabad':   (0,     0),
    'Ahmedabad':   (0,     0),     'Surat':       (0,     0),
    'Baroda':      (0,     0),     'Jaipur':      (3050,  5650),
    'Ajmer':       (3050,  5650),  'Kota':        (3050,  5650),
    'Lucknow':     (0,     0),     'Kanpur':      (0,     0),
    'Varanasi':    (0,     0),     'Noida':       (0,     0),
    'NCR':         (0,     0),     'Kochi':       (0,     0),
    'Chennai':     (0,     0),     'Agra':        (0,     0),
    'Coimbatore':  (0,     0),     'Warangal':    (0,     0),
    'Gurgaon':     (0,     0),     'Delhi NCR':   (0,     0),
    'Ghaziabad':   (0,     0),
}

def _is_3ph(ph_str):
    return (ph_str or '').strip().lower() in ('three phase', 'three', '3')

def formula_metering(city, ph_str):
    """
    Replicates Excel formula:
      = IF(ph=="Single Phase", VLOOKUP(city, NetMeterTable, 1ph_col), VLOOKUP(city, NetMeterTable, 3ph_col))
      + IF(solar_ph=="Single Phase", VLOOKUP(city, GenMeterTable, 1ph_col), VLOOKUP(city, GenMeterTable, 3ph_col))
    (solar phase assumed same as connection phase; SUMIFS modem/FRP items handled via residual)
    """
    idx = 1 if _is_3ph(ph_str) else 0
    net = NET_METER_RATE.get(city, (0, 0))[idx]
    gen = GEN_METER_RATE.get(city, (0, 0))[idx]
    return net + gen

CIVL_TO_ELEC   = {'CIVL-0012','CIVL-0013','CIVL-0014','CIVL-0015','CIVL-0016'}
METERING_REMAP = {'ACDB-2449-EATON'}
DONGLE_PFX     = {'DALO','DALA'}
COGS_CATS = {
    'Module','Inverter','Prefab MMS','Cables','I&C KIT','Conduit Pipe',
    'Earthing & LA','Junction Box','Tin Shed MMS','Safety','I&C Accessories',
    'Welded MMS','SS NBW','Electrical BoS','Data Logger','Metering','Welcome Kit and Board'
}

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
    'Kolhapur Kings':{'c':'Kolhapur','s':'MH West'},
    'Lucknow Lions':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Lucknow Lions 2':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Lucknow Lions 3':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Lucknow Lions 4':{'c':'Lucknow','s':'Uttar Pradesh'},
    'Noida Knight Riders':{'c':'Lucknow','s':'Uttar Pradesh'},
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
    'Nagpur Daredevils Temp':{'c':'Nagpur','s':'MH East'},
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
    'Surat Expansion':{'c':'Surat','s':'Gujrat'},
    'Surat Expansion 2':{'c':'Surat','s':'Gujrat'},
    'Jaipur Titans':{'c':'Jaipur','s':'Rajasthan'},
    'Kota Knights':{'c':'Jaipur','s':'Rajasthan'},
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

        if sse not in project_map:
            cell = row['Cell Name'].strip()
            cs   = CELL_CITY_STATE.get(cell)
            city = cs['c'] if cs else row['City'].strip()
            state= cs['s'] if cs else row['State'].strip()
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
        if cat == 'Inverter' and item_name and not p['it']:
            p['it'] = item_name; p['iq'] = qty

print(f"Built {len(project_map):,} projects")

# ── Metering: formula-based lookup + residual distribution ───────────────────
# Step 1: Apply per-project formula (Net Meter rate + Generation Meter rate by city+phase)
month_groups = defaultdict(list)
for p in project_map.values():
    if p['dt']:
        mkey = p['dt'][:7]  # YYYY-MM
        month_groups[mkey].append(p)
    # Base metering from lookup table (covers net meter + gen meter hardware costs)
    p['mtr'] = round(p['mtr'] + formula_metering(p['c'], p['ph']), 2)

# Step 2: For months with a confirmed GL target, distribute the residual (modem +
#         FRP meter box + meter box 400×300 items) proportionally by project kW.
for mkey, projs in month_groups.items():
    yr, mo = mkey.split('-')
    target = BACKEND_METER_BY_MONTH.get(f"{yr}-{int(mo)}", 0)
    if not target: continue
    formula_total = sum(p['mtr'] for p in projs)
    residual = target - formula_total
    if residual <= 0:
        print(f"  Metering {mkey}: formula ₹{formula_total:,.0f} already meets/exceeds target ₹{target:,}")
        continue
    total_kw = sum(p['kw'] for p in projs)
    if not total_kw: continue
    distributed = 0
    for i, p in enumerate(projs):
        share = (residual - distributed) if i == len(projs)-1 else round(residual * p['kw'] / total_kw)
        if share > 0:
            p['mtr'] = round(p['mtr'] + share, 2)
            distributed += share
    final_total = sum(p['mtr'] for p in projs)
    print(f"  Metering {mkey}: formula={formula_total:,.0f} + residual={distributed:,} = {final_total:,.0f} (target {target:,})")

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
for mo, label, actual_cogs, actual_rev in [
    (1, 'Jan 26', 332173601, 576507216),
    (2, 'Feb 26', 305964188, 532008767),
    (3, 'Mar 26', None, None),
]:
    ps = [p for p in projects if p['dt'].startswith(f'2026-0{mo}')]
    rev  = sum(p['rev']  for p in ps)
    cogs = sum(p['cogs'] for p in ps)
    mtr  = sum(p['mtr']  for p in ps)
    gm   = (rev-cogs)/rev*100 if rev else 0
    if actual_cogs and actual_rev:
        ag   = (actual_rev-actual_cogs)/actual_rev*100
        print(f"  {label}: {len(ps)} projects | MTR={mtr:,.0f} | COGS={cogs/1e7:.2f}Cr (actual {actual_cogs/1e7:.2f}Cr) | GM%={gm:.2f}% (actual {ag:.2f}%)")
    else:
        print(f"  {label}: {len(ps)} projects | MTR={mtr:,.0f} | COGS={cogs/1e7:.2f}Cr | GM%={gm:.2f}%")
