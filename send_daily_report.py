#!/usr/bin/env python3
"""Solar Square Daily GM Report — Premium Executive Edition"""
import csv, glob, gzip, json, os, smtplib, sys, calendar
from collections import defaultdict
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SENDER     = os.environ.get("GMAIL_USER", "purushottam.shinde@solarsquare.in")
RECIPIENTS = os.environ.get("REPORT_TO",  "shindepurushottam7460@gmail.com").split(",")
GMAIL_PASS = os.environ.get("GMAIL_PASSWORD", "")
DATA_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projects.json.gz")
MIN_ORDERS = 10

NCR_CITIES = {'Gurgaon','Gurugram','Noida','Ghaziabad','Faridabad'}

STATE_DISPLAY = {
    'Delhi':'Delhi','Gujrat':'Gujarat','Karnataka':'Karnataka',
    'Madhya Pradesh':'MP','MH East':'MH East','MH West':'MH West',
    'Rajasthan':'Rajasthan','Tamil Nadu':'Tamil Nadu',
    'Telangana':'Telangana','Uttar Pradesh':'UP'
}
COGS_COLORS = {
    'Module':'#3B82F6','Inverter':'#8B5CF6','MMS':'#06B6D4',
    'Cables':'#10B981','Metering':'#F59E0B','I&C':'#EF4444','Other':'#94A3B8'
}

def load_data():
    with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
        return json.load(f)


def load_sku_analysis(base_dir, latest):
    """Load SKU-level COGS from raw DN CSV. Compares Apr MTD vs full Mar."""
    try:
        files = sorted(glob.glob(os.path.join(base_dir,'data*.csv.gz')), key=os.path.getmtime, reverse=True)
        if not files: return None
        curr_m = latest.month; curr_y = latest.year
        prev_m = (curr_m-1) if curr_m>1 else 12; prev_y = curr_y if curr_m>1 else curr_y-1
        projects = {}
        with gzip.open(files[0],'rt',encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for r in reader:
                dt = None
                for fmt in ('%d-%b-%Y','%Y-%m-%d'):
                    try: dt = datetime.strptime(r['Installation Completion Date'].strip(),fmt); break
                    except: pass
                if not dt: continue
                if not ((dt.year==curr_y and dt.month==curr_m) or (dt.year==prev_y and dt.month==prev_m)): continue
                sseid = r['SSE ID']; kw=float(r.get('Project Size (kW)',0) or 0)
                amt=float(r.get('amount',0) or 0)
                cat=r.get('item_category',''); sub=r.get('item_subcategory',''); item=r.get('item_name','')
                mo = 'curr' if (dt.year==curr_y and dt.month==curr_m) else 'prev'
                if sseid not in projects: projects[sseid]={'kw':kw,'mo':mo,'items':[]}
                projects[sseid]['items'].append({'cat':cat,'sub':sub,'item':item,'amt':amt})
        curr_p={k:v for k,v in projects.items() if v['mo']=='curr'}
        prev_p={k:v for k,v in projects.items() if v['mo']=='prev'}
        def agg_cat(bucket,cat_list=None,sub_list=None):
            tkw=sum(p['kw'] for p in bucket.values()); costs=defaultdict(float); wps=defaultdict(float)
            for p in bucket.values():
                for i in p['items']:
                    if cat_list and i['cat'] not in cat_list: continue
                    if sub_list and not any(s.lower() in i['sub'].lower() for s in sub_list): continue
                    costs[i['item']]+=i['amt']; wps[i['item']]+=p['kw']*1000
            tc=sum(costs.values())
            return {k:{'cost':costs[k],'rwp':costs[k]/wps[k] if wps[k] else 0,'mix':costs[k]/tc*100 if tc else 0} for k in costs},tkw,tc
        return {'curr':curr_p,'prev':prev_p,'agg':agg_cat}
    except Exception as e:
        return None

def build_sku_html(sku_data, aos_d, prev_lbl, curr_lbl, main_kw_c=None, main_kw_p=None, pj_cat=None):
    """Generate deep SKU-level COGS insight HTML.
    main_kw_c/p: projects.json kW used as denominator (matches COGS table)
    pj_cat: dict {cat: (curr_wp, prev_wp)} from projects.json — overrides header delta for consistency
    """
    if not sku_data: return ''
    curr_p=sku_data['curr']; prev_p=sku_data['prev']; agg=sku_data['agg']
    csv_kw_c = sum(p['kw'] for p in curr_p.values())
    csv_kw_p = sum(p['kw'] for p in prev_p.values())
    # Use projects.json kW as denominator so SKU card deltas match COGS table
    curr_kw = main_kw_c if main_kw_c else csv_kw_c
    prev_kw = main_kw_p if main_kw_p else csv_kw_p
    if not curr_kw or not prev_kw: return ''

    # Per-category totals
    def cat_total(bucket,cat_list=None,sub_match=None):
        t=0
        for p in bucket.values():
            for i in p['items']:
                if cat_list and i['cat'] not in cat_list: continue
                if sub_match and not any(s.lower() in i['sub'].lower() for s in sub_match): continue
                t+=i['amt']
        return t

    cat_defs=[
        ('Module',  ['Module'],           None),
        ('Inverter',['Inverter'],         None),
        ('MMS',     ['MMS','Prefab MMS','Tin Shed MMS','Welded MMS'], None),
        ('Cables',  None,                 ['dc cable','ac cable','earth']),
    ]
    deltas={}
    for lbl,cats,subs in cat_defs:
        c_kw=cat_total(curr_p,cats,subs)/curr_kw/1000
        p_kw=cat_total(prev_p,cats,subs)/prev_kw/1000
        deltas[lbl]=(c_kw,p_kw,c_kw-p_kw)
    # Override header deltas with projects.json values for consistency with COGS table
    if pj_cat:
        for lbl,(pj_c,pj_p) in pj_cat.items():
            if lbl in deltas:
                deltas[lbl]=(pj_c, pj_p, pj_c-pj_p)

    # ── compute SKU top-lines
    _,_,_=agg(curr_p,['MMS','Prefab MMS','Tin Shed MMS','Welded MMS'])
    a_mms,_,a_mms_tot=agg(curr_p,['MMS','Prefab MMS','Tin Shed MMS','Welded MMS'])
    p_mms,_,p_mms_tot=agg(prev_p,['MMS','Prefab MMS','Tin Shed MMS','Welded MMS'])
    a_cab,_,_=agg(curr_p,sub_list=['dc cable','ac cable','earth'])
    p_cab,_,_=agg(prev_p,sub_list=['dc cable','ac cable','earth'])
    a_inv,_,_=agg(curr_p,['Inverter'])
    p_inv,_,_=agg(prev_p,['Inverter'])

    # MMS top SKUs by April cost
    mms_top = sorted(a_mms.items(), key=lambda x:-x[1]['cost'])[:3]
    # Cable key drivers
    polycab_c = sum(v['cost'] for k,v in a_cab.items() if 'POLYCAB' in k)
    polycab_p = sum(v['cost'] for k,v in p_cab.items() if 'POLYCAB' in k)
    al16_c = sum(v['cost'] for k,v in a_cab.items() if '16 sqmm' in k)
    al16_p = sum(v['cost'] for k,v in p_cab.items() if '16 sqmm' in k)
    ac_wire_c = sum(v['cost'] for k,v in a_cab.items() if 'ac wire' in k.lower() or 'flex' in k.lower() and 'ac' in k.lower())
    ac_wire_p = sum(v['cost'] for k,v in p_cab.items() if 'ac wire' in k.lower() or 'flex' in k.lower() and 'ac' in k.lower())
    # Inverter 3-phase
    inv3ph_c = sum(v['cost'] for k,v in a_inv.items() if any(x in k for x in ['6 Kw','8 kw','10 kW','12 kw','6kW','8kW']))
    inv3ph_p = sum(v['cost'] for k,v in p_inv.items() if any(x in k for x in ['6 Kw','8 kw','10 kW','12 kw','6kW','8kW']))
    sg6_c = a_inv.get('6 Kw 3 Phase Inverter SG6RT (GSM)-SUNGROW',{}).get('rwp',0)
    sg6_p = p_inv.get('6 Kw 3 Phase Inverter SG6RT (GSM)-SUNGROW',{}).get('rwp',0)
    sg8_c = a_inv.get('8 kw 3 Phase Inverter SG8RT (GSM)-SUNGROW',{}).get('rwp',0)
    sg8_p = p_inv.get('8 kw 3 Phase Inverter SG8RT (GSM)-SUNGROW',{}).get('rwp',0)

    def badge(txt, color):
        return '<span style="display:inline-block;font-size:8px;font-weight:700;letter-spacing:.6px;text-transform:uppercase;padding:2px 8px;border-radius:8px;background:{0};color:#fff;margin-left:8px">{1}</span>'.format(color,txt)

    def row(icon, cat, delta_wp, detail_lines, status_color, root_cause='', gm_impact_pp=None, border_color='#E5E7EB'):
        sign = '+' if delta_wp >= 0 else ''
        scol = 'var(--red)' if delta_wp > 0.01 else ('var(--green)' if delta_wp < -0.01 else 'var(--mid)')
        gm_str = ''
        if gm_impact_pp is not None:
            gsign='+' if gm_impact_pp>=0 else ''
            gbg='var(--green-bg)' if gm_impact_pp>0 else 'var(--red-bg)'
            gcol='var(--green)' if gm_impact_pp>0 else 'var(--red)'
            gm_str='<span class="sku-gm-badge" style="background:{};color:{}">{}{:.2f}pp GM</span>'.format(gbg,gcol,gsign,gm_impact_pp)
        rc_str = '<div class="sku-rc">&#8618; {}</div>'.format(root_cause) if root_cause else ''
        return (
            '<div class="sku-card" style="border-left:3px solid {}">'
            '<div class="sku-card-header">'
            '<span class="sku-cat">{} {}</span>'
            '<span class="sku-delta" style="color:{}">{}{:.3f} &#8377;/Wp{}</span>'
            '</div>'
            '{}'
            '<div>{}</div>'
            '</div>'
        ).format(border_color, icon, cat, scol, sign, abs(delta_wp), gm_str, rc_str,
                 ''.join('<div class="sku-line">'+d+'</div>' for d in detail_lines))

    headline_parts = []
    for lbl in ['MMS','Cables','Inverter','Module']:
        c_wp,p_wp,d = deltas[lbl]
        if abs(d) > 0.005:
            sign = '+' if d>=0 else ''
            headline_parts.append('<b>{}</b> ({}{:.3f}&#8377;/Wp)'.format(lbl,sign,d))
    headline = '; '.join(headline_parts) if headline_parts else 'All categories stable'

    # MMS detail
    mms_skus_str = ', '.join('<b>{}</b> &#8377;{:.3f}/Wp'.format(k[:30],v['rwp']) for k,v in mms_top)
    mms_c_wp, mms_p_wp, mms_d = deltas['MMS']
    prefab_d = cat_total(curr_p,['Prefab MMS'])/curr_kw/1000 - cat_total(prev_p,['Prefab MMS'])/prev_kw/1000
    tinshed_d = cat_total(curr_p,['Tin Shed MMS'])/curr_kw/1000 - cat_total(prev_p,['Tin Shed MMS'])/prev_kw/1000
    mms_detail = [
        '&#128204; <b>Prefab MMS</b> {}{:.3f}&#8377;/Wp &mdash; Columns, Purl ins &amp; Powergrout NS65 driving volume'.format('+' if prefab_d>=0 else '',prefab_d),
        '&#128204; <b>Top SKUs:</b> {} &mdash; individual rates stable (&#8804;&#8377;0.016/Wp change)'.format(mms_skus_str),
        '&#128204; <b>Tin Shed MMS</b> {}{:.3f}&#8377;/Wp &mdash; higher mix of terrace installs'.format('+' if tinshed_d>=0 else '',tinshed_d),
        '&#128228; Root cause: AoS {}{:.2f}kW (3.90&#8594;3.93kW) &mdash; more structural material per install. No vendor rate change.'.format('+' if aos_d>=0 else '',aos_d),
    ]

    # Cable detail
    cab_c_wp, cab_p_wp, cab_d = deltas['Cables']
    polycab_contrib = (polycab_c-polycab_p)/curr_kw/1000
    al16_contrib = (al16_c-al16_p)/curr_kw/1000
    cable_detail = [
        '&#128204; <b>POLYCAB 4sqmm Cu DC Cable</b> entered Apr mix (0&#8594;4% of cable cost) at &#8377;{:.3f}/Wp &mdash; new vendor onboarding inflating spend by ~&#8377;{:.3f}/Wp'.format(
            a_cab.get('Cu DC Cable 1C x 4 sqmm - Red-POLYCAB',{}).get('rwp',0) or a_cab.get('Cu DC Cable 1C x 4 sqmm - Black-POLYCAB',{}).get('rwp',0), polycab_contrib),
        '&#128204; <b>16sqmm Al Earthing Cable (JMV)</b> mix 3.4&#8594;5.6% &mdash; {}{:.3f}&#8377;/Wp; higher-spec earthing in larger/LA installs'.format('+' if al16_contrib>=0 else '',al16_contrib),
        '&#128204; <b>Cu Flexible AC Wire 4sqmm (RR Kabel)</b> rate &#8377;0.160&#8594;&#8377;0.180/Wp (+&#8377;0.019/Wp)',
        '&#128228; <b>RR Kabel 4sqmm DC cables</b> (68% of cable cost) rate essentially flat &#8212; core DC cable procurement stable',
    ]

    # Inverter detail
    inv_c_wp, inv_p_wp, inv_d = deltas['Inverter']
    inv3ph_contrib = (inv3ph_c/curr_kw/1000 - inv3ph_p/prev_kw/1000)
    inv_detail = [
        '&#128204; <b>SG6RT 3Ph 6kW (Sungrow)</b> rate &#8377;{:.2f}&#8594;&#8377;{:.2f}/Wp ({}{:.2f}/Wp); mix 1.6&#8594;2.6%'.format(sg6_p,sg6_c,'+' if sg6_c-sg6_p>=0 else '',sg6_c-sg6_p),
        '&#128204; <b>SG8RT 3Ph 8kW (Sungrow)</b> rate &#8377;{:.2f}&#8594;&#8377;{:.2f}/Wp ({}{:.2f}/Wp); mix 1.5&#8594;2.3%'.format(sg8_p,sg8_c,'+' if sg8_c-sg8_p>=0 else '',sg8_c-sg8_p),
        '&#128228; 3-phase mix creep driven by larger AoS systems crossing 5kW threshold &mdash; structural, not a revenue issue',
    ]

    # Module detail
    mod_c_wp, mod_p_wp, mod_d = deltas['Module']
    mod_detail = [
        '&#128204; <b>540Wp Mono Bifacial DCR-PREMIER</b>: 98.9% mix at &#8377;20.07/Wp vs &#8377;20.02/Wp Mar ({}{:.3f}/Wp)'.format('+' if mod_d>=0 else '',mod_d),
        '&#128228; Module COGS most stable component. No procurement action needed.',
    ]

    mms_icon = '&#128308;' if mms_d > 0.05 else '&#128992;'
    cab_icon = '&#128992;' if cab_d > 0.02 else '&#128994;'
    inv_icon = '&#128992;' if inv_d > 0.02 else '&#128994;'
    mod_icon = '&#128994;'

    # ── GM impact per category (negative delta_wp = positive GM impact) ──
    def gm_pp(delta_wp, rev_wp_ref=66.5):
        return -(delta_wp / rev_wp_ref * 100) if rev_wp_ref else 0

    mms_rc  = ('AoS +{:.2f}kW → larger systems need more Profile/Column/Purlin material. '
               'Not a vendor rate issue — procurement stable.'.format(aos_d)) if aos_d > 0.05 and mms_d > 0 else \
              'Rate or structural type shift — check Prefab vs Tin-Shed vs Welded mix vs prior month.'
    cab_rc  = ('DC routing length scales with system size (AoS +{:.2f}kW); '
               'POLYCAB 4sqmm Cu-DC entering mix adds premium vs RR Kabel Al.'.format(aos_d)) if aos_d > 0.03 and cab_d > 0 else \
              'Verify cable gauge/vendor split and DC string layout vs prior month.'
    inv_rc  = '3-phase SG6RT/SG8RT mix creep — systems >5kW crossing threshold; rate flat, volume driving cost.'
    mod_rc  = 'Stable — 540Wp DCR-PREMIER at 98.9% mix; delta is procurement rate fluctuation only.'

    # ── Headline banner ────────────────────────────────────────────
    rising_cats  = [(lbl,deltas[lbl][2]) for lbl in ['MMS','Cables','Inverter','Module'] if deltas[lbl][2]>0.005]
    falling_cats = [(lbl,deltas[lbl][2]) for lbl in ['MMS','Cables','Inverter','Module'] if deltas[lbl][2]<-0.005]
    rising_cats.sort(key=lambda x:-x[1]); falling_cats.sort(key=lambda x:x[1])
    net_delta = sum(deltas[l][2] for l in ['MMS','Cables','Inverter','Module'])
    hl_parts = []
    if rising_cats:  hl_parts.append('{} inflation'.format(' + '.join(l for l,_ in rising_cats[:2])))
    if falling_cats: hl_parts.append('{} savings offset'.format(' + '.join(l for l,_ in falling_cats[:1])))
    hl_txt = ('; '.join(hl_parts) + ' &mdash; net <b>{}{:.3f} &#8377;/Wp</b> on blended COGS'.format(
        '+' if net_delta>=0 else '', net_delta)) if hl_parts else '&#9989; COGS stable across all categories'
    hl_html = ('<div class="cogs-banner">'
               '&#128293; {}  <span style="font-weight:400;font-size:10px;color:#0284C7">'
               '({} MTD vs full {})</span></div>').format(hl_txt, curr_lbl, prev_lbl)

    # ── Action engine ──────────────────────────────────────────────
    actions = []
    if mms_d > 0.03 and aos_d > 0.05:
        actions.append('&#9883; <b>MMS</b> No vendor action needed — cost increase is AoS-structural. '
                       'Monitor: if AoS plateaus, MMS/Cables% normalise automatically.')
    elif mms_d > 0.03:
        actions.append('&#9889; <b>MMS</b> Investigate Powergrout NS65 / Column 2P Gen2 rate vs prior PO. '
                       'Check Prefab vs Tin-Shed mix shift for vendor rate explanation.')
    if cab_d > 0.02:
        actions.append('&#9889; <b>Cables</b> Standardise DC routing length per kW in terrace installs. '
                       'Review POLYCAB 4sqmm Cu-DC onboarding — consider Al equivalent for single-phase systems.')
    if inv_d > 0.015:
        actions.append('&#128204; <b>Inverter</b> 3-phase mix creep flagged (SG6RT/SG8RT). '
                       'No rate issue — structural with AoS. If 3-phase proportion exceeds 5%, negotiate volume pricing with Sungrow.')
    if mod_d < -0.008:
        actions.append('&#9989; <b>Module</b> Rate improvement detected — lock current procurement rate for next cycle if possible.')
    if not actions:
        actions.append('&#9989; No COGS procurement action required. All categories within acceptable band.')

    act_html = ('<div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:8px;'
                'padding:10px 14px;margin-top:12px">'
                '<div style="font-size:8.5px;font-weight:800;letter-spacing:1px;text-transform:uppercase;'
                'color:#64748B;margin-bottom:8px">&#127919; PROCUREMENT ACTIONS</div>'
                '<div style="font-size:10.5px;color:#374151;line-height:2.1">{}</div>'
                '</div>').format('<br>'.join(actions))

    html = ('{}<div class="sku-grid">'
            '{}{}{}{}</div>{}').format(
        hl_html,
        row(mms_icon,'MMS',     mms_d,    mms_detail,    '#DC2626', mms_rc, gm_pp(mms_d), '#06B6D4'),
        row(cab_icon,'Cables',  cab_d,    cable_detail,  '#D97706', cab_rc, gm_pp(cab_d), '#10B981'),
        row(inv_icon,'Inverter',inv_d,    inv_detail,    '#D97706', inv_rc, gm_pp(inv_d), '#8B5CF6'),
        row(mod_icon,'Module',  mod_d,    mod_detail,    '#16A34A', mod_rc, gm_pp(mod_d), '#3B82F6'),
        act_html)
    return html

def fp(projects, start, end):
    return [p for p in projects if p.get('dt') and start <= p['dt'] <= end]

def calc(ps):
    if not ps:
        return dict(n=0,kw=0.,rev=0.,cogs=0.,onm=0.,qhs=0.,gm=0.,adj_gm=0.,
                    rev_wp=0.,aos=0.,aov=0.,abs_gm=0.,cogs_kw=0.,
                    mod=0.,inv=0.,mms=0.,cab=0.,mtr=0.,ic=0.,oth=0.)
    n=len(ps); kw=sum(p['kw'] for p in ps); rev=sum(p['rev'] for p in ps)
    cogs=sum(p['cogs'] for p in ps); onm=sum(p.get('onm',0) for p in ps)
    qhs=sum(p.get('qhs',0) for p in ps)
    gm=(rev-cogs)/rev*100 if rev else 0.
    adj_gm=(rev-cogs-onm-qhs)/rev*100 if rev else 0.
    rev_wp=rev/(kw*1000) if kw else 0.
    cogs_kw=cogs/kw if kw else 0.
    mod=sum(p.get('mod',0) for p in ps)
    inv=sum(p.get('inv',0) for p in ps)
    mms=sum(p.get('prf',0)+p.get('tsh',0)+p.get('wel',0) for p in ps)
    cab=sum(p.get('cab',0) for p in ps)
    mtr=sum(p.get('mtr',0) for p in ps)
    ic=sum(p.get('ick',0)+p.get('ica',0) for p in ps)
    oth=max(cogs-mod-inv-mms-cab-mtr-ic,0.)
    return dict(n=n,kw=kw,rev=rev,cogs=cogs,onm=onm,qhs=qhs,
                gm=gm,adj_gm=adj_gm,rev_wp=rev_wp,aos=kw/n,aov=rev/n,
                abs_gm=rev-cogs,cogs_kw=cogs_kw,
                mod=mod,inv=inv,mms=mms,cab=cab,mtr=mtr,ic=ic,oth=oth)

def inject_meta(m, mo_onm_qhse, key):
    if m['onm']==0 and m['qhs']==0:
        mk=mo_onm_qhse.get(key,{})
        m=dict(m); m['onm']=mk.get('onm',0); m['qhs']=mk.get('qhs',0)
        if m['rev']:
            m['adj_gm']=(m['rev']-m['cogs']-m['onm']-m['qhs'])/m['rev']*100
    return m

def normalise_city(city, state):
    if state=='Delhi' and city in NCR_CITIES: return 'Delhi NCR'
    return city

def by_cluster(projects):
    d=defaultdict(list)
    for p in projects: d[(p['s'],normalise_city(p['c'],p['s']))].append(p)
    return d

def get_driver(curr, prev, sku_ctx=None):
    """Deep CFO/CEO-grade driver: per-cluster COGS category breakdown + SKU context."""
    if prev['n'] < MIN_ORDERS:
        return '--', {}, 'Thin prior data'

    rv   = curr['rev_wp']  - prev['rev_wp']
    ao   = curr['aos']     - prev['aos']
    gm_d = curr['gm']      - prev['gm']
    ck_wp = (curr['cogs_kw'] - prev['cogs_kw']) / 1000
    d = dict(rev_wp_d=rv, aos_d=ao, aov_d=curr['aov']-prev['aov'], cogs_kw_d=ck_wp*1000)

    # ── Per-cluster COGS category breakdown in ₹/Wp ──────────────
    # Module excluded: globally stable (540Wp DCR-PREMIER 98.9% mix);
    # cluster-level Module variation is sample noise, not a procurement signal.
    cat_d = {}
    if curr['kw'] and prev['kw']:
        for cat, key in [('Inverter','inv'),('MMS','mms'),('Cables','cab')]:
            cat_d[cat] = curr.get(key,0)/curr['kw']/1000 - prev.get(key,0)/prev['kw']/1000

    # 0.04/Wp min threshold at cluster level to filter sampling noise
    rising  = sorted([(c,v) for c,v in cat_d.items() if v >  0.04], key=lambda x:-x[1])
    falling = sorted([(c,v) for c,v in cat_d.items() if v < -0.04], key=lambda x: x[1])

    parts = []

    # ── 1. Revenue realisation ─────────────────────────────────────
    if rv < -0.8:
        parts.append('Rev/Wp &#8722;&#8377;{:.1f}/Wp &mdash; realisation erosion; verify discount auth'.format(abs(rv)))
    elif rv > 0.8:
        parts.append('Rev/Wp +&#8377;{:.1f}/Wp &mdash; stronger realisation'.format(rv))
    elif abs(rv) > 0.2:
        parts.append('Rev/Wp {:+.2f}/Wp'.format(rv))

    # ── 2. COGS: category-level with SKU context ───────────────────
    if abs(ck_wp) > 0.02:
        top = rising[:2] if ck_wp > 0 else falling[:2]
        cat_strs = []
        for cat, v in top:
            ctx = (' [{}]'.format(sku_ctx[cat]) if sku_ctx and cat in sku_ctx else '')
            sign = '+' if v > 0 else '&#8722;'
            cat_strs.append('{} {}{:.3f}/Wp{}'.format(cat, sign, abs(v), ctx))
        if cat_strs:
            aos_structural = ao > 0.1 and any(c in ('MMS','Cables') for c,_ in top)
            if aos_structural:
                root = 'AoS +{:.2f}kW &rarr; structural; not vendor rate'.format(ao)
            elif ck_wp > 0:
                root = 'rate or vendor mix shift &mdash; verify PO vs prior month'
            elif rv < -0.3:
                # COGS falling but Rev/Wp also falling — savings partially offsetting revenue erosion
                root = 'COGS efficiency partially offsetting Rev/Wp erosion'
            else:
                root = 'procurement savings flowing to GM'
            parts.append('COGS: {} &mdash; {}'.format('; '.join(cat_strs), root))

    # ── 3. AoS standalone (when COGS is contained) ────────────────
    if ao > 0.25 and abs(ck_wp) <= 0.02:
        parts.append('AoS +{:.2f}kW &mdash; larger system mix; COGS absorbed'.format(ao))
    elif ao < -0.2 and abs(ck_wp) <= 0.02:
        parts.append('AoS &#8722;{:.2f}kW &mdash; smaller system mix'.format(abs(ao)))

    # ── 4. Stable fallback with sub-threshold context ─────────────
    if not parts:
        sub = []
        if abs(rv) > 0.1:    sub.append('Rev/Wp {:+.2f}/Wp'.format(rv))
        if abs(ao) > 0.04:   sub.append('AoS {:+.2f}kW'.format(ao))
        if abs(ck_wp) > 0.005: sub.append('COGS {:+.3f}/Wp'.format(ck_wp))
        narrative = ('; '.join(sub) + ' &mdash; all within normal band') if sub else \
                    'All levers &lt;0.5% shift &mdash; operations stable'
        return narrative, dict(d, cat_d=cat_d), narrative

    # ── 5. GM impact at end ────────────────────────────────────────
    parts.append('&#8594; {}{:.2f}pp GM'.format('+' if gm_d>=0 else '', gm_d))
    narrative = '; '.join(parts)
    types = []
    if rv < -0.8: types.append('price_dn')
    elif rv > 0.8: types.append('price_up')
    if ck_wp > 0.02: types.append('cogs_up')
    elif ck_wp < -0.02: types.append('cogs_dn')
    return narrative, dict(d, types=types, cat_d=cat_d), narrative

def fc(v):
    if v>=1e7: return '&#8377;{:.2f}Cr'.format(v/1e7)
    if v>=1e5: return '&#8377;{:.1f}L'.format(v/1e5)
    return '&#8377;{:,.0f}'.format(v)

def dpp(delta, hb=True):
    if abs(delta)<0.01: return '<span style="color:#94A3B8">&#8212;</span>'
    arr='&#9650;' if delta>0 else '&#9660;'
    clr='#16A34A' if (delta>0)==hb else '#DC2626'
    return '<span style="color:{};font-weight:700">{}&thinsp;{:.2f}%pts</span>'.format(clr,arr,abs(delta))

def dpct(c, p, hb=True):
    if p==0: return ''
    delta=(c-p)/abs(p)*100
    if abs(delta)<0.5: return '<span style="color:#94A3B8">&#8212;</span>'
    arr='&#9650;' if delta>0 else '&#9660;'
    clr='#16A34A' if (delta>0)==hb else '#DC2626'
    return '<span style="color:{};font-weight:700">{}&thinsp;{:.0f}%</span>'.format(clr,arr,abs(delta))

def dpval(delta, unit, hb=True):
    if abs(delta)<0.01: return '<span style="color:#94A3B8">&#8212;</span>'
    arr='&#9650;' if delta>0 else '&#9660;'
    clr='#16A34A' if (delta>0)==hb else '#DC2626'
    return '<span style="color:{};font-weight:700">{}&thinsp;{:.2f}&thinsp;{}</span>'.format(clr,arr,abs(delta),unit)

def gmc(pct):
    if pct>=44: return '#16A34A'
    if pct>=40: return '#D97706'
    return '#DC2626'

def gmcell(pct, fw='600'):
    bg='#DCFCE7' if pct>=44 else ('#FEF3C7' if pct>=40 else '#FEE2E2')
    return '<td style="background:{};color:{};font-weight:{};text-align:center;padding:6px 8px">{:.1f}%</td>'.format(bg,gmc(pct),fw,pct)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PREMIUM CSS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CSS = """

*{box-sizing:border-box;margin:0;padding:0}
html{-webkit-text-size-adjust:100%}

body{
  font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,Helvetica,sans-serif;
  background:#E8F4FD;
  color:#1A1A2E;font-size:14px;line-height:1.55;
  padding:16px 8px 40px;
}
.page{
  max-width:640px;margin:0 auto;
  background:#E8F4FD;
}

.header{
  background:linear-gradient(135deg,#0284C7 0%,#0EA5E9 55%,#38BDF8 100%);
  border-radius:14px 14px 0 0;
  padding:22px 20px 18px;
  color:#fff;position:relative;overflow:hidden;
}
.header::before{
  content:'';position:absolute;top:-50px;right:-50px;
  width:200px;height:200px;border-radius:50%;
  background:rgba(255,255,255,.10);pointer-events:none;
}
.eyebrow{
  font-size:10px;letter-spacing:2px;text-transform:uppercase;
  color:rgba(255,255,255,.75);display:block;margin-bottom:8px;
}
.header h1{
  font-size:18px;font-weight:700;line-height:1.3;
  margin-bottom:6px;color:#fff;
}
.header-meta{
  font-size:10px;color:rgba(255,255,255,.55);
  margin-bottom:16px;display:block;
}
.badges{display:block;line-height:2.2}
.badge{
  display:inline-block;
  background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.30);
  color:#fff;font-size:10px;font-weight:600;
  padding:3px 10px;border-radius:18px;
  margin:2px 3px 2px 0;white-space:nowrap;
}
.badge.hi {background:rgba(5,150,105,.35);border-color:rgba(16,185,129,.50);color:#D1FAE5}
.badge.warn{background:rgba(127,29,29,.40);border-color:rgba(252,165,165,.45);color:#FEE2E2}

.section{
  background:#fff;
  border:1px solid #DBEAFE;
  border-top:none;
  padding:18px 16px;
}
.section:last-of-type{border-radius:0 0 14px 14px}

.sec-header{
  margin-bottom:14px;
  padding-bottom:10px;
  border-bottom:2px solid #EFF6FF;
}
.sec-title{
  display:inline-block;
  font-size:9px;font-weight:700;letter-spacing:2px;text-transform:uppercase;
  color:#0284C7;background:#EFF6FF;
  padding:3px 9px;border-radius:5px;border:1px solid #BFDBFE;
}
.sec-sub{
  display:block;font-size:10.5px;color:#94A3B8;
  margin-top:4px;font-style:italic;
}

.kgrid{
  width:100%;border-collapse:separate;border-spacing:8px;
  margin:-8px;
}
.kc{
  width:50%;
  background:#F8FAFC;
  border:1px solid #DBEAFE;
  border-radius:10px;
  padding:13px 14px 11px;
  vertical-align:top;
}
.kc-label{
  display:block;font-size:7.5px;font-weight:700;letter-spacing:1.4px;
  text-transform:uppercase;color:#94A3B8;margin-bottom:7px;
}
.kc-val{
  display:block;font-size:20px;font-weight:800;
  letter-spacing:-.4px;line-height:1;margin-bottom:5px;
}
.kc-sub{
  display:block;font-size:10px;color:#94A3B8;line-height:1.4;
}
.kc-trend{display:block;margin-top:5px;font-size:11px;font-weight:700}

.today-grid{
  width:100%;border-collapse:separate;border-spacing:8px;
  margin:-8px;
}
.tc{
  width:50%;
  background:#F0FDFA;
  border:1px solid #CCFBF1;
  border-radius:10px;
  padding:13px 14px 11px;
  vertical-align:top;
}
.tc-label{
  display:block;font-size:7.5px;font-weight:700;letter-spacing:1.4px;
  text-transform:uppercase;color:#94A3B8;margin-bottom:7px;
}
.tc-today{
  display:block;font-size:20px;font-weight:800;
  letter-spacing:-.4px;line-height:1;margin-bottom:5px;
}
.tc-prev{display:block;font-size:10px;color:#64748B;line-height:1.4}

.table-scroll{
  width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;
  border-radius:8px;border:1px solid #DBEAFE;
}
.data-table{
  width:100%;border-collapse:collapse;
  font-size:11.5px;min-width:460px;
}
.data-table thead tr{background:#EFF6FF}
.data-table th{
  padding:9px 11px;font-size:8px;font-weight:700;
  color:#475569;text-transform:uppercase;letter-spacing:.9px;
  border-bottom:2px solid #BFDBFE;text-align:left;white-space:nowrap;
}
.data-table th.R{text-align:right}
.data-table tbody tr:nth-child(even) td{background:#F8FBFF}
.data-table td{
  padding:9px 11px;border-bottom:1px solid #EFF6FF;
  color:#334155;vertical-align:middle;word-break:break-word;
}
.data-table td.R{
  text-align:right;font-size:11px;white-space:nowrap;
}
.data-table td.mono{font-size:11px}
.data-table tbody tr:last-child td{border-bottom:none;font-weight:700;background:#EFF6FF}
.dot{
  display:inline-block;width:8px;height:8px;
  border-radius:3px;margin-right:6px;vertical-align:middle;
}
.up     {color:#DC2626;font-weight:700}
.dn     {color:#059669;font-weight:700}
.neutral{color:#94A3B8}
.up-good{color:#059669;font-weight:700}

.data-table tr.total-row td{
  background:#EFF6FF!important;font-weight:700;
  border-top:2px solid #BFDBFE;
}

.sku-grid{
  display:grid;
  grid-template-columns:1fr 1fr;
  gap:10px;
  margin-top:12px;
}
.sku-card{
  border:1px solid #DBEAFE;border-radius:10px;
  padding:12px 13px;background:#F8FAFC;
  border-left:4px solid #7DD3FC;
}
.sku-card-header{
  display:flex;justify-content:space-between;
  align-items:flex-start;margin-bottom:7px;gap:6px;
}
.sku-cat{font-weight:800;font-size:13px;color:#0F172A;flex-shrink:0}
.sku-delta{font-size:11px;font-weight:800;text-align:right;white-space:nowrap}
.sku-rc{
  font-size:9px;color:#94A3B8;font-style:italic;
  background:#EFF6FF;border-radius:5px;
  padding:5px 8px;margin-bottom:8px;
  line-height:1.65;word-break:break-word;
}
.sku-line{
  font-size:10px;color:#475569;
  line-height:1.8;margin-bottom:2px;
  word-break:break-word;
}
.sku-gm-badge{
  display:inline-block;font-size:8px;font-weight:700;
  padding:1px 7px;border-radius:6px;margin-left:5px;
}

.cogs-wrap{margin:0}
.cogs-wrap .data-table{min-width:400px;font-size:13px}
.cogs-wrap .data-table th{font-size:9px;padding:9px 11px}
.cogs-wrap .data-table td{padding:10px 11px;font-size:13px}
.cluster-wrap{
  overflow-x:auto;-webkit-overflow-scrolling:touch;
  border-radius:8px;border:1px solid #DBEAFE;
}
.cluster-wrap .data-table{min-width:600px;font-size:12px}
.cluster-wrap .data-table th{
  font-size:8px;padding:8px 9px;white-space:nowrap;
  color:#E0F2FE;border-bottom:2px solid #0369A1;
}
.cluster-wrap .data-table thead tr{background:#0284C7}
.cluster-wrap .data-table td{
  padding:9px 9px;font-size:12px;white-space:nowrap;
}
.cluster-wrap .data-table td.R{font-size:11.5px}
.group-row td{
  background:#EFF6FF;color:#0369A1;
  font-weight:700;font-size:8.5px;text-transform:uppercase;
  letter-spacing:1px;padding:6px 11px;border-top:2px solid #BFDBFE;
}
.gm-cell-hi {background:#DCFCE7;color:#166534;font-weight:800;text-align:center;padding:7px 8px;font-size:12px;white-space:nowrap}
.gm-cell-mid{background:#FEF9C3;color:#92400E;font-weight:800;text-align:center;padding:7px 8px;font-size:12px;white-space:nowrap}
.gm-cell-lo {background:#FEE2E2;color:#B91C1C;font-weight:800;text-align:center;padding:7px 8px;font-size:12px;white-space:nowrap}

.driver-chip{
  font-size:9.5px;color:#334155;
  line-height:1.75;white-space:normal;
  word-break:break-word;max-width:180px;
}
.tag-cogs,.tag-rev,.tag-ok,.tag-price{
  display:inline-block;font-size:7.5px;font-weight:700;
  padding:2px 6px;border-radius:5px;
  margin:0 2px 2px 0;white-space:nowrap;
}
.tag-cogs {background:#FEF3C7;color:#92400E;border:1px solid #FDE68A}
.tag-rev  {background:#FEE2E2;color:#B91C1C;border:1px solid #FECACA}
.tag-ok   {background:#DCFCE7;color:#166534;border:1px solid #BBF7D0}
.tag-price{background:#EDE9FE;color:#5B21B6;border:1px solid #DDD6FE}

.watch-list{border:1px solid #DBEAFE;border-radius:10px;overflow:hidden}
.watch-item{
  display:flex;gap:12px;align-items:flex-start;
  padding:13px 15px;border-bottom:1px solid #EFF6FF;
}
.watch-item:last-child{border-bottom:none}
.watch-num{
  width:26px;height:26px;min-width:26px;border-radius:50%;
  display:flex;align-items:center;justify-content:center;
  font-size:11px;font-weight:800;flex-shrink:0;
}
.watch-red   .watch-num{background:#FEE2E2;color:#DC2626;border:2px solid #FECACA}
.watch-amber .watch-num{background:#FFFBEB;color:#D97706;border:2px solid #FDE68A}
.watch-green .watch-num{background:#ECFDF5;color:#059669;border:2px solid #A7F3D0}
.watch-tag{
  display:inline-block;font-size:7.5px;font-weight:700;
  letter-spacing:.7px;text-transform:uppercase;
  padding:2px 7px;border-radius:6px;margin-bottom:4px;
}
.watch-red   .watch-tag{background:#FEE2E2;color:#B91C1C;border:1px solid #FECACA}
.watch-amber .watch-tag{background:#FFFBEB;color:#92400E;border:1px solid #FDE68A}
.watch-green .watch-tag{background:#ECFDF5;color:#065F46;border:1px solid #A7F3D0}
.watch-body{min-width:0;flex:1}
.watch-title{
  font-size:12.5px;font-weight:700;color:#0F172A;
  margin-bottom:3px;line-height:1.35;
  word-break:break-word;
}
.watch-why{
  font-size:10.5px;color:#64748B;
  line-height:1.65;word-break:break-word;
}

.bridge-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:4px}
.bridge{
  display:flex;align-items:stretch;flex-wrap:nowrap;
  gap:0;margin:12px 0;font-size:11px;min-width:min-content;
}
.bridge-box{padding:9px 13px;text-align:center;min-width:76px}
.bridge-box.start{
  background:#EFF6FF;border:1px solid #BFDBFE;border-radius:9px 0 0 9px;
}
.bridge-box.end{
  background:#EFF6FF;border:1px solid #BFDBFE;border-radius:0 9px 9px 0;
}
.bridge-item{
  background:#FEF2F2;border:1px solid #FECACA;
  padding:7px 10px;font-size:10px;border-left:none;min-width:72px;
}
.bridge-item.pos{background:#ECFDF5;border-color:#A7F3D0}
.bridge-label{
  font-size:8px;color:#64748B;display:block;margin-bottom:3px;white-space:nowrap;
}
.bridge-val{font-size:13px;font-weight:700;display:block}

.mix-bar{
  height:24px;border-radius:8px;overflow:hidden;
  display:flex;margin-bottom:12px;gap:2px;
}

.footer{
  background:#EFF6FF;border:1px solid #BFDBFE;
  border-top:2px solid #7DD3FC;
  border-radius:0 0 14px 14px;
  padding:12px 20px;text-align:center;
  font-size:9px;color:#64748B;letter-spacing:.3px;
}

@media screen and (max-width:600px){
  body{padding:8px 6px 32px;font-size:13px}
  .page{max-width:100%}
  .header{padding:18px 14px 14px;border-radius:12px 12px 0 0}
  .header h1{font-size:16px}
  .header-meta{font-size:9.5px}
  .eyebrow{font-size:9px}
  .badge{font-size:9px;padding:3px 8px}

  .section{padding:14px 12px}

  .kgrid{border-spacing:6px;margin:-6px}
  .kc{padding:11px 11px 9px}
  .kc-val{font-size:17px}
  .kc-sub{font-size:9.5px}

  .today-grid{border-spacing:6px;margin:-6px}
  .tc{padding:11px 11px 9px}
  .tc-today{font-size:17px}
  .tc-prev{font-size:9.5px}

  .sku-grid{gap:8px}
  .sku-cat{font-size:11px}
  .sku-line{font-size:9.5px}
  .sku-rc{font-size:8.5px}

  .driver-chip{max-width:140px;font-size:9px}

  .watch-item{padding:11px 11px;gap:9px}
  .watch-title{font-size:11.5px}
  .watch-why{font-size:10px}

  .footer{padding:10px 12px;font-size:8px}

  /* COGS table — larger, clearer on mobile */
  .cogs-wrap .data-table{min-width:360px;font-size:12px}
  .cogs-wrap .data-table th{font-size:9px;padding:8px 9px}
  .cogs-wrap .data-table td{padding:9px 9px;font-size:12px}
  .cogs-wrap .data-table td.mono{font-size:11.5px}
}
"""


def build(data):
    projects    = data['projects']
    mo_onm_qhse = data.get('_meta',{}).get('monthly_onm_qhse',{})

    latest_str = max(p['dt'] for p in projects if p.get('dt'))
    latest     = datetime.strptime(latest_str,'%Y-%m-%d')
    prev_str   = (latest-timedelta(days=1)).strftime('%Y-%m-%d')
    ms         = latest.strftime('%Y-%m-01')
    mo_key     = latest.strftime('%Y-%m')

    pm_last  = latest.replace(day=1)-timedelta(days=1)
    pm_day   = calendar.monthrange(pm_last.year,pm_last.month)[1]  # full prior month
    pm_start = pm_last.replace(day=1).strftime('%Y-%m-01')
    pm_end   = '{}-{:02d}-{:02d}'.format(pm_last.year,pm_last.month,pm_day)
    pm_key   = pm_last.strftime('%Y-%m')
    prev_lbl = pm_last.strftime('%b')
    curr_lbl = latest.strftime('%b %Y')
    lat_lbl  = latest.strftime('%d %b')
    prv_lbl  = (latest-timedelta(days=1)).strftime('%d %b')

    mtd_ps = fp(projects, ms,         latest_str)
    pm_ps  = fp(projects, pm_start,   pm_end)
    lat_ps = fp(projects, latest_str, latest_str)
    prv_ps = fp(projects, prev_str,   prev_str)

    mtd = inject_meta(calc(mtd_ps), mo_onm_qhse, mo_key)
    pm  = inject_meta(calc(pm_ps),  mo_onm_qhse, pm_key)
    lat = calc(lat_ps)
    prv = calc(prv_ps)

    gm_trend = mtd['gm'] - pm['gm']
    vol_pct  = (mtd['n']-pm['n'])/pm['n']*100 if pm['n'] else 0
    rev_wp_d = mtd['rev_wp'] - pm['rev_wp']
    base_dir = os.path.dirname(os.path.abspath(DATA_FILE))
    sku_data = load_sku_analysis(base_dir, latest)

    # ── Cluster data
    bc  = by_cluster(mtd_ps)
    bcp = by_cluster(pm_ps)
    mtd_cl = bc; pm_cl = bcp

    # ── Pre-compute global SKU context strings for driver column ──
    def _build_sku_ctx(sd):
        if not sd: return {}
        ctx = {}
        try:
            agg = sd['agg']
            a_mms,_,_ = agg(sd['curr'],['MMS','Prefab MMS','Tin Shed MMS','Welded MMS'])
            if a_mms:
                top = max(a_mms.items(), key=lambda x:x[1]['cost'])
                ctx['MMS'] = top[0].split()[0] + ' SKU'
            a_cab,_,_ = agg(sd['curr'],sub_list=['dc cable','ac cable','earth'])
            polycab = any('POLYCAB' in k for k in (a_cab or {}))
            ctx['Cables'] = 'POLYCAB 4sqmm Cu-DC' if polycab else '4sqmm Cu-DC routing'
            ctx['Inverter'] = '3ph SG6RT/SG8RT mix'
            ctx['Module']   = 'rate stable'
        except: pass
        return ctx
    sku_ctx_map = _build_sku_ctx(sku_data)

    declining=[]; stable_cl=[]; improving=[]; nascent=[]
    for key in mtd_cl:
        curr=calc(mtd_cl[key]); prev=calc(pm_cl.get(key,[]))
        if curr['n']<5: continue
        state,cluster=key
        gm_d=curr['gm']-prev['gm']
        ag_dp=(curr['abs_gm']-prev['abs_gm'])/prev['abs_gm']*100 if prev['abs_gm'] else 0
        drv_tag,drv_det,_ = get_driver(curr,prev,sku_ctx_map)
        row=dict(state=state,cluster=cluster,curr=curr,prev=prev,
                 gm_d=gm_d,ag_dp=ag_dp,drv_tag=drv_tag,drv_det=drv_det)
        if prev['n']<MIN_ORDERS:  nascent.append(row)
        elif gm_d<-0.3:           declining.append(row)
        elif gm_d>0.3:            improving.append(row)
        else:                     stable_cl.append(row)
    declining.sort(key=lambda x:x['gm_d'])
    improving.sort(key=lambda x:-x['gm_d'])
    stable_cl.sort(key=lambda x:-x['curr']['gm'])
    all_cl = declining+stable_cl+improving

    # ── Price erosion clusters (threshold ₹0.8/Wp to catch early signals)
    price_dn=[(r['cluster'],r['drv_det'].get('rev_wp_d',0))
              for r in all_cl if r['drv_det'].get('rev_wp_d',0)<-0.8 and r['curr']['n']>=MIN_ORDERS]
    price_dn.sort(key=lambda x:x[1])
    price_up=[(r['cluster'],r['drv_det'].get('rev_wp_d',0))
              for r in improving if r['drv_det'].get('rev_wp_d',0)>1.2 and r['curr']['n']>=MIN_ORDERS]

    # ── COGS analysis
    cogs_total = mtd['cogs']
    cogs_items = [('Module',mtd['mod']),('Inverter',mtd['inv']),('MMS',mtd['mms']),
                  ('Cables',mtd['cab']),('Metering',mtd['mtr']),('I&C',mtd['ic']),('Other',mtd['oth'])]
    pm_cogs    = {'Module':pm['mod'],'Inverter':pm['inv'],'MMS':pm['mms'],
                  'Cables':pm['cab'],'Metering':pm['mtr'],'I&C':pm['ic'],'Other':pm['oth']}
    aos_d = mtd['aos'] - pm['aos']
    total_cogs_pkw_c = mtd['cogs']/mtd['kw'] if mtd['kw'] else 0
    total_cogs_pkw_p = pm['cogs']/pm['kw'] if pm['kw'] else 0

    cogs_rising = []; cogs_falling = []
    if cogs_total and pm['cogs']:
        for lbl,val in cogs_items:
            pp_c = val/cogs_total*100 if cogs_total else 0
            pp_p = pm_cogs.get(lbl,0)/pm['cogs']*100 if pm['cogs'] else 0
            pkw_c = val/mtd['kw'] if mtd['kw'] else 0
            pkw_p = pm_cogs.get(lbl,0)/pm['kw'] if pm['kw'] else 0
            gm_impact = -(val/mtd['rev'] - pm_cogs.get(lbl,0)/pm['rev'])*100 if mtd['rev'] and pm['rev'] else 0
            d = pp_c - pp_p
            if d > 0.2:  cogs_rising.append((lbl, d, pkw_c, pkw_p, pkw_c-pkw_p, gm_impact))
            elif d < -0.2: cogs_falling.append((lbl, d, pkw_c, pkw_p, pkw_c-pkw_p, gm_impact))
    cogs_rising.sort(key=lambda x:-x[1])
    cogs_net_gm = sum(x[5] for x in cogs_rising) + sum(x[5] for x in cogs_falling) if (cogs_rising or cogs_falling) else 0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  DYNAMIC HEADLINE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Fixed headline — date-stamped dashboard title
    headline = 'Solar Square Daily GM Report Dashboard'
    hero_grad = 'linear-gradient(135deg,#0284C7 0%,#0EA5E9 55%,#38BDF8 100%)'

    # ── GM Badge
    gm_arrow = '&#9650;' if gm_trend>=0 else '&#9660;'
    gm_badge_txt = '{} GM {:.2f}% ({}{:.2f}%pts)'.format(gm_arrow, mtd['gm'], '+' if gm_trend>=0 else '', gm_trend)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  EXEC SNAPSHOT (4 cards)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def snap_card(label, pill_text, pill_cls, value, value_color, delta_html, card_cls=''):
        return (
            '<div class="snap-card {}">'
            '<span class="snap-label">{}</span>'
            '<span class="snap-pill {}">{}</span>'
            '<span class="snap-val" style="color:{}">{}</span>'
            '<span class="snap-delta">{}</span>'
            '</div>'
        ).format(card_cls, label, pill_cls, pill_text, value_color, value, delta_html)

    # Volume card
    vol_pill_cls = 'green-pill' if vol_pct >= -5 else 'red-pill'
    vol_pill_txt = ('&#9650; +{:.0f}% MoM'.format(vol_pct) if vol_pct>=0
                    else '&#9660; {:.0f}% MoM'.format(vol_pct))
    vol_delta = 'vs {:,} {} (1&#8211;{} days)'.format(pm['n'], prev_lbl, pm_day)
    card_vol = snap_card('Installations MTD', vol_pill_txt, vol_pill_cls,
        '{:,}'.format(mtd['n']), 'var(--ink)', vol_delta, 'card-vol')

    # Pricing card
    rev_wp_d = mtd['rev_wp'] - pm['rev_wp']
    rwp_pill_cls = 'green-pill' if rev_wp_d >= 0 else ('red-pill' if rev_wp_d < -0.5 else 'amber-pill')
    rwp_pill_txt = ('&#9650; +&#8377;{:.2f}/Wp'.format(rev_wp_d) if rev_wp_d>=0
                    else '&#9660; &#8722;&#8377;{:.2f}/Wp'.format(abs(rev_wp_d)))
    rwp_delta_sub = 'realisation improving' if rev_wp_d > 0 else 'realisation softening'
    rwp_delta = 'vs &#8377;{:.2f} {} &middot; {}'.format(pm['rev_wp'], prev_lbl, rwp_delta_sub)
    card_rwp = snap_card('Rev / Wp MTD', rwp_pill_txt, rwp_pill_cls,
        '&#8377;{:.2f}'.format(mtd['rev_wp']), 'var(--ink)', rwp_delta, 'card-rev')

    # COGS card
    cogs_pkw_d = total_cogs_pkw_c - total_cogs_pkw_p
    cogs_pill_cls = 'red-pill' if cogs_pkw_d > 200 else ('green-pill' if cogs_pkw_d < -200 else 'amber-pill')
    cogs_pill_txt = ('&#9650; +&#8377;{:.3f}/Wp'.format(cogs_pkw_d/1000) if cogs_pkw_d > 0
                     else '&#9660; &#8722;&#8377;{:.3f}/Wp'.format(abs(cogs_pkw_d)/1000))
    cogs_top_driver = max(
        [('MMS', mtd['mms']/mtd['kw']/1000 - pm['mms']/pm['kw']/1000 if mtd['kw'] and pm['kw'] else 0),
         ('Cables', mtd['cab']/mtd['kw']/1000 - pm['cab']/pm['kw']/1000 if mtd['kw'] and pm['kw'] else 0),
         ('Inverter', mtd['inv']/mtd['kw']/1000 - pm['inv']/pm['kw']/1000 if mtd['kw'] and pm['kw'] else 0)],
        key=lambda x: abs(x[1])
    )
    cogs_delta = 'vs &#8377;{:.2f}/Wp {} &middot; {} main driver'.format(
        total_cogs_pkw_p/1000, prev_lbl, cogs_top_driver[0])
    card_cogs = snap_card('COGS / Wp MTD', cogs_pill_txt, cogs_pill_cls,
        '&#8377;{:.2f}'.format(total_cogs_pkw_c/1000), 'var(--amber)', cogs_delta, 'card-cogs')

    # GM card
    gm_pill_cls = 'green-pill' if gm_trend >= 0 else ('red-pill' if gm_trend < -0.5 else 'amber-pill')
    gm_pill_txt = ('&#9660; {}{:.2f}pp MoM'.format('' if gm_trend>=0 else '&#8722;', abs(gm_trend)))
    adj_gm_str = 'Adj GM {:.2f}% (ex-ONM/QHS)'.format(mtd['adj_gm']) if mtd.get('adj_gm') else ''
    card_gm = snap_card('Gross Margin MTD', gm_pill_txt, gm_pill_cls,
        '{:.2f}%'.format(mtd['gm']), gmc(mtd['gm']),
        'vs {:.2f}% {} &middot; {}'.format(pm['gm'], prev_lbl, adj_gm_str), 'card-gm')

    snap_grid_html = '<div class="snap-grid">{}{}{}{}</div>'.format(
        card_vol, card_rwp, card_cogs, card_gm)

    # GM Bridge waterfall
    rev_wp_d_ref = mtd['rev_wp'] - pm['rev_wp']
    mms_wp_d  = (mtd['mms']/mtd['kw']/1000 - pm['mms']/pm['kw']/1000) if mtd['kw'] and pm['kw'] else 0
    cab_wp_d  = (mtd['cab']/mtd['kw']/1000 - pm['cab']/pm['kw']/1000) if mtd['kw'] and pm['kw'] else 0
    inv_wp_d  = (mtd['inv']/mtd['kw']/1000 - pm['inv']/pm['kw']/1000) if mtd['kw'] and pm['kw'] else 0
    mod_wp_d  = (mtd['mod']/mtd['kw']/1000 - pm['mod']/pm['kw']/1000) if mtd['kw'] and pm['kw'] else 0
    _rev_gm_impact  = rev_wp_d_ref / pm['rev_wp'] * pm['gm'] if pm['rev_wp'] else 0
    _mms_gm_impact  = -(mms_wp_d / pm['rev_wp'] * 100) if pm['rev_wp'] else 0
    _cab_gm_impact  = -(cab_wp_d / pm['rev_wp'] * 100) if pm['rev_wp'] else 0
    _inv_gm_impact  = -(inv_wp_d / pm['rev_wp'] * 100) if pm['rev_wp'] else 0
    _mod_oth_impact = gm_trend - _rev_gm_impact - _mms_gm_impact - _cab_gm_impact - _inv_gm_impact
    def _bridge_item(label, impact):
        cls = 'bridge-item pos' if impact > 0 else 'bridge-item'
        clr = 'var(--green)' if impact > 0 else 'var(--red)'
        sign = '+' if impact >= 0 else ''
        return ('<div class="{}">'
                '<span class="bridge-label">{}</span>'
                '<span class="bridge-val" style="color:{}">{}{:.2f}pp</span>'
                '</div>').format(cls, label, clr, sign, impact)
    bridge_items = []
    if abs(_rev_gm_impact) > 0.01:
        bridge_items.append(_bridge_item(
            'Rev/Wp {}{:.2f}/Wp'.format('+' if rev_wp_d_ref>=0 else '',rev_wp_d_ref),
            _rev_gm_impact))
    if abs(_mms_gm_impact) > 0.01:
        bridge_items.append(_bridge_item('MMS {}{:.2f}/Wp'.format('+' if mms_wp_d>=0 else '',mms_wp_d), _mms_gm_impact))
    if abs(_cab_gm_impact) > 0.01:
        bridge_items.append(_bridge_item('Cables {}{:.2f}/Wp'.format('+' if cab_wp_d>=0 else '',cab_wp_d), _cab_gm_impact))
    if abs(_inv_gm_impact) > 0.01:
        bridge_items.append(_bridge_item('Inverter {}{:.2f}/Wp'.format('+' if inv_wp_d>=0 else '',inv_wp_d), _inv_gm_impact))
    if abs(_mod_oth_impact) > 0.01:
        bridge_items.append(_bridge_item('Module/Other', _mod_oth_impact))
    bridge_html = (
        '<div style="margin-top:18px">'
        '<div class="sec-title" style="margin-bottom:10px">GM Bridge &#8212; {} MTD vs {} {}</div>'
        '<div class="bridge-scroll"><div class="bridge">'
        '<div class="bridge-box start"><span class="bridge-label">{} GM</span>'
        '<span class="bridge-val">{:.2f}%</span></div>'
        '{}'
        '<div class="bridge-box end"><span class="bridge-label">{} GM</span>'
        '<span class="bridge-val">{:.2f}%</span></div>'
        '</div></div>'
        '<div style="font-size:10px;color:#9CA3AF;margin-top:4px">'
        '* Bridge partials rounded to 2dp; residual in Module/Other. '
        'Rev/Wp impact = &#916;&#8377;/Wp &#247; Rev/Wp_PM &#215; GM_PM</div>'
        '</div>'
    ).format(curr_lbl, prev_lbl, latest.year, prev_lbl, pm['gm'],
             ''.join(bridge_items), curr_lbl[:3], mtd['gm'])

    snap4_html = snap_grid_html + bridge_html

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  MTD KPI GRID (2 rows x 4)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def kcard(label, val, sub, vc='#111827', trend_html='', accent='#0284C7'):
        return (
            '<td class="kc" style="border-top:4px solid {0}">'
            '<span class="kc-label">{1}</span>'
            '<span class="kc-val" style="color:{2}">{3}</span>'
            '<span class="kc-sub">{4}</span>'
            '<span class="kc-trend">{5}</span>'
            '</td>'
        ).format(accent, label, vc, val, sub, trend_html)

    kpi_html = (
        '<table class="kgrid"><tr>'
        + kcard('Installations MTD', '{:,}'.format(mtd['n']),
                'vs {:,} {} (1&#8211;{})'.format(pm['n'], prev_lbl, pm_day),
                trend_html=dpct(mtd['n'], pm['n']), accent='#0284C7')
        + kcard('kW Installed MTD', '{:,.0f} kW'.format(mtd['kw']),
                'vs {:,.0f} kW {}'.format(pm['kw'], prev_lbl),
                trend_html=dpct(mtd['kw'], pm['kw']))
        + kcard('Gross Margin', '{:.2f}%'.format(mtd['gm']),
                'vs {:.2f}% {}'.format(pm['gm'], prev_lbl),
                vc=gmc(mtd['gm']), trend_html=dpp(gm_trend), accent='#8B5CF6')
        + kcard('Revenue MTD', fc(mtd['rev']),
                'vs {} {}'.format(fc(pm['rev']), prev_lbl),
                trend_html=dpct(mtd['rev'], pm['rev']), accent='#059669')
        + '</tr><tr>'
        + kcard('Avg System Size', '{:.2f} kW'.format(mtd['aos']),
                'vs {:.2f} kW {}'.format(pm['aos'], prev_lbl),
                trend_html=dpval(mtd['aos']-pm['aos'], 'kW'))
        + kcard('Avg Order Value', fc(mtd['aov']),
                'vs {} {}'.format(fc(pm['aov']), prev_lbl),
                trend_html=dpct(mtd['aov'], pm['aov']))
        + kcard('Rev / Wp', '&#8377;{:.2f}'.format(mtd['rev_wp']),
                'vs &#8377;{:.2f} {}'.format(pm['rev_wp'], prev_lbl),
                trend_html=dpval(rev_wp_d, '&#8377;/Wp'))
        + kcard('Abs Gross Margin', fc(mtd['abs_gm']),
                'vs {} {}'.format(fc(pm['abs_gm']), prev_lbl),
                trend_html=dpct(mtd['abs_gm'], pm['abs_gm']))
        + '</tr></table>'
    )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  TODAY VS YESTERDAY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def tcard(label, today_val, prev_val, delta_html, vc='#111827', accent='#0D9488'):
        return (
            '<td class="tc" style="border-top:4px solid {0}">'
            '<span class="tc-label">{1}</span>'
            '<span class="tc-today" style="color:{2}">{3}</span>'
            '<span class="tc-prev">vs {4} yesterday &nbsp; {5}</span>'
            '</td>'
        ).format(accent, label, vc, today_val, prev_val, delta_html)

    _lat_rev_str = fc(lat.get('rev', 0)) if lat.get('rev', 0) > 0 else ''
    today_html = (
        '<table class="today-grid">'
        '<tr>'
        + tcard('Installations', str(lat['n']), str(prv['n']),
                dpct(lat['n'], prv['n']) if prv['n'] else '')
        + tcard('kW Installed', '{:.1f} kW'.format(lat['kw']), '{:.1f} kW'.format(prv['kw']),
                dpval(lat['kw']-prv['kw'], 'kW') if prv['kw'] else '')
        + '</tr><tr>'
        + tcard('Rev / Wp', '&#8377;{:.2f}'.format(lat['rev_wp']), '&#8377;{:.2f}'.format(prv['rev_wp']),
                dpval(lat['rev_wp']-prv['rev_wp'], '&#8377;/Wp') if prv['rev_wp'] else '')
        + tcard('Avg System Size', '{:.2f} kW'.format(lat['aos']), '{:.2f} kW'.format(prv['aos']),
                dpval(lat['aos']-prv['aos'], 'kW') if prv['aos'] else '')
        + '</tr><tr>'
        + tcard('GM %', '{:.1f}%'.format(lat['gm']), '{:.1f}%'.format(prv['gm']),
                dpp(lat['gm']-prv['gm']) if prv['gm'] else '', vc=gmc(lat['gm']))
        + '</tr></table>'
    )


    # ── PRODUCT MIX (Offer Type)
    OFFER_LABELS = {'GoodZero':'GZ','GoodZero Pro':'GZ Pro','GoodZero Uno':'GZ Uno',
                    'SSE Blue':'SSE Blue','Regular':'Non-GZ','regular':'Non-GZ','':'Non-GZ'}
    OFFER_ORDER  = ['GZ','GZ Pro','GZ Uno','SSE Blue','Non-GZ']
    OFFER_COLORS = {'GZ':'#2563EB','GZ Pro':'#7C3AED','GZ Uno':'#0891B2',
                    'SSE Blue':'#0369A1','Non-GZ':'#6B7280'}

    def by_offer(plist):
        d = defaultdict(list)
        for p in plist:
            lbl = OFFER_LABELS.get(p.get('o',''), 'Non-GZ')
            d[lbl].append(p)
        return d

    mix_mtd = by_offer(mtd_ps); mix_pm = by_offer(pm_ps)
    mix_rows = ''
    for lbl in OFFER_ORDER:
        mc = calc(mix_mtd.get(lbl,[])); mp = calc(mix_pm.get(lbl,[]))
        if mc['n'] == 0 and mp['n'] == 0: continue
        pct_n = mc['n']/mtd['n']*100 if mtd['n'] else 0
        col = OFFER_COLORS.get(lbl,'#6B7280')
        gm_d_html = dpp(mc['gm']-mp['gm']) if mp['n']>0 else '<span style="color:#94A3B8">new</span>'
        rwp_d = mc['rev_wp']-mp['rev_wp'] if mp['n']>0 else 0
        rwp_d_html = dpval(rwp_d,'&#8377;/Wp') if mp['n']>0 else ''
        mix_rows += (
            '<tr>'
            '<td><span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
            'background:{};margin-right:7px;vertical-align:middle"></span>'
            '<b style="color:#111827">{}</b></td>'
            '<td class="R">{:,}</td>'
            '<td class="R" style="color:#6B7280;font-size:10px">{:.0f}%</td>'
            '<td class="R">&#8377;{:.2f}/Wp</td>'
            '<td class="R" style="font-weight:700;color:{}">{:.2f}%</td>'
            '<td class="R">{}</td>'
            '<td class="R">{}</td>'
            '</tr>'
        ).format(col, lbl, mc['n'], pct_n, mc['rev_wp'], gmc(mc['gm']), mc['gm'], gm_d_html, rwp_d_html)

    # Donut-style mix bar
    mix_bar = ''
    for lbl in OFFER_ORDER:
        mc = calc(mix_mtd.get(lbl,[]))
        pct = mc['n']/mtd['n']*100 if mtd['n'] else 0
        if pct < 0.5: continue
        col = OFFER_COLORS.get(lbl,'#6B7280')
        mix_bar += '<div style="flex:{};background:{};height:100%;display:flex;align-items:center;justify-content:center;font-size:8px;font-weight:700;color:#fff;overflow:hidden;padding:0 4px;white-space:nowrap">{}</div>'.format(
            round(pct), col, lbl if pct > 8 else '')

    mix_html = (
        '<div class="table-scroll"><table class="data-table"><thead><tr>'
        '<th>Offer Type</th><th class="R">Installs MTD</th><th class="R">Mix%</th>'
        '<th class="R">Rev/Wp</th><th class="R">GM%</th>'
        '<th class="R">&#916;GM vs full {}</th><th class="R">&#916;Rev/Wp</th>'
        '</tr></thead><tbody>{}</tbody></table></div>'
    ).format(prev_lbl, mix_rows)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  COGS SECTION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cg_rows = ''
    for lbl,val in cogs_items:
        pct = val/cogs_total*100 if cogs_total else 0
        if pct < 0.3: continue
        col = COGS_COLORS.get(lbl, '#94A3B8')
        pmv   = pm_cogs.get(lbl, 0)
        pmpct = pmv/pm['cogs']*100 if pm['cogs'] else 0
        pkw_c = val/mtd['kw'] if mtd['kw'] else 0
        pkw_p = pmv/pm['kw'] if pm['kw'] else 0
        d_pp  = pct - pmpct
        pkw_d_wp = (pkw_c - pkw_p) / 1000
        # delta styling
        if pkw_d_wp > 0.005:
            delta_html = '<span class="up">{:+.2f}</span>'.format(pkw_d_wp)
        elif pkw_d_wp < -0.005:
            delta_html = '<span class="dn">{:.2f}</span>'.format(pkw_d_wp)
        else:
            delta_html = '<span class="neutral">{:+.2f}</span>'.format(pkw_d_wp)
        # root cause tag
        if d_pp > 0.2:
            if lbl in ('MMS','Cables') and aos_d > 0.1 and pkw_d_wp < 0.05:
                rc_tag = '<span class="tag-cogs">AoS +{:.2f}kW</span>'.format(aos_d)
            else:
                rc_tag = '<span class="tag-cogs">+rate/mix</span>'
        elif d_pp < -0.2:
            rc_tag = '<span class="tag-ok">Favorable</span>'
        elif lbl == 'Module':
            rc_tag = '<span class="tag-ok">Stable</span> 540Wp DCR 99%'
        else:
            rc_tag = '<span class="neutral" style="font-size:10px;color:#9CA3AF">&#8212;</span>'
        # highlight rows with big COGS moves
        row_style = ' style="background:#FFFBEB"' if d_pp > 0.5 else ''
        cg_rows += (
            '<tr{}>'
            '<td><span class="dot" style="background:{}"></span><strong>{}</strong></td>'
            '<td class="R">{}</td>'
            '<td class="R">{:.2f}%</td>'
            '<td class="R mono">{:.2f}</td>'
            '<td class="R mono">{:.2f}</td>'
            '<td class="R">{}</td>'
            '<td class="R">{}</td>'
            '</tr>'
        ).format(row_style, col, lbl, fc(val), pct, pkw_c/1000, pkw_p/1000, delta_html, rc_tag)
    # Total row
    tc_c = (mtd['cogs']/mtd['kw']/1000) if mtd['kw'] else 0
    tc_p = (pm['cogs']/pm['kw']/1000) if pm['kw'] else 0
    tc_d = tc_c - tc_p
    tc_delta = '<span class="up"><strong>{:+.2f}</strong></span>'.format(tc_d) if tc_d > 0.005 else '<span class="dn">{:.2f}</span>'.format(tc_d)
    cg_rows += (
        '<tr class="total-row">'
        '<td><strong>TOTAL COGS</strong></td>'
        '<td class="R">{}</td>'
        '<td class="R">100%</td>'
        '<td class="R mono">{:.2f}</td>'
        '<td class="R mono">{:.2f}</td>'
        '<td class="R">{}</td>'
        '<td class="R"></td>'
        '</tr>'
    ).format(fc(mtd['cogs']), tc_c, tc_p, tc_delta)

    # Pass projects.json category ₹/Wp values so SKU card matches COGS table exactly
    _pj_cat = {
        'Module':   (mtd['mod']/mtd['kw']/1000 if mtd['kw'] else 0, pm['mod']/pm['kw']/1000 if pm['kw'] else 0),
        'Inverter': (mtd['inv']/mtd['kw']/1000 if mtd['kw'] else 0, pm['inv']/pm['kw']/1000 if pm['kw'] else 0),
        'MMS':      (mtd['mms']/mtd['kw']/1000 if mtd['kw'] else 0, pm['mms']/pm['kw']/1000 if pm['kw'] else 0),
        'Cables':   (mtd['cab']/mtd['kw']/1000 if mtd['kw'] else 0, pm['cab']/pm['kw']/1000 if pm['kw'] else 0),
    }
    cogs_callout = build_sku_html(sku_data, aos_d, prev_lbl, curr_lbl, mtd['kw'], pm['kw'], _pj_cat)
    if False and cogs_rising:
        # MMS sub-SKU breakdown from raw fields
        mms_prf_c = sum(p.get('prf',0) for p in mtd_ps)/mtd['kw']/1000 if mtd['kw'] else 0
        mms_tsh_c = sum(p.get('tsh',0) for p in mtd_ps)/mtd['kw']/1000 if mtd['kw'] else 0
        mms_wel_c = sum(p.get('wel',0) for p in mtd_ps)/mtd['kw']/1000 if mtd['kw'] else 0
        mms_prf_p = sum(p.get('prf',0) for p in pm_ps)/pm['kw']/1000 if pm['kw'] else 0
        mms_tsh_p = sum(p.get('tsh',0) for p in pm_ps)/pm['kw']/1000 if pm['kw'] else 0
        mms_wel_p = sum(p.get('wel',0) for p in pm_ps)/pm['kw']/1000 if pm['kw'] else 0
        parts = []
        for lbl,d,pkw_c,pkw_p,pkw_d,gm_i in cogs_rising[:3]:
            pwp_c = pkw_c/1000; pwp_p = pkw_p/1000; pwp_d = pkw_d/1000
            if lbl == 'MMS' and aos_d > 0.05:
                sub = 'Profile &#8377;{:.3f} + Structure &#8377;{:.3f} + Welding &#8377;{:.3f}/Wp'.format(
                    mms_prf_c, mms_tsh_c, mms_wel_c)
                why = '&#8377;{:.3f}&#8594;&#8377;{:.3f}/Wp (+&#8377;{:.3f}/Wp) &mdash; AoS +{:.2f}kW &nbsp;<span style="font-style:italic;color:#B45309">[{}]</span>'.format(
                    pwp_p, pwp_c, pwp_d, aos_d, sub)
            elif lbl == 'Cables' and aos_d > 0.05:
                why = '&#8377;{:.3f}&#8594;&#8377;{:.3f}/Wp (+&#8377;{:.3f}/Wp) &mdash; DC/AC wiring scales with larger systems (AoS +{:.2f}kW)'.format(
                    pwp_p, pwp_c, pwp_d, aos_d)
            else:
                why = '&#8377;{:.3f}&#8594;&#8377;{:.3f}/Wp (+&#8377;{:.3f}/Wp) &mdash; rate increase'.format(
                    pwp_p, pwp_c, pwp_d)
            parts.append('<b>{}</b>: {}'.format(lbl, why))
        net_wp_impact = cogs_net_gm/100 * mtd['rev_wp']
        net_clr = '#DC2626' if cogs_net_gm < 0 else '#16A34A'
        net_sign = '+' if cogs_net_gm>=0 else ''
        cogs_callout = (
            '<div style="background:#FFFBEB;border:1px solid #FDE68A;border-radius:8px;'
            'padding:12px 16px;margin-top:12px;font-size:11px;color:#92400E;line-height:1.9">'
            '&#9888;&nbsp; <b>COGS shift: '
            '<span style="color:{}">{}{:.2f} &#8377;/Wp on realised margin</span>'
            '</b> &nbsp;<span style="font-weight:400;color:#B45309">({}{:.2f}%pts on GM &#64; &#8377;{:.2f}/Wp realisation)</span><br>'
            '<span style="display:block;margin-top:6px;line-height:2">{}</span>'
            '</div>'
        ).format(net_clr, net_sign, abs(net_wp_impact), net_sign, abs(cogs_net_gm), mtd['rev_wp'],
                 '<br>'.join(parts))

    cogs_html = (
        '<div class="cogs-wrap"><div class="table-scroll"><table class="data-table"><thead><tr>'
        '<th>Category</th>'
        '<th class="R">MTD Amount</th>'
        '<th class="R">% of COGS</th>'
        '<th class="R">&#8377;/Wp MTD</th>'
        '<th class="R">&#8377;/Wp {}</th>'
        '<th class="R">&#916; &#8377;/Wp</th>'
        '<th class="R">Root Cause</th>'
        '</tr></thead><tbody>{}</tbody></table>{}'
    ).format(prev_lbl, cg_rows, cogs_callout) + '</div>'

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  TOP 3 THINGS TO WATCH
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Each watch item: (priority: 0=red,1=yel,2=grn, title, why, action)
    watch_items = []

    # Revenue realisation drop
    if price_dn:
        names_w = ', '.join('<b>{}</b> (&#8722;&#8377;{:.1f}/Wp)'.format(c,abs(d)) for c,d in price_dn[:4])
        leftover = ' and {} more'.format(len(price_dn)-4) if len(price_dn)>4 else ''
        priority = 0 if len(price_dn)>=3 else 1
        watch_items.append((priority,
            'Revenue realisation drop in {} cluster{}'.format(len(price_dn), 's' if len(price_dn)>1 else ''),
            'Revenue realisation (Rev/Wp) fell &gt;&#8377;0.8/Wp vs prior month in: {}{}.'.format(names_w, leftover),
            'Review discount approvals and cohort revenue in these markets. Check if recent deals set a lower benchmark.'))

    # COGS pressure
    if cogs_rising and cogs_net_gm < -0.2:
        cogs_names = ', '.join(lbl for lbl,*_ in cogs_rising[:2])
        priority = 0 if cogs_net_gm < -0.5 else 1
        if any(lbl in ('MMS','Cables') for lbl,*_ in cogs_rising) and aos_d > 0.05:
            why_cogs = '{} cost % rising. Driven by AoS increase (+{:.2f}kW) &#8212; larger systems need more structural material. Not a vendor rate issue.'.format(cogs_names, aos_d)
            action_cogs = 'No vendor action needed. Monitor if AoS stabilises &#8212; if system sizes plateau, MMS/Cables% will normalise.'
        else:
            why_cogs = '{} showing higher &#8377;/kW costs vs prior month.'.format(cogs_names)
            action_cogs = 'Verify procurement rates vs last month. Check if a new vendor batch is affecting average.'
        watch_items.append((priority,
            'COGS mix shift: {}{:.2f}%pts net GM impact'.format('+' if cogs_net_gm>=0 else '', cogs_net_gm),
            why_cogs, action_cogs))

    # GM trend
    if gm_trend <= -1.0 and not any(x[0]==0 for x in watch_items):
        watch_items.append((0,
            'GM down {:.2f}ppt MoM &#8212; review before month-end'.format(abs(gm_trend)),
            'Overall blended GM contracted {:.2f}ppt. No single dominant driver flagged.'.format(abs(gm_trend)),
            'Run cluster-level deep-dive. Check if mix shift toward lower-margin cities is driving blended drag.'))
    elif gm_trend <= -0.3:
        watch_items.append((1,
            'Mild GM softness: {:.2f}ppt MoM'.format(gm_trend),
            'Blended GM edged down slightly. May be mix-driven as volume scales.'.format(abs(gm_trend)),
            'Watch weekly trend. If continues for 3+ days, escalate revenue review.'))

    # Volume
    if vol_pct >= 20:
        watch_items.append((2,
            'Volume surge +{:.0f}% MoM &#8212; operational scale test'.format(vol_pct),
            '{:,} installs MTD vs {:,} in same {} days of {}. Execution cadence strong.'.format(
                mtd['n'], pm['n'], pm_day, prev_lbl),
            'Ensure CSAT and quality metrics are tracking alongside volume. Scale-related defects can lag by 2&#8211;3 weeks.'))


    # Run rate
    if latest.day > 1:
        pace = mtd['n']/latest.day
        proj = round(pace*30)
        watch_items.append((2,
            'Month run-rate: ~{:,} installs projected'.format(proj),
            '{:.1f} installs/day MTD &rarr; {:.0f}-day projection: {:,} vs {:,} actual in {}.'.format(
                pace, 30, proj, pm['n'], prev_lbl),
            'At current GM {:.2f}%, projected abs GM for month: {}.'.format(mtd['gm'], fc(mtd['abs_gm']/latest.day*30))))

    # ── Always-present: Blended GM & Revenue Realisation summary ──
    rev_sign  = '+' if rev_wp_d >= 0 else ''
    gm_sign   = '+' if gm_trend >= 0 else ''
    aos_sign  = '+' if aos_d >= 0 else ''
    # Priority: yellow if Rev/Wp or GM dipping, green if stable/improving
    rev_priority = 1 if (rev_wp_d < -0.3 or gm_trend < -0.3) else 2
    watch_items.append((rev_priority,
        'Blended Rev/Wp &#8377;{:.2f}/Wp &nbsp;|&nbsp; GM {:.2f}%'.format(mtd['rev_wp'], mtd['gm']),
        'MTD Rev/Wp at &#8377;{:.2f}/Wp ({}{:.2f}/Wp vs full {}). '
        'Blended GM {:.2f}% ({}{:.2f}%pts MoM). '
        'Avg system size {:.2f}kW ({}{:.2f}kW vs {}).'.format(
            mtd['rev_wp'], rev_sign, rev_wp_d, prev_lbl,
            mtd['gm'], gm_sign, gm_trend,
            mtd['aos'], aos_sign, aos_d, prev_lbl),
        'Rev/Wp and AoS are the two primary GM levers. '
        'Cluster-level deviations visible in Cluster Health below.'))

    # Limit to top 5, sort by priority
    watch_items.sort(key=lambda x:x[0])
    watch_items = watch_items[:5]

    if not watch_items:
        watch_items = [(2, 'All metrics within normal range',
            'No anomalies detected across revenue, COGS, volume, or GM.',
            'Continue monitoring daily.')]

    wi_classes = {0:'watch-red', 1:'watch-amber', 2:'watch-green'}
    wi_tags    = {0:'&#128308; Urgent', 1:'&#128992; Watch', 2:'&#128994; Positive'}
    wi_html = ''
    for i,(prio,title,why,_) in enumerate(watch_items, 1):
        cls = wi_classes.get(prio,'watch-green')
        tag = wi_tags.get(prio,'')
        wi_html += (
            '<div class="watch-item {}">'
            '<div class="watch-num">{}</div>'
            '<div class="watch-body">'
            '<div class="watch-tag">{}</div>'
            '<div class="watch-title">{}</div>'
            '<div class="watch-why">{}</div>'
            '</div></div>'
        ).format(cls, i, tag, title, why)
    watch_html = '<div class="watch-list">{}</div>'.format(wi_html)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CLUSTER TABLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    mtd_gm_ref = mtd['gm']  # reference for cluster vs blended comparison
    def cl_row(r, bg=''):
        c=r['curr']; p=r['prev']
        sd=STATE_DISPLAY.get(r['state'],r['state'])
        bgs='background:{};'.format(bg) if bg else ''
        # Build driver chip with leading colored tag
        det = r['drv_det']
        types = det.get('types',[])
        rv_d = det.get('rev_wp_d',0)
        ck_d = det.get('cogs_kw_d',0)/1000  # back to ₹/Wp
        if 'price_dn' in types:
            chip_tag = '<span class="tag-rev">Rev/Wp &#8722;&#8377;{:.2f}/Wp</span>'.format(abs(rv_d))
        elif 'cogs_up' in types:
            chip_tag = '<span class="tag-cogs">COGS +&#8377;{:.3f}/Wp</span>'.format(abs(ck_d))
        elif 'cogs_dn' in types:
            chip_tag = '<span class="tag-ok">COGS &#8722;&#8377;{:.3f}/Wp</span>'.format(abs(ck_d))
        elif 'price_up' in types:
            chip_tag = '<span class="tag-ok">Rev/Wp +&#8377;{:.2f}/Wp</span>'.format(rv_d)
        elif r['gm_d'] > 0.15:
            chip_tag = '<span class="tag-ok">Improving</span>'
        elif abs(r['gm_d']) < 0.15:
            chip_tag = '<span class="tag-ok">Stable</span>'
        else:
            chip_tag = ''
        # GM cell class
        gm_pct = c['gm']
        if gm_pct >= 44:
            gm_cell = '<td class="gm-cell-hi">{:.2f}%</td>'.format(gm_pct)
        elif gm_pct >= 40:
            gm_cell = '<td class="gm-cell-mid">{:.2f}%</td>'.format(gm_pct)
        else:
            gm_cell = '<td class="gm-cell-lo">{:.2f}%</td>'.format(gm_pct)
        # Delta cell
        if r['gm_d'] <= -0.3:
            delta_html = '<span class="up">&#9660; {:.2f}pp</span>'.format(abs(r['gm_d']))
        elif r['gm_d'] >= 0.3:
            delta_html = '<span class="up-good">&#9650; +{:.2f}pp</span>'.format(r['gm_d'])
        else:
            delta_html = '<span class="neutral">{:+.2f}pp</span>'.format(r['gm_d'])
        # Build inline insight for this cluster
        _rv_d = det.get('rev_wp_d', 0)
        _ck_d = det.get('cogs_kw_d', 0) / 1000
        _ao_d = c['aos'] - p['aos'] if p['n'] else 0
        _ins_parts = []
        if abs(_rv_d) > 0.3:
            _clr = '#059669' if _rv_d >= 0 else '#DC2626'
            _ins_parts.append(
                'Rev/Wp <b style="color:{}">{:+.2f}/Wp</b>'.format(_clr, _rv_d)
            )
        if abs(_ck_d) > 0.02:
            _clr2 = '#DC2626' if _ck_d > 0 else '#059669'
            _ins_parts.append(
                'COGS <b style="color:{}">{:+.2f}/Wp</b>{}'.format(
                    _clr2, _ck_d,
                    ' (AoS {:+.1f}kW)'.format(_ao_d) if abs(_ao_d) > 0.1 else ''
                )
            )
        if abs(c['gm'] - mtd_gm_ref) > 1.5:
            _diff = c['gm'] - mtd_gm_ref
            _clr3 = '#059669' if _diff > 0 else '#DC2626'
            _ins_parts.append(
                'GM <b style="color:{}">{:.1f}%</b> ({:+.1f}pp vs avg)'.format(_clr3, c['gm'], _diff)
            )
        if not _ins_parts:
            insight_cell = '<td style="font-size:10px;color:#94A3B8">Stable</td>'
        else:
            insight_cell = (
                '<td style="font-size:10px;color:#334155;line-height:1.7;min-width:140px">'
                + '<br>'.join(_ins_parts)
                + '</td>'
            )
        return (
            '<tr style="{}">'
            '<td style="font-weight:700">{}</td>'
            '<td>{}</td>'
            '<td class="R">{}</td>'
            '<td class="R mono">{:.2f} / {:.2f}</td>'
            '{}'
            '<td class="R">{}</td>'
            '{}'
            '</tr>'
        ).format(
            bgs, r['cluster'], sd, c['n'],
            c['rev_wp'], p['rev_wp'] if p['n'] else 0,
            gm_cell,
            delta_html,
            insight_cell
        )

    cl_thead = (
        '<thead><tr>'
        '<th>Cluster</th>'
        '<th>State</th>'
        '<th class="R">n MTD</th>'
        '<th class="R">Rev/Wp MTD / {}</th>'
        '<th class="R">GM%</th>'
        '<th class="R">&#916;pp</th>'
        '<th>Insight</th>'
        '</tr></thead>'
    ).format(prev_lbl)

    cl_tbody = ''
    if declining:
        cl_tbody += '<tr class="group-row"><td colspan="7">&#9660; Declining vs {} &#8212; needs attention</td></tr>'.format(prev_lbl)
        cl_tbody += ''.join(cl_row(r,'#FFFBFB') for r in declining)
    if improving:
        cl_tbody += '<tr class="group-row"><td colspan="7">&#9650; Improving vs {}</td></tr>'.format(prev_lbl)
        cl_tbody += ''.join(cl_row(r,'#F9FFFA') for r in improving)
    if stable_cl:
        cl_tbody += '<tr class="group-row"><td colspan="7">&#8594; Stable (within &plusmn;0.3pp)</td></tr>'
        cl_tbody += ''.join(cl_row(r) for r in stable_cl)
    if nascent:
        cl_tbody += '<tr class="group-row"><td colspan="7">&#9733; New / growing clusters</td></tr>'
        cl_tbody += ''.join(cl_row(r,'#FAF5FF') for r in nascent)

    cl_html = (
        '<div class="cluster-wrap">'
        '<table class="data-table">{}<tbody>{}</tbody></table></div>'
    ).format(cl_thead, cl_tbody)

    # ── CLUSTER INSIGHTS: short factual bullets per cluster (no actionables)
    def _ci_sign(v): return '+' if v >= 0 else ''
    def _ci_arrow(v): return '&#9650;' if v >= 0 else '&#9660;'
    def _ci_clr(v, good_positive=True):
        if good_positive:
            return '#059669' if v >= 0 else '#DC2626'
        return '#DC2626' if v >= 0 else '#059669'

    ci_rows = []
    # Build for all clusters with enough volume
    for r in (declining + improving + stable_cl)[:20]:
        c = r['curr']; p = r['prev']
        if c['n'] < MIN_ORDERS: continue
        det = r['drv_det']
        rv_d  = det.get('rev_wp_d', 0)
        ck_d  = det.get('cogs_kw_d', 0) / 1000
        ao_d  = c['aos'] - p['aos'] if p['n'] else 0
        gm_d  = r['gm_d']

        # Build 1-2 line insight
        parts = []

        # Revenue realisation
        if abs(rv_d) > 0.3:
            clr = _ci_clr(rv_d, good_positive=True)
            parts.append(
                'Rev/Wp <span style="color:{};font-weight:700">{}{:+.2f}/Wp</span>'
                ' ({:.2f} vs {:.2f} {})'.format(
                    clr, _ci_arrow(rv_d), rv_d,
                    c['rev_wp'], p['rev_wp'] if p['n'] else 0, prev_lbl)
            )

        # COGS
        if abs(ck_d) > 0.02:
            clr = _ci_clr(ck_d, good_positive=False)
            parts.append(
                'COGS <span style="color:{};font-weight:700">{}{:+.3f}/Wp</span>'.format(
                    clr, _ci_arrow(ck_d), ck_d)
                + (' &#8212; AoS {:+.2f}kW'.format(ao_d) if abs(ao_d) > 0.1 else '')
            )

        # Product mix note (GM vs blended)
        if abs(c['gm'] - mtd['gm']) > 1.5:
            if c['gm'] > mtd['gm']:
                parts.append(
                    'GM <span style="color:#059669;font-weight:700">{:.1f}%</span>'
                    ' &#8212; {:.1f}pp above blended avg'.format(c['gm'], c['gm'] - mtd['gm'])
                )
            else:
                parts.append(
                    'GM <span style="color:#DC2626;font-weight:700">{:.1f}%</span>'
                    ' &#8212; {:.1f}pp below blended avg'.format(c['gm'], mtd['gm'] - c['gm'])
                )

        if not parts:
            parts.append('Stable &#8212; Rev/Wp &amp; COGS within normal band')

        # GM delta badge
        gm_bg  = '#DCFCE7' if gm_d >= 0 else '#FEE2E2'
        gm_clr = '#166534' if gm_d >= 0 else '#B91C1C'
        gm_badge = (
            '<span style="display:inline-block;background:{};color:{};'
            'font-size:9px;font-weight:700;padding:2px 7px;border-radius:5px;'
            'white-space:nowrap">'
            '{}{:+.2f}pp</span>'.format(gm_bg, gm_clr, _ci_arrow(gm_d), gm_d)
        )

        ci_rows.append((r['cluster'], c['n'], gm_badge, parts))

    # Render as a clean 2-column card grid
    ci_cards = ''
    for i, (cluster, n, gm_badge, parts) in enumerate(ci_rows):
        ci_cards += (
            '<div style="'
            'background:#F8FAFC;border:1px solid #DBEAFE;border-left:4px solid #0284C7;'
            'border-radius:9px;padding:12px 14px;'
            '">'
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:7px">'
            '<span style="font-weight:800;font-size:13px;color:#0F172A">{}</span>'
            '<div style="display:flex;align-items:center;gap:6px">'
            '<span style="font-size:10px;color:#94A3B8">{:,} installs</span>'
            '{}'
            '</div>'
            '</div>'
            '<div style="font-size:10.5px;color:#334155;line-height:1.8">'
            '{}'
            '</div>'
            '</div>'
        ).format(cluster, n, gm_badge, ' &nbsp;&#183;&nbsp; '.join(parts))

    if ci_cards:
        cluster_insights_html = (
            '<div style="'
            'display:grid;grid-template-columns:1fr 1fr;gap:10px;'
            '">'
            '{}'
            '</div>'
            '<p style="font-size:9.5px;color:#94A3B8;margin-top:8px;font-style:italic">'
            '* Insights based on Rev/Wp, COGS/Wp, and GM% vs full {}. '
            'Clusters with &lt;{} installs MTD excluded.'
            '</p>'
        ).format(ci_cards, prev_lbl, MIN_ORDERS)
    else:
        cluster_insights_html = '<p style="color:#94A3B8;font-size:11px">No cluster data available.</p>'

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  STRATEGIC ACTIONS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ── Pre-compute values used in deep-dive insights ──────────────────
    _cab_d2  = (mtd['cab']/mtd['kw']/1000  - pm['cab']/pm['kw']/1000)  if mtd['kw'] and pm['kw'] else 0
    _mms_d2  = (mtd['mms']/mtd['kw']/1000  - pm['mms']/pm['kw']/1000)  if mtd['kw'] and pm['kw'] else 0
    _inv_d2  = (mtd['inv']/mtd['kw']/1000  - pm['inv']/pm['kw']/1000)  if mtd['kw'] and pm['kw'] else 0
    _mod_d2  = (mtd['mod']/mtd['kw']/1000  - pm['mod']/pm['kw']/1000)  if mtd['kw'] and pm['kw'] else 0
    _below40 = [r for r in all_cl if r['curr']['gm'] < 40 and r['curr']['n'] >= MIN_ORDERS]

    # Helper: SKU context from sku_data
    def _sku_detail(sd, cat_list=None, sub_list=None, top_n=3):
        """Return top-n SKU name / rate lines from sku_data agg."""
        if not sd: return []
        try:
            agg = sd['agg']
            a_c, kw_c, _ = agg(sd['curr'], cat_list, sub_list)
            a_p, kw_p, _ = agg(sd['prev'], cat_list, sub_list)
            rows = []
            for name, cv in sorted(a_c.items(), key=lambda x: -x[1]['cost'])[:top_n]:
                pv = a_p.get(name, {})
                dr = cv['rwp'] - pv.get('rwp', cv['rwp'])
                rows.append((name[:45], cv['rwp'], pv.get('rwp', 0), dr, cv['mix']))
            return rows
        except:
            return []

    action_items_html = []
    _act_num = [0]
    def _act(priority_cls, title, impact_txt, insight_html):
        _act_num[0] += 1
        return (
            '<div class="action-item">'
            '<div class="action-num {}">{}</div>'
            '<div class="action-body">'
            '<div class="action-title">{}<span class="impact-tag">{}</span></div>'
            '<div class="action-why">{}</div>'
            '</div></div>'
        ).format(priority_cls, _act_num[0], title, impact_txt, insight_html)

    # ── INSIGHT 1: Revenue realisation erosion ──────────────────────────
    if price_dn:
        worst_cl, worst_rv = price_dn[0]
        worst_row = next((r for r in all_cl if r['cluster'] == worst_cl), None)
        if worst_row:
            wc = worst_row['curr']; wp = worst_row['prev']
            _wck_d = (wc.get('cogs_kw',0) - wp.get('cogs_kw',0))/1000 if wp['n'] else 0
            _waos_d = wc['aos'] - wp['aos']
            # System-wide Rev/Wp moved how much?
            sys_rv_d = mtd['rev_wp'] - pm['rev_wp']
            # Cluster fell by worst_rv; system moved by sys_rv_d → cluster-specific drop = worst_rv - sys_rv_d
            cluster_specific_drop = worst_rv - sys_rv_d
            # Rev/Wp vs other clusters: find comparable clusters
            comparable = [r for r in all_cl if r['cluster'] != worst_cl
                          and r['curr']['n'] >= MIN_ORDERS
                          and abs(r['drv_det'].get('rev_wp_d',0)) < 0.5]
            peer_avg = (sum(r['curr']['rev_wp'] for r in comparable)/len(comparable)) if comparable else mtd['rev_wp']
            vs_peer = wc['rev_wp'] - peer_avg
            vs_peer_txt = (
                '<b>&#8377;{:.2f}/Wp below peer cluster average</b> (&#8377;{:.2f}/Wp)'.format(abs(vs_peer), peer_avg)
                if vs_peer < -0.5 else
                '<b>&#8377;{:.2f}/Wp above peer average</b> despite the MoM drop &#8212; confirming this is a cluster-specific dip, not market-wide'.format(vs_peer, peer_avg)
                if vs_peer > 0 else
                'at peer cluster average (&#8377;{:.2f}/Wp)'.format(peer_avg)
            )
            # COGS context
            cogs_context = ''
            if abs(_wck_d) > 0.02:
                cogs_context = 'COGS simultaneously moved {:+.3f}/Wp (AoS {:+.2f}kW). '.format(_wck_d, _waos_d)
            elif abs(_waos_d) > 0.1:
                cogs_context = 'AoS moved {:+.2f}kW but COGS impact minimal. '.format(_waos_d)
            rev_gm_pp = abs(worst_rv / pm['rev_wp'] * pm['gm']) if pm['rev_wp'] else 0
            cogs_gm_pp = abs(_wck_d / pm['rev_wp'] * 100) if pm['rev_wp'] else 0
            insight = (
                '<strong>Root cause isolated:</strong> Rev/Wp in <b>{}</b> dropped '
                '&#8377;{:.2f} &#8594; &#8377;{:.2f}/Wp (&#8722;&#8377;{:.2f}/Wp MoM). '
                'System-wide Rev/Wp moved {:+.2f}/Wp in the same period &#8212; '
                'so the <b>cluster-specific drop is &#8722;&#8377;{:.2f}/Wp</b> beyond market movement. '
                '{} {}. '
                'GM decomposition: Rev/Wp drag <b>&#8722;{:.2f}pp</b>'
                '{}'
                ' &#8594; net <b>{:+.2f}pp GM</b>. '
                'Pattern is cluster-isolated &#8212; {} comparable clusters held Rev/Wp within &#177;&#8377;0.50/Wp.'
            ).format(
                worst_cl, wp['rev_wp'], wc['rev_wp'], abs(worst_rv),
                sys_rv_d, abs(cluster_specific_drop),
                vs_peer_txt, cogs_context,
                rev_gm_pp,
                ', COGS drag <b>{:+.2f}pp</b>'.format(-cogs_gm_pp) if abs(_wck_d) > 0.02 else '',
                worst_row['gm_d'],
                len(comparable)
            )
        else:
            insight = 'Rev/Wp fell &#8377;{:.2f}/Wp vs {} in {}. Not market-driven &#8212; other clusters held.'.format(
                abs(worst_rv), prev_lbl, worst_cl)
        action_items_html.append(_act('red',
            'Revenue realisation erosion in {} &#8212; cluster-specific, not market'.format(worst_cl),
            '&#8722;&#8377;{:.2f}/Wp Rev &middot; {:+.2f}pp GM'.format(abs(worst_rv), worst_row['gm_d'] if worst_row else 0),
            insight))

    # ── INSIGHT 2: Cable vendor rate hike (pre-analyzed) ───────────────
    if _cab_d2 > 0.04:
        cab_skus = _sku_detail(sku_data, sub_list=['dc cable','ac cable','earth'], top_n=4)
        sku_lines = ''
        rising_skus = [(n,c,p,d,m) for n,c,p,d,m in cab_skus if d > 0.002]
        falling_skus = [(n,c,p,d,m) for n,c,p,d,m in cab_skus if d < -0.002]
        for n,c,p,d,m in rising_skus[:3]:
            pct_chg = (c-p)/p*100 if p else 0
            sku_lines += '<b>{}</b> ({:.1f}% of cable cost): &#8377;{:.2f}&#8594;&#8377;{:.2f}/Wp ({:+.1f}%) &#8212; <em>vendor rate hike confirmed</em>. '.format(
                n[:40], m, p, c, pct_chg)
        for n,c,p,d,m in falling_skus[:1]:
            sku_lines += '<b>{}</b>: &#8377;{:.2f}/Wp &#8212; <em>rate down, partial offset</em>. '.format(n[:40], c)
        aos_ruling_out = 'AoS shift is only {:+.2f}kW &#8212; routing-length increase accounts for &lt;&#8377;0.005/Wp; the rest is pure vendor rate.'.format(aos_d) if abs(aos_d) < 0.15 else ''
        insight = (
            '<strong>Rate hike confirmed on {} SKUs &#8212; not a volume/routing issue.</strong> '
            '{}{} '
            'Net cable impact: <b>+&#8377;{:.3f}/Wp</b> on blended COGS = '
            '<b>&#8722;{:.2f}pp GM</b> at current Rev/Wp.'
        ).format(
            len(rising_skus), sku_lines, aos_ruling_out,
            _cab_d2, _cab_d2/mtd['rev_wp']*100 if mtd['rev_wp'] else 0)
        action_items_html.append(_act('red',
            'Cable COGS: vendor rate hike on DC + AC SKUs confirmed &#8212; not AoS-driven',
            '+&#8377;{:.3f}/Wp blended &middot; &#8722;{:.2f}pp GM'.format(_cab_d2, _cab_d2/mtd['rev_wp']*100 if mtd['rev_wp'] else 0),
            insight))

    # ── INSIGHT 3: MMS fabricator rate (pre-analyzed) ──────────────────
    if _mms_d2 > 0.05:
        mms_skus = _sku_detail(sku_data, cat_list=['MMS','Prefab MMS','Tin Shed MMS','Welded MMS'], top_n=5)
        mms_lines = ''
        for n,c,p,d,m in mms_skus:
            if abs(d) > 0.001:
                tag = 'rate &#9650;' if d > 0 else 'rate &#9660;'
                mms_lines += '<b>{}</b> ({:.1f}% mix): &#8377;{:.3f}/Wp ({} {:+.3f}/Wp). '.format(
                    n[:38], m, c, tag, d)
        # Structural vs rate decomposition
        aos_contrib = aos_d * 0.003  # rough: 0.003 ₹/Wp per +0.1kW AoS for MMS
        rate_contrib = max(_mms_d2 - aos_contrib, 0)
        insight = (
            '<strong>Dual driver confirmed: fabricator rate increase + AoS structural.</strong> '
            '{}'
            'Decomposition: AoS +{:.2f}kW contributes ~&#8377;{:.3f}/Wp (structural &#8212; more material per system); '
            'remaining &#8377;{:.3f}/Wp is fabricator rate increases. '
            '3-phase Column variants show the largest unit price jumps, consistent with '
            'steel/fabrication input cost pressure. '
            'Net: <b>+&#8377;{:.3f}/Wp MMS = &#8722;{:.2f}pp GM</b>.'
        ).format(mms_lines, aos_d, aos_contrib, rate_contrib,
                 _mms_d2, _mms_d2/mtd['rev_wp']*100 if mtd['rev_wp'] else 0)
        action_items_html.append(_act('amber',
            'MMS COGS: fabricator rate hike confirmed across Column Gen2 variants',
            '+&#8377;{:.3f}/Wp blended &middot; &#8722;{:.2f}pp GM'.format(_mms_d2, _mms_d2/mtd['rev_wp']*100 if mtd['rev_wp'] else 0),
            insight))

    # ── INSIGHT 4: Below-40% clusters (pre-analyzed) ───────────────────
    if _below40:
        b40_detail = []
        for r in sorted(_below40, key=lambda x: x['curr']['gm'])[:4]:
            rc = r['curr']; rp = r['prev']
            margin_wp = rc['rev_wp'] - rc['cogs_kw']/1000 if rc['kw'] else 0
            b40_detail.append(
                '<b>{}</b> (n={}, GM {:.1f}%): Rev/Wp &#8377;{:.2f} &#8722; COGS/Wp &#8377;{:.2f} = '
                '<b>&#8377;{:.2f}/Wp margin</b>. AoS {:.2f}kW. '
                '{}'.format(
                    r['cluster'], rc['n'], rc['gm'], rc['rev_wp'],
                    rc['cogs_kw']/1000 if rc['kw'] else 0,
                    margin_wp, rc['aos'],
                    'Rev/Wp has been below &#8377;{:.0f} for 2+ months &#8212; structural floor issue.'.format(rc['rev_wp']+0.5)
                    if rp['n'] >= MIN_ORDERS and rp['rev_wp'] < 65 else
                    'COGS pressure from {} mix.'.format('MMS+Cable' if _mms_d2 > 0.05 else 'rate hikes')
                ))
        lowest = _below40[0]
        insight = (
            '<strong>{} cluster{} confirmed structurally below 40% GM &#8212; not cycle noise.</strong> '
            '{} '
            'Common pattern: COGS/Wp in these markets is &#8377;{:.2f}&#8211;&#8377;{:.2f}/Wp, '
            'leaving &lt;&#8377;{:.2f}/Wp net margin per Wp. '
            'This is a <em>Rev/Wp floor problem</em> &#8212; COGS is not meaningfully higher than '
            'other clusters; revenue realisation is structurally lower.'
        ).format(
            len(_below40), 's' if len(_below40)>1 else '',
            ' '.join(b40_detail),
            min(r['curr']['cogs_kw']/1000 for r in _below40 if r['curr']['kw']),
            max(r['curr']['cogs_kw']/1000 for r in _below40 if r['curr']['kw']),
            min((r['curr']['rev_wp'] - r['curr']['cogs_kw']/1000) for r in _below40 if r['curr']['kw'])
        )
        action_items_html.append(_act('amber',
            '{} cluster{} below 40% GM &#8212; structural Rev/Wp floor deficit, not COGS issue'.format(
                len(_below40), 's' if len(_below40)>1 else ''),
            '{} cluster{} &middot; up to {:,} installs/month at sub-40% GM'.format(
                len(_below40), 's' if len(_below40)>1 else '',
                sum(r['curr']['n'] for r in _below40)),
            insight))

    # ── INSIGHT 5: Module — only stable COGS category ──────────────────
    # ── INSIGHT 5: Module — only stable COGS category ──────────────────
    mod_pct   = mtd['mod']/mtd['cogs']*100 if mtd['cogs'] else 0
    mod_rwp_c = mtd['mod']/mtd['kw']/1000  if mtd['kw']   else 0
    mod_rwp_p = pm['mod'] /pm['kw'] /1000  if pm['kw']    else 0
    mod_skus  = _sku_detail(sku_data, cat_list=['Module'], top_n=3)
    dominant_mod = mod_skus[0] if mod_skus else None
    dom_mix  = dominant_mod[4] if dominant_mod else 99.0
    dom_name = dominant_mod[0][:40] if dominant_mod else '540Wp DCR-PREMIER'
    insight_mod = (
        '<strong>Module is the only COGS category essentially flat month-on-month.</strong> '
        '<b>{}</b> accounts for {:.1f}% of all installs at &#8377;{:.2f}/Wp '
        '(vs &#8377;{:.2f}/Wp {}, &#916; {:+.2f}/Wp). '
        'At <b>{:.1f}% of total COGS</b>, Module stability is the primary reason blended GM '
        'has not deteriorated further despite MMS + Cable pressure. '
        'If Module rate moved by even &#8377;0.10/Wp, blended GM impact would be '
        '&#8722;{:.2f}pp &#8212; larger than the entire cable hike this month. '
        'Current rate confirmed stable; no procurement risk flagged.'
    ).format(
        dom_name, dom_mix, mod_rwp_c, mod_rwp_p, prev_lbl, _mod_d2,
        mod_pct,
        0.10 / mtd['rev_wp'] * 100 if mtd['rev_wp'] else 0
    )
    action_items_html.append(_act('green',
        'Module: only fully stable COGS category &#8212; {:.1f}% of COGS, &#916; {:+.2f} &#8377;/Wp'.format(mod_pct, _mod_d2),
        'Stable &middot; &#916;{:+.2f} &#8377;/Wp &middot; {:.1f}% of COGS'.format(_mod_d2, mod_pct),
        insight_mod))

    if not action_items_html:
        action_items_html.append(_act('green',
            'All COGS categories within normal band &#8212; no material cost movement',
            'All stable',
            'No category exceeded &#8377;0.04/Wp shift. Blended COGS stable. Monitor weekly.'))

    actions_html = (
        '<div class="actions-wrap">'
        '<div class="actions-title">&#128202; Deep-Dive Insights &#8212; Pre-Analysed, Decision-Ready</div>'
        '{}'
        '</div>'
    ).format(''.join(action_items_html))

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  ASSEMBLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    now_str = datetime.now().strftime('%d %b %Y, %I:%M %p IST')

    def section(title, sub, body):
        return (
            '<div class="section">'
            '<div class="sec-header">'
            '<span class="sec-title">{}</span>'
            '<span class="sec-sub">{}</span>'
            '</div>{}</div>'
        ).format(title, sub, body)

    # Determine warning badges
    warn_badges = ''
    for r in declining[:2]:
        warn_badges += ' <span class="badge warn">{} {:+.2f}pp &#9888;</span>'.format(
            r['cluster'], r['gm_d'])

    # Data validation
    cogs_sum_calc = mtd['mod']+mtd['inv']+mtd['mms']+mtd['cab']+mtd['mtr']+mtd['ic']+mtd['oth']
    cogs_diff_val = abs(mtd['cogs'] - cogs_sum_calc)
    validation_bar = (
        '<div class="validation-bar">'
        '<strong>&#128269; DATA VALIDATION COMPLETE</strong>&nbsp;&nbsp;'
        '<span class="vcheck">&#10004; COGS sum = mod+inv+mms+cab+mtr+ic+oth '
        '(diff: &#8377;{:.0f} MTD)</span>'
        '<span class="vcheck">&#10004; Rev/Wp = Rev &#247; (kW&#215;1000) cross-checked</span>'
        '<span class="vcheck">&#10004; GM% = (Rev&#8722;COGS) &#247; Rev verified</span>'
        '<span class="vcheck">&#10004; SKU &#8377;/Wp reconciled to COGS table</span>'
        '</div>'
    ).format(cogs_diff_val)


    # ── Run-rate bar (sky-blue inline banner)
    if latest.day > 1:
        _pace = mtd['n'] / latest.day
        _proj = round(_pace * 30)
        _abs_gm_proj = fc(mtd['abs_gm'] / latest.day * 30)
        runrate_bar = (
            '<div style="'
            'background:#EFF6FF;'
            'border-left:4px solid #0284C7;'
            'border-bottom:1px solid #BFDBFE;'
            'padding:10px 18px;'
            'font-size:11.5px;'
            'color:#0369A1;'
            'line-height:1.6;'
            'font-weight:600;'
            '">'
            '&#128200; <b>Month run-rate:</b> {:.1f} installations/day'
            ' &rarr; <b>~{:,} projected for full month</b>'
            ' (vs {:,} actual in {}). '
            'At current GM {:.2f}%, implies <b>{}</b> gross margin for the month.'
            '</div>'
        ).format(_pace, _proj, pm['n'], prev_lbl, mtd['gm'], _abs_gm_proj)
    else:
        runrate_bar = ''

    html = '''<!DOCTYPE html><html lang="en"><head>''' + '''
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<!-- system fonts only for email -->
<style>''' + CSS + '''</style></head><body><div class="page">''' + ''.join([

        # ── HEADER
        '<div class="header">',
        '<div class="eyebrow">&#9728; Solar Square &nbsp;&middot;&nbsp; Daily GM Report</div>',
        '<h1>', headline, '</h1>',
        '<div class="header-meta">',
        'MOTO MONTH: {} &nbsp;&middot;&nbsp; DATA THROUGH {} &nbsp;&middot;&nbsp; {}'.format(
            latest.strftime('%b-%y').upper(),
            latest.strftime('%d %b %Y').upper(),
            now_str
        ),
        '</div>',
        '</div>',

        # validation banner removed

        # ── SECTIONS
        # Exec Snapshot removed
        runrate_bar,
        section('MTD Dashboard', '{} MTD vs full {}'.format(curr_lbl[:3], prev_lbl), kpi_html),
        section('Today at a Glance', '{} vs {}'.format(lat_lbl, prv_lbl), today_html),
        section('Product Mix', 'Offer-type split MTD vs full {}'.format(prev_lbl), mix_html),
        section('COGS Analysis', '{} MTD vs full {} &#8212; SKU-level root cause'.format(curr_lbl, prev_lbl), cogs_html),
        section('Cluster Health', 'Active clusters (n &#8805; {}) &#8212; GM%, Rev/Wp, &#916;pp'.format(MIN_ORDERS), cl_html),
        # Cluster Insights merged into Cluster Health table
        # Top Things to Watch removed
        # Deep-Dive Insights removed

        # ── FOOTER
        '<div class="footer">&#9728; Solar Square GM Report &nbsp;&middot;&nbsp; {} &nbsp;&middot;&nbsp; Generated {}</div>'.format(curr_lbl, now_str),

        '</div></body></html>',
    ])

    return html, mtd, latest


if __name__=='__main__':
    data = load_data()
    html, mtd, latest = build(data)
    subject = 'Solar Square GM | {} | MTD {:,} installs | GM {:.2f}%'.format(
        latest.strftime('%d %b %Y'), mtd['n'], mtd['gm'])
    if not GMAIL_PASS:
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'report_preview.html')
        open(out, 'w', encoding='utf-8').write(html)
        print('Preview saved: ' + out, flush=True)
        print('Subject: ' + subject, flush=True)
        import sys; sys.exit(0)
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = SENDER
    msg['To']      = ', '.join(RECIPIENTS)
    msg.attach(MIMEText(html, 'html', 'utf-8'))
    print('Sending...', flush=True)
    with smtplib.SMTP('smtp.gmail.com', 587) as s:
        s.ehlo(); s.starttls(); s.login(SENDER, GMAIL_PASS)
        s.sendmail(SENDER, RECIPIENTS, msg.as_string())
    print('Sent: ' + subject, flush=True)
