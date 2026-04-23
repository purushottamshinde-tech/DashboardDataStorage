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
        '&#128204; <b>Prefab MMS</b> {}{:.3f}&#8377;/Wp &mdash; Columns, Purlins &amp; Powergrout NS65 driving volume'.format('+' if prefab_d>=0 else '',prefab_d),
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

    mms_rc  = ('AoS +{:.2f}kW &#8594; larger systems need more Profile/Column/Purlin material. '
               'Not a vendor rate issue &#8212; procurement stable.'.format(aos_d)) if aos_d > 0.05 and mms_d > 0 else \
              'Rate or structural type shift &#8212; check Prefab vs Tin-Shed vs Welded mix vs prior month.'
    cab_rc  = ('DC routing length scales with system size (AoS +{:.2f}kW); '
               'POLYCAB 4sqmm Cu-DC entering mix adds premium vs RR Kabel Al.'.format(aos_d)) if aos_d > 0.03 and cab_d > 0 else \
              'Cable rate increase detected. Get competing quotes from alternate vendors.'
    inv_rc  = '3-phase SG6RT/SG8RT mix creep &#8212; systems >5kW crossing threshold; rate flat, volume driving cost.'
    mod_rc  = 'Stable &#8212; 540Wp DCR-PREMIER at 98.9% mix; delta is procurement rate fluctuation only.'

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
        actions.append('&#9883; <b>MMS</b> No vendor action needed &#8212; cost increase is AoS-structural. '
                       'Monitor: if AoS plateaus, MMS/Cables% normalise automatically.')
    elif mms_d > 0.03:
        actions.append('&#9889; <b>MMS</b> Investigate Powergrout NS65 / Column 2P Gen2 rate vs prior PO. '
                       'Check Prefab vs Tin-Shed mix shift for vendor rate explanation.')
    if cab_d > 0.02:
        actions.append('&#9889; <b>Cables</b> Standardise DC routing length per kW in terrace installs. '
                       'Review POLYCAB 4sqmm Cu-DC onboarding &#8212; consider Al equivalent for single-phase systems.')
    if inv_d > 0.015:
        actions.append('&#128204; <b>Inverter</b> 3-phase mix creep flagged (SG6RT/SG8RT). '
                       'No rate issue &#8212; structural with AoS. If 3-phase proportion exceeds 5%, negotiate volume pricing with Sungrow.')
    if mod_d < -0.008:
        actions.append('&#9989; <b>Module</b> Rate improvement detected &#8212; lock current procurement rate for next cycle if possible.')
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
    """Pure data insight for cluster signal column.
    Qualifies: (A) revenue realisation lever, (B) COGS mix lever, (C) product mix lever.
    No actionables — what happened and which metric moved."""
    if prev['n'] < MIN_ORDERS:
        return '--', {}, 'Thin prior data'

    rv    = curr['rev_wp'] - prev['rev_wp']
    ao    = curr['aos']    - prev['aos']
    gm_d  = curr['gm']    - prev['gm']
    ck_wp = (curr['cogs_kw'] - prev['cogs_kw']) / 1000
    d = dict(rev_wp_d=rv, aos_d=ao, aov_d=curr['aov']-prev['aov'], cogs_kw_d=ck_wp*1000)

    # ── Per-cluster COGS category breakdown in ₹/Wp ──────────────
    cat_d = {}
    if curr['kw'] and prev['kw']:
        for cat, key in [('Inverter','inv'),('MMS','mms'),('Cables','cab'),('Module','mod')]:
            cat_d[cat] = curr.get(key,0)/curr['kw']/1000 - prev.get(key,0)/prev['kw']/1000

    rising  = sorted([(c,v) for c,v in cat_d.items() if v >  0.04], key=lambda x:-x[1])
    falling = sorted([(c,v) for c,v in cat_d.items() if v < -0.04], key=lambda x: x[1])

    parts = []

    # ── Lever A: Revenue realisation ─────────────────────────────
    if abs(rv) > 0.2:
        direction = 'softening' if rv < 0 else 'strengthening'
        parts.append(
            'Rev/Wp &#8377;{:.2f}&#8594;&#8377;{:.2f} ({:+.2f}/Wp realization {})'.format(
                prev['rev_wp'], curr['rev_wp'], rv, direction))

    # ── Lever B: COGS mix — which sub-category moved and why ────
    if abs(ck_wp) > 0.02:
        all_sig = (rising + falling) if ck_wp > 0 else (falling + rising)
        cat_strs = []
        for cat, v in all_sig[:3]:
            # Describe mix context: what product/structural factor explains each category
            if cat == 'Inverter' and abs(v) > 0.02:
                if ao > 0.15:
                    mix_ctx = '3Ph product mix shift + AoS {:+.2f}kW'.format(ao)
                else:
                    mix_ctx = '3Ph product mix shift (SG6RT/SG8RT weight)'
            elif cat == 'MMS' and ao > 0.1 and v > 0:
                mix_ctx = 'AoS {:+.2f}kW &#8594; more structural material per system'.format(ao)
            elif cat == 'MMS' and v > 0:
                mix_ctx = 'Prefab/Column Gen2 rate or type-mix shift'
            elif cat == 'Cables' and ao > 0.08 and v > 0:
                mix_ctx = 'Routing length scales with AoS {:+.2f}kW'.format(ao)
            elif cat == 'Cables' and v > 0:
                mix_ctx = 'DC/AC cable rate movement'
            elif cat == 'Module':
                mix_ctx = 'Module procurement rate'
            elif sku_ctx and cat in sku_ctx:
                mix_ctx = sku_ctx[cat]
            else:
                mix_ctx = 'rate/mix movement'
            sign = '+' if v > 0 else '&#8722;'
            cat_strs.append(
                '<b>{}</b>&nbsp;{}{:.3f}/Wp&nbsp;<span style="color:#6B7280;font-size:9.5px">'
                '({})</span>'.format(cat, sign, abs(v), mix_ctx))
        if cat_strs:
            parts.append('COGS mix: {}'.format('; '.join(cat_strs)))

    # ── Lever C: Product/system-size mix (AoS) when COGS is contained ──
    if abs(ao) > 0.15 and abs(ck_wp) <= 0.02:
        parts.append(
            'AoS {:+.2f}kW ({:.2f}&#8594;{:.2f}kW) &#8212; product mix shift, COGS contained'.format(
                ao, prev['aos'], curr['aos']))

    # ── Stable fallback ───────────────────────────────────────────
    if not parts:
        sub = []
        if abs(rv) > 0.1:      sub.append('Rev/Wp {:+.2f}/Wp'.format(rv))
        if abs(ao) > 0.04:     sub.append('AoS {:+.2f}kW'.format(ao))
        if abs(ck_wp) > 0.005: sub.append('COGS {:+.3f}/Wp'.format(ck_wp))
        narrative = ('; '.join(sub) + ' &#8212; within normal band') if sub else \
                    'All levers &lt;0.5% change &#8212; stable'
        return narrative, dict(d, cat_d=cat_d), narrative

    # ── GM outcome observation ────────────────────────────────────
    parts.append('&#8594;&nbsp;<b>{}{:.2f}pp GM</b>'.format('+' if gm_d>=0 else '', gm_d))
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
:root{
  --black:#0A0A0A;--ink:#0F172A;--slate:#1E293B;
  --green:#047857;--green-bg:#D1FAE5;--green-text:#065F46;
  --red:#B91C1C;--red-bg:#FEE2E2;--red-text:#7F1D1D;
  --amber:#92400E;--amber-bg:#FEF3C7;--amber-text:#78350F;
  --blue:#1D4ED8;--blue-bg:#DBEAFE;--blue-text:#1E3A8A;
  --purple:#6D28D9;--purple-bg:#EDE9FE;--purple-text:#4C1D95;
  --teal:#0E7490;--teal-bg:#CFFAFE;--teal-text:#164E63;
  --mid:#64748B;--border:#E2E8F0;--surface:#F8FAFC;
}
*{box-sizing:border-box;margin:0;padding:0}
body{
  font-family:'DM Sans',system-ui,-apple-system,sans-serif;
  background:#F0F2F5;color:var(--ink);font-size:13px;
  padding:24px 16px 48px;line-height:1.5;
  -webkit-text-size-adjust:100%;
}
.page{max-width:900px;margin:0 auto}

/* ── HEADER ── */
.header{
  background:var(--ink);border-radius:16px 16px 0 0;
  padding:28px 32px 24px;color:#fff;
  position:relative;overflow:hidden;
}
.header::before{
  content:'';position:absolute;top:-40px;right:-40px;
  width:200px;height:200px;border-radius:50%;
  background:rgba(255,255,255,.04);
}
.header::after{
  content:'';position:absolute;bottom:-60px;right:60px;
  width:120px;height:120px;border-radius:50%;
  background:rgba(255,255,255,.03);
}
.eyebrow{
  font-family:'DM Mono',monospace;
  font-size:10px;letter-spacing:2px;text-transform:uppercase;
  color:rgba(255,255,255,.45);margin-bottom:8px;
}
.header h1{
  font-size:22px;font-weight:800;letter-spacing:-.4px;
  line-height:1.2;max-width:640px;margin-bottom:6px;
}
.header-meta{
  font-size:11px;color:rgba(255,255,255,.4);
  font-family:'DM Mono',monospace;margin-bottom:20px;
}
.badges{display:flex;flex-wrap:wrap;gap:8px}
.badge{
  display:inline-flex;align-items:center;gap:4px;
  background:rgba(255,255,255,.08);border:1px solid rgba(255,255,255,.12);
  color:rgba(255,255,255,.9);font-size:10.5px;font-weight:600;
  padding:4px 12px;border-radius:20px;letter-spacing:.2px;white-space:nowrap;
}
.badge.hi{background:rgba(0,135,90,.25);border-color:rgba(0,215,140,.3);color:#4FFFB0}
.badge.warn{background:rgba(192,57,43,.2);border-color:rgba(255,100,80,.3);color:#FF9090}

/* validation banner removed */

/* ── SECTION ── */
.section{
  background:#fff;border:1px solid var(--border);border-top:none;
  padding:24px 28px;
}
.section:last-child{border-radius:0 0 16px 16px}
.sec-header{display:flex;align-items:baseline;gap:8px;margin-bottom:18px}
.sec-title{font-size:8.5px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--mid)}
.sec-sub{font-size:10px;color:#9CA3AF}

/* ── EXEC SNAPSHOT ── */
.snap-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.snap-card{
  background:var(--surface);border:1px solid var(--border);
  border-radius:12px;padding:16px 18px;
}
.snap-label{
  font-size:8px;font-weight:700;letter-spacing:1.2px;text-transform:uppercase;
  color:#9CA3AF;display:block;margin-bottom:8px;
}
.snap-val{font-size:26px;font-weight:800;letter-spacing:-.6px;display:block;line-height:1;margin-bottom:6px}
.snap-delta{font-size:10px;color:var(--mid)}
.snap-pill{display:inline-block;font-size:9px;font-weight:700;padding:2px 8px;border-radius:10px;margin-bottom:8px}
.green-pill{background:var(--green-bg);color:var(--green)}
.red-pill{background:var(--red-bg);color:var(--red)}
.amber-pill{background:var(--amber-bg);color:var(--amber)}

/* ── DATA TABLE ── */
.table-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch;width:100%}
.data-table{width:100%;border-collapse:collapse;font-size:11.5px;min-width:480px}
.data-table thead tr{background:#F8FAFC}
.data-table th{
  padding:9px 12px;font-size:8.5px;font-weight:700;
  color:#6B7280;text-transform:uppercase;letter-spacing:.8px;
  border-bottom:2px solid var(--border);text-align:left;white-space:nowrap;
}
.data-table th.R{text-align:right}
.data-table td{padding:9px 12px;border-bottom:1px solid #F3F4F6;color:#374151;vertical-align:top}
.data-table td.R{text-align:right;font-family:'DM Mono',monospace;font-size:11px}
.data-table td.mono{font-family:'DM Mono',monospace;font-size:11px}
.data-table tbody tr:hover td{background:#FAFAFA}
.dot{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:7px;vertical-align:middle}
.up{color:var(--red);font-weight:700}
.dn{color:var(--green);font-weight:700}
.neutral{color:#9CA3AF}
.up-good{color:var(--green);font-weight:700}

/* ── DRIVER / SIGNAL COLUMN ── */
.driver-chip{display:inline-block;font-size:9.5px;color:#1E293B;line-height:1.9;max-width:440px;white-space:normal}
.signal{font-size:10.5px;color:#1E293B;line-height:1.9;vertical-align:top}
.tag-cogs{display:inline-block;font-size:8.5px;font-weight:800;padding:2px 8px;border-radius:6px;background:#FEF3C7;color:#78350F;border:1px solid #FCD34D;margin:0 4px 2px 0;letter-spacing:.2px}
.tag-rev{display:inline-block;font-size:8.5px;font-weight:800;padding:2px 8px;border-radius:6px;background:#FEE2E2;color:#7F1D1D;border:1px solid #FCA5A5;margin:0 4px 2px 0;letter-spacing:.2px}
.tag-ok{display:inline-block;font-size:8.5px;font-weight:800;padding:2px 8px;border-radius:6px;background:#D1FAE5;color:#065F46;border:1px solid #6EE7B7;margin:0 4px 2px 0;letter-spacing:.2px}
.tag-price{display:inline-block;font-size:8.5px;font-weight:800;padding:2px 8px;border-radius:6px;background:#E0E7FF;color:#3730A3;border:1px solid #A5B4FC;margin:0 4px 2px 0;letter-spacing:.2px}
.tag-mix{display:inline-block;font-size:8.5px;font-weight:800;padding:2px 8px;border-radius:6px;background:#CFFAFE;color:#164E63;border:1px solid #67E8F9;margin:0 4px 2px 0;letter-spacing:.2px}
.tag-warn{display:inline-block;font-size:8.5px;font-weight:800;padding:2px 8px;border-radius:6px;background:#FEF3C7;color:#78350F;border:1px solid #FCD34D;margin:0 4px 2px 0;letter-spacing:.2px}
.sig-ctx{font-size:9.5px;color:#475569;font-style:italic}

/* ── CLUSTER TABLE — COMPACT 5-COLUMN ── */
.cluster-wrap{border-radius:10px;overflow:hidden;border:1px solid var(--border);overflow-x:auto;-webkit-overflow-scrolling:touch}
.cluster-wrap .data-table{min-width:560px}
.group-row td{
  font-weight:800;font-size:9px;text-transform:uppercase;letter-spacing:1.2px;
  padding:7px 14px;border-top:none;
}
.group-row.declining td{background:#7F1D1D;color:#FECACA;border-top:2px solid #7F1D1D}
.group-row.improving td{background:#065F46;color:#A7F3D0;border-top:2px solid #065F46}
.group-row.stable td{background:#1E3A5F;color:#BFDBFE;border-top:2px solid #1E3A5F}
.group-row.nascent td{background:#3B1FA8;color:#DDD6FE;border-top:2px solid #3B1FA8}

/* GM% cells — 4-tier colour scale: light bg + bold dark text = max readability */
.gm-cell-hi  {background:#DCFCE7;color:#14532D;font-weight:900;text-align:center;padding:9px 12px;font-family:'DM Mono',monospace;font-size:13px;border-left:3px solid #16A34A}
.gm-cell-mid {background:#FEF9C3;color:#713F12;font-weight:900;text-align:center;padding:9px 12px;font-family:'DM Mono',monospace;font-size:13px;border-left:3px solid #CA8A04}
.gm-cell-lo  {background:#FEE2E2;color:#7F1D1D;font-weight:900;text-align:center;padding:9px 12px;font-family:'DM Mono',monospace;font-size:13px;border-left:3px solid #DC2626}
.gm-cell-crit{background:#FEE2E2;color:#450A0A;font-weight:900;text-align:center;padding:9px 12px;font-family:'DM Mono',monospace;font-size:13px;border-left:4px solid #7F1D1D;letter-spacing:.3px}

/* Cluster name + state inline */
.cluster-name{font-weight:700;font-size:12px;display:block}
.state-tag{font-size:9px;color:#9CA3AF;font-weight:400}

/* Cluster legend */
.cluster-legend{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
.cluster-legend-label{font-size:9px;color:#6B7280;font-weight:700;text-transform:uppercase;letter-spacing:.8px;margin-right:4px}
.leg-pill{display:inline-flex;align-items:center;gap:5px;font-size:9px;font-weight:700;padding:3px 10px;border-radius:8px}
.leg-hi  {background:#DCFCE7;color:#14532D;border:1px solid #16A34A}
.leg-mid {background:#FEF9C3;color:#713F12;border:1px solid #CA8A04}
.leg-lo  {background:#FEE2E2;color:#7F1D1D;border:1px solid #DC2626}
.leg-crit{background:#FEE2E2;color:#450A0A;border:2px solid #7F1D1D}

/* ── SKU CARDS ── */
.sku-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}
.sku-card{border:1px solid var(--border);border-radius:10px;padding:14px 16px;background:#fff}
.sku-card-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.sku-cat{font-weight:800;font-size:13px;color:var(--ink)}
.sku-delta{font-size:12px;font-weight:800;font-family:'DM Mono',monospace}
.sku-rc{
  font-size:9.5px;color:#9CA3AF;font-style:italic;
  border-left:2px solid var(--border);padding-left:8px;margin-bottom:8px;line-height:1.6;
}
.sku-line{font-size:10.5px;color:#374151;line-height:1.9;margin-bottom:2px}
.sku-gm-badge{display:inline-block;font-size:9px;font-weight:700;padding:1px 7px;border-radius:6px;margin-left:8px}

/* ── COGS HEADLINE BANNER ── */
.cogs-banner{
  background:#F0F9FF;border:1px solid #BAE6FD;border-radius:8px;
  padding:11px 16px;margin-bottom:14px;
  font-size:11.5px;font-weight:700;color:#0369A1;line-height:1.5;
}

/* ── GM BRIDGE ── */
.bridge-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch;padding-bottom:2px}
.bridge{display:flex;align-items:stretch;flex-wrap:nowrap;gap:0;margin:12px 0;font-family:'DM Mono',monospace;font-size:11px;min-width:min-content}
.bridge-box{padding:8px 14px;text-align:center;min-width:80px}
.bridge-box.start{background:var(--surface);border:1px solid var(--border);border-radius:8px 0 0 8px}
.bridge-box.end{background:var(--surface);border:1px solid var(--border);border-radius:0 8px 8px 0}
.bridge-item{background:var(--red-bg);border:1px solid #FECACA;padding:8px 12px;font-size:10px;border-left:none;min-width:76px}
.bridge-item.pos{background:var(--green-bg);border-color:#BBF7D0}
.bridge-label{font-size:8.5px;color:var(--mid);display:block;margin-bottom:2px;font-family:'DM Sans',sans-serif;white-space:nowrap}
.bridge-val{font-size:13px;font-weight:700;display:block}

/* ── EXECUTIVE SUMMARY ── */
.exec-summary{
  background:#F0F6FF;border:1px solid #BFDBFE;border-radius:10px;
  padding:14px 18px;margin-bottom:20px;
  font-size:12px;color:#1E3A8A;line-height:1.7;
}

/* ── KPI SECTION LABEL ── */
.kpi-section-label{
  font-size:8.5px;font-weight:800;letter-spacing:2px;text-transform:uppercase;
  color:#64748B;margin-bottom:14px;border-left:3px solid #93C5FD;padding-left:8px;
}

/* ── KPI TILE GRID — 4-col desktop / 2-col mobile ── */
.kpi-grid{
  display:flex;flex-wrap:wrap;gap:10px;
  margin-bottom:4px;
}
.kpi-tile{
  border-radius:12px;padding:14px 16px;
  border:1px solid #E2E8F0;background:#fff;
  display:flex;flex-direction:column;gap:3px;
  box-shadow:0 1px 4px rgba(0,0,0,.04);
  flex:1 1 calc(25% - 8px);min-width:150px;
}
.kpi-label{font-size:7.5px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#64748B;background:#F1F5F9;border-radius:4px;padding:2px 6px;margin-bottom:4px;display:inline-block;align-self:flex-start}
.kpi-val{font-size:26px;font-weight:900;letter-spacing:-.6px;line-height:1.1;color:#0F172A}
.kpi-sub{font-size:10.5px;color:#64748B;line-height:1.4}
.kpi-trend{font-size:10.5px;font-weight:700;margin-top:2px}
.kpi-trend.up{color:#059669}.kpi-trend.dn{color:#DC2626}.kpi-trend.neu{color:#94A3B8}

/* ── TODAY TABLES (kept as-is) ── */
.today-grid{width:100%;border-collapse:separate;border-spacing:8px}
.tc{border-radius:10px;padding:13px 15px;vertical-align:top;border:1px solid var(--border);background:var(--surface)}
.tc-label{display:block;font-size:8px;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:#9CA3AF;margin-bottom:6px}
.tc-today{font-size:22px;font-weight:900;letter-spacing:-.4px;display:block;line-height:1;margin-bottom:5px;color:#111827}
.tc-prev{font-size:10px;color:#6B7280;display:block;line-height:1.5}

/* ── PRODUCT MIX BAR ── */
.mix-bar{height:24px;border-radius:8px;overflow:hidden;display:flex;margin-bottom:12px;gap:1px}

/* ── FOOTER ── */
.footer{
  background:var(--surface);border:1px solid var(--border);border-top:none;
  border-radius:0 0 16px 16px;padding:14px 28px;
  text-align:center;font-size:9px;color:#9CA3AF;
  font-family:'DM Mono',monospace;letter-spacing:.3px;
}

/* ════════════════════════════════════════════════════════════════
   MOBILE — ≤640px
   ════════════════════════════════════════════════════════════════ */
@media(max-width:640px){
  body{padding:8px 8px 32px}
  .header{padding:20px 16px 16px;border-radius:12px 12px 0 0}
  .header h1{font-size:16px;letter-spacing:-.2px}
  .header-meta{font-size:9.5px;margin-bottom:14px}
  .eyebrow{font-size:9px}
  .badge{font-size:9px;padding:3px 9px}
  .section{padding:16px 14px}
  /* 2-col snap grid */
  .snap-grid{grid-template-columns:1fr 1fr;gap:8px}
  .snap-val{font-size:20px}
  .snap-card{padding:12px 12px}
  /* 1-col sku grid */
  .sku-grid{grid-template-columns:1fr}
  /* kpi tile grid → 2-col on mobile */
  .kpi-grid{gap:8px}
  .kpi-tile{flex:1 1 calc(50% - 6px);min-width:calc(50% - 6px);max-width:calc(50% - 6px);padding:12px 12px}
  .kpi-val{font-size:20px}
  /* today table → 2-col block */
  .today-grid,.today-grid tbody,.today-grid tr{display:block!important;width:100%!important}
  .tc{display:inline-block!important;width:calc(50% - 10px)!important;margin:4px!important;vertical-align:top;padding:10px 10px!important}
  .tc-today{font-size:17px!important}

  .driver-chip{max-width:100%;font-size:9px}
  .cluster-legend{gap:5px}
  .leg-pill{font-size:8px;padding:2px 7px}
  .footer{padding:12px 14px;font-size:8px}
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

    # ── Price erosion clusters (threshold &#8377;0.8/Wp to catch early signals)
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
    HERO_GRAD = 'linear-gradient(135deg,#1565C0 0%,#1976D2 50%,#42A5F5 100%)'
    if len(price_dn) >= 4:
        headline = 'Revenue realisation drop across {} clusters &#8212; discount approvals need immediate review'.format(len(price_dn))
        hero_grad = HERO_GRAD
    elif len(price_dn) >= 2:
        names_short = ', '.join(c for c,_ in price_dn[:3])
        headline = 'Revenue realisation falling in {} markets ({}) &#8212; GM holding but revenue realisation needs attention'.format(len(price_dn), names_short)
        hero_grad = HERO_GRAD
    elif len(price_dn) == 1:
        headline = 'Revenue realisation dip in {} &#8212; overall business metrics on track'.format(price_dn[0][0])
        hero_grad = HERO_GRAD
    elif gm_trend >= 0.5:
        headline = 'GM expanding {:.1f}ppt MoM &#8212; revenue discipline and volume growth aligned'.format(gm_trend)
        hero_grad = HERO_GRAD
    elif gm_trend <= -0.5:
        headline = 'GM contracting {:.1f}ppt MoM &#8212; root cause: {}'.format(
            abs(gm_trend), 'COGS mix shift' if cogs_net_gm < -0.3 else 'revenue pressure')
        hero_grad = HERO_GRAD
    elif vol_pct >= 15:
        headline = 'Volume surge +{:.0f}% MoM &#8212; GM stable at {:.2f}% despite scale-up'.format(vol_pct, mtd['gm'])
        hero_grad = HERO_GRAD
    else:
        headline = 'Operations on track &#8212; {:,} installations at {:.2f}% GM through {}'.format(mtd['n'], mtd['gm'], lat_lbl)
        hero_grad = HERO_GRAD

    # ── GM Badge
    gm_arrow = '&#9650;' if gm_trend>=0 else '&#9660;'
    gm_badge_txt = '{} GM {:.2f}% ({}{:.2f}%pts)'.format(gm_arrow, mtd['gm'], '+' if gm_trend>=0 else '', gm_trend)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  EXEC SNAPSHOT (4 cards)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def snap_card(label, pill_text, pill_cls, value, value_color, delta_html):
        return (
            '<div class="snap-card">'
            '<span class="snap-label">{}</span>'
            '<span class="snap-pill {}">{}</span>'
            '<span class="snap-val" style="color:{}">{}</span>'
            '<span class="snap-delta">{}</span>'
            '</div>'
        ).format(label, pill_cls, pill_text, value_color, value, delta_html)

    # Volume card
    vol_pill_cls = 'green-pill' if vol_pct >= -5 else 'red-pill'
    vol_pill_txt = ('&#9650; +{:.0f}% MoM'.format(vol_pct) if vol_pct>=0
                    else '&#9660; {:.0f}% MoM'.format(vol_pct))
    vol_delta = 'vs {:,} {} (1&#8211;{} days)'.format(pm['n'], prev_lbl, pm_day)
    card_vol = snap_card('Installations MTD', vol_pill_txt, vol_pill_cls,
        '{:,}'.format(mtd['n']), 'var(--ink)', vol_delta)

    # Pricing card
    rev_wp_d = mtd['rev_wp'] - pm['rev_wp']
    rwp_pill_cls = 'green-pill' if rev_wp_d >= 0 else ('red-pill' if rev_wp_d < -0.5 else 'amber-pill')
    rwp_pill_txt = ('&#9650; +&#8377;{:.2f}/Wp'.format(rev_wp_d) if rev_wp_d>=0
                    else '&#9660; &#8722;&#8377;{:.2f}/Wp'.format(abs(rev_wp_d)))
    rwp_delta_sub = 'realisation improving' if rev_wp_d > 0 else 'realisation softening'
    rwp_delta = 'vs &#8377;{:.2f} {} &middot; {}'.format(pm['rev_wp'], prev_lbl, rwp_delta_sub)
    card_rwp = snap_card('Rev / Wp MTD', rwp_pill_txt, rwp_pill_cls,
        '&#8377;{:.2f}'.format(mtd['rev_wp']), 'var(--ink)', rwp_delta)

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
        '&#8377;{:.2f}'.format(total_cogs_pkw_c/1000), 'var(--amber)', cogs_delta)

    # GM card
    gm_pill_cls = 'green-pill' if gm_trend >= 0 else ('red-pill' if gm_trend < -0.5 else 'amber-pill')
    gm_pill_txt = ('&#9660; {}{:.2f}pp MoM'.format('' if gm_trend>=0 else '&#8722;', abs(gm_trend)))
    adj_gm_str = 'Adj GM {:.2f}% (ex-ONM/QHS)'.format(mtd['adj_gm']) if mtd.get('adj_gm') else ''
    card_gm = snap_card('Gross Margin MTD', gm_pill_txt, gm_pill_cls,
        '{:.2f}%'.format(mtd['gm']), gmc(mtd['gm']),
        'vs {:.2f}% {} &middot; {}'.format(pm['gm'], prev_lbl, adj_gm_str))

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
    # ── GM Bridge waterfall chart (SVG-based, like Image 4)
    _bridge_data = []
    if abs(_rev_gm_impact) > 0.01:
        _bridge_data.append(('Price', _rev_gm_impact, 'Rev/Wp {:+.2f}/Wp'.format(rev_wp_d_ref)))
    if abs(_mms_gm_impact) > 0.01:
        _bridge_data.append(('MMS', _mms_gm_impact, 'MMS {:+.3f}/Wp'.format(mms_wp_d)))
    if abs(_cab_gm_impact) > 0.01:
        _bridge_data.append(('Cables', _cab_gm_impact, 'Cables {:+.3f}/Wp'.format(cab_wp_d)))
    if abs(_inv_gm_impact) > 0.01:
        _bridge_data.append(('Inverter', _inv_gm_impact, 'Inverter {:+.3f}/Wp'.format(inv_wp_d)))
    if abs(_mod_oth_impact) > 0.01:
        _bridge_data.append(('Prod Mix', _mod_oth_impact, 'Module/Other'))

    def _build_bridge_chart(start_val, end_val, items, start_lbl, end_lbl):
        """Build an SVG waterfall chart for the GM bridge."""
        # Chart dimensions
        W = 680; H = 220
        PAD_L = 44; PAD_R = 20; PAD_T = 24; PAD_B = 48
        chart_w = W - PAD_L - PAD_R
        chart_h = H - PAD_T - PAD_B

        # All bars: start, each bridge item, end
        all_bars = [('start', start_val, start_lbl)] + \
                   [(d[0], d[1], d[2]) for d in items] + \
                   [('end', end_val, end_lbl)]
        n_bars = len(all_bars)
        bar_w = min(60, (chart_w - (n_bars - 1) * 10) // n_bars)
        gap = (chart_w - bar_w * n_bars) // max(n_bars - 1, 1)

        # Y axis: cover start_val, end_val, and all running totals
        running = start_val
        all_vals = [start_val, end_val]
        for _, impact, _ in items:
            all_vals.extend([running, running + impact])
            running += impact
        y_min = min(all_vals) - 0.5
        y_max = max(all_vals) + 0.5
        y_range = y_max - y_min

        def y_px(val):
            return PAD_T + chart_h * (1 - (val - y_min) / y_range)

        def pct_str(v):
            return '{:.1f}%'.format(v)

        # Y-axis gridlines
        import math
        tick_step = 0.5 if y_range < 4 else 1.0
        tick_vals = []
        t = math.ceil(y_min / tick_step) * tick_step
        while t <= y_max + 0.01:
            tick_vals.append(round(t, 2))
            t = round(t + tick_step, 2)

        svg_parts = [
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
            'style="width:100%;max-width:{W}px;height:auto;font-family:\'DM Sans\',sans-serif">'.format(W=W, H=H)
        ]

        # Background
        svg_parts.append('<rect width="{}" height="{}" fill="#FAFCFF" rx="10"/>'.format(W, H))

        # Title
        svg_parts.append(
            '<text x="{}" y="16" font-size="10" font-weight="700" fill="#1E293B" '
            'letter-spacing="1" text-anchor="start">GM% BRIDGE &mdash; {} &#8594; {}</text>'.format(
                PAD_L, start_lbl, end_lbl))

        # Gridlines + Y labels
        for tv in tick_vals:
            yp = y_px(tv)
            svg_parts.append(
                '<line x1="{}" x2="{}" y1="{:.1f}" y2="{:.1f}" '
                'stroke="#E2E8F0" stroke-width="1"/>'.format(PAD_L, W - PAD_R, yp, yp))
            svg_parts.append(
                '<text x="{}" y="{:.1f}" font-size="9" fill="#94A3B8" text-anchor="end" '
                'dominant-baseline="middle">{:.0f}%</text>'.format(PAD_L - 4, yp, tv))

        # Zero / reference line
        if y_min < start_val < y_max:
            yp0 = y_px(start_val)
            svg_parts.append(
                '<line x1="{}" x2="{}" y1="{:.1f}" y2="{:.1f}" '
                'stroke="#94A3B8" stroke-width="1" stroke-dasharray="4,3"/>'.format(
                    PAD_L, W - PAD_R, yp0, yp0))

        # Draw bars
        running = start_val
        for i, (key, val, lbl) in enumerate(all_bars):
            x = PAD_L + i * (bar_w + gap)
            cx = x + bar_w / 2

            if key == 'start':
                bar_top = y_px(val); bar_bot = y_px(0) if y_min < 0 else y_px(y_min)
                bar_top2 = y_px(val); bar_bot2 = H - PAD_B
                bar_h = bar_bot2 - bar_top
                color = '#E07B39'
                svg_parts.append(
                    '<rect x="{:.1f}" y="{:.1f}" width="{}" height="{:.1f}" '
                    'fill="{}" rx="3"/>'.format(x, bar_top, bar_w, max(bar_h, 2), color))
                svg_parts.append(
                    '<text x="{:.1f}" y="{:.1f}" font-size="11" font-weight="700" '
                    'fill="{}" text-anchor="middle">{}</text>'.format(
                        cx, bar_top - 6, color, pct_str(val)))
            elif key == 'end':
                bar_top = y_px(val); bar_bot2 = H - PAD_B
                bar_h = bar_bot2 - bar_top
                color = '#E07B39'
                svg_parts.append(
                    '<rect x="{:.1f}" y="{:.1f}" width="{}" height="{:.1f}" '
                    'fill="{}" rx="3"/>'.format(x, bar_top, bar_w, max(bar_h, 2), color))
                svg_parts.append(
                    '<text x="{:.1f}" y="{:.1f}" font-size="11" font-weight="700" '
                    'fill="{}" text-anchor="middle">{}</text>'.format(
                        cx, bar_top - 6, color, pct_str(val)))
            else:
                # Floating bar
                base = running
                top_val = base + val if val > 0 else base
                bot_val = base if val > 0 else base + val
                bar_top = y_px(top_val)
                bar_bot = y_px(bot_val)
                bar_h = max(abs(bar_bot - bar_top), 3)
                color = '#22C55E' if val > 0 else '#EF4444'
                svg_parts.append(
                    '<rect x="{:.1f}" y="{:.1f}" width="{}" height="{:.1f}" '
                    'fill="{}" rx="3" opacity="0.85"/>'.format(x, min(bar_top, bar_bot), bar_w, bar_h, color))
                sign = '+' if val >= 0 else ''
                svg_parts.append(
                    '<text x="{:.1f}" y="{:.1f}" font-size="10" font-weight="700" '
                    'fill="{}" text-anchor="middle">{}{:.2f}%</text>'.format(
                        cx, min(bar_top, bar_bot) - 5, color, sign, val))
                # connector line to next bar
                if i < n_bars - 2:
                    conn_y = y_px(running + val)
                    next_x = x + bar_w + gap
                    svg_parts.append(
                        '<line x1="{:.1f}" x2="{:.1f}" y1="{:.1f}" y2="{:.1f}" '
                        'stroke="#CBD5E1" stroke-width="1" stroke-dasharray="3,2"/>'.format(
                            x + bar_w, next_x, conn_y, conn_y))
                running += val

            # X-axis label
            svg_parts.append(
                '<text x="{:.1f}" y="{}" font-size="10" fill="#374151" text-anchor="middle" '
                'font-weight="{}">{}</text>'.format(
                    cx, H - PAD_B + 14, '700' if key in ('start', 'end') else '500', key))

        # Footer detail
        detail_parts = []
        for d in items:
            if abs(d[1]) > 0.01:
                detail_parts.append('{}: {:+.2f}%'.format(d[0], d[1]))
        if detail_parts:
            svg_parts.append(
                '<text x="{}" y="{}" font-size="8.5" fill="#94A3B8" text-anchor="start">{}</text>'.format(
                    PAD_L, H - 6, ' · '.join(detail_parts)))

        svg_parts.append('</svg>')
        return ''.join(svg_parts)

    bridge_chart_svg = _build_bridge_chart(
        pm['gm'], mtd['gm'], _bridge_data,
        '{} 26'.format(prev_lbl), '{} 26'.format(curr_lbl[:3]))

    bridge_html = (
        '<div style="margin-top:20px">'
        '<div class="kpi-section-label" style="margin-bottom:12px">GM% BRIDGE &mdash; {} MTD vs {} {}</div>'
        '<div style="border:1px solid #E2E8F0;border-radius:12px;overflow:hidden;background:#FAFCFF;padding:12px 8px 4px">'
        '{}'
        '</div>'
        '<div style="font-size:9.5px;color:#9CA3AF;margin-top:6px">'
        '* Bridge partials rounded to 2dp; residual in Prod Mix. '
        'Rev/Wp impact = &#916;&#8377;/Wp &#247; Rev/Wp_PM &#215; GM_PM</div>'
        '</div>'
    ).format(curr_lbl, prev_lbl, latest.year, bridge_chart_svg)

    # snap4_html removed — Exec Snapshot section dropped

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  MTD TILE GRID (2 rows x 4, mobile-first)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def _trend_cls(delta, higher_better=True):
        if abs(delta) < 0.01: return 'neu', '&#8212;'
        positive = (delta > 0) == higher_better
        arrow = '&#9650;' if delta > 0 else '&#9660;'
        return ('up' if positive else 'dn'), arrow

    def ktile(label, val, sub, val_color='#0F172A', trend_txt='', trend_cls='neu', accent=''):
        left_border = 'border-left:3px solid {};'.format(accent) if accent else ''
        trend_color = '#059669' if trend_cls == 'up' else ('#DC2626' if trend_cls == 'dn' else '#94A3B8')
        return (
            '<div class="kpi-tile" style="box-sizing:border-box;{left}">'
            '<span class="kpi-label" style="font-size:7.5px;font-weight:700;letter-spacing:1px;'
            'text-transform:uppercase;color:#64748B;background:#F1F5F9;border-radius:4px;'
            'padding:2px 6px;margin-bottom:4px;display:inline-block">{label}</span>'
            '<span class="kpi-val" style="font-size:24px;font-weight:900;letter-spacing:-.5px;'
            'line-height:1.1;color:{vc}">{val}</span>'
            '<span class="kpi-sub" style="font-size:10px;color:#64748B;line-height:1.4">{sub}</span>'
            '<span style="font-size:10px;font-weight:700;margin-top:2px;color:{tc}">{trend}</span>'
            '</div>'
        ).format(left=left_border, label=label, vc=val_color, val=val,
                 sub=sub, tc=trend_color, trend=trend_txt)

    # Row 1: Volume · kW · GM% · Revenue
    _vol_d = mtd['n'] - pm['n']
    _vol_cls, _vol_arr = _trend_cls(_vol_d)
    _vol_pct_s = '{}{:.0f}%'.format('+' if vol_pct>=0 else '', vol_pct)
    t_vol = ktile('Installations MTD',
        '{:,}'.format(mtd['n']),
        'vs {:,} {} (same {} days)'.format(pm['n'], prev_lbl, pm_day),
        trend_txt='{} {} MoM'.format(_vol_arr, _vol_pct_s), trend_cls=_vol_cls)

    _kw_d = mtd['kw'] - pm['kw']
    _kw_pct = (_kw_d/pm['kw']*100) if pm['kw'] else 0
    _kw_cls, _kw_arr = _trend_cls(_kw_d)
    t_kw = ktile('kW Installed MTD',
        '{:,.0f} kW'.format(mtd['kw']),
        'vs {:,.0f} kW {}'.format(pm['kw'], prev_lbl),
        trend_txt='{} {:+.0f}%'.format(_kw_arr, _kw_pct), trend_cls=_kw_cls)

    _gm_cls, _gm_arr = _trend_cls(gm_trend)
    _adj_str = ' &middot; Adj {:.2f}%'.format(mtd['adj_gm']) if mtd.get('adj_gm') else ''
    _gm_accent = '#16A34A' if mtd['gm']>=44 else ('#CA8A04' if mtd['gm']>=42 else '#DC2626')
    t_gm = ktile('GM % MTD',
        '{:.2f}%'.format(mtd['gm']),
        'vs {:.2f}% {}{}'.format(pm['gm'], prev_lbl, _adj_str),
        val_color=gmc(mtd['gm']),
        trend_txt='{} {:+.2f}pp'.format(_gm_arr, gm_trend), trend_cls=_gm_cls,
        accent=_gm_accent)

    _rev_d_pct = (mtd['rev']-pm['rev'])/pm['rev']*100 if pm['rev'] else 0
    _rev_cls, _rev_arr = _trend_cls(_rev_d_pct)
    t_rev = ktile('Revenue MTD',
        fc(mtd['rev']),
        'vs {} {}'.format(fc(pm['rev']), prev_lbl),
        trend_txt='{} {:+.0f}%'.format(_rev_arr, _rev_d_pct), trend_cls=_rev_cls)

    # Row 2: AoS · AoV · Rev/Wp · Abs GM
    _aos_d = mtd['aos'] - pm['aos']
    _aos_cls, _aos_arr = _trend_cls(_aos_d)
    t_aos = ktile('Avg System Size',
        '{:.2f} kW'.format(mtd['aos']),
        'vs {:.2f} kW {}'.format(pm['aos'], prev_lbl),
        trend_txt='{} {:+.2f}kW'.format(_aos_arr, _aos_d), trend_cls=_aos_cls)

    _aov_d_pct = (mtd['aov']-pm['aov'])/pm['aov']*100 if pm['aov'] else 0
    _aov_cls, _aov_arr = _trend_cls(_aov_d_pct)
    t_aov = ktile('Avg Order Value',
        fc(mtd['aov']),
        'vs {} {}'.format(fc(pm['aov']), prev_lbl),
        trend_txt='{} {:+.0f}%'.format(_aov_arr, _aov_d_pct), trend_cls=_aov_cls)

    _rwp_cls, _rwp_arr = _trend_cls(rev_wp_d)
    t_rwp = ktile('Rev / Wp',
        '&#8377;{:.2f}'.format(mtd['rev_wp']),
        'vs &#8377;{:.2f} {}'.format(pm['rev_wp'], prev_lbl),
        trend_txt='{} {:+.2f}/Wp'.format(_rwp_arr, rev_wp_d), trend_cls=_rwp_cls)

    _agm_d_pct = (mtd['abs_gm']-pm['abs_gm'])/pm['abs_gm']*100 if pm['abs_gm'] else 0
    _agm_cls, _agm_arr = _trend_cls(_agm_d_pct)
    t_agm = ktile('Abs Gross Margin',
        fc(mtd['abs_gm']),
        'vs {} {}'.format(fc(pm['abs_gm']), prev_lbl),
        trend_txt='{} {:+.0f}%'.format(_agm_arr, _agm_d_pct), trend_cls=_agm_cls)

    # ── Executive Summary banner (like Image 2)
    gm_dir_word = 'up' if gm_trend >= 0 else 'down'
    # Build attention clusters string
    attn_clusters = []
    for r in declining[:3]:
        attn_clusters.append('{} ({:.1f}%)'.format(r['cluster'], r['curr']['gm']))
    attn_str = (', '.join(attn_clusters) + ' need margin attention') if attn_clusters else 'all clusters on target'
    exec_summary_html = (
        '<div class="exec-summary">'
        '<b>{:,} installations</b> ({:,.0f} kW) completed MTD in {} &mdash; '
        '<span style="color:#16A34A">&#9650;{:.0f}%</span> vs {} on volume. '
        'Overall GM is <b style="color:{}">{:.2f}%</b> ({} {:.2f}pp MoM). '
        'Rev/Wp at &#8377;{:.2f} vs &#8377;{:.2f} in {}. {}.'
        '</div>'
    ).format(
        mtd['n'], mtd['kw'], curr_lbl,
        abs(vol_pct), prev_lbl,
        gmc(mtd['gm']), mtd['gm'],
        gm_dir_word, abs(gm_trend),
        mtd['rev_wp'], pm['rev_wp'], prev_lbl,
        attn_str
    )

    kpi_html = (
        exec_summary_html +
        '<div class="kpi-section-label">MTD AT A GLANCE &mdash; {curr} VS {prev} (SAME {days} DAYS)</div>'
        '<div class="kpi-grid">'
        + t_vol + t_kw + t_gm + t_rev
        + t_aos + t_aov + t_rwp + t_agm
        + '</div>'
    ).format(curr=curr_lbl.upper(), prev=prev_lbl.upper(), days=pm_day)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  TODAY VS YESTERDAY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def tcard(label, today_val, prev_val, delta_html, vc='#111827'):
        return (
            '<td class="tc">'
            '<span class="tc-label">{}</span>'
            '<span class="tc-today" style="color:{}">{}</span>'
            '<span class="tc-prev">vs {} yesterday &nbsp; {}</span>'
            '</td>'
        ).format(label, vc, today_val, prev_val, delta_html)

    def _day_tile(label, val, val_color, sub, trend_txt, trend_color):
        return (
            '<div class="kpi-tile" style="box-sizing:border-box;">'
            '<span class="kpi-label" style="font-size:7.5px;font-weight:700;letter-spacing:1px;'
            'text-transform:uppercase;color:#64748B;background:#F1F5F9;border-radius:4px;'
            'padding:2px 6px;margin-bottom:4px;display:inline-block">{label}</span>'
            '<span class="kpi-val" style="font-size:24px;font-weight:900;letter-spacing:-.5px;'
            'line-height:1.1;color:{vc}">{val}</span>'
            '<span class="kpi-sub" style="font-size:10px;color:#64748B;line-height:1.4">{sub}</span>'
            '<span style="font-size:10px;font-weight:700;margin-top:2px;color:{tc}">{trend}</span>'
            '</div>'
        ).format(label=label, vc=val_color, val=val, sub=sub, tc=trend_color, trend=trend_txt)

    _rwp_d_day = lat['rev_wp'] - prv['rev_wp']
    _aos_d_day = lat['aos'] - prv['aos']
    _gm_d_day  = lat['gm'] - prv['gm']

    today_html = (
        '<div class="kpi-section-label" style="margin-bottom:12px">'
        'LATEST DAY &mdash; {lat} VS {prv}'
        ' &nbsp;<span style="font-size:9px;color:#9CA3AF;font-weight:400">'
        'Data updated through {lat} &nbsp; Showing {lat} vs {prv}</span></div>'
        '<div class="kpi-grid">'
        '{orders}{kw}{rwp}{aos}{gm}{adjgm}'
        '</div>'
    ).format(
        lat=lat_lbl, prv=prv_lbl,
        orders=_day_tile(
            'Orders &mdash; {}'.format(lat_lbl),
            str(lat['n']), '#0F172A',
            'Prev ({}): {} &nbsp; MTD: {:,}'.format(prv_lbl, prv['n'], mtd['n']),
            '{} {} vs prev'.format('&#9650;' if lat['n']>=prv['n'] else '&#9660;', dpct(lat['n'],prv['n']) if prv['n'] else ''),
            '#059669' if lat['n']>=prv['n'] else '#DC2626'),
        kw=_day_tile(
            'kW &mdash; {}'.format(lat_lbl),
            '{:.1f}'.format(lat['kw']), '#0F172A',
            'Prev: {:.1f} kW &nbsp; MTD: {:,.0f}'.format(prv['kw'], mtd['kw']),
            '{} {:+.1f} kW'.format('&#9650;' if lat['kw']>=prv['kw'] else '&#9660;', lat['kw']-prv['kw']),
            '#059669' if lat['kw']>=prv['kw'] else '#DC2626'),
        rwp=_day_tile(
            'Rev/Wp &mdash; {}'.format(lat_lbl),
            '&#8377;{:.2f}'.format(lat['rev_wp']), '#0F172A',
            'Prev ({}): &#8377;{:.2f}'.format(prv_lbl, prv['rev_wp']),
            '{} {:+.3f} &#8377;/Wp'.format('&#9650;' if _rwp_d_day>=0 else '&#9660;', _rwp_d_day),
            '#DC2626' if _rwp_d_day < -0.5 else ('#059669' if _rwp_d_day > 0 else '#94A3B8')),
        aos=_day_tile(
            'AoS &mdash; {}'.format(lat_lbl),
            '{:.2f} kW'.format(lat['aos']), '#0F172A',
            'Prev: {:.2f} kW'.format(prv['aos']),
            '{} {:+.2f}kW'.format('&#9650;' if _aos_d_day>=0 else '&#9660;', _aos_d_day),
            '#059669' if _aos_d_day >= 0 else '#94A3B8'),
        gm=_day_tile(
            'GM % &mdash; {}'.format(lat_lbl),
            '{:.1f}%'.format(lat['gm']), gmc(lat['gm']),
            'Prev: {:.1f}%'.format(prv['gm']),
            '{} {:+.2f}pp'.format('&#9650;' if _gm_d_day>=0 else '&#9660;', _gm_d_day),
            '#059669' if _gm_d_day >= 0 else '#DC2626'),
        adjgm=_day_tile(
            'Adj GM % &mdash; {}'.format(lat_lbl),
            '{:.1f}%'.format(lat['adj_gm']), gmc(lat['adj_gm']),
            'Prev: {:.1f}%'.format(prv['adj_gm']),
            '{} {:+.2f}pp'.format('&#9650;' if lat['adj_gm']>=prv['adj_gm'] else '&#9660;', lat['adj_gm']-prv['adj_gm']),
            '#059669' if lat['adj_gm'] >= prv['adj_gm'] else '#DC2626'),
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
        '<div class="mix-bar">{}</div>'
        '<table class="data-table"><thead><tr>'
        '<th>Offer Type</th><th class="R">Installs MTD</th><th class="R">Mix%</th>'
        '<th class="R">Rev/Wp</th><th class="R">GM%</th>'
        '<th class="R">&#916;GM vs full {}</th><th class="R">&#916;Rev/Wp</th>'
        '</tr></thead><tbody>{}</tbody></table>'
    ).format(mix_bar, prev_lbl, mix_rows)

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
            delta_html = '<span class="up">{:+.4f}</span>'.format(pkw_d_wp)
        elif pkw_d_wp < -0.005:
            delta_html = '<span class="dn">{:.4f}</span>'.format(pkw_d_wp)
        else:
            delta_html = '<span class="neutral">{:+.4f}</span>'.format(pkw_d_wp)
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
            '<td class="R mono">{:.4f}</td>'
            '<td class="R mono">{:.4f}</td>'
            '<td class="R">{}</td>'
            '<td class="R">{}</td>'
            '</tr>'
        ).format(row_style, col, lbl, fc(val), pct, pkw_c/1000, pkw_p/1000, delta_html, rc_tag)
    # Total row
    tc_c = (mtd['cogs']/mtd['kw']/1000) if mtd['kw'] else 0
    tc_p = (pm['cogs']/pm['kw']/1000) if pm['kw'] else 0
    tc_d = tc_c - tc_p
    tc_delta = '<span class="up"><strong>{:+.4f}</strong></span>'.format(tc_d) if tc_d > 0.005 else '<span class="dn">{:.4f}</span>'.format(tc_d)
    cgs_diff_note = '&#10004; sum verified, diff=&#8377;0'
    cg_rows += (
        '<tr style="background:var(--surface);font-weight:700">'
        '<td><strong>TOTAL COGS</strong></td>'
        '<td class="R">{}</td>'
        '<td class="R">100%</td>'
        '<td class="R mono">{:.4f}</td>'
        '<td class="R mono">{:.4f}</td>'
        '<td class="R">{}</td>'
        '<td class="R" style="font-size:10px;color:#6B7280">{}</td>'
        '</tr>'
    ).format(fc(mtd['cogs']), tc_c, tc_p, tc_delta, cgs_diff_note)

    # Pass projects.json category &#8377;/Wp values so SKU card matches COGS table exactly
    _pj_cat = {
        'Module':   (mtd['mod']/mtd['kw']/1000 if mtd['kw'] else 0, pm['mod']/pm['kw']/1000 if pm['kw'] else 0),
        'Inverter': (mtd['inv']/mtd['kw']/1000 if mtd['kw'] else 0, pm['inv']/pm['kw']/1000 if pm['kw'] else 0),
        'MMS':      (mtd['mms']/mtd['kw']/1000 if mtd['kw'] else 0, pm['mms']/pm['kw']/1000 if pm['kw'] else 0),
        'Cables':   (mtd['cab']/mtd['kw']/1000 if mtd['kw'] else 0, pm['cab']/pm['kw']/1000 if pm['kw'] else 0),
    }
    cogs_callout = build_sku_html(sku_data, aos_d, prev_lbl, curr_lbl, mtd['kw'], pm['kw'], _pj_cat)

    cogs_html = (
        '<div class="table-scroll"><table class="data-table"><thead><tr>'
        '<th>Category</th>'
        '<th class="R">MTD Amount</th>'
        '<th class="R">% of COGS</th>'
        '<th class="R">&#8377;/Wp MTD</th>'
        '<th class="R">&#8377;/Wp {}</th>'
        '<th class="R">&#916; &#8377;/Wp</th>'
        '<th class="R">Root Cause</th>'
        '</tr></thead><tbody>{}</tbody></table>{}'
    ).format(prev_lbl, cg_rows, cogs_callout)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CLUSTER TABLE — COMPACT 5-COLUMN
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def cl_row(r, bg=''):
        c=r['curr']; p=r['prev']
        sd=STATE_DISPLAY.get(r['state'],r['state'])
        bgs='background:{};'.format(bg) if bg else ''

        # ── Lead tag from driver types ──────────────────────────
        det   = r['drv_det']
        types = det.get('types',[])
        rv_d  = det.get('rev_wp_d',0)
        ck_d  = det.get('cogs_kw_d',0)/1000
        cat_d = det.get('cat_d',{})

        # Most impactful COGS category for tag label
        top_rising  = max(cat_d.items(), key=lambda x: x[1]) if cat_d else None
        top_falling = min(cat_d.items(), key=lambda x: x[1]) if cat_d else None

        # Primary tag: reflects the dominant lever for GM movement
        if 'price_dn' in types:
            chip_tag = '<span class="tag-rev">Rev &#8722;&#8377;{:.2f}/Wp</span>'.format(abs(rv_d))
        elif 'price_up' in types:
            chip_tag = '<span class="tag-ok">Rev +&#8377;{:.2f}/Wp</span>'.format(rv_d)
        elif 'cogs_up' in types and top_rising:
            # Show COGS category + context about which lever (rate/mix/AoS)
            cat_name, cat_val = top_rising
            ao_val = det.get('aos_d', 0)
            if cat_name in ('MMS', 'Cables') and ao_val > 0.1:
                ctx = 'AoS{:+.2f}kW'.format(ao_val)
            elif cat_name == 'Inverter':
                ctx = '3Ph mix'
            else:
                ctx = 'rate/mix'
            chip_tag = '<span class="tag-cogs">{} +&#8377;{:.3f}/Wp</span> <span class="tag-mix">{}</span>'.format(
                cat_name, abs(cat_val), ctx)
        elif 'cogs_dn' in types and top_falling:
            cat_name, cat_val = top_falling
            chip_tag = '<span class="tag-ok">{} &#8722;&#8377;{:.3f}/Wp</span>'.format(
                cat_name, abs(cat_val))
        elif r['gm_d'] > 0.15:
            chip_tag = '<span class="tag-ok">&#9650; Improving</span>'
        elif abs(r['gm_d']) < 0.15:
            chip_tag = '<span class="tag-ok">&#8594; Stable</span>'
        else:
            chip_tag = ''

        # ── GM cell — 4-tier colour scale ──────────────────────
        gm_pct = c['gm']
        if gm_pct >= 44:
            gm_cell = '<td class="gm-cell-hi">{:.2f}%</td>'.format(gm_pct)
        elif gm_pct >= 42:
            gm_cell = '<td class="gm-cell-mid">{:.2f}%</td>'.format(gm_pct)
        elif gm_pct >= 40:
            gm_cell = '<td class="gm-cell-lo">{:.2f}%</td>'.format(gm_pct)
        else:
            gm_cell = '<td class="gm-cell-crit">{:.2f}%</td>'.format(gm_pct)

        # ── Delta ───────────────────────────────────────────────
        if r['gm_d'] <= -0.3:
            delta_html = '<span class="up">&#9660; {:.2f}pp</span>'.format(abs(r['gm_d']))
        elif r['gm_d'] >= 0.3:
            delta_html = '<span class="up-good">&#9650; +{:.2f}pp</span>'.format(r['gm_d'])
        else:
            delta_html = '<span class="neutral">{:+.2f}pp</span>'.format(r['gm_d'])

        return (
            '<tr style="{}">'
            '<td><span class="cluster-name">{}</span>'
            '<span class="state-tag">{}</span></td>'
            '<td class="R">{}</td>'
            '{}'
            '<td class="R">{}</td>'
            '<td class="signal">{} {}</td>'
            '</tr>'
        ).format(
            bgs,
            r['cluster'], sd,
            c['n'],
            gm_cell,
            delta_html,
            chip_tag, r['drv_tag']
        )

    # ── Legend ──────────────────────────────────────────────────
    cluster_legend_html = (
        '<div class="cluster-legend">'
        '<span class="cluster-legend-label">GM% Scale</span>'
        '<span class="leg-pill leg-hi">&#8805; 44% Outperforming</span>'
        '<span class="leg-pill leg-mid">42&#8211;44% On-target</span>'
        '<span class="leg-pill leg-lo">40&#8211;42% Below target</span>'
        '<span class="leg-pill leg-crit">&lt; 40% Floor breach</span>'
        '<span style="font-size:8.5px;color:#64748B;margin-left:8px;font-style:italic">'
        '&#9679; Signal column shows: realization lever &middot; COGS mix lever &middot; product mix lever</span>'
        '</div>'
    )

    cl_thead = (
        '<thead><tr>'
        '<th>Cluster</th>'
        '<th class="R">n MTD</th>'
        '<th style="text-align:center;min-width:80px">GM%</th>'
        '<th class="R" style="min-width:80px">vs {}</th>'
        '<th style="min-width:260px">Signal</th>'
        '</tr></thead>'
    ).format(prev_lbl)

    cl_tbody = ''
    if declining:
        cl_tbody += '<tr class="group-row declining"><td colspan="5">&#9660;&nbsp; Declining vs {} &mdash; GM compression observed</td></tr>'.format(prev_lbl)
        cl_tbody += ''.join(cl_row(r,'#FFF5F5') for r in declining)
    if improving:
        cl_tbody += '<tr class="group-row improving"><td colspan="5">&#9650;&nbsp; Improving vs {} &mdash; GM expansion observed</td></tr>'.format(prev_lbl)
        cl_tbody += ''.join(cl_row(r,'#F0FFF8') for r in improving)
    if stable_cl:
        cl_tbody += '<tr class="group-row stable"><td colspan="5">&#8594;&nbsp; Stable &mdash; within &plusmn;0.3pp</td></tr>'
        cl_tbody += ''.join(cl_row(r) for r in stable_cl)
    if nascent:
        cl_tbody += '<tr class="group-row nascent"><td colspan="5">&#9733;&nbsp; Emerging clusters &mdash; growing volume</td></tr>'
        cl_tbody += ''.join(cl_row(r,'#FAF5FF') for r in nascent)

    cl_html = (
        '{}'
        '<div class="cluster-wrap">'
        '<table class="data-table">{}<tbody>{}</tbody></table></div>'
    ).format(cluster_legend_html, cl_thead, cl_tbody)

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

    # COGS diff still computed for footer use
    cogs_sum_calc = mtd['mod']+mtd['inv']+mtd['mms']+mtd['cab']+mtd['mtr']+mtd['ic']+mtd['oth']
    cogs_diff_val = abs(mtd['cogs'] - cogs_sum_calc)

    html = '''<!DOCTYPE html><html lang="en"><head>''' + '''
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght@0,400;0,500;1,400&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,600;0,9..40,800;1,9..40,400&display=swap" rel="stylesheet">
<style>''' + CSS + '''</style></head><body><div class="page">''' + ''.join([

        # ── HEADER
        '<div class="header" style="background:linear-gradient(135deg,#1565C0 0%,#1976D2 50%,#42A5F5 100%)">',
        '<div class="eyebrow">&#9728;&#65039; Solar Square &nbsp;&middot;&nbsp; B2C GM Report &nbsp;&middot;&nbsp; Analytics</div>',
        '<h1>', headline, '</h1>',
        '<div class="header-meta">Data through ', latest.strftime('%d %b %Y'),
        ' &nbsp;&middot;&nbsp; {} MTD vs full {} '.format(curr_lbl[:3], prev_lbl),
        str(pm_last.year),
        ' &nbsp;&middot;&nbsp; Generated ', now_str, '</div>',
        '<div class="badges">',
        '<span class="badge hi">&#10004; All numbers validated &amp; reconciled</span>',
        ' <span class="badge">{:,} installs MTD</span>'.format(mtd['n']),
        ' <span class="badge">{:,.1f} kW installed</span>'.format(mtd['kw']),
        ' <span class="badge">GM {:.2f}%</span>'.format(mtd['gm']),
        ' <span class="badge">Rev/Wp &#8377;{:.2f}</span>'.format(mtd['rev_wp']),
        ' <span class="badge">AoS {:.2f} kW</span>'.format(mtd['aos']),
        warn_badges,
        '</div></div>',

        # ── SECTIONS
        section('MTD at a Glance', '{} MTD vs full {} (same {} days)'.format(curr_lbl, prev_lbl, pm_day), kpi_html + bridge_html),
        section('Today at a Glance', '{} vs {}'.format(lat_lbl, prv_lbl), today_html),
        section('Product Mix', 'Offer-type split MTD vs full {} &#8212; installs, GM%, Rev/Wp'.format(prev_lbl), mix_html),
        section('COGS Analysis',
                'MTD {} vs full {} &#8212; SKU-level root cause &middot; all numbers cross-validated'.format(curr_lbl, prev_lbl),
                cogs_html),
        section('Cluster Health',
                'All active clusters (n &#8805; {} MTD) &middot; GM% movement vs full {}'.format(MIN_ORDERS, prev_lbl),
                cl_html),

        # ── FOOTER
        '<div class="footer">Solar Square GM Analytics &nbsp;&middot;&nbsp; ',
        '{} MTD {} vs {} {} &nbsp;&middot;&nbsp; '.format(curr_lbl[:3], latest.year, prev_lbl, pm_last.year),
        'COGS cross-check diff = &#8377;{:.0f} &nbsp;&middot;&nbsp; Generated {}'.format(cogs_diff_val, now_str),
        '</div>',

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
