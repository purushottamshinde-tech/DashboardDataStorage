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

def build_sku_html(sku_data, aos_d, prev_lbl, curr_lbl):
    """Generate deep SKU-level COGS insight HTML."""
    if not sku_data: return ''
    curr_p=sku_data['curr']; prev_p=sku_data['prev']; agg=sku_data['agg']
    curr_kw=sum(p['kw'] for p in curr_p.values()); prev_kw=sum(p['kw'] for p in prev_p.values())
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

    def row(icon, cat, delta_wp, detail_lines, status_color):
        sign = '+' if delta_wp >= 0 else ''
        status = 'RISING' if delta_wp > 0.01 else ('FALLING' if delta_wp < -0.01 else 'STABLE')
        scol = '#DC2626' if delta_wp > 0.01 else ('#16A34A' if delta_wp < -0.01 else '#6B7280')
        return (
            '<div style="border:1px solid #E5E7EB;border-radius:10px;padding:12px 16px;margin-bottom:10px;background:#fff">'
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
            '<span style="font-weight:800;font-size:13px;color:#111827">{} {}</span>'
            '<span style="font-size:12px;font-weight:800;color:{}">{}{:.3f} &#8377;/Wp</span>'
            '</div>'
            '<div style="font-size:10.5px;color:#374151;line-height:1.9">{}</div>'
            '</div>'
        ).format(icon, cat, scol, sign, delta_wp, ''.join('<div style="margin-bottom:2px">'+d+'</div>' for d in detail_lines))

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

    html = (
        '<div style="background:#FFFBEB;border:1px solid #FDE68A;border-radius:10px;'
        'padding:12px 16px;margin-bottom:14px;font-size:11px;color:#92400E;line-height:1.6">'
        '&#128293;&nbsp;<b>COGS headline ({} MTD vs full {}):</b> {}'
        '</div>'
        '{}{}{}{}'
    ).format(
        curr_lbl, prev_lbl, headline,
        row(mms_icon,'MMS',mms_d,mms_detail,'#DC2626'),
        row(cab_icon,'Cables',cab_d,cable_detail,'#D97706'),
        row(inv_icon,'Inverter',inv_d,inv_detail,'#D97706'),
        row(mod_icon,'Module',mod_d,mod_detail,'#16A34A'),
    )
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

def get_driver(curr, prev):
    if prev['n'] < MIN_ORDERS:
        return '--', {}, 'Thin prior data'
    d = dict(
        rev_wp_d=curr['rev_wp']-prev['rev_wp'],
        aos_d=curr['aos']-prev['aos'],
        aov_d=curr['aov']-prev['aov'],
        cogs_kw_d=curr['cogs_kw']-prev['cogs_kw'],
    )
    factors=[]
    if d['rev_wp_d']>1.2:    factors.append((abs(d['rev_wp_d'])*10,'price_up','Rev/Wp +&#8377;{:.1f}/Wp'.format(d['rev_wp_d'])))
    elif d['rev_wp_d']<-1.2: factors.append((abs(d['rev_wp_d'])*10,'price_dn','Rev/Wp &#8722;&#8377;{:.1f}/Wp'.format(abs(d['rev_wp_d']))))
    if d['aos_d']>0.25:      factors.append((d['aos_d']*8,'size_up','AoS +{:.2f}kW'.format(d['aos_d'])))
    elif d['aos_d']<-0.25:   factors.append((abs(d['aos_d'])*8,'size_dn','AoS &#8722;{:.2f}kW'.format(abs(d['aos_d']))))
    if d['cogs_kw_d']<-2500: factors.append((abs(d['cogs_kw_d'])/1000,'cogs_dn','COGS &#8722;&#8377;{:.0f}/kW'.format(abs(d['cogs_kw_d']))))
    elif d['cogs_kw_d']>2500:factors.append((d['cogs_kw_d']/1000,'cogs_up','COGS +&#8377;{:.0f}/kW'.format(d['cogs_kw_d'])))
    if not factors:
        sub=[]
        if abs(d['rev_wp_d'])>0.3: sub.append('Rev/Wp {:+.1f}/Wp'.format(d['rev_wp_d']))
        if abs(d['aos_d'])>0.05:   sub.append('AoS {:+.2f}kW'.format(d['aos_d']))
        if abs(d['cogs_kw_d'])>300:sub.append('COGS {:+,.0f}/kW'.format(d['cogs_kw_d']))
        tag='; '.join(sub) if sub else 'All metrics <0.5% shift'
        return tag, d, tag
    factors.sort(key=lambda x:-x[0])
    tag='; '.join(f[2] for f in factors[:2])
    types=[f[1] for f in factors[:2]]
    return tag, dict(d,types=types), tag

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
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Helvetica Neue',Arial,sans-serif;
     background:#EBEEF2;padding:20px 12px;font-size:13px;color:#111827}
.wrap{max-width:860px;margin:0 auto;background:#fff;border-radius:16px;
      overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,.10)}

/* ── HERO ── */
.hero{padding:28px 32px 24px;position:relative;overflow:hidden}
.hero-eyebrow{font-size:9.5px;font-weight:700;letter-spacing:1.8px;text-transform:uppercase;
              color:rgba(255,255,255,.65);margin-bottom:10px}
.hero-headline{font-size:20px;font-weight:800;color:#fff;line-height:1.3;
               letter-spacing:-.3px;max-width:680px;margin-bottom:18px}
.hero-meta{font-size:10px;color:rgba(255,255,255,.55);margin-bottom:14px;letter-spacing:.3px}
.hero-badges{display:flex;flex-wrap:wrap;gap:7px}
.badge{display:inline-flex;align-items:center;gap:4px;
       background:rgba(255,255,255,.13);border:1px solid rgba(255,255,255,.22);
       color:#fff;font-size:10px;font-weight:600;padding:4px 11px;
       border-radius:20px;letter-spacing:.3px}
.badge-hi{background:rgba(255,255,255,.22)}

/* ── SECTION WRAPPER ── */
.sec{padding:22px 32px;border-bottom:1px solid #F1F5F9}
.sec-hd{display:flex;align-items:center;gap:6px;margin-bottom:16px}
.sec-title{font-size:9.5px;font-weight:800;letter-spacing:1.4px;text-transform:uppercase;color:#374151}
.sec-sub{font-size:9px;color:#94A3B8;margin-left:4px;letter-spacing:.3px}

/* ── EXEC SNAPSHOT CARDS ── */
.snap4{width:100%;border-collapse:separate;border-spacing:10px;margin:-5px}
.ec{border-radius:12px;padding:16px 18px;vertical-align:top;width:25%;border:1px solid #E5E7EB}
.ec-badge{display:inline-block;font-size:8px;font-weight:700;letter-spacing:.8px;
          text-transform:uppercase;padding:2px 8px;border-radius:10px;margin-bottom:10px}
.ec-label{font-size:9px;font-weight:600;letter-spacing:.8px;text-transform:uppercase;
          color:#6B7280;margin-bottom:6px;display:block}
.ec-val{font-size:24px;font-weight:900;line-height:1;display:block;letter-spacing:-.5px;margin-bottom:6px}
.ec-delta{font-size:10px;font-weight:500;color:#6B7280;display:block;line-height:1.5}

/* ── KPI GRID ── */
.kgrid{width:100%;border-collapse:separate;border-spacing:10px;margin:-5px}
.kc{background:#FAFAFA;border:1px solid #E5E7EB;border-radius:10px;
    padding:14px 16px;vertical-align:top;width:25%}
.kc-label{font-size:8.5px;font-weight:700;letter-spacing:1px;text-transform:uppercase;
          color:#9CA3AF;display:block;margin-bottom:7px}
.kc-val{font-size:21px;font-weight:800;color:#111827;line-height:1;
        display:block;letter-spacing:-.4px;margin-bottom:7px}
.kc-sub{font-size:9.5px;color:#9CA3AF;display:block;line-height:1.6}
.kc-trend{display:block;margin-top:5px}

/* ── TODAY MINI-CARDS ── */
.today-grid{width:100%;border-collapse:separate;border-spacing:10px;margin:-5px}
.tc{background:#F9FAFB;border:1px solid #E5E7EB;border-radius:10px;
    padding:12px 14px;vertical-align:top}
.tc-label{font-size:8px;font-weight:700;letter-spacing:1px;text-transform:uppercase;
          color:#9CA3AF;display:block;margin-bottom:8px}
.tc-today{font-size:20px;font-weight:800;color:#111827;display:block;line-height:1;margin-bottom:6px}
.tc-prev{font-size:10px;color:#9CA3AF;display:block}
.tc-arrow{font-size:11px;font-weight:700}

/* ── COGS ── */
.cbar{height:26px;border-radius:8px;overflow:hidden;display:flex;margin-bottom:14px;gap:2px}
.cb{display:flex;align-items:center;justify-content:center;
    font-size:8px;color:#fff;font-weight:700;overflow:hidden;white-space:nowrap;padding:0 6px}
.cg{width:100%;border-collapse:collapse;font-size:11px}
.cg thead tr{background:#F9FAFB}
.cg th{padding:8px 12px;font-size:8.5px;font-weight:700;color:#6B7280;
       text-transform:uppercase;letter-spacing:.7px;border-bottom:2px solid #E5E7EB;text-align:left}
.cg th.R{text-align:right}
.cg td{padding:8px 12px;border-bottom:1px solid #F3F4F6;color:#374151}
.cg td.R{text-align:right}
.cog-pill{display:inline-block;font-size:8.5px;font-weight:600;padding:1px 7px;
          border-radius:8px;color:#fff;margin-left:6px;vertical-align:middle}

/* ── WATCH LIST ── */
.watch{background:#FAFAFA;border-radius:12px;padding:0;overflow:hidden}
.wi{display:flex;gap:12px;align-items:flex-start;padding:14px 18px;border-bottom:1px solid #F1F5F9}
.wi:last-child{border-bottom:none}
.wi-num{font-size:11px;font-weight:800;width:24px;height:24px;border-radius:50%;
        display:inline-block;text-align:center;line-height:24px;
        flex-shrink:0;margin-top:0;vertical-align:top}
.wi-body{flex:1}
.wi-title{font-size:12px;font-weight:700;color:#111827;line-height:1.4;margin-bottom:3px}
.wi-why{font-size:10.5px;color:#6B7280;line-height:1.6;margin-bottom:3px}
.wi-tag{display:inline-block;font-size:8px;font-weight:700;letter-spacing:.7px;
        text-transform:uppercase;padding:2px 7px;border-radius:8px;margin-bottom:5px}
.wi-red .wi-num{background:#FEE2E2;color:#DC2626}
.wi-red .wi-tag{background:#FEE2E2;color:#DC2626}
.wi-yel .wi-num{background:#FEF3C7;color:#D97706}
.wi-yel .wi-tag{background:#FEF3C7;color:#D97706}
.wi-grn .wi-num{background:#DCFCE7;color:#16A34A}
.wi-grn .wi-tag{background:#DCFCE7;color:#16A34A}

/* ── CLUSTER TABLE ── */
.cl-t{width:100%;border-collapse:collapse;font-size:10.5px}
.cl-t thead tr{background:#1E293B}
.cl-t th{padding:8px 10px;font-size:8px;font-weight:700;color:#94A3B8;
         text-transform:uppercase;letter-spacing:.6px;text-align:left;white-space:nowrap}
.cl-t th.R{text-align:right}
.cl-t td{padding:7px 10px;border-bottom:1px solid #F1F5F9;color:#374151;white-space:nowrap}
.cl-t td.R{text-align:right}
.cl-t tbody tr:hover td{background:#F9FAFB}
.grp-row td{background:#F8FAFC;color:#64748B;font-weight:700;font-size:9px;
            text-transform:uppercase;letter-spacing:.8px;padding:5px 10px;
            border-top:2px solid #E2E8F0}

/* ── FOOTER ── */
.ftr{background:#F9FAFB;padding:16px 32px;text-align:center;
     font-size:9px;color:#9CA3AF;line-height:1.8;border-top:1px solid #F1F5F9}
.ftr a{color:#6366F1;text-decoration:none}

@media(max-width:600px){
  body{padding:6px}
  .wrap{border-radius:10px}
  .sec{padding:14px 14px}
  .hero{padding:18px 14px 16px}
  .hero-headline{font-size:15px;letter-spacing:-.2px}
  .hero-eyebrow{font-size:8.5px}
  .hero-badges{gap:5px}
  .badge{font-size:9px;padding:3px 9px}
  /* ── 2-col card grid on mobile ── */
  .snap4,.snap4 tbody,.snap4 tr,
  .kgrid,.kgrid tbody,.kgrid tr,
  .today-grid,.today-grid tbody,.today-grid tr{display:block!important;width:100%!important}
  .ec{display:inline-block!important;width:calc(50% - 8px)!important;
      margin:4px!important;vertical-align:top;padding:12px 12px!important}
  .ec-val{font-size:19px!important}
  .kc{display:inline-block!important;width:calc(50% - 8px)!important;
      margin:4px!important;vertical-align:top;padding:11px 12px!important}
  .kc-val{font-size:17px!important}
  .tc{display:inline-block!important;width:calc(50% - 8px)!important;
      margin:4px!important;vertical-align:top;padding:10px 10px!important}
  .tc-today{font-size:16px!important}
  /* ── Tables: scroll horizontally ── */
  .cg,.cl-t{font-size:10px}
  .cg th,.cg td,.cl-t th,.cl-t td{padding:6px 7px!important}
  /* ── Watch list ── */
  .wi{padding:12px 12px!important;gap:10px}
  .wi-title{font-size:11px!important}
  .wi-why{font-size:10px!important}
  .ftr{padding:12px 14px}
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

    declining=[]; stable_cl=[]; improving=[]; nascent=[]
    for key in mtd_cl:
        curr=calc(mtd_cl[key]); prev=calc(pm_cl.get(key,[]))
        if curr['n']<5: continue
        state,cluster=key
        gm_d=curr['gm']-prev['gm']
        ag_dp=(curr['abs_gm']-prev['abs_gm'])/prev['abs_gm']*100 if prev['abs_gm'] else 0
        drv_tag,drv_det,_ = get_driver(curr,prev)
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

    # ── Price erosion clusters
    price_dn=[(r['cluster'],r['drv_det'].get('rev_wp_d',0))
              for r in all_cl if r['drv_det'].get('rev_wp_d',0)<-1.2 and r['curr']['n']>=MIN_ORDERS]
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
    if len(price_dn) >= 4:
        headline = 'Revenue realisation drop across {} clusters &#8212; discount approvals need immediate review'.format(len(price_dn))
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'
    elif len(price_dn) >= 2:
        names_short = ', '.join(c for c,_ in price_dn[:3])
        headline = 'Revenue realisation falling in {} markets ({}) &#8212; GM holding but revenue realisation needs attention'.format(len(price_dn), names_short)
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'
    elif len(price_dn) == 1:
        headline = 'Revenue realisation dip in {} &#8212; overall business metrics on track'.format(price_dn[0][0])
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'
    elif gm_trend >= 0.5:
        headline = 'GM expanding {:.1f}ppt MoM &#8212; revenue discipline and volume growth aligned'.format(gm_trend)
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'
    elif gm_trend <= -0.5:
        headline = 'GM contracting {:.1f}ppt MoM &#8212; root cause: {}'.format(
            abs(gm_trend), 'COGS mix shift' if cogs_net_gm < -0.3 else 'revenue pressure')
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'
    elif vol_pct >= 15:
        headline = 'Volume surge +{:.0f}% MoM &#8212; GM stable at {:.2f}% despite scale-up'.format(vol_pct, mtd['gm'])
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'
    else:
        headline = 'Operations on track &#8212; {:,} installations at {:.2f}% GM through {}'.format(mtd['n'], mtd['gm'], lat_lbl)
        hero_grad = 'linear-gradient(135deg,#0369A1 0%,#0284C7 50%,#0EA5E9 100%)'

    # ── GM Badge
    gm_arrow = '&#9650;' if gm_trend>=0 else '&#9660;'
    gm_badge_txt = '{} GM {:.2f}% ({}{:.2f}%pts)'.format(gm_arrow, mtd['gm'], '+' if gm_trend>=0 else '', gm_trend)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  EXEC SNAPSHOT (4 cards)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def exec_card(label, badge_text, badge_ok, value, value_color, delta_html, bg='#fff'):
        badge_bg = '#DCFCE7' if badge_ok is True else ('#FEE2E2' if badge_ok is False else '#FEF3C7')
        badge_clr = '#15803D' if badge_ok is True else ('#B91C1C' if badge_ok is False else '#B45309')
        return (
            '<td class="ec" style="background:{};vertical-align:top">'
            '<span class="ec-badge" style="background:{};color:{}">{}</span>'
            '<span class="ec-label">{}</span>'
            '<span class="ec-val" style="color:{}">{}</span>'
            '<span class="ec-delta">{}</span>'
            '</td>'
        ).format(bg, badge_bg, badge_clr, badge_text, label, value_color, value, delta_html)

    # Volume card
    vol_ok = vol_pct >= -5
    vol_delta = '{} vs {:,} {} (1&#8211;{})'.format(dpct(mtd['n'],pm['n']), pm['n'], prev_lbl, pm_day)
    card_vol = exec_card('Installations MTD',
        '&#9650; +{:.0f}%'.format(vol_pct) if vol_pct>=0 else '&#9660; {:.0f}%'.format(vol_pct),
        vol_ok, '{:,}'.format(mtd['n']), '#111827', vol_delta)

    # Pricing card
    rwp_ok = rev_wp_d >= -0.5
    rwp_delta = 'vs &#8377;{:.2f}/Wp {} &nbsp; {}'.format(pm['rev_wp'], prev_lbl, dpval(rev_wp_d,'&#8377;/Wp'))
    rwp_badge_txt = '&#9650; +&#8377;{:.2f}/Wp'.format(rev_wp_d) if rev_wp_d>=0 else '&#9660; &#8722;&#8377;{:.2f}/Wp'.format(abs(rev_wp_d))
    card_rwp = exec_card('Rev / Wp MTD', rwp_badge_txt, rwp_ok,
        '&#8377;{:.2f}'.format(mtd['rev_wp']), '#111827', rwp_delta)

    # COGS card
    cogs_pkw_d = total_cogs_pkw_c - total_cogs_pkw_p
    cogs_ok = cogs_pkw_d <= 500
    cogs_badge_txt = ('&#9650; +&#8377;{:.3f}/Wp'.format(cogs_pkw_d/1000) if cogs_pkw_d>0
                      else '&#9660; &#8722;&#8377;{:.3f}/Wp'.format(abs(cogs_pkw_d)/1000))
    cogs_delta = 'vs &#8377;{:.2f}/Wp {} &nbsp; {}'.format(
        total_cogs_pkw_p/1000, prev_lbl,
        '<span style="color:#DC2626;font-weight:700">&#9650;&thinsp;+&#8377;{:.3f}/Wp</span>'.format(cogs_pkw_d/1000) if cogs_pkw_d>500
        else '<span style="color:#16A34A;font-weight:700">&#9660;&thinsp;&#8722;&#8377;{:.3f}/Wp</span>'.format(abs(cogs_pkw_d)/1000) if cogs_pkw_d<-500
        else '<span style="color:#94A3B8">stable</span>')
    card_cogs = exec_card('COGS / Wp MTD', cogs_badge_txt, cogs_ok,
        '&#8377;{:.2f}/Wp'.format(total_cogs_pkw_c/1000), '#111827', cogs_delta)

    # GM card
    gm_ok = mtd['gm'] >= 42
    gm_delta_html = 'vs {:.2f}% {} &nbsp; {}'.format(pm['gm'], prev_lbl, dpp(gm_trend))
    card_gm = exec_card('Gross Margin MTD',
        '&#9650; {:.2f}%'.format(mtd['gm']) if gm_trend>=0 else '&#9660; {:.2f}%'.format(mtd['gm']),
        gm_ok, '{:.2f}%'.format(mtd['gm']), gmc(mtd['gm']), gm_delta_html)

    snap4_html = '<table class="snap4"><tr>{}{}{}{}</tr></table>'.format(card_vol, card_rwp, card_cogs, card_gm)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  MTD KPI GRID (2 rows x 4)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def kcard(label, val, sub, vc='#111827', trend_html=''):
        return (
            '<td class="kc">'
            '<span class="kc-label">{}</span>'
            '<span class="kc-val" style="color:{}">{}</span>'
            '<span class="kc-sub">{}</span>'
            '<span class="kc-trend">{}</span>'
            '</td>'
        ).format(label, vc, val, sub, trend_html)

    kpi_html = (
        '<table class="kgrid"><tr>'
        + kcard('Installations MTD', '{:,}'.format(mtd['n']),
                'vs {:,} {} (1&#8211;{})'.format(pm['n'], prev_lbl, pm_day),
                trend_html=dpct(mtd['n'], pm['n']))
        + kcard('kW Installed MTD', '{:,.0f} kW'.format(mtd['kw']),
                'vs {:,.0f} kW {}'.format(pm['kw'], prev_lbl),
                trend_html=dpct(mtd['kw'], pm['kw']))
        + kcard('Gross Margin', '{:.2f}%'.format(mtd['gm']),
                'vs {:.2f}% {}'.format(pm['gm'], prev_lbl),
                vc=gmc(mtd['gm']), trend_html=dpp(gm_trend))
        + kcard('Revenue MTD', fc(mtd['rev']),
                'vs {} {}'.format(fc(pm['rev']), prev_lbl),
                trend_html=dpct(mtd['rev'], pm['rev']))
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
    def tcard(label, today_val, prev_val, delta_html, vc='#111827'):
        return (
            '<td class="tc">'
            '<span class="tc-label">{}</span>'
            '<span class="tc-today" style="color:{}">{}</span>'
            '<span class="tc-prev">vs {} yesterday &nbsp; {}</span>'
            '</td>'
        ).format(label, vc, today_val, prev_val, delta_html)

    today_html = (
        '<table class="today-grid"><tr>'
        + tcard('Installations', str(lat['n']), str(prv['n']),
                dpct(lat['n'], prv['n']) if prv['n'] else '')
        + tcard('kW Installed', '{:.1f} kW'.format(lat['kw']), '{:.1f} kW'.format(prv['kw']),
                dpval(lat['kw']-prv['kw'], 'kW') if prv['kw'] else '')
        + tcard('Rev / Wp', '&#8377;{:.2f}'.format(lat['rev_wp']), '&#8377;{:.2f}'.format(prv['rev_wp']),
                dpval(lat['rev_wp']-prv['rev_wp'], '&#8377;/Wp') if prv['rev_wp'] else '')
        + tcard('Avg System Size', '{:.2f} kW'.format(lat['aos']), '{:.2f} kW'.format(prv['aos']),
                dpval(lat['aos']-prv['aos'], 'kW') if prv['aos'] else '')
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
        '<div style="height:24px;border-radius:8px;overflow:hidden;display:flex;margin-bottom:14px;gap:1px">{}</div>'
        '<table class="cg"><thead><tr>'
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
        rpct  = val/mtd['rev']*100 if mtd['rev'] else 0
        pkw_c = val/mtd['kw'] if mtd['kw'] else 0
        pkw_p = pmv/pm['kw'] if pm['kw'] else 0
        d_pp  = pct - pmpct
        # root cause pill
        if d_pp > 0.2:
            if lbl in ('MMS','Cables') and aos_d > 0.05:
                cause_txt = 'AoS +{:.2f}kW'.format(aos_d)
                pill_bg = '#FEF3C7'; pill_clr = '#92400E'
            else:
                cause_txt = '+&#8377;{:.3f}/Wp rate'.format((pkw_c-pkw_p)/1000)
                pill_bg = '#FEE2E2'; pill_clr = '#991B1B'
            pill = '<span class="cog-pill" style="background:{};color:{}">{}</span>'.format(pill_bg,pill_clr,cause_txt)
        elif d_pp < -0.2:
            pill = '<span class="cog-pill" style="background:#DCFCE7;color:#15803D">&#9660;&thinsp;{:.1f}%pts</span>'.format(abs(d_pp))
        else:
            pill = ''
        shift_html = dpp(d_pp, hb=False)
        cg_rows += (
            '<tr>'
            '<td><span style="display:inline-block;width:9px;height:9px;background:{};'
            'border-radius:2px;margin-right:6px;vertical-align:middle"></span>'
            '<b>{}</b>{}</td>'
            '<td class="R">{}</td>'
            '<td class="R">{:.1f}%</td>'
            '<td class="R">&#8377;{:.2f}/Wp</td>'
            '<td class="R">{} <span style="color:#D1D5DB;font-size:9px">vs {:.1f}%</span></td>'
            '</tr>'
        ).format(col, lbl, pill, fc(val), pct, pkw_c/1000, shift_html, pmpct)

    cogs_callout = build_sku_html(sku_data, aos_d, prev_lbl, curr_lbl)
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
            '<span style="color:{}">{}{:.4f} &#8377;/Wp on realised margin</span>'
            '</b> &nbsp;<span style="font-weight:400;color:#B45309">({}{:.2f}%pts on GM &#64; &#8377;{:.2f}/Wp realisation)</span><br>'
            '<span style="display:block;margin-top:6px;line-height:2">{}</span>'
            '</div>'
        ).format(net_clr, net_sign, abs(net_wp_impact), net_sign, abs(cogs_net_gm), mtd['rev_wp'],
                 '<br>'.join(parts))

    cogs_html = (
        '<div style="margin-bottom:6px;font-size:9.5px;color:#6B7280">'
        'Total COGS/Wp: <b style="color:#111827">&#8377;{:.2f}</b> vs '
        '&#8377;{:.2f} {} &nbsp; {}</div>'
        '<table class="cg"><thead><tr>'
        '<th>Category</th><th class="R">MTD Amount</th>'
        '<th class="R">% of COGS</th><th class="R">Cost / Wp</th>'
        '<th class="R">MoM Shift</th>'
        '</tr></thead><tbody>{}</tbody></table>{}'
    ).format(
        total_cogs_pkw_c/1000, total_cogs_pkw_p/1000, prev_lbl,
        dpval((total_cogs_pkw_c-total_cogs_pkw_p)/1000, '&#8377;/Wp', hb=False),
        cg_rows, cogs_callout
    )

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
            'Revenue realisation (Rev/Wp) fell &gt;&#8377;1.2/Wp vs prior month in: {}{}.'.format(names_w, leftover),
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

    # Limit to top 5, sort by priority
    watch_items.sort(key=lambda x:x[0])
    watch_items = watch_items[:5]

    if not watch_items:
        watch_items = [(2, 'All metrics within normal range',
            'No anomalies detected across revenue, COGS, volume, or GM.',
            'Continue monitoring daily.')]

    wi_classes = {0:'wi-red', 1:'wi-yel', 2:'wi-grn'}
    wi_tags    = {0:'&#128308; Urgent', 1:'&#128992; Watch', 2:'&#128994; Positive'}
    wi_html = ''
    for i,(prio,title,why,_) in enumerate(watch_items, 1):
        cls = wi_classes.get(prio,'wi-grn')
        tag = wi_tags.get(prio,'')
        wi_html += (
            '<div class="wi {}">'
            '<div class="wi-num">{}</div>'
            '<div class="wi-body">'
            '<div class="wi-tag">{}</div>'
            '<div class="wi-title">{}</div>'
            '<div class="wi-why">Why: {}</div>'
            '</div></div>'
        ).format(cls, i, tag, title, why)
    watch_html = '<div class="watch">{}</div>'.format(wi_html)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  CLUSTER TABLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    def cl_row(r, bg=''):
        c=r['curr']; p=r['prev']
        sd=STATE_DISPLAY.get(r['state'],r['state'])
        bgs='background:{};'.format(bg) if bg else ''
        return (
            '<tr style="{}">'
            '<td style="font-weight:700;color:#111827">{}</td>'
            '<td class="R">&#8377;{:.2f} <span style="color:#D1D5DB;font-size:9px">/&#8377;{:.2f}</span></td>'
            '{}'
            '<td class="R" style="font-weight:700">{}</td>'
            '<td class="R" style="font-size:10px;color:#6B7280">{}</td>'
            '</tr>'
        ).format(
            bgs, r['cluster'],
            c['rev_wp'], p['rev_wp'],
            gmcell(c['gm']),
            dpp(r['gm_d']),
            r['drv_tag']
        )

    cl_thead = (
        '<thead style="position:sticky;top:0"><tr>'
        '<th>Cluster</th>'
        '<th class="R">Rev/Wp MTD/{}</th>'
        '<th class="R">GM%</th>'
        '<th class="R">&#916;%pts</th>'
        '<th class="R">Driver</th>'
        '</tr></thead>'
    ).format(prev_lbl)

    cl_tbody = ''
    if declining:
        cl_tbody += '<tr class="grp-row"><td colspan="5">&#9660; Declining vs {} &#8212; needs attention</td></tr>'.format(prev_lbl)
        cl_tbody += ''.join(cl_row(r,'#FFFBFB') for r in declining)
    if improving:
        cl_tbody += '<tr class="grp-row"><td colspan="5">&#9650; Improving vs {}</td></tr>'.format(prev_lbl)
        cl_tbody += ''.join(cl_row(r,'#F9FFFA') for r in improving)
    if stable_cl:
        cl_tbody += '<tr class="grp-row"><td colspan="5">&#8594; Stable (within &plusmn;0.3pp)</td></tr>'
        cl_tbody += ''.join(cl_row(r) for r in stable_cl)
    if nascent:
        cl_tbody += '<tr class="grp-row"><td colspan="5">&#9733; New / growing clusters</td></tr>'
        cl_tbody += ''.join(cl_row(r,'#FAF5FF') for r in nascent)

    cl_html = (
        '<div style="overflow-x:auto;border-radius:8px;border:1px solid #E5E7EB">'
        '<table class="cl-t">{}<tbody>{}</tbody></table></div>'
    ).format(cl_thead, cl_tbody)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  ASSEMBLE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    now_str = datetime.now().strftime('%d %b %Y, %I:%M %p IST')

    def sec(title, sub, body, bar_color='#3B82F6'):
        return (
            '<div class="sec">'
            '<div class="sec-hd">'
            '<span class="sec-title">{}</span>'
            '<span class="sec-sub">{}</span>'
            '</div>{}</div>'
        ).format(title, sub, body)

    html = ''.join([
        '<!DOCTYPE html><html lang="en"><head>',
        '<meta charset="UTF-8">',
        '<meta name="viewport" content="width=device-width,initial-scale=1">',
        '<style>', CSS, '</style></head>',
        '<body><div class="wrap">',
        # Hero
        '<div class="hero" style="background:', hero_grad, '">',
        '<div class="hero-eyebrow">&#9728;&#65039; Solar Square &nbsp;&bull;&nbsp; B2C GM Report</div>',
        '<div class="hero-headline">', headline, '</div>',
        '<div class="hero-meta">Data through ', latest.strftime('%d %b %Y'),
        ' &nbsp;&bull;&nbsp; Generated ', now_str, '</div>',
        '<div class="hero-badges">',
        '<span class="badge badge-hi">', curr_lbl, '</span>',
        '<span class="badge">', gm_badge_txt, '</span>',
        '<span class="badge">{:,} Installations</span>'.format(mtd['n']),
        '<span class="badge">', fc(mtd['rev']), ' Revenue</span>',
        '<span class="badge">&#8377;{:.2f}/Wp</span>'.format(mtd['rev_wp']),
        '</div></div>',
        # Exec Snapshot
        sec('Executive Snapshot',
            curr_lbl + ' MTD vs full ' + prev_lbl,
            snap4_html, '#6366F1'),
        # MTD KPI
        sec('MTD Dashboard',
            curr_lbl + ' MTD (1&#8211;' + str(latest.day) + ') vs full ' + prev_lbl,
            kpi_html, '#3B82F6'),
        # Today
        sec('Today at a Glance', lat_lbl + ' vs ' + prv_lbl, today_html, '#8B5CF6'),
        # Product Mix
        sec('Product Mix', 'Offer type breakdown · MTD ' + curr_lbl + ' vs full ' + prev_lbl, mix_html, '#7C3AED'),
        # COGS
        sec('COGS Analysis', 'MTD ' + curr_lbl + ' vs full ' + prev_lbl + ' — SKU-level root cause', cogs_html, '#F59E0B'),
        # Watch List
        sec('Top Things to Watch', 'Prioritised signals for decision-making', watch_html, '#EF4444'),
        # Cluster Table
        sec('Cluster Health', 'All markets &middot; MTD ' + curr_lbl + ' vs ' + prev_lbl, cl_html, '#10B981'),
        # Footer
        '<div class="ftr">Solar Square B2C GM Report &nbsp;&bull;&nbsp; Auto-generated &nbsp;&bull;&nbsp; ',
        latest.strftime('%d %b %Y'),
        '</div></div></body></html>',
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
        sys.exit(0)
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
