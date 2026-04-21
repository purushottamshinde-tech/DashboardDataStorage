#!/usr/bin/env python3
"""Solar Square Daily GM Report v3"""
import gzip, json, os, smtplib, sys, calendar
from collections import defaultdict
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

SENDER     = os.environ.get("GMAIL_USER", "purushottam.shinde@solarsquare.in")
RECIPIENTS = os.environ.get("REPORT_TO",  "shindepurushottam7460@gmail.com").split(",")
GMAIL_PASS = os.environ.get("GMAIL_PASSWORD", "")
DATA_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projects.json.gz")
MIN_ORDERS = 10

STATE_DISPLAY = {
    'Delhi':'Delhi','Gujrat':'Gujarat','Karnataka':'Karnataka',
    'Madhya Pradesh':'MP','MH East':'MH East','MH West':'MH West',
    'Rajasthan':'Rajasthan','Tamil Nadu':'Tamil Nadu',
    'Telangana':'Telangana','Uttar Pradesh':'UP'
}
COGS_COLORS = {
    'Module':'#2563A8','Inverter':'#7C3AED','MMS':'#0891B2',
    'Cables':'#16A34A','Metering':'#D97706','I&C':'#E11D48','Other':'#94A3B8'
}

def load_data():
    with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
        return json.load(f)

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

def by_cluster(projects):
    d=defaultdict(list)
    for p in projects: d[(p['s'],p['c'])].append(p)
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
    if d['rev_wp_d']>1.2:   factors.append((abs(d['rev_wp_d'])*10,'price_up','Pricing +Rs{:.1f}/Wp'.format(d['rev_wp_d'])))
    elif d['rev_wp_d']<-1.2: factors.append((abs(d['rev_wp_d'])*10,'price_dn','Pricing -Rs{:.1f}/Wp'.format(abs(d['rev_wp_d']))))
    if d['aos_d']>0.25:      factors.append((d['aos_d']*8,'size_up','AoS +{:.2f}kW'.format(d['aos_d'])))
    elif d['aos_d']<-0.25:   factors.append((abs(d['aos_d'])*8,'size_dn','AoS {:.2f}kW'.format(d['aos_d'])))
    if d['cogs_kw_d']<-2500: factors.append((abs(d['cogs_kw_d'])/1000,'cogs_dn','COGS -Rs{:.0f}/kW'.format(abs(d['cogs_kw_d']))))
    elif d['cogs_kw_d']>2500:factors.append((d['cogs_kw_d']/1000,'cogs_up','COGS +Rs{:.0f}/kW'.format(d['cogs_kw_d'])))
    if not factors:
        return 'Stable', d, 'Minor blended shifts'
    factors.sort(key=lambda x:-x[0])
    tag='; '.join(f[2] for f in factors[:2])
    types=[f[1] for f in factors[:2]]
    return tag, dict(d,types=types), tag

def fc(v):
    if v>=1e7: return '&#8377;{:.2f}Cr'.format(v/1e7)
    if v>=1e5: return '&#8377;{:.1f}L'.format(v/1e5)
    return '&#8377;{:,.0f}'.format(v)

def dpp(delta, hb=True):
    if abs(delta)<0.01: return '<span style="color:#CBD5E1">--</span>'
    arr='&uarr;' if delta>0 else '&darr;'
    clr='#16A34A' if (delta>0)==hb else '#DC2626'
    return '<span style="color:{};font-weight:700">{}{:.2f}pp</span>'.format(clr,arr,abs(delta))

def dpct(c, p, hb=True):
    if p==0: return ''
    delta=(c-p)/abs(p)*100
    if abs(delta)<0.5: return '<span style="color:#CBD5E1">--</span>'
    arr='&uarr;' if delta>0 else '&darr;'
    clr='#16A34A' if (delta>0)==hb else '#DC2626'
    return '<span style="color:{};font-weight:700">{}{:.0f}%</span>'.format(clr,arr,abs(delta))

def dpval(delta, unit, hb=True):
    if abs(delta)<0.01: return '<span style="color:#CBD5E1">--</span>'
    arr='&uarr;' if delta>0 else '&darr;'
    clr='#16A34A' if (delta>0)==hb else '#DC2626'
    return '<span style="color:{};font-weight:700">{}{:.2f}{}</span>'.format(clr,arr,abs(delta),unit)

def gmc(pct):
    if pct>=44: return '#16A34A'
    if pct>=40: return '#D97706'
    return '#DC2626'

def gmcell(pct, fw='600'):
    bg='#DCFCE7' if pct>=44 else ('#FEF3C7' if pct>=40 else '#FEE2E2')
    return '<td style="background:{};color:{};font-weight:{};text-align:center">{:.1f}%</td>'.format(bg,gmc(pct),fw,pct)

CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',Arial,sans-serif;background:#E4EDF7;padding:16px}
.wrap{max-width:900px;margin:0 auto;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 4px 20px rgba(26,44,78,.14)}
.hdr{background:linear-gradient(135deg,#1A2C4E,#1D4ED8);padding:20px 24px}
.hdr-t{color:#fff;font-size:18px;font-weight:700}
.hdr-s{color:rgba(255,255,255,.5);font-size:9px;margin-top:4px;font-family:monospace;letter-spacing:1.1px;text-transform:uppercase}
.sec{padding:16px 24px;border-bottom:1px solid #EBF2FA}
.sec-t{font-size:9px;font-weight:700;color:#1A2C4E;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;border-left:3px solid #1D4ED8;padding-left:7px}
.summ{background:#EFF6FF;border:1px solid #BFDBFE;border-radius:8px;padding:13px 16px;font-size:12px;line-height:1.75;color:#1e3a5f}
.kpi-t{width:100%;border-collapse:separate;border-spacing:7px}
.kpi-c{background:#F5F8FC;border:1px solid #D4E4F4;border-radius:8px;padding:12px 14px;vertical-align:top;width:25%}
.kpi-l{font-size:7.5px;color:#5A7A96;text-transform:uppercase;letter-spacing:.9px;font-family:monospace;display:block;margin-bottom:4px}
.kpi-v{font-size:20px;font-weight:700;color:#1A2C4E;line-height:1;display:block}
.kpi-d{font-size:8.5px;color:#94A3B8;margin-top:5px;line-height:1.5;display:block}
.snap-t{width:100%;border-collapse:separate;border-spacing:7px}
.snap-c{background:#F5F8FC;border:1px solid #D4E4F4;border-radius:8px;padding:10px 13px;vertical-align:top}
.cbar{height:20px;border-radius:4px;overflow:hidden;display:flex;margin-bottom:10px}
.cb{display:flex;align-items:center;justify-content:center;font-size:8px;color:#fff;font-weight:600;overflow:hidden;white-space:nowrap;padding:0 4px}
.cl-t{width:100%;border-collapse:collapse;font-size:10.5px}
.cl-t th{background:#1A2C4E;color:#fff;padding:7px 8px;font-size:7.5px;font-weight:600;letter-spacing:.5px;text-transform:uppercase;text-align:center;white-space:nowrap}
.cl-t th.L{text-align:left}
.cl-t td{padding:6px 8px;border-bottom:1px solid #EBF2FA;text-align:center;white-space:nowrap;color:#1A2C4E}
.cl-t td.L{text-align:left}
.cl-t .grp td{background:#F0F4FA;font-size:7.5px;font-weight:700;color:#5A7A96;text-transform:uppercase;letter-spacing:.8px;padding:4px 8px}
.cl-t tbody tr:hover td{background:#F7FAFF}
.cg-t{width:100%;border-collapse:collapse;font-size:10.5px}
.cg-t th{background:#1A2C4E;color:#fff;padding:6px 8px;font-size:7.5px;font-weight:600;letter-spacing:.5px;text-transform:uppercase;text-align:left}
.cg-t th.R{text-align:right}
.cg-t td{padding:5px 8px;border-bottom:1px solid #EBF2FA;color:#1A2C4E}
.cg-t td.R{text-align:right}
.ins{border-radius:7px;padding:9px 13px;margin-bottom:7px;font-size:11px;line-height:1.65;border-left:4px solid}
.ins-r{background:#FEF2F2;border-color:#EF4444;color:#7F1D1D}
.ins-a{background:#FFFBEB;border-color:#F59E0B;color:#78350F}
.ins-g{background:#F0FDF4;border-color:#22C55E;color:#14532D}
.ins-b{background:#EFF6FF;border-color:#3B82F6;color:#1e3a5f}
.ftr{background:#F5F8FC;padding:12px 24px;font-size:8.5px;color:#94A3B8;text-align:center;line-height:1.7}
@media(max-width:600px){
  .kpi-t,.kpi-t tbody,.kpi-t tr,.snap-t,.snap-t tbody,.snap-t tr{display:block!important}
  .kpi-c,.snap-c{display:inline-block!important;width:calc(50% - 8px)!important;margin:3px!important;vertical-align:top}
  body{padding:8px}
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
    pm_day   = min(latest.day, calendar.monthrange(pm_last.year,pm_last.month)[1])
    pm_start = pm_last.replace(day=1).strftime('%Y-%m-01')
    pm_end   = '{}-{:02d}-{:02d}'.format(pm_last.year,pm_last.month,pm_day)
    pm_key   = pm_last.strftime('%Y-%m')
    prev_lbl = pm_last.strftime('%b')
    curr_lbl = latest.strftime('%b %Y')
    lat_lbl  = latest.strftime('%d %b')
    prv_lbl  = (latest-timedelta(days=1)).strftime('%d %b')

    mtd_ps = fp(projects, ms,        latest_str)
    pm_ps  = fp(projects, pm_start,  pm_end)
    lat_ps = fp(projects, latest_str, latest_str)
    prv_ps = fp(projects, prev_str,   prev_str)

    mtd = inject_meta(calc(mtd_ps), mo_onm_qhse, mo_key)
    pm  = inject_meta(calc(pm_ps),  mo_onm_qhse, pm_key)
    lat = calc(lat_ps)
    prv = calc(prv_ps)

    # ── Executive Summary
    gm_trend  = mtd['gm'] - pm['gm']
    trend_wd  = 'up' if gm_trend>=0.1 else ('down' if gm_trend<=-0.1 else 'flat')
    bc  = by_cluster(mtd_ps); bcp = by_cluster(pm_ps)
    concern = []
    for key in bc:
        c=calc(bc[key])
        if c['n']>=5 and c['gm']<40: concern.append('{} ({:.1f}%)'.format(key[1],c['gm']))
    cs = (', '.join(concern[:3])+' need margin attention.' if concern
          else 'All clusters tracking above 40% GM threshold.')
    summary = (
        '<b>{:,} installations</b> ({:,.0f} kW) completed MTD in {} -- '
        '{} vs {} on volume. '
        'Overall GM is <b style="color:{}">{:.2f}%</b> '
        '({} {:.2f}pp MoM). '
        'Rev/Wp at &#8377;{:.2f} vs &#8377;{:.2f} in {}. {}'
    ).format(
        mtd['n'],mtd['kw'],curr_lbl,
        dpct(mtd['n'],pm['n']),prev_lbl,
        gmc(mtd['gm']),mtd['gm'],
        trend_wd,abs(gm_trend),
        mtd['rev_wp'],pm['rev_wp'],prev_lbl,cs
    )

    # ── Cluster summary mini-table (for Executive Summary)
    cl_summ_rows = ''
    all_cl_keys = sorted(
        set(list(bc.keys())+list(bcp.keys())),
        key=lambda k: k[1]
    )
    summ_rows = []
    for key in all_cl_keys:
        c = calc(bc.get(key,[]))
        p = calc(bcp.get(key,[]))
        if c['n'] < 5: continue
        gd = c['gm'] - p['gm']
        rwp_d = c['rev_wp'] - p['rev_wp']
        summ_rows.append((gd, key[1], key[0], c, p, gd, rwp_d))
    summ_rows.sort(key=lambda x: x[0])  # worst first
    for _,cluster,state,c,p,gd,rwp_d in summ_rows:
        sd = STATE_DISPLAY.get(state, state)
        gd_html = dpp(gd)
        rwp_html = ('<span style="color:#DC2626;font-weight:600">&#8595;&#8377;{:.1f}</span>'.format(abs(rwp_d))
                    if rwp_d < -0.5 else
                    ('<span style="color:#16A34A;font-weight:600">&#8593;&#8377;{:.1f}</span>'.format(rwp_d)
                     if rwp_d > 0.5 else
                     '<span style="color:#94A3B8">~</span>'))
        row_bg = '#FFF5F5' if gd < -0.3 else ('#F0FDF4' if gd > 0.3 else '')
        cl_summ_rows += (
            '<tr style="background:{}">'
            '<td style="font-weight:600;padding:5px 9px;white-space:nowrap">{}</td>'
            '<td style="color:#94A3B8;font-size:9px;padding:5px 6px">{}</td>'
            '<td style="text-align:center;padding:5px 9px">{} <span style="color:#CBD5E1;font-size:9px">/{}</span></td>'
            '<td style="text-align:center;padding:5px 9px;font-weight:600;color:{}">{:.1f}%</td>'
            '<td style="text-align:center;padding:5px 9px">{}</td>'
            '<td style="text-align:center;padding:5px 9px">&#8377;{:.2f} <span style="color:#CBD5E1;font-size:9px">/&#8377;{:.2f}</span> {}</td>'
            '</tr>'
        ).format(
            row_bg, cluster, sd,
            c['n'], p['n'],
            gmc(c['gm']), c['gm'],
            gd_html,
            c['rev_wp'], p['rev_wp'], rwp_html
        )
    cl_summ_html = (
        '<div style="overflow-x:auto;margin-top:10px">'
        '<table style="width:100%;border-collapse:collapse;font-size:10.5px">'
        '<thead><tr style="background:#1A2C4E;color:#fff">'
        '<th style="padding:5px 9px;text-align:left;font-size:7.5px;letter-spacing:.5px;font-weight:600;text-transform:uppercase">Cluster</th>'
        '<th style="padding:5px 6px;text-align:left;font-size:7.5px;letter-spacing:.5px;font-weight:600;text-transform:uppercase">State</th>'
        '<th style="padding:5px 9px;text-align:center;font-size:7.5px;letter-spacing:.5px;font-weight:600;text-transform:uppercase">Orders<br><span style="opacity:.6;font-weight:400">MTD / {}</span></th>'
        '<th style="padding:5px 9px;text-align:center;font-size:7.5px;letter-spacing:.5px;font-weight:600;text-transform:uppercase">GM% MTD</th>'
        '<th style="padding:5px 9px;text-align:center;font-size:7.5px;letter-spacing:.5px;font-weight:600;text-transform:uppercase">&Delta;pp MoM</th>'
        '<th style="padding:5px 9px;text-align:center;font-size:7.5px;letter-spacing:.5px;font-weight:600;text-transform:uppercase">Rev/Wp MTD / {} &nbsp; Trend</th>'
        '</tr></thead>'
        '<tbody>{}</tbody>'
        '</table></div>'
    ).format(prev_lbl, prev_lbl, cl_summ_rows)

    # ── KPI Table (2 rows x 4 cols)
    def kc(lbl,val,sub,vc='#1A2C4E'):
        return ('<td class="kpi-c">'
                '<span class="kpi-l">{}</span>'
                '<span class="kpi-v" style="color:{}">{}</span>'
                '<span class="kpi-d">{}</span>'
                '</td>').format(lbl,vc,val,sub)

    kpi_html = (
        '<table class="kpi-t"><tr>'
        + kc('Installations MTD', '{:,}'.format(mtd['n']),
             '{} vs {:,} {} (1-{})'.format(dpct(mtd['n'],pm['n']),pm['n'],prev_lbl,pm_day))
        + kc('kW MTD', '{:,.0f}'.format(mtd['kw']),
             '{} vs {:,.0f} kW {}'.format(dpct(mtd['kw'],pm['kw']),pm['kw'],prev_lbl))
        + kc('GM % MTD', '{:.2f}%'.format(mtd['gm']),
             '{} vs {:.2f}% {}'.format(dpp(mtd['gm']-pm['gm']),pm['gm'],prev_lbl), gmc(mtd['gm']))
        + kc('Revenue MTD', fc(mtd['rev']),
             'vs {} {} &nbsp; {}'.format(fc(pm['rev']),prev_lbl,dpct(mtd['rev'],pm['rev'])))
        + '</tr><tr>'
        + kc('Avg System Size', '{:.2f} kW'.format(mtd['aos']),
             'vs {:.2f} kW {} &nbsp; {}'.format(pm['aos'],prev_lbl,dpval(mtd['aos']-pm['aos'],'kW')))
        + kc('Avg Order Value', fc(mtd['aov']),
             'vs {} {}'.format(fc(pm['aov']),prev_lbl))
        + kc('Rev / Wp', '&#8377;{:.2f}'.format(mtd['rev_wp']),
             'vs &#8377;{:.2f} {} &nbsp; {}'.format(pm['rev_wp'],prev_lbl,dpval(mtd['rev_wp']-pm['rev_wp'],'&#8377;/Wp')))
        + kc('Abs GM MTD', fc(mtd['abs_gm']),
             'vs {} {} &nbsp; {}'.format(fc(pm['abs_gm']),prev_lbl,dpct(mtd['abs_gm'],pm['abs_gm'])))
        + '</tr></table>'
    )

    # ── Latest Day Snapshot (1 row x 6 cols)
    def sc(lbl,val,sub,vc='#1A2C4E'):
        return ('<td class="snap-c">'
                '<span class="kpi-l">{}</span>'
                '<span class="kpi-v" style="font-size:17px;color:{}">{}</span>'
                '<span class="kpi-d">{}</span>'
                '</td>').format(lbl,vc,val,sub)

    snap_note = ('Data updated through <b style="color:#1A2C4E">{}</b> &nbsp;&bull;&nbsp; '
                 'Showing <b>{}</b> vs <b>{}</b>').format(latest.strftime('%d %b %Y'),lat_lbl,prv_lbl)
    snap_html = (
        '<div style="font-size:9.5px;color:#5A7A96;font-family:monospace;margin-bottom:10px">'
        +snap_note+'</div>'
        '<table class="snap-t"><tr>'
        + sc('Installations -- {}'.format(lat_lbl), str(lat['n']),
             'Prev ({}): {} &nbsp;&bull;&nbsp; MTD: {:,}'.format(prv_lbl,prv['n'],mtd['n']))
        + sc('kW -- {}'.format(lat_lbl), '{:.1f}'.format(lat['kw']),
             'Prev: {:.1f} kW &nbsp;&bull;&nbsp; MTD: {:,.0f}'.format(prv['kw'],mtd['kw']))
        + sc('Rev/Wp -- {}'.format(lat_lbl), '&#8377;{:.2f}'.format(lat['rev_wp']),
             'Prev: &#8377;{:.2f} &nbsp; {}'.format(prv['rev_wp'],dpval(lat['rev_wp']-prv['rev_wp'],'&#8377;/Wp')))
        + sc('AoS -- {}'.format(lat_lbl), '{:.2f} kW'.format(lat['aos']),
             'Prev: {:.2f} kW &nbsp; {}'.format(prv['aos'],dpval(lat['aos']-prv['aos'],'kW')))
        + sc('GM % -- {}'.format(lat_lbl), '{:.1f}%'.format(lat['gm']),
             'Prev: {:.1f}% &nbsp; {}'.format(prv['gm'],dpp(lat['gm']-prv['gm'])), gmc(lat['gm']))
        + '</tr></table>'
    )

    # ── COGS
    cogs_total = mtd['cogs']
    cogs_items = [('Module',mtd['mod']),('Inverter',mtd['inv']),('MMS',mtd['mms']),
                  ('Cables',mtd['cab']),('Metering',mtd['mtr']),('I&C',mtd['ic']),('Other',mtd['oth'])]
    pm_cogs    = {'Module':pm['mod'],'Inverter':pm['inv'],'MMS':pm['mms'],
                  'Cables':pm['cab'],'Metering':pm['mtr'],'I&C':pm['ic'],'Other':pm['oth']}
    bars = ''; cg_rows = ''
    for lbl,val in cogs_items:
        pct = val/cogs_total*100 if cogs_total else 0
        if pct<0.3: continue
        col = COGS_COLORS.get(lbl,'#94A3B8')
        bars += '<div class="cb" style="width:{:.1f}%;background:{}" title="{}: {:.1f}%">{}</div>'.format(
            pct,col,lbl,pct,lbl if pct>6 else '')
        pmv=pm_cogs.get(lbl,0); pmpct=pmv/pm['cogs']*100 if pm['cogs'] else 0
        rpct=val/mtd['rev']*100 if mtd['rev'] else 0
        cg_rows += ('<tr>'
            '<td><span style="display:inline-block;width:9px;height:9px;background:{};'
            'border-radius:2px;margin-right:5px;vertical-align:middle"></span>{}</td>'
            '<td class="R">{}</td><td class="R">{:.1f}%</td>'
            '<td class="R">{:.1f}%</td>'
            '<td class="R">{} vs {:.1f}% {}</td>'
            '</tr>').format(col,lbl,fc(val),pct,rpct,dpp(pct-pmpct,hb=False),pmpct,prev_lbl)
    cogs_html = ('<div class="cbar">{}</div>'
        '<table class="cg-t"><thead><tr><th>Category</th>'
        '<th class="R">Amount MTD</th><th class="R">% of COGS</th>'
        '<th class="R">% of Rev</th><th class="R">MoM Shift</th></tr></thead>'
        '<tbody>{}</tbody></table>').format(bars,cg_rows)

    # ── Cluster Trend Table
    mtd_cl=by_cluster(mtd_ps); pm_cl=by_cluster(pm_ps)
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

    def cl_row(r, bg=''):
        c=r['curr']; p=r['prev']
        sd=STATE_DISPLAY.get(r['state'],r['state'])
        bgs='background:{};'.format(bg) if bg else ''
        return ('<tr style="{}">'
            '<td class="L" style="font-weight:700">{}</td>'
            '<td class="L" style="color:#94A3B8;font-size:9.5px">{}</td>'
            '<td>{} <span style="color:#CBD5E1;font-size:9px">/{}</span></td>'
            '<td>{:,.0f} <span style="color:#CBD5E1;font-size:9px">/{:,.0f}</span></td>'
            '<td>&#8377;{:.1f} <span style="color:#CBD5E1;font-size:9px">/&#8377;{:.1f}</span></td>'
            '<td>{:.2f} <span style="color:#CBD5E1;font-size:9px">/{:.2f}</span></td>'
            '{}'
            '<td style="font-weight:700;text-align:center">{}</td>'
            '<td style="text-align:center">{}<br><span style="color:#94A3B8;font-size:9px">{}</span></td>'
            '<td style="text-align:center">{}</td>'
            '<td class="L" style="color:#374151;font-size:9.5px;max-width:150px;'
            'white-space:normal;min-width:90px">{}</td>'
            '</tr>').format(
            bgs,r['cluster'],sd,
            c['n'],p['n'],c['kw'],p['kw'],
            c['rev_wp'],p['rev_wp'],c['aos'],p['aos'],
            gmcell(c['gm']),
            dpp(r['gm_d']),
            fc(c['abs_gm']),fc(p['abs_gm']),
            dpct(c['abs_gm'],p['abs_gm']),
            r['drv_tag']
        )

    def grp(label,color):
        return ('<tr class="grp"><td colspan="11" style="color:{};'
                'border-left:3px solid {};padding-left:9px">{}</td></tr>').format(color,color,label)

    thead = ('<thead><tr>'
        '<th class="L">Cluster</th><th class="L">State</th>'
        '<th>Orders<br><span style="opacity:.6;font-weight:400">MTD / {}</span></th>'
        '<th>kW<br><span style="opacity:.6;font-weight:400">MTD / {}</span></th>'
        '<th>Rev/Wp<br><span style="opacity:.6;font-weight:400">MTD / {}</span></th>'
        '<th>AoS<br><span style="opacity:.6;font-weight:400">MTD / {}</span></th>'
        '<th>GM%</th><th>&#916;pp MoM</th>'
        '<th>Abs GM<br><span style="opacity:.6;font-weight:400">MTD / {}</span></th>'
        '<th>Abs&#916;%</th><th class="L">Driver</th>'
        '</tr></thead>').format(prev_lbl,prev_lbl,prev_lbl,prev_lbl,prev_lbl)

    tbody=''
    if declining: tbody+=grp('&#8595; Declining vs {} -- needs attention'.format(prev_lbl),'#DC2626')+''.join(cl_row(r,'#FFF9F9') for r in declining)
    if improving: tbody+=grp('&#8593; Improving vs {}'.format(prev_lbl),'#16A34A')+''.join(cl_row(r,'#F9FFFE') for r in improving)
    if stable_cl: tbody+=grp('&#8594; Stable (within +/-0.3pp)','#64748B')+''.join(cl_row(r) for r in stable_cl)
    if nascent:   tbody+=grp('New / growing clusters (thin prior data)','#7C3AED')+''.join(cl_row(r,'#FAF5FF') for r in nascent)
    cl_html='<div style="overflow-x:auto"><table class="cl-t">{}<tbody>{}</tbody></table></div>'.format(thead,tbody)

    # ── Actionable Insights
    all_cl = declining+stable_cl+improving

    insights=[]

    price_dn=[(r['cluster'],r['drv_det'].get('rev_wp_d',0))
              for r in all_cl if r['drv_det'].get('rev_wp_d',0)<-1.2 and r['curr']['n']>=MIN_ORDERS]
    price_dn.sort(key=lambda x:x[1])
    if price_dn:
        names=', '.join('<b>{}</b> (&#8595;&#8377;{:.1f}/Wp)'.format(c,abs(d)) for c,d in price_dn[:5])
        insights.append(('ins-r',
            '&#9888; <b>Pricing erosion in {} cluster(s):</b> {}. '
            'Rev/Wp fell below prior month -- review if discount approvals '
            'or cohort pricing changed in these markets.'.format(len(price_dn),names)))

    price_up=[(r['cluster'],r['drv_det'].get('rev_wp_d',0))
              for r in improving if r['drv_det'].get('rev_wp_d',0)>1.2 and r['curr']['n']>=MIN_ORDERS]
    if price_up:
        names=', '.join('<b>{}</b> (+&#8377;{:.1f}/Wp)'.format(c,d) for c,d in price_up[:3])
        insights.append(('ins-g',
            '&#128176; <b>Pricing discipline working:</b> {}. '
            'Rev/Wp rising with volume holding -- identify what changed and replicate.'.format(names)))

    # COGS insight -- full picture with root cause
    if cogs_total and pm['cogs'] and mtd['kw'] and pm['kw'] and mtd['rev'] and pm['rev']:
        aos_d = mtd['aos'] - pm['aos']
        total_cogs_pkw_c = mtd['cogs']/mtd['kw']
        total_cogs_pkw_p = pm['cogs']/pm['kw']
        rising, falling = [], []
        for lbl,val in cogs_items:
            pp_c = val/cogs_total*100
            pp_p = pm_cogs.get(lbl,0)/pm['cogs']*100
            pkw_c = val/mtd['kw']; pkw_p = pm_cogs.get(lbl,0)/pm['kw']
            gm_impact = -(val/mtd['rev'] - pm_cogs.get(lbl,0)/pm['rev'])*100
            d = pp_c - pp_p
            if d > 0.2:  rising.append((lbl, d, pkw_c, pkw_p, pkw_c-pkw_p, gm_impact))
            elif d < -0.2: falling.append((lbl, d, pkw_c, pkw_p, pkw_c-pkw_p, gm_impact))

        if rising or falling:
            net_gm_impact = sum(x[5] for x in rising) + sum(x[5] for x in falling)
            rise_parts = []
            for lbl,d,pkw_c,pkw_p,pkw_d,gm_i in sorted(rising,key=lambda x:-x[1]):
                if lbl in ('MMS','Cables') and aos_d > 0.05:
                    cause = 'AoS +{:.2f}kW drives more structural material per kW'.format(aos_d)
                else:
                    cause = '&#8377;{:.0f}/kW rate increase'.format(pkw_d)
                rise_parts.append('<b>{}</b> +{:.2f}pp (&#8377;{:,.0f}&#8594;&#8377;{:,.0f}/kW -- {})'.format(
                    lbl, d, pkw_p, pkw_c, cause))
            fall_parts = []
            for lbl,d,pkw_c,pkw_p,pkw_d,gm_i in sorted(falling,key=lambda x:x[1]):
                fall_parts.append('<b>{}</b> {:.2f}pp (&#8377;{:,.0f}&#8594;&#8377;{:,.0f}/kW)'.format(
                    lbl, d, pkw_p, pkw_c))
            msg = '&#129521; <b>COGS mix shift -- net {}{:.2f}pp on GM:</b> '.format(
                '+' if net_gm_impact>=0 else '', net_gm_impact)
            if rise_parts:
                msg += 'Rising: {}. '.format('; '.join(rise_parts))
            if fall_parts:
                msg += 'Offsetting: {}. '.format('; '.join(fall_parts))
            msg += 'Total COGS/kW &#8377;{:,.0f} vs &#8377;{:,.0f} prev.'.format(
                total_cogs_pkw_c, total_cogs_pkw_p)
            insights.append(('ins-a', msg))

    if latest.day>1:
        pace=mtd['n']/latest.day; proj=round(pace*30)
        mo_gm_proj=fc(mtd['abs_gm']/latest.day*30)
        insights.append(('ins-b',
            '&#128202; <b>Month run-rate:</b> {:.1f} installations/day &rarr; '
            '<b>~{:,} projected for full month</b> '
            '(vs {:,} actual in {}). At current GM, implies {} gross margin for the month.'.format(
            pace,proj,pm['n'],prev_lbl,mo_gm_proj)))

    if not insights:
        insights=[('ins-g','&#9989; All clusters within expected range. No anomalies detected.')]

    ins_html=''.join('<div class="ins {}">{}</div>'.format(t,m) for t,m in insights)

    # ── Assemble
    now_str=datetime.now().strftime('%d %b %Y, %I:%M %p')
    html="""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>"""+CSS+"""</style></head>
<body><div class="wrap">
<div class="hdr">
  <div class="hdr-t">&#9728;&#65039; Solar Square -- Daily GM Report</div>
  <div class="hdr-s">HOTO Month: """+latest.strftime('%b-%y')+""" &nbsp;&bull;&nbsp; Data through """+latest.strftime('%d %b %Y')+""" &nbsp;&bull;&nbsp; """+now_str+"""</div>
</div>
<div class="sec"><div class="sec-t">Executive Summary</div><div class="summ">"""+summary+"""</div></div>
<div class="sec"><div class="sec-t">MTD at a Glance -- """+curr_lbl+""" vs """+prev_lbl+""" (same """+str(pm_day)+""" days)</div>"""+kpi_html+"""</div>
<div class="sec"><div class="sec-t">Latest Day -- """+lat_lbl+""" vs """+prv_lbl+"""</div>"""+snap_html+"""</div>
<div class="sec"><div class="sec-t">COGS Breakdown -- MTD """+curr_lbl+"""</div>"""+cogs_html+"""</div>
<div class="sec"><div class="sec-t">Actionable Insights</div>"""+ins_html+"""</div>
<div class="ftr">Solar Square B2C GM &nbsp;&bull;&nbsp; Auto-generated from projects.json.gz &nbsp;&bull;&nbsp; """+latest.strftime('%d %b %Y')+"""</div>
</div></body></html>"""
    return html, mtd, latest


if __name__=='__main__':
    data=load_data()
    html,mtd,latest=build(data)
    subject=('Solar Square GM | {} | MTD {:,} installs | GM {:.2f}%'.format(
        latest.strftime('%d %b %Y'),mtd['n'],mtd['gm']))
    if not GMAIL_PASS:
        out=os.path.join(os.path.dirname(os.path.abspath(__file__)),'report_preview.html')
        open(out,'w',encoding='utf-8').write(html)
        print('Preview saved: '+out,flush=True)
        print('Subject: '+subject,flush=True)
        sys.exit(0)
    msg=MIMEMultipart('alternative')
    msg['Subject']=subject; msg['From']=SENDER; msg['To']=', '.join(RECIPIENTS)
    msg.attach(MIMEText(html,'html','utf-8'))
    print('Sending...',flush=True)
    with smtplib.SMTP('smtp.gmail.com',587) as s:
        s.ehlo(); s.starttls(); s.login(SENDER,GMAIL_PASS)
        s.sendmail(SENDER,RECIPIENTS,msg.as_string())
    print('Sent: '+subject,flush=True)
