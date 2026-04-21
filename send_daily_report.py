#!/usr/bin/env python3
"""
Solar Square Daily GM Report Emailer
Reads projects.json.gz → computes all metrics → sends HTML email via Gmail SMTP

Usage:
  GMAIL_PASSWORD=<app_password> python send_daily_report.py

GitHub Action:  set GMAIL_PASSWORD as a repo secret named GMAIL_PASSWORD
"""

import gzip, json, os, smtplib, sys, calendar
from collections import defaultdict
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Config ────────────────────────────────────────────────────────────────────
SENDER        = os.environ.get("GMAIL_USER", "purushottam.shinde@solarsquare.in")
RECIPIENTS    = os.environ.get("REPORT_TO",  "shindepurushottam7460@gmail.com").split(",")
GMAIL_PASS    = os.environ.get("GMAIL_PASSWORD", "")
DATA_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projects.json.gz")

STATE_ORDER = [
    'Delhi','Gujrat','Karnataka','Madhya Pradesh',
    'MH East','MH West','Rajasthan','Tamil Nadu','Telangana','Uttar Pradesh'
]
STATE_DISPLAY = {
    'Delhi':'Delhi','Gujrat':'Gujarat','Karnataka':'Karnataka',
    'Madhya Pradesh':'Madhya Pradesh','MH East':'MH East','MH West':'MH West',
    'Rajasthan':'Rajasthan','Tamil Nadu':'Tamil Nadu','Telangana':'Telangana',
    'Uttar Pradesh':'Uttar Pradesh'
}
CLUSTER_ORDER = {
    'Delhi':          ['NCR','Noida'],
    'Gujrat':         ['Ahmedabad','Surat'],
    'Karnataka':      ['Bengaluru'],
    'Madhya Pradesh': ['Bhopal','Indore','Jabalpur','Gwalior'],
    'MH East':        ['Amravati','Nagpur','Aurangabad'],
    'MH West':        ['Kolhapur','Nashik','Pune','Jalgaon','Solapur'],
    'Rajasthan':      ['Jaipur'],
    'Tamil Nadu':     ['Chennai','Coimbatore'],
    'Telangana':      ['Hyderabad'],
    'Uttar Pradesh':  ['Kanpur','Varanasi','Lucknow','Agra'],
}
COGS_COLORS = {
    'Module':'#2563A8','Inverter':'#7C3AED','MMS':'#0891B2',
    'Cables':'#16A34A','Metering':'#D97706','I&C':'#E11D48','Other':'#94A3B8'
}

# ── Data helpers ──────────────────────────────────────────────────────────────
def load_data():
    print(f"Loading {DATA_FILE}...", flush=True)
    with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
        return json.load(f)

def filter_projects(projects, start, end):
    """Filter by installation date (inclusive)."""
    return [p for p in projects if p.get('dt') and start <= p['dt'] <= end]

def calc(ps):
    """Compute core metrics for a list of projects."""
    if not ps:
        return dict(n=0, kw=0.0, rev=0.0, cogs=0.0, onm=0.0, qhs=0.0,
                    gm=0.0, adj_gm=0.0, rev_wp=0.0, aos=0.0, aov=0.0,
                    mod=0.0, inv=0.0, mms=0.0, cab=0.0, mtr=0.0, ic=0.0, oth=0.0)
    n    = len(ps)
    kw   = sum(p['kw']  for p in ps)
    rev  = sum(p['rev'] for p in ps)
    cogs = sum(p['cogs']for p in ps)
    onm  = sum(p.get('onm', 0) for p in ps)
    qhs  = sum(p.get('qhs', 0) for p in ps)
    gm      = (rev - cogs) / rev * 100                if rev else 0.0
    adj_gm  = (rev - cogs - onm - qhs) / rev * 100   if rev else 0.0
    rev_wp  = rev / (kw * 1000)                       if kw  else 0.0
    mod  = sum(p.get('mod', 0)                                      for p in ps)
    inv  = sum(p.get('inv', 0)                                      for p in ps)
    mms  = sum(p.get('prf', 0) + p.get('tsh', 0) + p.get('wel', 0) for p in ps)
    cab  = sum(p.get('cab', 0)                                      for p in ps)
    mtr  = sum(p.get('mtr', 0)                                      for p in ps)
    ic   = sum(p.get('ick', 0) + p.get('ica', 0)                   for p in ps)
    oth  = max(cogs - mod - inv - mms - cab - mtr - ic, 0.0)
    return dict(n=n, kw=kw, rev=rev, cogs=cogs, onm=onm, qhs=qhs,
                gm=gm, adj_gm=adj_gm, rev_wp=rev_wp, aos=kw/n, aov=rev/n,
                mod=mod, inv=inv, mms=mms, cab=cab, mtr=mtr, ic=ic, oth=oth)

def group_sc(projects):
    """Returns {state: {cluster/city: [projects]}}."""
    out = defaultdict(lambda: defaultdict(list))
    for p in projects:
        out[p['s']][p['c']].append(p)
    return out

# ── Formatting ────────────────────────────────────────────────────────────────
def fc(v):
    if v >= 1e7: return f"&#8377;{v/1e7:.2f}Cr"
    if v >= 1e5: return f"&#8377;{v/1e5:.1f}L"
    return f"&#8377;{v:,.0f}"

def fdelta_pct(curr, prev, higher_better=True):
    """Show % growth (for counts/kW/revenue)."""
    if prev == 0: return ''
    delta = curr - prev
    if abs(delta) < 0.001: return '<span style="color:#94A3B8;font-size:9px">—</span>'
    arrow = '↑' if delta > 0 else '↓'
    color = '#16A34A' if (delta > 0) == higher_better else '#DC2626'
    pct = abs(delta / prev * 100)
    return f'<span style="color:{color};font-size:9px;font-weight:600">{arrow}{pct:.0f}%</span>'

def fdelta_pp(curr, prev, higher_better=True):
    """Show pp change (for GM%, Rev/Wp, AoS)."""
    delta = curr - prev
    if abs(delta) < 0.01: return '<span style="color:#94A3B8;font-size:9px">—</span>'
    arrow = '↑' if delta > 0 else '↓'
    color = '#16A34A' if (delta > 0) == higher_better else '#DC2626'
    return f'<span style="color:{color};font-size:9px;font-weight:600">{arrow}{abs(delta):.2f}pp</span>'

def gm_badge(pct):
    if pct >= 44:   bg, fg = '#DCFCE7', '#15803D'
    elif pct >= 40: bg, fg = '#FEF3C7', '#92400E'
    elif pct >  0:  bg, fg = '#FEE2E2', '#991B1B'
    else:           bg, fg = '#F1F5F9', '#64748B'
    return f'<td style="background:{bg};color:{fg};font-weight:700;text-align:center;white-space:nowrap">{pct:.1f}%</td>'

def kpi_card(label, val, sub='', val_color='#1A2C4E'):
    return (f'<div class="kpi">'
            f'<div class="kpi-lbl">{label}</div>'
            f'<div class="kpi-val" style="color:{val_color}">{val}</div>'
            f'<div class="kpi-sub">{sub}</div>'
            f'</div>')

# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
body{font-family:'Segoe UI',Arial,sans-serif;background:#DDE8F5;margin:0;padding:20px}
.wrap{max-width:980px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;
      box-shadow:0 6px 32px rgba(26,44,78,0.15)}
.hdr{background:linear-gradient(135deg,#1A2C4E 0%,#2563A8 100%);padding:22px 28px}
.hdr-title{color:#fff;font-size:20px;font-weight:700;margin:0;letter-spacing:-0.3px}
.hdr-sub{color:rgba(255,255,255,0.55);font-size:10px;margin-top:5px;font-family:monospace;
         letter-spacing:1.1px;text-transform:uppercase}
.sec{padding:20px 28px;border-bottom:1px solid #E2EAF4}
.sec-title{font-size:10.5px;font-weight:700;color:#1A2C4E;margin:0 0 14px;
           text-transform:uppercase;letter-spacing:.9px;border-left:3px solid #2563A8;
           padding-left:8px;display:flex;align-items:center;gap:7px}
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.kpi{background:#F5F8FC;border:1px solid #C8DCEE;border-radius:8px;padding:12px 14px}
.kpi-lbl{font-size:8px;color:#5A7A96;text-transform:uppercase;letter-spacing:.8px;
          font-family:monospace;margin-bottom:5px}
.kpi-val{font-size:19px;font-weight:700;color:#1A2C4E;line-height:1}
.kpi-sub{font-size:9px;color:#94A3B8;margin-top:5px;line-height:1.4}
table{width:100%;border-collapse:collapse;font-size:11px}
th{background:#1A2C4E;color:#fff;padding:7px 9px;font-size:8px;font-weight:600;
   letter-spacing:.5px;text-transform:uppercase;white-space:nowrap;text-align:center}
th.left{text-align:left}
td{padding:5px 9px;border-bottom:1px solid #EBF2FA;vertical-align:middle;
   white-space:nowrap;text-align:center}
td.left{text-align:left}
.state-row td{background:#EBF2FA;font-weight:700;color:#1A2C4E}
.grand-row td{background:#1A2C4E;color:#fff;font-weight:700;font-size:11.5px}
.alert{border-radius:7px;padding:10px 14px;margin-bottom:9px;font-size:11px;line-height:1.6}
.alert-r{background:#FEF2F2;border:1px solid #FECACA}
.alert-a{background:#FFFBEB;border:1px solid #FCD34D}
.alert-g{background:#F0FDF4;border:1px solid #86EFAC}
.cbar{height:22px;border-radius:5px;overflow:hidden;display:flex;margin-bottom:10px}
.cb{display:flex;align-items:center;justify-content:center;font-size:8.5px;color:#fff;
    font-weight:600;overflow:hidden;white-space:nowrap;padding:0 4px}
.ftr{background:#F5F8FC;padding:14px 28px;font-size:9px;color:#94A3B8;text-align:center;
     line-height:1.7}
"""

# ── HTML builder ──────────────────────────────────────────────────────────────
def build_html(data, today=None):
    if today is None:
        today = datetime.now()

    projects    = data['projects']
    mo_onm_qhse = data.get('_meta', {}).get('monthly_onm_qhse', {})

    ts = today.strftime('%Y-%m-%d')
    ys = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    ms = today.strftime('%Y-%m-01')

    # Previous month same-day range
    pm_last = today.replace(day=1) - timedelta(days=1)
    pm_start = pm_last.replace(day=1).strftime('%Y-%m-01')
    pm_day   = min(today.day, calendar.monthrange(pm_last.year, pm_last.month)[1])
    pm_end   = f"{pm_last.year}-{pm_last.month:02d}-{pm_day:02d}"

    mo_key = today.strftime('%Y-%m')
    pm_key = pm_last.strftime('%Y-%m')

    # --- Compute metric sets ---
    mtd_ps = filter_projects(projects, ms, ts)
    pm_ps  = filter_projects(projects, pm_start, pm_end)
    tod_ps = filter_projects(projects, ts, ts)
    yes_ps = filter_projects(projects, ys, ys)

    mtd = calc(mtd_ps)
    pm  = calc(pm_ps)
    tod = calc(tod_ps)
    yes = calc(yes_ps)

    # Inject monthly ONM/QHSE totals from meta for grand total
    def inject_onm(m, key):
        if m['onm'] == 0 and m['qhs'] == 0:
            mok = mo_onm_qhse.get(key, {})
            m = dict(m)
            m['onm'] = mok.get('onm', 0)
            m['qhs'] = mok.get('qhs', 0)
            if m['rev']:
                m['adj_gm'] = (m['rev'] - m['cogs'] - m['onm'] - m['qhs']) / m['rev'] * 100
        return m

    mtd = inject_onm(mtd, mo_key)
    pm  = inject_onm(pm,  pm_key)

    # --- Section 1: MTD KPI Grid ---
    gm_c  = '#16A34A' if mtd['gm']  >= 44 else ('#D97706' if mtd['gm']  >= 40 else '#DC2626')
    agm_c = '#16A34A' if mtd['adj_gm'] >= 44 else ('#D97706' if mtd['adj_gm'] >= 40 else '#DC2626')

    s1 = f"""
<div class="kpi-grid">
  {kpi_card('Orders Installed (MTD)', f'{mtd["n"]:,}',
    f'{fdelta_pct(mtd["n"],pm["n"])} vs {pm["n"]:,} ({pm_last.strftime("%b")} 1–{pm_day})')}
  {kpi_card('kW Installed (MTD)', f'{mtd["kw"]:,.1f}',
    f'{fdelta_pct(mtd["kw"],pm["kw"])} vs {pm["kw"]:,.1f} kW last month')}
  {kpi_card('Revenue (MTD)', fc(mtd["rev"]),
    f'vs {fc(pm["rev"])} last month')}
  {kpi_card('Avg Order Size', f'{mtd["aos"]:.2f} kW',
    f'vs {pm["aos"]:.2f} kW last month {fdelta_pp(mtd["aos"],pm["aos"])}')}
  {kpi_card('Rev / Wp (Installed)', f'&#8377;{mtd["rev_wp"]:.2f}',
    f'vs &#8377;{pm["rev_wp"]:.2f} last month {fdelta_pp(mtd["rev_wp"],pm["rev_wp"])}')}
  {kpi_card('Avg Order Value', fc(mtd["aov"]),
    f'vs {fc(pm["aov"])} last month')}
  {kpi_card('GM Inst %', f'{mtd["gm"]:.2f}%',
    f'{fdelta_pp(mtd["gm"],pm["gm"])} vs {pm["gm"]:.2f}% last month', gm_c)}
  {kpi_card('Adjusted GM %', f'{mtd["adj_gm"]:.2f}%',
    f'{fdelta_pp(mtd["adj_gm"],pm["adj_gm"])} vs {pm["adj_gm"]:.2f}% last month', agm_c)}
</div>"""

    # --- Section 2: Today vs Yesterday ---
    gm_ct = '#16A34A' if tod['gm'] >= 44 else ('#D97706' if tod['gm'] >= 40 else '#DC2626')
    s2 = f"""
<div class="kpi-grid">
  {kpi_card(f'Orders — {today.strftime("%d %b")}', str(tod['n']),
    f'Yesterday ({(today-timedelta(days=1)).strftime("%d %b")}): {yes["n"]}')}
  {kpi_card(f'kW — {today.strftime("%d %b")}', f'{tod["kw"]:.1f} kW',
    f'Yesterday: {yes["kw"]:.1f} kW')}
  {kpi_card('GM % Today', f'{tod["gm"]:.1f}%',
    f'{fdelta_pp(tod["gm"],yes["gm"])} vs {yes["gm"]:.1f}% yesterday', gm_ct)}
  {kpi_card('Adj GM% Today', f'{tod["adj_gm"]:.1f}%',
    f'{fdelta_pp(tod["adj_gm"],yes["adj_gm"])} vs {yes["adj_gm"]:.1f}% yesterday', gm_ct)}
</div>"""

    # --- Section 3: State-Cluster Table ---
    sc = group_sc(mtd_ps)
    rows = []

    for state in STATE_ORDER:
        cd = sc.get(state, {})
        if not cd:
            continue

        state_ps = [p for cps in cd.values() for p in cps]
        st = calc(state_ps)
        disp = STATE_DISPLAY.get(state, state)

        rows.append(
            f'<tr class="state-row">'
            f'<td class="left" colspan="2">{disp}</td>'
            f'<td>{st["n"]}</td>'
            f'<td>{st["kw"]:,.2f}</td>'
            f'<td>{st["aos"]:.1f}</td>'
            f'<td>{fc(st["aov"])}</td>'
            f'<td>&#8377;{st["rev_wp"]:.2f}</td>'
            f'{gm_badge(st["gm"])}'
            f'<td>{"&#8377;{:,.0f}".format(st["onm"]+st["qhs"]) if st["onm"]+st["qhs"]>0 else "—"}</td>'
            f'{gm_badge(st["adj_gm"])}'
            f'</tr>'
        )

        ordered = CLUSTER_ORDER.get(state, sorted(cd.keys()))
        seen = set()
        for cluster in ordered:
            cps = cd.get(cluster)
            if not cps:
                continue
            seen.add(cluster)
            ct = calc(cps)
            onm_qhs_val = ct['onm'] + ct['qhs']
            rows.append(
                f'<tr>'
                f'<td></td>'
                f'<td class="left" style="color:#5A7A96;padding-left:18px">{cluster}</td>'
                f'<td>{ct["n"]}</td>'
                f'<td>{ct["kw"]:,.2f}</td>'
                f'<td>{ct["aos"]:.1f}</td>'
                f'<td>{fc(ct["aov"])}</td>'
                f'<td>&#8377;{ct["rev_wp"]:.2f}</td>'
                f'{gm_badge(ct["gm"])}'
                f'<td>{"&#8377;{:,.0f}".format(onm_qhs_val) if onm_qhs_val>0 else "—"}</td>'
                f'{gm_badge(ct["adj_gm"])}'
                f'</tr>'
            )
        # unlisted clusters
        for cluster, cps in sorted(cd.items()):
            if cluster in seen:
                continue
            ct = calc(cps)
            onm_qhs_val = ct['onm'] + ct['qhs']
            rows.append(
                f'<tr>'
                f'<td></td>'
                f'<td class="left" style="color:#5A7A96;padding-left:18px">{cluster}</td>'
                f'<td>{ct["n"]}</td>'
                f'<td>{ct["kw"]:,.2f}</td>'
                f'<td>{ct["aos"]:.1f}</td>'
                f'<td>{fc(ct["aov"])}</td>'
                f'<td>&#8377;{ct["rev_wp"]:.2f}</td>'
                f'{gm_badge(ct["gm"])}'
                f'<td>{"&#8377;{:,.0f}".format(onm_qhs_val) if onm_qhs_val>0 else "—"}</td>'
                f'{gm_badge(ct["adj_gm"])}'
                f'</tr>'
            )

    # Grand total row
    rows.append(
        f'<tr class="grand-row">'
        f'<td class="left" colspan="2">Grand Total</td>'
        f'<td>{mtd["n"]:,}</td>'
        f'<td>{mtd["kw"]:,.2f}</td>'
        f'<td>{mtd["aos"]:.2f}</td>'
        f'<td>{fc(mtd["aov"])}</td>'
        f'<td>&#8377;{mtd["rev_wp"]:.2f}</td>'
        f'<td style="background:#16A34A;color:#fff;font-weight:700">{mtd["gm"]:.2f}%</td>'
        f'<td>{fc(mtd["onm"]+mtd["qhs"])}</td>'
        f'<td style="background:#16A34A;color:#fff;font-weight:700">{mtd["adj_gm"]:.2f}%</td>'
        f'</tr>'
    )

    s3 = f"""
<table>
  <thead><tr>
    <th class="left">State</th><th class="left">Cluster</th>
    <th># Orders</th><th>kW</th><th>AoS</th><th>AoV</th>
    <th>Rev/Wp</th><th>GM Inst%</th><th>COGS O&amp;M+QHSE</th><th>Adj GM%</th>
  </tr></thead>
  <tbody>{''.join(rows)}</tbody>
</table>"""

    # --- Section 4: COGS Breakdown ---
    cogs_total = mtd['cogs']
    cogs_items = [
        ('Module',   mtd['mod']),
        ('Inverter', mtd['inv']),
        ('MMS',      mtd['mms']),
        ('Cables',   mtd['cab']),
        ('Metering', mtd['mtr']),
        ('I&C',      mtd['ic']),
        ('Other',    mtd['oth']),
    ]
    pm_cogs_items = {
        'Module':   pm['mod'],  'Inverter': pm['inv'],
        'MMS':      pm['mms'],  'Cables':   pm['cab'],
        'Metering': pm['mtr'],  'I&C':      pm['ic'],
        'Other':    pm['oth'],
    }

    bars = ''
    cogs_rows = ''
    for label, val in cogs_items:
        pct = val / cogs_total * 100 if cogs_total else 0
        if pct < 0.3:
            continue
        color = COGS_COLORS.get(label, '#94A3B8')
        bars += (f'<div class="cb" style="width:{pct:.1f}%;background:{color}" '
                 f'title="{label}: {pct:.1f}%">{label if pct > 6 else ""}</div>')

        pm_val  = pm_cogs_items.get(label, 0)
        pm_pct  = pm_val / pm['cogs'] * 100 if pm['cogs'] else 0
        rev_pct = val / mtd['rev'] * 100 if mtd['rev'] else 0

        cogs_rows += (
            f'<tr>'
            f'<td class="left"><span style="display:inline-block;width:10px;height:10px;'
            f'background:{color};border-radius:2px;margin-right:6px;vertical-align:middle">'
            f'</span>{label}</td>'
            f'<td>{fc(val)}</td>'
            f'<td>{pct:.1f}%</td>'
            f'<td>{rev_pct:.1f}%</td>'
            f'<td>{fdelta_pp(pct, pm_pct)} vs {pm_pct:.1f}% last month</td>'
            f'</tr>'
        )

    s4 = f"""
<div class="cbar">{bars}</div>
<table>
  <thead><tr>
    <th class="left">Category</th><th>Amount (MTD)</th>
    <th>% of COGS</th><th>% of Revenue</th><th>vs Last Month</th>
  </tr></thead>
  <tbody>{cogs_rows}</tbody>
</table>"""

    # --- Section 5: Alerts ---
    alerts = []

    # Zero / critically low clusters
    for state, cd in sc.items():
        for cluster, cps in cd.items():
            ct = calc(cps)
            if ct['n'] > 0 and ct['adj_gm'] < 40:
                cls = 'alert-r'
                msg = (f'<b>{STATE_DISPLAY.get(state,state)} — {cluster}</b>: '
                       f'{ct["n"]} orders, {ct["kw"]:.0f} kW — '
                       f'Adj GM% = <b>{ct["adj_gm"]:.1f}%</b> '
                       f'{"(0% — check data)" if ct["adj_gm"]==0 else "(below 40% threshold)"}')
                alerts.append((cls, msg))

    # Top performers (≥5 orders, ≥46% GM)
    top = []
    for state, cd in sc.items():
        for cluster, cps in cd.items():
            ct = calc(cps)
            if ct['n'] >= 5 and ct['gm'] >= 46:
                top.append((ct['gm'], f'{STATE_DISPLAY.get(state,state)} – {cluster} ({ct["gm"]:.1f}%)'))
    top.sort(reverse=True)
    if top:
        alerts.append(('alert-g',
            f'🏆 <b>Top performers this month:</b> {", ".join(v for _,v in top[:4])}'))

    # Monthly pace
    if today.day > 1:
        pace      = mtd['n'] / today.day
        projected = round(pace * 30)
        alerts.append(('alert-a',
            f'📈 <b>Run-rate:</b> {pace:.1f} installs/day → '
            f'<b>~{projected:,} orders</b> projected for full month at current pace '
            f'(vs {pm["n"]:,} actual last month)'))

    # MoM GM trend
    gm_delta = mtd['gm'] - pm['gm']
    if abs(gm_delta) >= 0.5:
        icon  = '📉' if gm_delta < 0 else '📊'
        color = 'alert-r' if gm_delta < -0.5 else 'alert-g'
        alerts.append((color,
            f'{icon} <b>GM% trend:</b> {mtd["gm"]:.2f}% this month vs '
            f'{pm["gm"]:.2f}% last month — '
            f'{"down" if gm_delta<0 else "up"} {abs(gm_delta):.2f}pp MoM'))

    s5 = ''.join(f'<div class="alert {t}">{m}</div>' for t, m in alerts) \
         or '<div class="alert alert-g">✅ All clusters within normal range.</div>'

    # ── Assemble ──────────────────────────────────────────────────────────────
    prev_label = pm_last.strftime('%b %Y')
    curr_label = today.strftime('%b %Y')
    now_str    = datetime.now().strftime('%d %b %Y, %I:%M %p')

    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head>
<body><div class="wrap">

<div class="hdr">
  <div class="hdr-title">&#9728;&#65039; Solar Square &mdash; Daily GM Report</div>
  <div class="hdr-sub">
    HOTO Month: {today.strftime('%b-%y')} &nbsp;&bull;&nbsp;
    Installed till {today.strftime('%d %B %Y')} &nbsp;&bull;&nbsp;
    Auto-generated &middot; {now_str}
  </div>
</div>

<div class="sec">
  <div class="sec-title">&#128202; MTD Performance &mdash; {curr_label} (1&ndash;{today.day}) vs {prev_label} (1&ndash;{pm_day})</div>
  {s1}
</div>

<div class="sec">
  <div class="sec-title">&#128197; Today vs Yesterday</div>
  {s2}
</div>

<div class="sec">
  <div class="sec-title">&#128203; State &middot; Cluster Breakdown &mdash; MTD {curr_label}</div>
  {s3}
</div>

<div class="sec">
  <div class="sec-title">&#129521; COGS Breakdown &mdash; MTD {curr_label}</div>
  {s4}
</div>

<div class="sec">
  <div class="sec-title">&#128680; Alerts &amp; Highlights</div>
  {s5}
</div>

<div class="ftr">
  Solar Square B2C GM Dashboard &nbsp;&bull;&nbsp;
  Auto-generated from <code>projects.json.gz</code> &nbsp;&bull;&nbsp;
  {today.strftime('%d %b %Y')}<br>
  Data source: GitHub &rarr; DN dump &rarr; generate_projects_json.py
</div>

</div></body></html>"""
    return html, mtd

# ── Email sending ─────────────────────────────────────────────────────────────
def send_email(html, subject):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = SENDER
    msg['To']      = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, 'html', 'utf-8'))

    print(f"Connecting smtp.gmail.com:587...", flush=True)
    with smtplib.SMTP('smtp.gmail.com', 587) as s:
        s.ehlo()
        s.starttls()
        s.login(SENDER, GMAIL_PASS)
        s.sendmail(SENDER, RECIPIENTS, msg.as_string())
    print(f"✅ Sent to: {RECIPIENTS}", flush=True)

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    today = datetime.now()
    data  = load_data()
    html, mtd = build_html(data, today)

    subject = (f"Solar Square GM Report | {today.strftime('%d %b %Y')} | "
               f"MTD: {mtd['n']:,} installs | Adj GM: {mtd['adj_gm']:.2f}%")

    if not GMAIL_PASS:
        # Save preview HTML for local inspection
        preview = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'report_preview.html')
        with open(preview, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"ℹ  GMAIL_PASSWORD not set — saved preview to: {preview}", flush=True)
        print(f"   Subject would be: {subject}", flush=True)
        sys.exit(0)

    send_email(html, subject)
    print(f"   Subject: {subject}", flush=True)
