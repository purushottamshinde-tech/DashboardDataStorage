#!/usr/bin/env python3
"""
Solar Square Daily GM Report Emailer  v2
- Cluster vs its own previous month trend (not cluster-to-cluster)
- AOV / AOS / COGS driver decomposition
- Absolute GM MoM comparison
- Latest data date auto-detected (not datetime.now())
- No raw data dump — analysis + numbers only

Usage:  GMAIL_PASSWORD=<app_pwd> python send_daily_report.py
"""

import gzip, json, os, smtplib, sys, calendar
from collections import defaultdict
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Config ────────────────────────────────────────────────────────────────────
SENDER     = os.environ.get("GMAIL_USER", "purushottam.shinde@solarsquare.in")
RECIPIENTS = os.environ.get("REPORT_TO",  "shindepurushottam7460@gmail.com").split(",")
GMAIL_PASS = os.environ.get("GMAIL_PASSWORD", "")
DATA_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "projects.json.gz")

STATE_DISPLAY = {
    'Delhi':'Delhi','Gujrat':'Gujarat','Karnataka':'Karnataka',
    'Madhya Pradesh':'Madhya Pradesh','MH East':'MH East','MH West':'MH West',
    'Rajasthan':'Rajasthan','Tamil Nadu':'Tamil Nadu','Telangana':'Telangana',
    'Uttar Pradesh':'Uttar Pradesh'
}
STATE_ORDER = list(STATE_DISPLAY.keys())

COGS_COLORS = {
    'Module':'#2563A8','Inverter':'#7C3AED','MMS':'#0891B2',
    'Cables':'#16A34A','Metering':'#D97706','I&C':'#E11D48','Other':'#94A3B8'
}

# ── Core helpers ──────────────────────────────────────────────────────────────
def load_data():
    print(f"Loading data...", flush=True)
    with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
        return json.load(f)

def filter_projects(projects, start, end):
    return [p for p in projects if p.get('dt') and start <= p['dt'] <= end]

def calc(ps):
    if not ps:
        return dict(n=0, kw=0.0, rev=0.0, cogs=0.0, onm=0.0, qhs=0.0,
                    gm=0.0, adj_gm=0.0, rev_wp=0.0, aos=0.0, aov=0.0,
                    abs_gm=0.0, cogs_kw=0.0,
                    mod=0.0, inv=0.0, mms=0.0, cab=0.0, mtr=0.0, ic=0.0, oth=0.0)
    n    = len(ps)
    kw   = sum(p['kw']   for p in ps)
    rev  = sum(p['rev']  for p in ps)
    cogs = sum(p['cogs'] for p in ps)
    onm  = sum(p.get('onm', 0) for p in ps)
    qhs  = sum(p.get('qhs', 0) for p in ps)
    gm      = (rev - cogs) / rev * 100              if rev else 0.0
    adj_gm  = (rev - cogs - onm - qhs) / rev * 100 if rev else 0.0
    rev_wp  = rev / (kw * 1000)                     if kw  else 0.0
    cogs_kw = cogs / kw                             if kw  else 0.0
    mod = sum(p.get('mod', 0)                                      for p in ps)
    inv = sum(p.get('inv', 0)                                      for p in ps)
    mms = sum(p.get('prf', 0) + p.get('tsh', 0) + p.get('wel', 0) for p in ps)
    cab = sum(p.get('cab', 0)                                      for p in ps)
    mtr = sum(p.get('mtr', 0)                                      for p in ps)
    ic  = sum(p.get('ick', 0) + p.get('ica', 0)                   for p in ps)
    oth = max(cogs - mod - inv - mms - cab - mtr - ic, 0.0)
    return dict(n=n, kw=kw, rev=rev, cogs=cogs, onm=onm, qhs=qhs,
                gm=gm, adj_gm=adj_gm, rev_wp=rev_wp, aos=kw/n, aov=rev/n,
                abs_gm=rev-cogs, cogs_kw=cogs_kw,
                mod=mod, inv=inv, mms=mms, cab=cab, mtr=mtr, ic=ic, oth=oth)

def inject_meta_onm(m, mo_onm_qhse, key):
    if m['onm'] == 0 and m['qhs'] == 0:
        mok = mo_onm_qhse.get(key, {})
        m = dict(m)
        m['onm'] = mok.get('onm', 0)
        m['qhs'] = mok.get('qhs', 0)
        if m['rev']:
            m['adj_gm'] = (m['rev'] - m['cogs'] - m['onm'] - m['qhs']) / m['rev'] * 100
    return m

def group_by_cluster(projects):
    out = defaultdict(list)
    for p in projects:
        out[(p['s'], p['c'])].append(p)
    return out

# ── Driver analysis ───────────────────────────────────────────────────────────
def driver_analysis(curr, prev, gm_delta):
    """
    Decompose GM% change into its root causes.
    Returns (primary_label, details_dict) for rendering.
    """
    if prev['n'] < 5:
        return "Thin prev data", {}
    if abs(gm_delta) < 0.15:
        return "Stable", {}

    rev_wp_d  = curr['rev_wp']  - prev['rev_wp']   # ₹/Wp pricing effect
    aos_d     = curr['aos']     - prev['aos']       # kW system size effect
    aov_d     = curr['aov']     - prev['aov']       # ₹ per order
    cogs_kw_d = curr['cogs_kw'] - prev['cogs_kw']  # ₹/kW cost effect

    factors = []

    # 1. Pricing / Rev-per-Wp effect
    if rev_wp_d > 1.2:
        factors.append((abs(rev_wp_d) * 10, 'pricing_up',
                        f"Rev/Wp ↑&#8377;{rev_wp_d:.1f}/Wp (pricing improvement)"))
    elif rev_wp_d < -1.2:
        factors.append((abs(rev_wp_d) * 10, 'pricing_down',
                        f"Rev/Wp ↓&#8377;{abs(rev_wp_d):.1f}/Wp (pricing pressure / discounts)"))

    # 2. System size effect (AoS)
    if aos_d > 0.25:
        factors.append((abs(aos_d) * 8, 'size_up',
                        f"AoS ↑{aos_d:.2f} kW (larger systems → better economics)"))
    elif aos_d < -0.25:
        factors.append((abs(aos_d) * 8, 'size_down',
                        f"AoS ↓{abs(aos_d):.2f} kW (smaller systems → thinner margins)"))

    # 3. COGS per kW efficiency
    if cogs_kw_d < -2500:
        factors.append((abs(cogs_kw_d) / 1000, 'cogs_save',
                        f"COGS/kW ↓&#8377;{abs(cogs_kw_d/1000):.1f}K (procurement saving)"))
    elif cogs_kw_d > 2500:
        factors.append((cogs_kw_d / 1000, 'cogs_rise',
                        f"COGS/kW ↑&#8377;{cogs_kw_d/1000:.1f}K (cost inflation)"))

    # 4. Fallback — AoV shift
    if not factors:
        if abs(aov_d) > 20000:
            d = '↑' if aov_d > 0 else '↓'
            factors.append((1, 'mix',
                            f"AoV {d}&#8377;{abs(aov_d/1000):.0f}K (product mix shift)"))
        else:
            return "Minor blended shifts", {
                'rev_wp_d': rev_wp_d, 'aos_d': aos_d, 'cogs_kw_d': cogs_kw_d
            }

    factors.sort(key=lambda x: -x[0])
    label = "; ".join(f[2] for f in factors[:2])
    return label, {
        'rev_wp_d': rev_wp_d,
        'aos_d': aos_d,
        'aov_d': aov_d,
        'cogs_kw_d': cogs_kw_d,
        'top_type': factors[0][1] if factors else None,
    }

# ── Formatting helpers ────────────────────────────────────────────────────────
def fc(v):
    if v >= 1e7: return f"&#8377;{v/1e7:.2f}Cr"
    if v >= 1e5: return f"&#8377;{v/1e5:.1f}L"
    return f"&#8377;{v:,.0f}"

def pp(delta, higher_better=True, decimals=2):
    if abs(delta) < 0.01: return '<span style="color:#94A3B8">—</span>'
    arrow = '↑' if delta > 0 else '↓'
    color = '#16A34A' if (delta > 0) == higher_better else '#DC2626'
    fmt = f".{decimals}f"
    return f'<span style="color:{color};font-weight:700">{arrow}{abs(delta):{fmt}}pp</span>'

def pct_chg(curr, prev, higher_better=True):
    if prev == 0: return ''
    delta = (curr - prev) / abs(prev) * 100
    if abs(delta) < 0.5: return '<span style="color:#94A3B8">—</span>'
    arrow = '↑' if delta > 0 else '↓'
    color = '#16A34A' if (delta > 0) == higher_better else '#DC2626'
    return f'<span style="color:{color};font-weight:700">{arrow}{abs(delta):.0f}%</span>'

def gm_col(pct):
    if pct >= 44:   return '#16A34A'
    if pct >= 40:   return '#D97706'
    return '#DC2626'

def kpi_card(label, val, sub='', val_color='#1A2C4E'):
    return (f'<div class="kpi"><div class="kpi-lbl">{label}</div>'
            f'<div class="kpi-val" style="color:{val_color}">{val}</div>'
            f'<div class="kpi-sub">{sub}</div></div>')

def trend_badge(gm_delta):
    if gm_delta > 1.5:    return '<span class="badge badge-g">↑ Strong</span>'
    if gm_delta > 0.3:    return '<span class="badge badge-g">↑ Improving</span>'
    if gm_delta >= -0.3:  return '<span class="badge badge-n">→ Stable</span>'
    if gm_delta >= -1.5:  return '<span class="badge badge-a">↓ Slipping</span>'
    return '<span class="badge badge-r">↓ Alert</span>'

# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
body{font-family:'Segoe UI',Arial,sans-serif;background:#DDE8F5;margin:0;padding:20px}
.wrap{max-width:960px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;
      box-shadow:0 6px 32px rgba(26,44,78,0.15)}
.hdr{background:linear-gradient(135deg,#1A2C4E 0%,#2563A8 100%);padding:22px 28px}
.hdr-title{color:#fff;font-size:20px;font-weight:700;margin:0}
.hdr-sub{color:rgba(255,255,255,0.55);font-size:10px;margin-top:5px;font-family:monospace;
         letter-spacing:1px;text-transform:uppercase}
.sec{padding:20px 28px;border-bottom:1px solid #E2EAF4}
.sec-title{font-size:10.5px;font-weight:700;color:#1A2C4E;margin:0 0 14px;
           text-transform:uppercase;letter-spacing:.9px;border-left:3px solid #2563A8;
           padding-left:8px}
.kpi-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.kpi{background:#F5F8FC;border:1px solid #C8DCEE;border-radius:8px;padding:12px 14px}
.kpi-lbl{font-size:8px;color:#5A7A96;text-transform:uppercase;letter-spacing:.8px;
          font-family:monospace;margin-bottom:5px}
.kpi-val{font-size:19px;font-weight:700;color:#1A2C4E;line-height:1}
.kpi-sub{font-size:9px;color:#94A3B8;margin-top:5px;line-height:1.5}
/* Cluster cards */
.cluster-grid{display:flex;flex-direction:column;gap:8px}
.cluster-card{border:1px solid #E2EAF4;border-radius:8px;padding:12px 16px;
              background:#FAFCFF;border-left:4px solid #C8DCEE}
.cluster-card.up{border-left-color:#16A34A;background:#F0FDF4}
.cluster-card.down{border-left-color:#DC2626;background:#FEF2F2}
.cluster-card.alert{border-left-color:#DC2626;background:#FEF2F2}
.cluster-card.stable{border-left-color:#94A3B8;background:#F8FAFC}
.cc-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:6px}
.cc-name{font-size:13px;font-weight:700;color:#1A2C4E}
.cc-state{font-size:10px;color:#94A3B8;margin-left:6px}
.cc-metrics{display:flex;gap:16px;font-size:10.5px;margin-bottom:5px;flex-wrap:wrap}
.cc-m{display:flex;flex-direction:column;gap:1px}
.cc-ml{font-size:8px;color:#94A3B8;text-transform:uppercase;letter-spacing:.5px;font-family:monospace}
.cc-mv{font-weight:600;color:#1A2C4E;font-size:12px}
.cc-driver{font-size:10px;color:#374151;background:rgba(37,99,168,0.07);
           padding:5px 9px;border-radius:5px;margin-top:4px;line-height:1.5}
.badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:9px;font-weight:700}
.badge-g{background:#DCFCE7;color:#15803D}
.badge-a{background:#FEF3C7;color:#92400E}
.badge-r{background:#FEE2E2;color:#991B1B}
.badge-n{background:#F1F5F9;color:#475569}
.insight-block{background:#EFF6FF;border:1px solid #BFDBFE;border-radius:8px;
               padding:12px 16px;margin-bottom:8px;font-size:11px;line-height:1.7;color:#1e3a5f}
.insight-block b{color:#1A2C4E}
/* Snap grid (today vs prev) */
.snap-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.snap{background:#F5F8FC;border:1px solid #C8DCEE;border-radius:8px;padding:11px 13px}
.snap-lbl{font-size:8px;color:#5A7A96;font-family:monospace;text-transform:uppercase;
           letter-spacing:.7px;margin-bottom:4px}
.snap-val{font-size:17px;font-weight:700;color:#1A2C4E;line-height:1}
.snap-sub{font-size:9px;color:#94A3B8;margin-top:4px;line-height:1.5}
/* COGS */
.cbar{height:22px;border-radius:5px;overflow:hidden;display:flex;margin-bottom:10px}
.cb{display:flex;align-items:center;justify-content:center;font-size:8.5px;color:#fff;
    font-weight:600;overflow:hidden;white-space:nowrap;padding:0 4px}
table.cogs{width:100%;border-collapse:collapse;font-size:11px}
table.cogs th{background:#1A2C4E;color:#fff;padding:7px 10px;font-size:8px;
              letter-spacing:.5px;text-transform:uppercase;text-align:left}
table.cogs td{padding:6px 10px;border-bottom:1px solid #EBF2FA;color:#1A2C4E}
table.cogs td.r{text-align:right}
.divider{color:#C8DCEE;margin:0 6px;font-size:10px}
.ftr{background:#F5F8FC;padding:14px 28px;font-size:9px;color:#94A3B8;text-align:center;line-height:1.7}
"""

# ── HTML builder ──────────────────────────────────────────────────────────────
def build_html(data):
    projects     = data['projects']
    mo_onm_qhse  = data.get('_meta', {}).get('monthly_onm_qhse', {})

    # Auto-detect latest date in data (don't trust datetime.now() — data may lag)
    all_dates = [p['dt'] for p in projects if p.get('dt')]
    latest_str = max(all_dates)
    latest     = datetime.strptime(latest_str, '%Y-%m-%d')
    prev_str   = (latest - timedelta(days=1)).strftime('%Y-%m-%d')

    ms = latest.strftime('%Y-%m-01')
    mo_key = latest.strftime('%Y-%m')

    pm_last  = latest.replace(day=1) - timedelta(days=1)
    pm_day   = min(latest.day, calendar.monthrange(pm_last.year, pm_last.month)[1])
    pm_start = pm_last.replace(day=1).strftime('%Y-%m-01')
    pm_end   = f"{pm_last.year}-{pm_last.month:02d}-{pm_day:02d}"
    pm_key   = pm_last.strftime('%Y-%m')

    # ── Filtered sets ──────────────────────────────────────────────────────────
    mtd_ps  = filter_projects(projects, ms,      latest_str)
    pm_ps   = filter_projects(projects, pm_start, pm_end)
    lat_ps  = filter_projects(projects, latest_str, latest_str)
    prev_ps = filter_projects(projects, prev_str,   prev_str)

    mtd = inject_meta_onm(calc(mtd_ps),  mo_onm_qhse, mo_key)
    pm  = inject_meta_onm(calc(pm_ps),   mo_onm_qhse, pm_key)
    lat = calc(lat_ps)
    prv = calc(prev_ps)

    curr_lbl = latest.strftime('%b %Y')
    prev_lbl = pm_last.strftime('%b %Y')
    lat_lbl  = latest.strftime('%d %b')
    prv_lbl  = (latest - timedelta(days=1)).strftime('%d %b')

    # ── Section 1 — MTD KPIs ──────────────────────────────────────────────────
    s1 = f"""<div class="kpi-grid">
  {kpi_card('Orders Installed (MTD)', f'{mtd["n"]:,}',
    f'{pct_chg(mtd["n"],pm["n"])} vs {pm["n"]:,} in {prev_lbl} (1–{pm_day})')}
  {kpi_card('kW Installed (MTD)', f'{mtd["kw"]:,.1f}',
    f'{pct_chg(mtd["kw"],pm["kw"])} vs {pm["kw"]:,.1f} kW last month')}
  {kpi_card('Avg Order Size', f'{mtd["aos"]:.2f} kW',
    f'vs {pm["aos"]:.2f} kW {prev_lbl} &nbsp;{pp(mtd["aos"]-pm["aos"],decimals=2)}')}
  {kpi_card('Avg Order Value', fc(mtd["aov"]),
    f'vs {fc(pm["aov"])} {prev_lbl}')}
  {kpi_card('Revenue (MTD)', fc(mtd["rev"]),
    f'vs {fc(pm["rev"])} {prev_lbl} &nbsp;{pct_chg(mtd["rev"],pm["rev"])}')}
  {kpi_card('Rev / Wp', f'&#8377;{mtd["rev_wp"]:.2f}',
    f'vs &#8377;{pm["rev_wp"]:.2f} {prev_lbl} &nbsp;{pp(mtd["rev_wp"]-pm["rev_wp"],decimals=2)}')}
  {kpi_card('GM Inst % (MTD)', f'{mtd["gm"]:.2f}%',
    f'{pp(mtd["gm"]-pm["gm"])} vs {pm["gm"]:.2f}% {prev_lbl}', gm_col(mtd["gm"]))}
  {kpi_card('Adjusted GM % (MTD)', f'{mtd["adj_gm"]:.2f}%',
    f'{pp(mtd["adj_gm"]-pm["adj_gm"])} vs {pm["adj_gm"]:.2f}% {prev_lbl}',
    gm_col(mtd["adj_gm"]))}
</div>"""

    # ── Section 2 — Latest Day Snapshot ───────────────────────────────────────
    def snap(lbl, val, sub='', vc='#1A2C4E'):
        return (f'<div class="snap"><div class="snap-lbl">{lbl}</div>'
                f'<div class="snap-val" style="color:{vc}">{val}</div>'
                f'<div class="snap-sub">{sub}</div></div>')

    s2 = f"""<div style="font-size:10px;color:#5A7A96;margin-bottom:10px;font-family:monospace">
  Data updated through <b>{latest.strftime('%d %b %Y')}</b>
  &nbsp;&#8226;&nbsp; Showing {lat_lbl} vs {prv_lbl}
</div>
<div class="snap-grid">
  {snap(f'Orders — {lat_lbl}', str(lat["n"]),
    f'Prev day ({prv_lbl}): {prv["n"]} &nbsp; MTD total: {mtd["n"]:,}')}
  {snap(f'kW — {lat_lbl}', f'{lat["kw"]:.1f}',
    f'Prev day: {prv["kw"]:.1f} kW &nbsp; MTD: {mtd["kw"]:,.0f} kW')}
  {snap(f'Rev/Wp — {lat_lbl}', f'&#8377;{lat["rev_wp"]:.2f}',
    f'Prev day: &#8377;{prv["rev_wp"]:.2f} &nbsp; {pp(lat["rev_wp"]-prv["rev_wp"],decimals=2)}')}
  {snap(f'AoS — {lat_lbl}', f'{lat["aos"]:.2f} kW',
    f'Prev day: {prv["aos"]:.2f} kW &nbsp; {pp(lat["aos"]-prv["aos"],decimals=2)}')}
  {snap(f'GM % — {lat_lbl}', f'{lat["gm"]:.1f}%',
    f'Prev day: {prv["gm"]:.1f}% &nbsp; {pp(lat["gm"]-prv["gm"])}', gm_col(lat["gm"]))}
  {snap(f'Adj GM % — {lat_lbl}', f'{lat["adj_gm"]:.1f}%',
    f'Prev day: {prv["adj_gm"]:.1f}% &nbsp; {pp(lat["adj_gm"]-prv["adj_gm"])}',
    gm_col(lat["adj_gm"]))}
</div>"""

    # ── Section 3 — Cluster Trend vs Own History ───────────────────────────────
    mtd_by_cl = group_by_cluster(mtd_ps)
    pm_by_cl  = group_by_cluster(pm_ps)
    all_keys  = set(mtd_by_cl.keys())

    clusters = []
    for key in all_keys:
        curr = calc(mtd_by_cl.get(key, []))
        prev = calc(pm_by_cl.get(key, []))
        if curr['n'] < 3:
            continue
        state, cluster = key
        gm_d   = curr['gm'] - prev['gm']
        abs_d  = curr['abs_gm'] - prev['abs_gm']
        abs_dp = (abs_d / prev['abs_gm'] * 100) if prev['abs_gm'] else 0
        driver_label, driver_detail = driver_analysis(curr, prev, gm_d)
        clusters.append(dict(
            state=state, cluster=cluster,
            curr=curr, prev=prev,
            gm_d=gm_d, abs_d=abs_d, abs_dp=abs_dp,
            driver_label=driver_label, driver_detail=driver_detail
        ))

    # Sort: biggest declines first (alerts on top), then stable, then gains
    clusters.sort(key=lambda x: x['gm_d'])

    # Split into buckets
    declining  = [c for c in clusters if c['gm_d'] < -0.3]
    stable_cl  = [c for c in clusters if -0.3 <= c['gm_d'] <= 0.3]
    improving  = [c for c in clusters if c['gm_d'] > 0.3]
    improving.sort(key=lambda x: -x['gm_d'])

    def render_cluster_card(c):
        curr = c['curr']
        prev = c['prev']
        gm_d = c['gm_d']
        css_cls = ('up' if gm_d > 0.3 else ('down' if gm_d < -0.3 else 'stable'))
        state_disp = STATE_DISPLAY.get(c['state'], c['state'])

        abs_gm_str = f"{fc(curr['abs_gm'])} vs {fc(prev['abs_gm'])}"
        abs_chg    = pct_chg(curr['abs_gm'], prev['abs_gm'])

        dd = c['driver_detail']
        metric_pills = ''
        if dd:
            def pill(lbl, val, good):
                clr = '#16A34A' if good else '#DC2626'
                return (f'<span style="display:inline-block;background:#fff;border:1px solid #E2EAF4;'
                        f'border-radius:5px;padding:2px 7px;margin-right:5px;font-size:9.5px;">'
                        f'{lbl}: <b style="color:{clr}">{val}</b></span>')
            if 'rev_wp_d' in dd and abs(dd['rev_wp_d']) >= 0.5:
                g = dd['rev_wp_d'] > 0
                metric_pills += pill('Rev/Wp', f'{"+" if g else ""}&#8377;{dd["rev_wp_d"]:.1f}', g)
            if 'aos_d' in dd and abs(dd['aos_d']) >= 0.1:
                g = dd['aos_d'] > 0
                metric_pills += pill('AoS', f'{"+" if g else ""}{dd["aos_d"]:.2f}kW', g)
            if 'cogs_kw_d' in dd and abs(dd['cogs_kw_d']) >= 500:
                g = dd['cogs_kw_d'] < 0
                metric_pills += pill('COGS/kW', f'{"+" if dd["cogs_kw_d"]>0 else ""}&#8377;{dd["cogs_kw_d"]/1000:.1f}K', g)

        return f"""<div class="cluster-card {css_cls}">
  <div class="cc-header">
    <div>
      <span class="cc-name">{c['cluster']}</span>
      <span class="cc-state">{state_disp}</span>
    </div>
    <div style="display:flex;align-items:center;gap:8px">
      {trend_badge(gm_d)}
      <span style="font-size:11px;font-weight:700;color:{gm_col(curr['gm'])}">{curr['gm']:.1f}%</span>
      <span style="font-size:10px;color:#94A3B8">GM</span>
    </div>
  </div>
  <div class="cc-metrics">
    <div class="cc-m">
      <span class="cc-ml">Orders</span>
      <span class="cc-mv">{curr['n']} <span style="color:#94A3B8;font-size:9px">vs {prev['n']}</span></span>
    </div>
    <div class="cc-m">
      <span class="cc-ml">kW</span>
      <span class="cc-mv">{curr['kw']:,.0f} <span style="color:#94A3B8;font-size:9px">vs {prev['kw']:,.0f}</span></span>
    </div>
    <div class="cc-m">
      <span class="cc-ml">GM% vs {prev_lbl}</span>
      <span class="cc-mv">{curr['gm']:.1f}% &rarr; {prev['gm']:.1f}%
        &nbsp;{pp(gm_d)}</span>
    </div>
    <div class="cc-m">
      <span class="cc-ml">Abs GM &mdash; MoM</span>
      <span class="cc-mv">{abs_gm_str} &nbsp;{abs_chg}</span>
    </div>
    <div class="cc-m">
      <span class="cc-ml">Rev/Wp</span>
      <span class="cc-mv">&#8377;{curr['rev_wp']:.2f} <span style="color:#94A3B8;font-size:9px">vs &#8377;{prev['rev_wp']:.2f}</span></span>
    </div>
    <div class="cc-m">
      <span class="cc-ml">AoS (kW)</span>
      <span class="cc-mv">{curr['aos']:.2f} <span style="color:#94A3B8;font-size:9px">vs {prev['aos']:.2f}</span></span>
    </div>
  </div>
  {f'<div style="margin-bottom:5px">{metric_pills}</div>' if metric_pills else ''}
  <div class="cc-driver">&#128270; <b>Driver:</b> {c['driver_label']}</div>
</div>"""

    def section_group(title, items, color):
        if not items: return ''
        cards = ''.join(render_cluster_card(c) for c in items)
        return (f'<div style="font-size:10px;font-weight:700;color:{color};'
                f'text-transform:uppercase;letter-spacing:.8px;margin:10px 0 6px">'
                f'{title}</div>'
                f'<div class="cluster-grid">{cards}</div>')

    s3 = (section_group(f'↓ Declining vs {prev_lbl} — Needs Attention', declining, '#DC2626') +
          section_group(f'→ Stable Clusters (within ±0.3pp)', stable_cl, '#64748B') +
          section_group(f'↑ Improving vs {prev_lbl}', improving, '#16A34A'))

    if not s3:
        s3 = '<p style="color:#94A3B8;font-size:11px">Insufficient data for cluster comparison.</p>'

    # ── Section 4 — Actionable Insights ───────────────────────────────────────
    insights = []

    # Big movers
    if improving:
        top3 = improving[:3]
        names = ', '.join(f'<b>{c["cluster"]}</b> (+{c["gm_d"]:.1f}pp)' for c in top3)
        drivers = '; '.join(f'{c["cluster"]}: {c["driver_label"].split(";")[0]}' for c in top3)
        insights.append(('&#128200; <b>Top GM gainers:</b> ' + names +
                         f'<br><span style="color:#5A7A96">{drivers}</span>'))

    if declining:
        bot3 = declining[:3]
        names = ', '.join(f'<b>{c["cluster"]}</b> ({c["gm_d"]:.1f}pp)' for c in bot3)
        drivers = '; '.join(f'{c["cluster"]}: {c["driver_label"].split(";")[0]}' for c in bot3)
        insights.append(('&#128201; <b>Clusters losing margin:</b> ' + names +
                         f'<br><span style="color:#5A7A96">{drivers}</span>'))

    # Pricing vs size analysis
    pricing_up   = [c for c in clusters if c['driver_detail'].get('top_type') == 'pricing_up']
    pricing_down = [c for c in clusters if c['driver_detail'].get('top_type') == 'pricing_down']
    size_up      = [c for c in clusters if c['driver_detail'].get('top_type') == 'size_up']
    size_down    = [c for c in clusters if c['driver_detail'].get('top_type') == 'size_down']

    if pricing_up:
        insights.append(f'&#128176; <b>Pricing improvement clusters</b> (Rev/Wp led): '
                        + ', '.join(f'<b>{c["cluster"]}</b>' for c in pricing_up))
    if size_up:
        insights.append(f'&#128295; <b>System-size-led gainers</b> (AoS ↑): '
                        + ', '.join(f'<b>{c["cluster"]}</b>' for c in size_up))
    if pricing_down:
        names = ', '.join(f'<b>{c["cluster"]}</b> (Rev/Wp ↓&#8377;{abs(c["driver_detail"]["rev_wp_d"]):.1f})' for c in pricing_down)
        insights.append(f'&#9888;&#65039; <b>Pricing pressure detected:</b> {names} — check if discount policy changed')
    if size_down:
        names = ', '.join(f'<b>{c["cluster"]}</b> (AoS ↓{abs(c["driver_detail"]["aos_d"]):.2f}kW)' for c in size_down)
        insights.append(f'&#128201; <b>Shrinking system sizes:</b> {names} — review sales team upsell strategy')

    # Run-rate
    if latest.day > 1:
        pace      = mtd['n'] / latest.day
        proj      = round(pace * 30)
        insights.append(f'&#128202; <b>Month run-rate:</b> {pace:.1f} installs/day &rarr; '
                        f'<b>~{proj:,} orders projected</b> for full month '
                        f'(vs {pm["n"]:,} actual in {prev_lbl})')

    # Abs GM standout
    if clusters:
        abs_gm_sorted = sorted(clusters, key=lambda x: x['abs_dp'], reverse=True)
        best = abs_gm_sorted[0]
        worst = abs_gm_sorted[-1]
        insights.append(
            f'&#128181; <b>Abs GM leaders:</b> '
            f'Biggest gain — <b>{best["cluster"]}</b> ({fc(best["curr"]["abs_gm"])} vs {fc(best["prev"]["abs_gm"])}, {pct_chg(best["curr"]["abs_gm"],best["prev"]["abs_gm"])}); '
            f'Biggest drop — <b>{worst["cluster"]}</b> ({fc(worst["curr"]["abs_gm"])} vs {fc(worst["prev"]["abs_gm"])}, {pct_chg(worst["curr"]["abs_gm"],worst["prev"]["abs_gm"])})'
        )

    s4 = ''.join(f'<div class="insight-block">{i}</div>' for i in insights) \
         or '<div class="insight-block">&#9989; All clusters within expected range.</div>'

    # ── Section 5 — COGS Breakdown ─────────────────────────────────────────────
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
    pm_cogs = {'Module':pm['mod'],'Inverter':pm['inv'],'MMS':pm['mms'],
               'Cables':pm['cab'],'Metering':pm['mtr'],'I&C':pm['ic'],'Other':pm['oth']}

    bars = ''
    cogs_rows = ''
    for label, val in cogs_items:
        pct_of_cogs = val / cogs_total * 100 if cogs_total else 0
        if pct_of_cogs < 0.3:
            continue
        color = COGS_COLORS.get(label, '#94A3B8')
        bars += (f'<div class="cb" style="width:{pct_of_cogs:.1f}%;background:{color}"'
                 f' title="{label}: {pct_of_cogs:.1f}%">'
                 f'{label if pct_of_cogs > 5 else ""}</div>')

        pm_val      = pm_cogs.get(label, 0)
        pm_pct      = pm_val / pm['cogs'] * 100 if pm['cogs'] else 0
        rev_pct     = val / mtd['rev'] * 100 if mtd['rev'] else 0
        delta_pp_v  = pct_of_cogs - pm_pct

        cogs_rows += (
            f'<tr>'
            f'<td><span style="display:inline-block;width:10px;height:10px;background:{color};'
            f'border-radius:2px;margin-right:6px;vertical-align:middle"></span>{label}</td>'
            f'<td class="r">{fc(val)}</td>'
            f'<td class="r">{pct_of_cogs:.1f}%</td>'
            f'<td class="r">{rev_pct:.1f}%</td>'
            f'<td class="r">{pp(delta_pp_v, higher_better=False)} vs {pm_pct:.1f}% {prev_lbl}</td>'
            f'</tr>'
        )

    s5 = f"""<div class="cbar">{bars}</div>
<table class="cogs">
  <thead><tr>
    <th>Category</th><th style="text-align:right">Amount MTD</th>
    <th style="text-align:right">% of COGS</th>
    <th style="text-align:right">% of Revenue</th>
    <th style="text-align:right">MoM shift</th>
  </tr></thead>
  <tbody>{cogs_rows}</tbody>
</table>"""

    # ── Assemble ──────────────────────────────────────────────────────────────
    now_str = datetime.now().strftime('%d %b %Y, %I:%M %p')
    html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{CSS}</style></head>
<body><div class="wrap">

<div class="hdr">
  <div class="hdr-title">&#9728;&#65039; Solar Square &mdash; Daily GM Report</div>
  <div class="hdr-sub">
    HOTO Month: {latest.strftime('%b-%y')}
    &nbsp;&#8226;&nbsp; Data through {latest.strftime('%d %B %Y')}
    &nbsp;&#8226;&nbsp; Generated {now_str}
  </div>
</div>

<div class="sec">
  <div class="sec-title">&#128202; MTD at a Glance &mdash; {curr_lbl} vs {prev_lbl} (same {pm_day} days)</div>
  {s1}
</div>

<div class="sec">
  <div class="sec-title">&#128197; Latest Day Snapshot &mdash; {lat_lbl} vs {prv_lbl}</div>
  {s2}
</div>

<div class="sec">
  <div class="sec-title">&#128270; Cluster Trend &mdash; Each Cluster vs Its Own {prev_lbl} MTD</div>
  {s3}
</div>

<div class="sec">
  <div class="sec-title">&#9889; Actionable Insights</div>
  {s4}
</div>

<div class="sec">
  <div class="sec-title">&#129521; COGS Breakdown &mdash; MTD {curr_lbl}</div>
  {s5}
</div>

<div class="ftr">
  Solar Square B2C GM Dashboard &nbsp;&#8226;&nbsp;
  Auto-generated from <code>projects.json.gz</code> &nbsp;&#8226;&nbsp;
  Data: DN dump &rarr; GitHub Actions &rarr; {latest.strftime('%d %b %Y')}
</div>

</div></body></html>"""
    return html, mtd, latest

# ── Send ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    data = load_data()
    html, mtd, latest = build_html(data)

    subject = (f"Solar Square GM | {latest.strftime('%d %b %Y')} | "
               f"MTD {mtd['n']:,} installs | GM {mtd['gm']:.2f}% | Adj {mtd['adj_gm']:.2f}%")

    if not GMAIL_PASS:
        preview = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'report_preview.html')
        with open(preview, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"ℹ  No GMAIL_PASSWORD — preview saved: {preview}", flush=True)
        print(f"   Subject: {subject}", flush=True)
        sys.exit(0)

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = SENDER
    msg['To']      = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html, 'html', 'utf-8'))

    print(f"Sending to {RECIPIENTS}...", flush=True)
    with smtplib.SMTP('smtp.gmail.com', 587) as s:
        s.ehlo(); s.starttls()
        s.login(SENDER, GMAIL_PASS)
        s.sendmail(SENDER, RECIPIENTS, msg.as_string())
    print(f"✅ Sent — {subject}", flush=True)
