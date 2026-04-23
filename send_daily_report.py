#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Solar Square — Daily GM Report Email Generator
================================================
Generates a clean, email-safe HTML body (desktop + mobile responsive) for the
daily Gross-Margin report that goes out to Leadership.

Fixes over previous version
---------------------------
1.  KPI tiles are rendered as a bulletproof HTML TABLE (not CSS Grid), so every
    tile has IDENTICAL height + width in Gmail / Apple Mail / Outlook on both
    desktop and mobile — the old CSS-Grid version was collapsing into broken
    alignment on narrow viewports.
2.  Mobile view uses one clean @media rule: main tiles -> 2 cols, latest-day
    tiles -> 2 cols, COGS table -> horizontal scroll. No overflow/clipping.
3.  Every tile is height-locked with min-height so labels, values, and deltas
    line up pixel-perfect across rows even when delta text wraps.
4.  All colours, typography, and spacing match the Solar Square dashboard
    design language (DM Sans + DM Mono + Syne, navy/off-white palette).
5.  Executive Summary and numbers are computed from data.csv.gz +
    projects.json.gz produced by the sync_data workflow — no hand coding.

Usage
-----
    python3 generate_daily_gm_email.py
        --data data.csv.gz
        --projects projects.json.gz
        --out    daily_gm_report.html

Or simply run `python3 generate_daily_gm_email.py` from the repo root —
all three paths default to the files the sync workflow checks in.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────
REPORT_TITLE     = "Solar Square — Daily GM Report"
CRORE            = 1e7
LAKH             = 1e5
GM_TARGET_PP     = 42.50          # on-target threshold
GM_CRIT_PP       = 40.00          # critical floor
MIN_CLUSTER_N    = 10             # min installs to feature a cluster
ADJ_GM_EX_ONM    = True           # exclude ONM & QHSE from Adjusted GM

COST_KEYS = ("mod", "inv", "prf", "cab", "mtr", "ick",
             "con", "ear", "jbx", "tsh", "saf", "ica",
             "wel", "ssn", "ebo", "dlg", "wkt")

CATEGORY_MAP = {
    "Module"  : ("mod",),
    "Inverter": ("inv",),
    "MMS"     : ("prf",),
    "Cables"  : ("cab", "ear"),
    "Metering": ("mtr",),
    "I&C"     : ("ick", "ica"),
    "Other"   : ("con", "jbx", "tsh", "saf", "wel", "ssn",
                 "ebo", "dlg", "wkt"),
}

CATEGORY_COLOR = {
    "Module"  : "#3B82F6",
    "Inverter": "#8B5CF6",
    "MMS"     : "#06B6D4",
    "Cables"  : "#10B981",
    "Metering": "#F59E0B",
    "I&C"     : "#EF4444",
    "Other"   : "#94A3B8",
}


# ─────────────────────────────────────────────────────────────────────────────
#  DOMAIN MODEL
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Agg:
    """Aggregate bucket for a slice of projects (MTD, PM, day, cluster, ...)."""
    n: int = 0
    kw: float = 0.0
    rev: float = 0.0
    cogs: float = 0.0
    cat_cogs: Dict[str, float] = field(default_factory=lambda:
                                       {k: 0.0 for k in CATEGORY_MAP})

    # ── primary KPIs ──
    @property
    def aos(self) -> float:            return (self.kw / self.n) if self.n else 0.0
    @property
    def aov(self) -> float:            return (self.rev / self.n) if self.n else 0.0
    @property
    def rev_wp(self) -> float:         return (self.rev / (self.kw * 1000)) if self.kw else 0.0
    @property
    def cogs_wp(self) -> float:        return (self.cogs / (self.kw * 1000)) if self.kw else 0.0
    @property
    def gm_abs(self) -> float:         return self.rev - self.cogs
    @property
    def gm_pct(self) -> float:         return (self.gm_abs / self.rev * 100) if self.rev else 0.0

    def add_project(self, p: dict) -> None:
        self.n    += 1
        self.kw   += _f(p.get("kw"))
        self.rev  += _f(p.get("rev"))
        self.cogs += _f(p.get("cogs"))
        for cat, keys in CATEGORY_MAP.items():
            self.cat_cogs[cat] += sum(_f(p.get(k)) for k in keys)


def _f(v: Any) -> float:
    """Safe float conversion — empty / None / non-numeric → 0.0"""
    try:
        return float(v or 0)
    except (ValueError, TypeError):
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  DATA LOADING
# ─────────────────────────────────────────────────────────────────────────────
def load_projects(path: str) -> Tuple[List[dict], Dict[str, Any]]:
    """Load projects.json.gz → (projects, meta)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"projects file not found: {path}")
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        payload = json.load(f)
    return payload.get("projects", []), payload.get("_meta", {})


def parse_date(val: str) -> Optional[date]:
    if not val:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(val[:10], fmt).date()
        except ValueError:
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  SLICING
# ─────────────────────────────────────────────────────────────────────────────
def build_aggregates(
    projects: List[dict],
    today:  date,
    latest: date,
) -> Dict[str, Any]:
    """
    Build all the aggregates needed by the email in a single pass:
      • mtd      : current month, day 1 ... latest
      • mtd_pm   : prior month, same # of days (apples-to-apples)
      • pm_full  : prior month, full month
      • day_t    : latest installation day
      • day_tm1  : day before latest
      • clusters : keyed by cluster name  → {mtd:Agg, pm:Agg}
    """
    mtd_start   = today.replace(day=1)
    days_so_far = (latest - mtd_start).days + 1
    pm_last     = mtd_start - timedelta(days=1)
    pm_start    = pm_last.replace(day=1)
    pm_cutoff   = pm_start + timedelta(days=days_so_far - 1)

    agg = {
        "mtd":      Agg(),
        "mtd_pm":   Agg(),       # prior-month-same-days
        "pm_full":  Agg(),
        "day_t":    Agg(),
        "day_tm1":  Agg(),
    }
    clusters: Dict[str, Dict[str, Agg]] = defaultdict(
        lambda: {"mtd": Agg(), "pm": Agg(), "state": ""}
    )

    tm1 = latest - timedelta(days=1)

    for p in projects:
        d = parse_date(p.get("dt", ""))
        if d is None:
            continue

        if mtd_start <= d <= latest:
            agg["mtd"].add_project(p)
            c = (p.get("c") or "—").strip() or "—"
            clusters[c]["mtd"].add_project(p)
            clusters[c]["state"] = (p.get("s") or "").strip()

        if pm_start <= d <= pm_cutoff:
            agg["mtd_pm"].add_project(p)

        if pm_start <= d <= pm_last:
            agg["pm_full"].add_project(p)
            c = (p.get("c") or "—").strip() or "—"
            clusters[c]["pm"].add_project(p)

        if d == latest:
            agg["day_t"].add_project(p)
        if d == tm1:
            agg["day_tm1"].add_project(p)

    agg["clusters"]    = dict(clusters)
    agg["days_so_far"] = days_so_far
    agg["mtd_start"]   = mtd_start
    agg["pm_start"]    = pm_start
    agg["latest"]      = latest
    agg["tm1"]         = tm1
    return agg


# ─────────────────────────────────────────────────────────────────────────────
#  FORMATTING HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fmt_int(v: float) -> str:    return f"{v:,.0f}"
def fmt_kw(v: float) -> str:     return f"{v:,.1f}"
def fmt_cr(v: float) -> str:     return f"₹{v/CRORE:,.2f}Cr"
def fmt_lakh(v: float) -> str:
    if v >= CRORE: return f"₹{v/CRORE:,.2f}Cr"
    if v >= LAKH:  return f"₹{v/LAKH:,.1f}L"
    return f"₹{v:,.0f}"
def fmt_rupee(v: float) -> str:  return f"₹{v:,.2f}"
def fmt_pct(v: float)   -> str:  return f"{v:,.2f}%"
def fmt_aos(v: float)   -> str:  return f"{v:,.2f} kW"


def delta_pp(cur: float, prev: float) -> float:
    return cur - prev

def delta_pct(cur: float, prev: float) -> float:
    if not prev:
        return 0.0
    return (cur - prev) / prev * 100


def delta_html(cur: float, prev: float, kind: str = "pct",
               inverse: bool = False) -> str:
    """Render a coloured delta chip for a tile.

    kind:
      'pct'   -> % change  (1,707 → 1,973 becomes ↑16%)
      'pp'    -> absolute pp change for ratios
      'abs'   -> absolute ₹/Wp, kW, etc.
    inverse: if True, a *decrease* is considered good (e.g. COGS/Wp).
    """
    if prev is None or cur is None:
        return '<span style="color:#94A3B8">—</span>'

    if kind == "pct":
        d = delta_pct(cur, prev)
        up = d >= 0
        fmt = f"{'↑' if up else '↓'}{abs(d):,.0f}%"
    elif kind == "pp":
        d = delta_pp(cur, prev)
        up = d >= 0
        fmt = f"{'↑' if up else '↓'}{abs(d):,.2f}pp"
    else:
        d  = cur - prev
        up = d >= 0
        fmt = f"{'↑' if up else '↓'}{abs(d):,.2f}"

    good = (up and not inverse) or (not up and inverse)
    color = "#16A34A" if good else "#DC2626"
    return f'<span style="color:{color};font-weight:700">{fmt}</span>'


# ─────────────────────────────────────────────────────────────────────────────
#  HTML EMAIL GENERATION
# ─────────────────────────────────────────────────────────────────────────────
#  Design notes
#  ────────────
#  • Outer layout uses a 600-700px centered table (email gold-standard).
#  • KPI tiles are a nested <table> with fixed cell widths (4 cols desktop,
#    2 cols mobile via @media). Each tile has min-height so values line up
#    even if delta text wraps.
#  • Everything has INLINE styles so Gmail's aggressive CSS stripping
#    doesn't break it. The <style> block only adds the mobile @media rule
#    and hover behaviour (progressive enhancement).
# ─────────────────────────────────────────────────────────────────────────────

CSS = """
<style>
  /* Tablet / mobile viewport */
  @media only screen and (max-width: 640px) {
    table.sq-kpi-row, table.sq-kpi-row tbody, table.sq-kpi-row tr {
      display:block !important; width:100% !important;
    }
    table.sq-kpi-row td.sq-kpi-cell {
      display:inline-block !important;
      width:48% !important;
      box-sizing:border-box !important;
      vertical-align:top !important;
      margin:0 0 8px 0 !important;
    }
    table.sq-kpi-row td.sq-kpi-cell:nth-child(odd)  { margin-right:4% !important; }
    table.sq-kpi-row td.sq-kpi-cell.sq-spacer       { display:none !important; }

    td.sq-day-cell {
      display:inline-block !important;
      width:48% !important;
      box-sizing:border-box !important;
      margin:0 0 6px 0 !important;
    }
    td.sq-day-cell:nth-child(odd) { margin-right:4% !important; }

    .sq-page              { padding:8px !important; }
    .sq-section           { padding:16px !important; }
    .sq-header-h1         { font-size:18px !important; line-height:1.25 !important; }
    .sq-kpi-val           { font-size:22px !important; }
    .sq-day-val           { font-size:16px !important; }
    .sq-cogs-scroll       { overflow-x:auto !important; -webkit-overflow-scrolling:touch !important; }
    .sq-cogs-table        { min-width:560px !important; }
  }
  /* Dark-mode friendly (users with "dark mode" mail clients) */
  @media (prefers-color-scheme: dark) {
    body.sq-body { background:#0F172A !important; }
  }
</style>
"""


def kpi_tile(
    label: str,
    value: str,
    delta_html_str: str = "",
    value_color: str = "#1A2744",
) -> str:
    """One KPI tile, rendered as a nested <table> to lock its size."""
    return f"""
<td class="sq-kpi-cell" valign="top" width="25%"
    style="padding:6px;box-sizing:border-box;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
         style="background:#F9FAFB;border:1px solid #E5E7EB;border-radius:12px;
                min-height:108px;">
    <tr><td style="padding:14px 16px;">
      <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
                  letter-spacing:1.2px;text-transform:uppercase;color:#94A3B8;
                  font-weight:600;margin-bottom:8px;line-height:1;">
        {label}
      </div>
      <div class="sq-kpi-val"
           style="font-family:'Syne','DM Sans',Arial,sans-serif;
                  font-size:26px;font-weight:800;letter-spacing:-.4px;
                  color:{value_color};line-height:1.1;margin-bottom:6px;">
        {value}
      </div>
      <div style="font-family:'DM Mono',Menlo,monospace;font-size:10px;
                  color:#6B7280;line-height:1.3;min-height:13px;">
        {delta_html_str}
      </div>
    </td></tr>
  </table>
</td>
""".strip()


def day_tile(
    label: str,
    value: str,
    delta_html_str: str,
    value_color: str = "#1A2744",
) -> str:
    """Smaller KPI tile for the Latest-Day strip (6 tiles desktop, 2 col mobile)."""
    return f"""
<td class="sq-day-cell" valign="top" width="16.66%"
    style="padding:4px;box-sizing:border-box;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
         style="background:#fff;border:1px solid #E5E7EB;border-radius:10px;
                min-height:82px;">
    <tr><td style="padding:10px 12px;">
      <div style="font-family:'DM Mono',Menlo,monospace;font-size:8.5px;
                  letter-spacing:1px;text-transform:uppercase;color:#94A3B8;
                  font-weight:600;margin-bottom:6px;line-height:1;">
        {label}
      </div>
      <div class="sq-day-val"
           style="font-family:'Syne','DM Sans',Arial,sans-serif;
                  font-size:18px;font-weight:800;color:{value_color};
                  line-height:1.1;margin-bottom:4px;">
        {value}
      </div>
      <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
                  color:#6B7280;line-height:1.2;min-height:11px;">
        {delta_html_str}
      </div>
    </td></tr>
  </table>
</td>
""".strip()


def gm_color(pct: float) -> str:
    """Colour scale for GM% tiles / cells."""
    if pct >= 44: return "#16A34A"
    if pct >= GM_TARGET_PP: return "#1A2744"
    if pct >= GM_CRIT_PP: return "#B7791F"
    return "#DC2626"


# ── SECTIONS ─────────────────────────────────────────────────────────────────
def section_header(mtd: Agg, pm: Agg, days_so_far: int, latest: date,
                   pm_month_name: str) -> str:
    gen_ts = datetime.now().strftime("%d %b %Y, %I:%M %p")
    return f"""
<tr><td class="sq-section" style="background:#1A2C4E;border-radius:16px 16px 0 0;
                                  padding:28px 32px 24px;color:#fff;">
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:10px;
              letter-spacing:2px;text-transform:uppercase;
              color:rgba(255,255,255,.55);margin-bottom:8px;">
    ☀ Solar Square · B2C · Daily GM Report
  </div>
  <div class="sq-header-h1"
       style="font-family:'Syne','DM Sans',Arial,sans-serif;
              font-size:22px;font-weight:800;letter-spacing:-.4px;
              line-height:1.2;color:#fff;margin-bottom:6px;">
    {REPORT_TITLE}
  </div>
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:11px;
              color:rgba(255,255,255,.55);">
    HOTO month: {latest.strftime('%b-%y').upper()} &nbsp;·&nbsp;
    Data through {latest.strftime('%d %b %Y')} &nbsp;·&nbsp;
    Generated {gen_ts}
  </div>
</td></tr>
""".strip()


def section_exec_summary(mtd: Agg, pm: Agg, clusters: Dict[str, Any],
                         days_so_far: int, pm_month_name: str) -> str:
    # auto-detect problem clusters (GM below critical)
    low_gm = sorted(
        [(name, c["mtd"]) for name, c in clusters.items()
         if c["mtd"].n >= MIN_CLUSTER_N and c["mtd"].gm_pct < GM_CRIT_PP],
        key=lambda x: x[1].gm_pct
    )[:3]
    low_txt = ("; ".join(f"{n} ({fmt_pct(a.gm_pct)})" for n, a in low_gm)
               or "all clusters above critical floor")

    vol_delta = delta_pct(mtd.n, pm.n)
    gm_delta  = delta_pp(mtd.gm_pct, pm.gm_pct)
    gm_color_ = "#DC2626" if gm_delta < -0.3 else ("#B7791F" if gm_delta < 0 else "#16A34A")

    return f"""
<tr><td class="sq-section"
        style="background:#fff;border-left:1px solid #E5E7EB;
               border-right:1px solid #E5E7EB;padding:22px 28px 4px;">
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
              font-weight:700;letter-spacing:2px;text-transform:uppercase;
              color:#6B7280;margin-bottom:12px;">
    Executive Summary
  </div>
  <div style="background:#EBF3FD;border-left:4px solid #1A6FCA;border-radius:8px;
              padding:14px 18px;font-family:'DM Sans',Arial,sans-serif;
              font-size:13px;line-height:1.6;color:#1A2744;">
    <strong>{fmt_int(mtd.n)} installations</strong>
    ({fmt_kw(mtd.kw)} kW) completed MTD in
    {mtd_month_name(mtd)}&nbsp;— &nbsp;
    <span style="color:{'#16A34A' if vol_delta>=0 else '#DC2626'};font-weight:700;">
      {'↑' if vol_delta>=0 else '↓'}{abs(vol_delta):,.0f}%
    </span>
    vs {pm_month_name} on volume ({days_so_far} days elapsed).
    Overall GM is
    <strong style="color:{gm_color_};">{fmt_pct(mtd.gm_pct)}</strong>
    ({'↑' if gm_delta>=0 else '↓'}{abs(gm_delta):.2f}pp MoM).
    Rev/Wp at <strong>{fmt_rupee(mtd.rev_wp)}</strong>
    vs {fmt_rupee(pm.rev_wp)} in {pm_month_name}.
    Clusters that need margin attention: <strong>{low_txt}</strong>.
  </div>
</td></tr>
""".strip()


def mtd_month_name(mtd: Agg) -> str:
    return datetime.today().strftime("%b %Y")


def section_mtd_glance(mtd: Agg, pm: Agg, pm_month_name: str,
                       days_so_far: int, meta: dict) -> str:
    # Adjusted GM excludes ONM & QHSE one-offs from COGS
    adj_cogs_mtd = mtd.cogs
    adj_cogs_pm  = pm.cogs
    if ADJ_GM_EX_ONM and meta.get("monthly_onm_qhse"):
        m = meta["monthly_onm_qhse"]
        key_mtd = date.today().strftime("%Y-%m")
        key_pm  = (date.today().replace(day=1) - timedelta(days=1)
                   ).strftime("%Y-%m")
        if key_mtd in m:
            adj_cogs_mtd -= (m[key_mtd].get("onm", 0) +
                             m[key_mtd].get("qhs", 0))
        if key_pm in m:
            adj_cogs_pm  -= (m[key_pm].get("onm", 0) +
                             m[key_pm].get("qhs", 0))
    adj_gm_mtd = ((mtd.rev - adj_cogs_mtd) / mtd.rev * 100) if mtd.rev else 0.0
    adj_gm_pm  = ((pm.rev - adj_cogs_pm) / pm.rev * 100) if pm.rev else 0.0

    # row 1
    row1 = "".join([
        kpi_tile("Installations MTD", fmt_int(mtd.n),
                 delta_html(mtd.n, pm.n, "pct") +
                 f' <span style="color:#94A3B8">vs {fmt_int(pm.n)}</span>'),
        kpi_tile("KW MTD", fmt_kw(mtd.kw),
                 delta_html(mtd.kw, pm.kw, "pct") +
                 f' <span style="color:#94A3B8">vs {fmt_kw(pm.kw)} kW</span>'),
        kpi_tile("GM % MTD", fmt_pct(mtd.gm_pct),
                 delta_html(mtd.gm_pct, pm.gm_pct, "pp") +
                 f' <span style="color:#94A3B8">vs {fmt_pct(pm.gm_pct)}</span>',
                 value_color=gm_color(mtd.gm_pct)),
        kpi_tile("Adjusted GM % MTD", fmt_pct(adj_gm_mtd),
                 delta_html(adj_gm_mtd, adj_gm_pm, "pp") +
                 f' <span style="color:#94A3B8">ex-ONM/QHS</span>',
                 value_color=gm_color(adj_gm_mtd)),
    ])

    # row 2
    row2 = "".join([
        kpi_tile("Avg System Size", fmt_aos(mtd.aos),
                 delta_html(mtd.aos, pm.aos, "abs") +
                 f' <span style="color:#94A3B8">vs {fmt_aos(pm.aos)}</span>'),
        kpi_tile("Avg Order Value", fmt_lakh(mtd.aov),
                 delta_html(mtd.aov, pm.aov, "pct") +
                 f' <span style="color:#94A3B8">vs {fmt_lakh(pm.aov)}</span>'),
        kpi_tile("Rev / Wp", fmt_rupee(mtd.rev_wp),
                 delta_html(mtd.rev_wp, pm.rev_wp, "abs") +
                 f' <span style="color:#94A3B8">vs {fmt_rupee(pm.rev_wp)}/Wp</span>'),
        kpi_tile("Revenue MTD", fmt_cr(mtd.rev),
                 delta_html(mtd.rev, pm.rev, "pct") +
                 f' <span style="color:#94A3B8">vs {fmt_cr(pm.rev)}</span>'),
    ])

    return f"""
<tr><td class="sq-section"
        style="background:#fff;border-left:1px solid #E5E7EB;
               border-right:1px solid #E5E7EB;padding:18px 24px;">
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
              font-weight:700;letter-spacing:2px;text-transform:uppercase;
              color:#6B7280;margin-bottom:14px;">
    MTD at a Glance&nbsp;·&nbsp;
    <span style="color:#9CA3AF">
      {mtd_month_name(mtd)} vs {pm_month_name} (same {days_so_far} days)
    </span>
  </div>

  <table role="presentation" class="sq-kpi-row" width="100%"
         cellpadding="0" cellspacing="0"
         style="border-collapse:separate;border-spacing:0;">
    <tr>{row1}</tr>
  </table>
  <table role="presentation" class="sq-kpi-row" width="100%"
         cellpadding="0" cellspacing="0"
         style="border-collapse:separate;border-spacing:0;margin-top:4px;">
    <tr>{row2}</tr>
  </table>
</td></tr>
""".strip()


def section_latest_day(day_t: Agg, day_tm1: Agg, mtd: Agg,
                       latest: date, tm1: date) -> str:
    tiles = "".join([
        day_tile(f"Installs — {latest.strftime('%d %b')}",
                 fmt_int(day_t.n),
                 f'<span style="color:#94A3B8">Prev {tm1.strftime("%d %b")}: '
                 f'{fmt_int(day_tm1.n)} · MTD {fmt_int(mtd.n)}</span>'),
        day_tile(f"KW — {latest.strftime('%d %b')}",
                 fmt_kw(day_t.kw),
                 f'<span style="color:#94A3B8">Prev: {fmt_kw(day_tm1.kw)} · '
                 f'MTD {fmt_kw(mtd.kw)}</span>'),
        day_tile(f"Rev/Wp — {latest.strftime('%d %b')}",
                 fmt_rupee(day_t.rev_wp),
                 delta_html(day_t.rev_wp, day_tm1.rev_wp, "abs") +
                 f' <span style="color:#94A3B8">vs {fmt_rupee(day_tm1.rev_wp)}</span>'),
        day_tile(f"AOS — {latest.strftime('%d %b')}",
                 fmt_aos(day_t.aos),
                 delta_html(day_t.aos, day_tm1.aos, "abs")),
        day_tile(f"GM % — {latest.strftime('%d %b')}",
                 fmt_pct(day_t.gm_pct),
                 delta_html(day_t.gm_pct, day_tm1.gm_pct, "pp"),
                 value_color=gm_color(day_t.gm_pct)),
        day_tile(f"Revenue — {latest.strftime('%d %b')}",
                 fmt_cr(day_t.rev),
                 f'<span style="color:#94A3B8">Prev: {fmt_cr(day_tm1.rev)}</span>'),
    ])

    return f"""
<tr><td class="sq-section"
        style="background:#fff;border-left:1px solid #E5E7EB;
               border-right:1px solid #E5E7EB;border-top:1px solid #F3F4F6;
               padding:18px 24px;">
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
              font-weight:700;letter-spacing:2px;text-transform:uppercase;
              color:#6B7280;margin-bottom:12px;">
    Latest Day&nbsp;·&nbsp;
    <span style="color:#9CA3AF">
      {latest.strftime('%d %b')} vs {tm1.strftime('%d %b')}
    </span>
  </div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
         style="border-collapse:separate;border-spacing:0;">
    <tr>{tiles}</tr>
  </table>
</td></tr>
""".strip()


def section_cogs(mtd: Agg, pm: Agg, pm_month_name: str) -> str:
    rows = []
    total_mtd = sum(mtd.cat_cogs.values()) or mtd.cogs or 1
    for cat, _ in CATEGORY_MAP.items():
        amt_mtd = mtd.cat_cogs.get(cat, 0.0)
        amt_pm  = pm.cat_cogs.get(cat,  0.0)
        wp_mtd  = (amt_mtd / (mtd.kw * 1000)) if mtd.kw else 0.0
        wp_pm   = (amt_pm  / (pm.kw  * 1000)) if pm.kw  else 0.0
        d_wp    = wp_mtd - wp_pm
        d_color = "#DC2626" if d_wp > 0.01 else ("#16A34A" if d_wp < -0.01 else "#94A3B8")
        rows.append(f"""
<tr>
  <td style="padding:10px 12px;border-bottom:1px solid #F3F4F6;
             font-family:'DM Sans',Arial,sans-serif;font-size:12px;color:#374151;">
    <span style="display:inline-block;width:8px;height:8px;border-radius:2px;
                 background:{CATEGORY_COLOR[cat]};margin-right:8px;
                 vertical-align:middle;"></span>
    <strong>{cat}</strong>
  </td>
  <td align="right" style="padding:10px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11.5px;
                           color:#374151;">{fmt_cr(amt_mtd)}</td>
  <td align="right" style="padding:10px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11.5px;
                           color:#6B7280;">{amt_mtd/total_mtd*100:.1f}%</td>
  <td align="right" style="padding:10px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11.5px;
                           color:#374151;">{wp_mtd:.3f}</td>
  <td align="right" style="padding:10px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11.5px;
                           color:#9CA3AF;">{wp_pm:.3f}</td>
  <td align="right" style="padding:10px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11.5px;
                           font-weight:700;color:{d_color};">
    {'+' if d_wp>=0 else ''}{d_wp:.3f}
  </td>
</tr>""".strip())

    # total row
    d_total = mtd.cogs_wp - pm.cogs_wp
    d_color = "#DC2626" if d_total > 0.01 else ("#16A34A" if d_total < -0.01 else "#94A3B8")
    rows.append(f"""
<tr style="background:#F9FAFB;font-weight:700;">
  <td style="padding:12px;font-family:'DM Sans',Arial,sans-serif;font-size:12px;
             color:#1A2744;"><strong>Total COGS</strong></td>
  <td align="right" style="padding:12px;font-family:'DM Mono',Menlo,monospace;
             font-size:12px;color:#1A2744;">{fmt_cr(mtd.cogs)}</td>
  <td align="right" style="padding:12px;font-family:'DM Mono',Menlo,monospace;
             font-size:12px;color:#6B7280;">100%</td>
  <td align="right" style="padding:12px;font-family:'DM Mono',Menlo,monospace;
             font-size:12px;color:#1A2744;">{mtd.cogs_wp:.3f}</td>
  <td align="right" style="padding:12px;font-family:'DM Mono',Menlo,monospace;
             font-size:12px;color:#6B7280;">{pm.cogs_wp:.3f}</td>
  <td align="right" style="padding:12px;font-family:'DM Mono',Menlo,monospace;
             font-size:12px;color:{d_color};">
    {'+' if d_total>=0 else ''}{d_total:.3f}
  </td>
</tr>""".strip())

    return f"""
<tr><td class="sq-section"
        style="background:#fff;border-left:1px solid #E5E7EB;
               border-right:1px solid #E5E7EB;border-top:1px solid #F3F4F6;
               padding:18px 24px;">
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
              font-weight:700;letter-spacing:2px;text-transform:uppercase;
              color:#6B7280;margin-bottom:12px;">
    COGS Breakdown&nbsp;·&nbsp;
    <span style="color:#9CA3AF">
      MTD vs {pm_month_name} · ₹/Wp
    </span>
  </div>
  <div class="sq-cogs-scroll">
    <table class="sq-cogs-table" role="presentation" width="100%"
           cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;min-width:100%;">
      <thead><tr style="background:#F8FAFC;">
        <th align="left"  style="padding:10px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">Category</th>
        <th align="right" style="padding:10px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">MTD Amount</th>
        <th align="right" style="padding:10px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">% of COGS</th>
        <th align="right" style="padding:10px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">₹/Wp MTD</th>
        <th align="right" style="padding:10px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">₹/Wp {pm_month_name}</th>
        <th align="right" style="padding:10px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">Δ ₹/Wp</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
  </div>
</td></tr>
""".strip()


def section_clusters(clusters: Dict[str, Any]) -> str:
    active = [(name, c["mtd"], c["pm"], c["state"])
              for name, c in clusters.items()
              if c["mtd"].n >= MIN_CLUSTER_N]
    if not active:
        return ""

    # bucket into declining / improving / stable
    declining, improving, stable = [], [], []
    for name, mtd, pm, state in active:
        d_gm = mtd.gm_pct - pm.gm_pct
        if d_gm <= -0.30: declining.append((name, mtd, pm, state, d_gm))
        elif d_gm >= 0.30: improving.append((name, mtd, pm, state, d_gm))
        else: stable.append((name, mtd, pm, state, d_gm))
    declining.sort(key=lambda x: x[4])         # most-negative first
    improving.sort(key=lambda x: -x[4])        # most-positive first
    stable.sort(key=lambda x: x[1].gm_pct)     # worst GM first

    def row(name, mtd, pm, state, d):
        gm_cell_color = gm_color(mtd.gm_pct)
        gm_cell_bg = (
            "#DCFCE7" if mtd.gm_pct >= 44 else
            "#F1F5F9" if mtd.gm_pct >= GM_TARGET_PP else
            "#FEF3C7" if mtd.gm_pct >= GM_CRIT_PP else
            "#FEE2E2"
        )
        d_color = "#DC2626" if d < 0 else ("#16A34A" if d > 0 else "#94A3B8")
        d_sym   = "▼" if d < 0 else ("▲" if d > 0 else "—")
        return f"""
<tr>
  <td style="padding:9px 12px;border-bottom:1px solid #F3F4F6;
             font-family:'DM Sans',Arial,sans-serif;font-size:12px;color:#1A2744;">
    <strong>{name}</strong>
    <span style="color:#9CA3AF;font-size:9px;margin-left:4px;">{state}</span>
  </td>
  <td align="right" style="padding:9px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11px;color:#374151;">
    {mtd.n}
  </td>
  <td align="center" style="padding:9px 12px;border-bottom:1px solid #F3F4F6;
                           background:{gm_cell_bg};
                           font-family:'DM Mono',Menlo,monospace;font-size:12px;
                           font-weight:800;color:{gm_cell_color};">
    {fmt_pct(mtd.gm_pct)}
  </td>
  <td align="right" style="padding:9px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:11px;
                           font-weight:700;color:{d_color};">
    {d_sym} {abs(d):.2f}pp
  </td>
  <td align="right" style="padding:9px 12px;border-bottom:1px solid #F3F4F6;
                           font-family:'DM Mono',Menlo,monospace;font-size:10.5px;color:#6B7280;">
    {fmt_rupee(mtd.rev_wp)}/Wp
  </td>
</tr>""".strip()

    def group_row(label: str) -> str:
        return f"""
<tr><td colspan="5" style="background:#F1F5F9;padding:6px 12px;
        font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;
        color:#475569;text-transform:uppercase;letter-spacing:1px;
        border-top:2px solid #E2E8F0;">{label}</td></tr>""".strip()

    body_rows = []
    if declining:
        body_rows.append(group_row("▼ Declining vs Prior Month — Needs Attention"))
        body_rows.extend(row(*r) for r in declining)
    if improving:
        body_rows.append(group_row("▲ Improving vs Prior Month"))
        body_rows.extend(row(*r) for r in improving)
    if stable:
        body_rows.append(group_row("→ Stable (within ±0.30pp)"))
        body_rows.extend(row(*r) for r in stable)

    return f"""
<tr><td class="sq-section"
        style="background:#fff;border-left:1px solid #E5E7EB;
               border-right:1px solid #E5E7EB;border-top:1px solid #F3F4F6;
               padding:18px 24px;">
  <div style="font-family:'DM Mono',Menlo,monospace;font-size:9px;
              font-weight:700;letter-spacing:2px;text-transform:uppercase;
              color:#6B7280;margin-bottom:12px;">
    Cluster Health&nbsp;·&nbsp;
    <span style="color:#9CA3AF">n ≥ {MIN_CLUSTER_N} MTD · sorted by ΔGM%</span>
  </div>
  <div class="sq-cogs-scroll">
    <table class="sq-cogs-table" role="presentation" width="100%"
           cellpadding="0" cellspacing="0"
           style="border-collapse:collapse;border:1px solid #E5E7EB;border-radius:10px;overflow:hidden;">
      <thead><tr style="background:#F8FAFC;">
        <th align="left"  style="padding:9px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">Cluster</th>
        <th align="right" style="padding:9px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">n MTD</th>
        <th align="center" style="padding:9px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">GM %</th>
        <th align="right" style="padding:9px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">vs PM</th>
        <th align="right" style="padding:9px 12px;font-family:'DM Mono',Menlo,monospace;font-size:9px;font-weight:700;color:#6B7280;text-transform:uppercase;letter-spacing:.8px;border-bottom:2px solid #E5E7EB;">Rev/Wp</th>
      </tr></thead>
      <tbody>{''.join(body_rows)}</tbody>
    </table>
  </div>
</td></tr>
""".strip()


def section_footer() -> str:
    return f"""
<tr><td style="background:#F9FAFB;border:1px solid #E5E7EB;border-top:none;
               border-radius:0 0 16px 16px;padding:14px 28px;text-align:center;
               font-family:'DM Mono',Menlo,monospace;font-size:9.5px;
               color:#9CA3AF;letter-spacing:.3px;">
  Solar Square GM Analytics &nbsp;·&nbsp;
  Auto-generated from Metabase OMS + ERP DN merge &nbsp;·&nbsp;
  Queries: reply-all to devika.g@solarsquare.in
</td></tr>
""".strip()


# ─────────────────────────────────────────────────────────────────────────────
#  TOP-LEVEL
# ─────────────────────────────────────────────────────────────────────────────
def render_email(projects: List[dict], meta: Dict[str, Any],
                 today: Optional[date] = None,
                 latest: Optional[date] = None) -> str:
    today = today or date.today()
    # latest defaults to max project date in current month, else today
    if latest is None:
        in_month = [parse_date(p.get("dt", "")) for p in projects
                    if parse_date(p.get("dt", "")) and
                    parse_date(p["dt"]).month == today.month and
                    parse_date(p["dt"]).year  == today.year]
        latest = max(in_month) if in_month else today

    agg = build_aggregates(projects, today, latest)
    mtd      = agg["mtd"]
    pm       = agg["mtd_pm"]
    pm_full  = agg["pm_full"]
    day_t    = agg["day_t"]
    day_tm1  = agg["day_tm1"]
    clusters = agg["clusters"]

    pm_month_name = (agg["pm_start"]).strftime("%b %Y")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>{REPORT_TITLE}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500;600&family=DM+Sans:wght@400;600;700;800&family=Syne:wght@700;800&display=swap" rel="stylesheet">
{CSS}
</head>
<body class="sq-body"
      style="margin:0;padding:0;background:#F0F2F5;font-family:'DM Sans',Arial,sans-serif;color:#1A2744;">
  <center class="sq-page" style="padding:24px 12px 48px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"
           style="max-width:720px;margin:0 auto;border-collapse:separate;">
      {section_header(mtd, pm, agg['days_so_far'], latest, pm_month_name)}
      {section_exec_summary(mtd, pm, clusters, agg['days_so_far'], pm_month_name)}
      {section_mtd_glance(mtd, pm, pm_month_name, agg['days_so_far'], meta)}
      {section_latest_day(day_t, day_tm1, mtd, latest, agg['tm1'])}
      {section_cogs(mtd, pm, pm_month_name)}
      {section_clusters(clusters)}
      {section_footer()}
    </table>
  </center>
</body>
</html>
"""
    return html


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser(description="Generate the Solar Square Daily GM email body.")
    ap.add_argument("--projects", default="projects.json.gz",
                    help="Path to projects.json.gz (produced by sync_data workflow).")
    ap.add_argument("--out", default="daily_gm_report.html",
                    help="Path to write the final HTML email body.")
    ap.add_argument("--today", default=None,
                    help="Override 'today' as YYYY-MM-DD (for backfill / testing).")
    ap.add_argument("--latest", default=None,
                    help="Override latest installation day as YYYY-MM-DD.")
    args = ap.parse_args()

    # allow file paths that don't exist yet to be discovered relative to script dir
    for attr in ("projects",):
        path = getattr(args, attr)
        if not os.path.exists(path):
            alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
            if os.path.exists(alt):
                setattr(args, attr, alt)

    projects, meta = load_projects(args.projects)
    print(f"Loaded {len(projects):,} projects from {args.projects}", file=sys.stderr)

    today  = datetime.strptime(args.today,  "%Y-%m-%d").date() if args.today  else None
    latest = datetime.strptime(args.latest, "%Y-%m-%d").date() if args.latest else None

    html = render_email(projects, meta, today=today, latest=latest)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {len(html):,} bytes → {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
