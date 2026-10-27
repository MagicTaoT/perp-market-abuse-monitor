"""Local, read-only research dashboard for normalized squeeze observations.

The dashboard deliberately depends only on the normalized store. It has no
exchange client, credential handling, trading route, or service control.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .discovery import discovery_payload
from .models import NormalizedSnapshot, utc_datetime
from .storage import NormalizedSqliteStore


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8770
DEFAULT_DATA_MAX_AGE_SECONDS = 180
CHART_METRICS = (
    "perp_last",
    "spot_last",
    "mark_price",
    "index_price",
    "open_interest",
    "funding_rate",
    "premium_bps",
)
_ASSET_RE = re.compile(r"[A-Z0-9][A-Z0-9_-]{0,23}\Z")
_VENUE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_EXPECTED_UNITS = {
    "perp_last": "USD",
    "spot_last": "USD",
    "mark_price": "USD",
    "index_price": "USD",
    "open_interest": "tokens",
    "funding_rate": "decimal",
    "premium_bps": "bps",
}


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return utc_datetime(value).isoformat().replace("+00:00", "Z")


def _event_time(snapshot: NormalizedSnapshot) -> datetime:
    """Place a point at source time, falling back explicitly to receipt time."""
    return snapshot.provenance.source_timestamp or snapshot.provenance.received_at


def _chartable(snapshot: NormalizedSnapshot) -> bool:
    """Reject points whose units cannot be interpreted by the fixed chart axes."""
    if snapshot.unit != _EXPECTED_UNITS.get(snapshot.metric):
        return False
    if snapshot.metric == "open_interest" and (
        snapshot.related_value is None or snapshot.related_unit != "USD"
    ):
        return False
    return not any(
        "unit" in flag.lower() and any(
            word in flag.lower() for word in ("unknown", "unverified", "invalid", "unsupported", "mismatch")
        )
        for flag in snapshot.quality_flags
    )


def snapshot_json(snapshot: NormalizedSnapshot, *, now: datetime) -> dict[str, Any]:
    provenance = snapshot.provenance
    event_time = _event_time(snapshot)
    return {
        "asset": snapshot.asset,
        "metric": snapshot.metric,
        # Decimal strings preserve the ingested value exactly. The chart converts
        # these only at the final display step.
        "value": str(snapshot.value),
        "unit": snapshot.unit,
        "related_value": str(snapshot.related_value) if snapshot.related_value is not None else None,
        "related_unit": snapshot.related_unit,
        "as_of": _iso(event_time),
        "age_seconds": round((now - event_time).total_seconds(), 3),
        "source": provenance.source,
        "venue": provenance.venue,
        "instrument": provenance.instrument,
        "source_timestamp": _iso(provenance.source_timestamp),
        "received_at": _iso(provenance.received_at),
        "available_at": _iso(snapshot.as_of),
        "receipt_delay_seconds": (
            round((provenance.received_at - provenance.source_timestamp).total_seconds(), 3)
            if provenance.source_timestamp is not None else None
        ),
        "quality_flags": list(snapshot.quality_flags),
        "quality_status": snapshot.quality_status,
        "chartable": _chartable(snapshot),
    }


def _single(query: dict[str, list[str]], key: str, *, default: str | None = None) -> str | None:
    values = query.get(key)
    if values is None:
        return default
    if len(values) != 1:
        raise ValueError(f"{key} must occur once")
    return values[0]


def _asset(query: dict[str, list[str]]) -> str:
    asset = (_single(query, "asset", default="IRYS") or "").upper()
    if not _ASSET_RE.fullmatch(asset):
        raise ValueError("asset must be 1-24 ASCII letters, numbers, '_' or '-'")
    return asset


def _venue(query: dict[str, list[str]]) -> str | None:
    venue = _single(query, "venue")
    if venue is not None and not _VENUE_RE.fullmatch(venue):
        raise ValueError("venue has invalid characters")
    return venue


def _time(query: dict[str, list[str]], key: str) -> datetime | None:
    raw = _single(query, key)
    if raw is None:
        return None
    if not raw or len(raw) > 64:
        raise ValueError(f"{key} must be a timezone-aware ISO 8601 timestamp")
    try:
        return utc_datetime(datetime.fromisoformat(raw.replace("Z", "+00:00")), key)
    except ValueError as exc:
        raise ValueError(f"{key} must be a timezone-aware ISO 8601 timestamp") from exc


def _limit(query: dict[str, list[str]]) -> int:
    raw = _single(query, "limit", default="500")
    try:
        limit = int(raw or "")
    except ValueError as exc:
        raise ValueError("limit must be an integer from 1 to 2000") from exc
    if not 1 <= limit <= 2000:
        raise ValueError("limit must be an integer from 1 to 2000")
    return limit


def _latest_received_at(store: NormalizedSqliteStore, now: datetime) -> datetime | None:
    """Read market-wide ingestion freshness; a late old event still counts."""
    uri = Path(store.path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=2)) as connection:
        row = connection.execute(
            # The as_of range includes nearly every historical row. Force the
            # receipt index so LIMIT 1 can skip future-source rows in order.
            "SELECT received_at FROM normalized_snapshots "
            "INDEXED BY idx_normalized_received_at WHERE as_of <= ? "
            "ORDER BY received_at DESC LIMIT 1",
            (utc_datetime(now).isoformat(timespec="microseconds"),),
        ).fetchone()
    return datetime.fromisoformat(row[0]) if row and row[0] else None


def _discovery_limit(query: dict[str, list[str]]) -> int:
    raw = _single(query, "limit", default="50")
    try:
        limit = int(raw or "")
    except ValueError as exc:
        raise ValueError("limit must be an integer from 1 to 200") from exc
    if not 1 <= limit <= 200:
        raise ValueError("limit must be an integer from 1 to 200")
    return limit


HTML = r"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Perp Market Abuse Monitor · 标的发现</title>
<style>
:root{color-scheme:dark;font-family:ui-sans-serif,system-ui,-apple-system,sans-serif;background:#0c1423;color:#e3eaf4}
*{box-sizing:border-box}body{margin:0 auto;max-width:1480px;padding:24px}h1{margin:0 0 5px;font-size:25px;letter-spacing:-.02em}h2{font-size:17px;margin:0 0 12px}
.sub,.muted{color:#99a9bc}.sub{margin:0 0 22px}.controls{display:flex;gap:12px;align-items:end;flex-wrap:wrap;margin:22px 0 8px}
label{font-size:13px;color:#b9c8d9;display:grid;gap:5px}input,select,button{font:inherit;color:#e3eaf4;background:#15243a;border:1px solid #35506c;border-radius:7px;padding:8px 10px}
input{width:150px}button{cursor:pointer;background:#1e5674;border-color:#3988a9}button:hover{background:#286b8c}button:focus-visible,select:focus-visible,input:focus-visible,summary:focus-visible,tr[role=button]:focus-visible{outline:2px solid #83d8e7;outline-offset:2px}
.status{min-height:22px;color:#b9c8d9;margin:8px 0}.warning{color:#f7bb6c}.error{color:#ff8c8c}
.panel{background:#111e30;border:1px solid #2c435c;border-radius:11px;padding:20px;margin:14px 0}
.panel-head{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}.panel-head h2{margin:0}
.coverage-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:16px 0}.stat{background:#17283d;border:1px solid #2b4561;border-radius:9px;padding:10px 13px}.stat-label{display:block;color:#97abc0;font-size:12px}.stat strong{display:block;margin-top:3px;font-size:21px;font-variant-numeric:tabular-nums}
.chart-wrap{position:relative;overflow-x:auto}canvas{display:block;width:100%;min-width:700px}
#tooltip{position:fixed;display:none;pointer-events:none;white-space:pre-line;background:#20324a;border:1px solid #5b7895;box-shadow:0 10px 28px #07101dcc;border-radius:8px;padding:9px 11px;font-size:12px;line-height:1.45;max-width:min(330px,calc(100vw - 20px));z-index:20}
.legend{display:flex;flex-wrap:wrap;gap:7px 14px;color:#b9c8d9;font-size:12px;margin:12px 0}.swatch{display:inline-block;width:19px;border-top:3px solid;vertical-align:middle;margin-right:5px}.legend-venue{border-left:1px solid #47617b;padding-left:12px}
.table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;font-size:13px}th{text-align:left;color:#a8bad0}th,td{padding:9px;border-bottom:1px solid #2b4058;vertical-align:top}td{white-space:nowrap}.flag{color:#f7bb6c}
.discovery-table{min-width:1050px;table-layout:fixed}.discovery-table th:nth-child(1){width:10%}.discovery-table th:nth-child(2),.discovery-table th:nth-child(3){width:13%}.discovery-table th:nth-child(4),.discovery-table th:nth-child(5){width:11%}.discovery-table th:nth-child(6){width:15%}.discovery-table th:nth-child(7){width:16%}.discovery-table th:nth-child(8){width:11%}
.discovery-table .signal-row{cursor:pointer}.discovery-table .signal-row:hover,.discovery-table .signal-row.selected{background:#1a3049}.signal-row td{border-bottom:0;padding-bottom:5px;font-variant-numeric:tabular-nums;overflow:hidden;text-overflow:ellipsis}.signal-row .asset-name{font-weight:700;font-size:15px}.signal-row .score{font-weight:700;color:#8addd4}.signal-row .positive{color:#8addd4}.signal-row .negative{color:#f7a689}.signal-row .candidate{color:#8addd4}.signal-row .watch{color:#f7bb6c}.signal-row .secondary{display:block;color:#91a9c0;font-size:11px;font-weight:400;margin-top:3px;overflow:hidden;text-overflow:ellipsis}.signal-row .risk-high{color:#f4a58e}.signal-row .risk-medium{color:#f7bb6c}.signal-row .risk-low{color:#8addd4}.signal-row .risk-unknown{color:#91a9c0}
.detail-row td{padding-top:0;padding-bottom:12px;white-space:normal}.detail-row details{color:#b7cadd}.detail-row summary{cursor:pointer;display:flex;gap:10px;align-items:center;max-width:100%;list-style:none}.detail-row summary::-webkit-details-marker{display:none}.detail-row summary::before{content:"▸";color:#69bfd0}.detail-row details[open] summary::before{content:"▾"}.detail-row .summary-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.detail-row .quality-count{color:#f7bb6c;flex:none}.detail-content{margin:12px 0 3px 18px;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.detail-group{background:#14263b;border:1px solid #29435d;border-radius:8px;padding:10px 12px;min-width:0}.detail-group h3{font-size:12px;color:#9cd3dc;margin:0 0 7px}.detail-line{font-size:12px;line-height:1.5;overflow-wrap:anywhere;margin:4px 0}.detail-line .key{color:#91a9c0;margin-right:6px}.detail-line .quality-tag{display:inline-block;border:1px solid #62523f;border-radius:5px;padding:2px 6px;color:#efbd7b;margin:2px 5px 2px 0}.detail-line a{color:#91d6e4}.detail-note{color:#8fa8c0;font-size:11px;margin:7px 0 0;line-height:1.45}.detail-group.wide{grid-column:1 / -1}
.empty{color:#f7bb6c}.mobile-hint{display:none}footer{color:#8297ae;font-size:12px;margin-top:20px}
@media(max-width:1100px){.discovery-table{min-width:900px}.discovery-table th:nth-child(8),.discovery-table .signal-row td:nth-child(8){display:none}}
@media(max-width:720px){body{padding:12px}h1{font-size:22px}.panel{padding:14px}.coverage-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.stat strong{font-size:18px}.controls{gap:8px}.controls label{flex:1 1 140px}.controls input,.controls select{width:100%}.chart-tools{width:100%}.mobile-hint{display:block;color:#8ca4bb;font-size:12px;margin:8px 0}.legend-venue{border:0;padding:0}
.discovery-table,.discovery-table tbody{display:block;min-width:0}.discovery-table thead{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0,0,0,0)}.discovery-table .signal-row{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;background:#17283d;border:1px solid #2b4561;border-bottom:0;border-radius:8px 8px 0 0;margin-top:10px;padding:10px 10px 3px}.signal-row td{display:block!important;border:0;padding:0;white-space:normal;line-height:1.25;font-size:12px}.signal-row td::before{display:block;color:#8da4bb;font-size:10px;font-weight:400;margin-bottom:3px}.signal-row td:nth-child(1){grid-column:1}.signal-row td:nth-child(2){grid-column:2}.signal-row td:nth-child(3){grid-column:3}.signal-row td:nth-child(4){grid-column:1}.signal-row td:nth-child(5){grid-column:2}.signal-row td:nth-child(6){grid-column:3}.signal-row td:nth-child(7){grid-column:1 / -1;color:#aabed1}.signal-row td:nth-child(8){display:none!important}.signal-row td:nth-child(1)::before{content:"标的"}.signal-row td:nth-child(2)::before{content:"早期建仓"}.signal-row td:nth-child(3)::before{content:"高位空头观察"}.signal-row td:nth-child(4)::before{content:"多头风险"}.signal-row td:nth-child(5)::before{content:"空头风险"}.signal-row td:nth-child(6)::before{content:"价格 / OI 1h"}.signal-row td:nth-child(7)::before{content:"场所"}.discovery-table .detail-row{display:block;background:#17283d;border:1px solid #2b4561;border-top:0;border-radius:0 0 8px 8px}.detail-row td{display:block;width:100%;border:0;padding:5px 10px 10px}.detail-row summary{font-size:11px}.detail-row .quality-count{font-size:10px}.detail-content{grid-template-columns:1fr;margin-left:0}.detail-group.wide{grid-column:1}}
</style></head><body>
<h1>Perp Market Abuse Monitor</h1><p class="sub">多标的机会观察 · 归一化市场数据 · 统一 UTC 时间轴 · 只读研究</p>
<section class="panel"><div class="panel-head"><h2>发现榜单 · 结构机会观察</h2><span id="discovery-status" class="muted" role="status"></span></div><p class="muted">分别观察早期建仓与高位空头机会；风险分数越高，持有该方向仓位的风险越高。点击标的查看时间轴，展开依据可核对覆盖率、数据来源和未知项。所有评分只表示研究优先级，不是操纵证据或交易指令。</p><div id="coverage" class="coverage-grid" aria-label="市场覆盖情况"></div><div class="table-wrap" tabindex="0" aria-label="结构机会榜单"><table class="discovery-table"><thead><tr><th>标的</th><th>早期建仓观察</th><th>高位空头观察</th><th>多头持仓风险</th><th>空头持仓风险</th><th>价格 / OI 1h</th><th>场所</th><th>更新 UTC</th></tr></thead><tbody id="candidates"></tbody></table></div></section>
<div class="controls">
<label>资产<input id="asset" maxlength="24" value="IRYS" spellcheck="false"></label>
<label>场所（可选）<input id="venue" maxlength="64" placeholder="全部" spellcheck="false"></label>
<label>回看窗口<select id="hours"><option value="auto" selected>自动适配</option><option value="6">6 小时</option><option value="24">24 小时</option><option value="168">7 天</option></select></label>
<button id="refresh" type="button">刷新</button></div>
<div id="status" class="status" role="status"></div>
<section class="panel"><div class="panel-head"><h2>市场时间轴</h2><label class="chart-tools">指标视图<select id="chart-view"><option value="focus" selected>价格 + OI 变化</option><option value="all">全部指标</option></select></label></div><div class="legend" id="legend"></div><p class="mobile-hint">左右滑动图表可查看完整时间轴</p><div class="chart-wrap"><canvas id="chart" aria-label="价格、持仓、资金费率和溢价的同步 UTC 时间轴" role="img"></canvas><div id="tooltip" role="status"></div></div></section>
<section class="panel"><h2>最新观测</h2><div id="missing" class="empty"></div><div class="table-wrap"><table><thead><tr><th>指标</th><th>场所</th><th>数值</th><th>来源 / 合约</th><th>来源时间 UTC</th><th>接收时间 UTC</th><th>接收延迟</th><th>距今</th><th>质量</th></tr></thead><tbody id="rows"></tbody></table></div></section>
<footer>USD 数值目前按 USDT 报价 1:1 名义估算，未换算 USDT/USD。原始数值、来源时间和质量标记见最新观测；缺失指标不会填补或推算。</footer>
<script>
"use strict";
const METRICS=["perp_last","spot_last","mark_price","index_price","open_interest","funding_rate","premium_bps"];
const LABELS={perp_last:"永续成交价",spot_last:"现货成交价",mark_price:"标记价",index_price:"指数价",open_interest:"未平仓量",funding_rate:"资金费率",premium_bps:"溢价"};
const COLORS={perp_last:"#56c8e8",spot_last:"#e6c16b",mark_price:"#82d4a2",index_price:"#aa9bef",open_interest:"#72b7ed",funding_rate:"#e8a06d",premium_bps:"#d690d7"};
const PANELS=[
  {id:"price",title:"价格 · USDT≈USD",metrics:["perp_last","spot_last","mark_price","index_price"],field:"value"},
  {id:"oi_change",title:"OI · 区间变化 %",metrics:["open_interest"],field:"value",relative:true},
  {id:"oi_tokens",title:"OI · tokens",metrics:["open_interest"],field:"value"},
  {id:"oi_usd",title:"OI · USDT≈USD",metrics:["open_interest"],field:"related_value"},
  {id:"funding",title:"Funding · % / interval",metrics:["funding_rate"],field:"value"},
  {id:"premium",title:"Premium · bps",metrics:["premium_bps"],field:"value"}
];
const VIEWS={focus:["price","oi_change"],all:["price","oi_tokens","oi_usd","funding","premium"]};
const CHART_TOP=18,PANEL_H=145,PANEL_GAP=10;
const canvas=document.getElementById("chart"), tooltip=document.getElementById("tooltip");
let allSeries=[], domain=[0,1], hoverTime=null, panelRanges=[];
const fmt=n=>Number.isFinite(n)?new Intl.NumberFormat("en-US",{maximumSignificantDigits:7}).format(n):"—";
const pct=v=>v===null||v===undefined||!Number.isFinite(Number(v))?"—":(Number(v)>0?"+":"")+Number(v).toFixed(2)+"%";
const decimal=v=>v===null||v===undefined||!Number.isFinite(Number(v))?"—":fmt(Number(v));
function compact(v){const n=Number(v);if(!Number.isFinite(n))return "—";const a=Math.abs(n);if(a>=1e9)return (n/1e9).toFixed(2)+"B";if(a>=1e6)return (n/1e6).toFixed(2)+"M";if(a>=1e3)return (n/1e3).toFixed(1)+"K";return fmt(n);}
function shortUtc(value,seconds=false){if(!value)return "—";const date=new Date(value);if(!Number.isFinite(date.getTime()))return String(value);return date.toISOString().slice(5,seconds?19:16).replace("T"," ")+" UTC";}
function unlockDate(value){if(!value)return "—";return /^\d{4}-\d{2}-\d{2}$/.test(String(value))?String(value)+"（仅有日期，时刻未知）":shortUtc(value,true);}
function fundingPct(value){if(value===null||value===undefined)return "—";const n=Number(value);return Number.isFinite(n)?(n>0?"+":"")+(n*100).toFixed(4)+"%":"—";}
function panelValue(panel,value){if(panel.id==="oi_change")return pct(value);if(panel.id==="oi_tokens")return compact(value)+" tokens";if(panel.id==="oi_usd")return compact(value)+" USDT";if(panel.id==="funding")return fundingPct(value);if(panel.id==="premium")return Number(value).toFixed(2)+" bps";return Math.abs(value)<1?Number(value).toFixed(5):Number(value).toFixed(3);}
function axisValue(panel,value){if(panel.id==="oi_tokens"||panel.id==="oi_usd")return compact(value);if(panel.id==="oi_change")return pct(value);if(panel.id==="funding")return fundingPct(value);if(panel.id==="premium")return Number(value).toFixed(2);return Math.abs(value)<1?Number(value).toFixed(5):Number(value).toFixed(3);}
function currentPanels(){const ids=VIEWS[document.getElementById("chart-view").value]||VIEWS.focus;return ids.map(id=>PANELS.find(panel=>panel.id===id));}
function venueDash(venue){return venue==="bybit"?[7,4]:venue==="kucoin"?[2,4]:[];}
function venueLineStyle(venue){return venue==="bybit"?"dashed":venue==="kucoin"?"dotted":"solid";}
function age(seconds){if(seconds===null)return "—";if(seconds<0)return "未来时间";if(seconds<60)return fmt(seconds)+" 秒";if(seconds<3600)return fmt(seconds/60)+" 分";return fmt(seconds/3600)+" 小时";}
function delay(seconds){return seconds===null?"—":seconds<0?"来源时间晚于接收 "+age(-seconds):age(seconds);}
function validPoint(row,field){const v=Number(row[field]);return row.chartable&&row[field]!==null&&Number.isFinite(v)?v:null;}
function tracesFor(panel){
  return allSeries.filter(s=>panel.metrics.includes(s.metric)).map(s=>{
    const raw=s.rows.map(row=>({row,t:Date.parse(row.as_of),raw:validPoint(row,panel.field)})).filter(point=>Number.isFinite(point.t)&&point.raw!==null);
    const baseline=raw.find(point=>point.raw>0)?.raw;
    const pts=raw.map(point=>({...point,value:panel.relative?(baseline?((point.raw/baseline)-1)*100:null):point.raw})).filter(point=>point.value!==null&&Number.isFinite(point.value));
    return {s,pts};
  });
}
function renderLegend(){
  const visible=new Set(currentPanels().flatMap(panel=>panel.metrics)),legend=document.getElementById("legend");legend.replaceChildren();
  for(const metric of METRICS){if(!visible.has(metric)||!allSeries.some(s=>s.metric===metric))continue;const span=document.createElement("span"),swatch=document.createElement("span");swatch.className="swatch";swatch.style.borderTopColor=COLORS[metric]||"#fff";span.appendChild(swatch);span.append(document.createTextNode(LABELS[metric]||metric));legend.appendChild(span);}
  for(const venue of [...new Set(allSeries.filter(s=>visible.has(s.metric)).map(s=>s.venue))]){const span=document.createElement("span"),swatch=document.createElement("span");span.className="legend-venue";swatch.className="swatch";swatch.style.borderTopColor="#a7bcd2";swatch.style.borderTopStyle=venueLineStyle(venue);span.appendChild(swatch);span.append(document.createTextNode(venue));legend.appendChild(span);}
}
function draw(){
  const panels=currentPanels(),h=CHART_TOP+panels.length*(PANEL_H+PANEL_GAP)+20;
  panelRanges=[];
  canvas.style.height=h+"px";
  const rect=canvas.getBoundingClientRect(),dpr=window.devicePixelRatio||1,w=rect.width;
  canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);
  const c=canvas.getContext("2d");c.scale(dpr,dpr);
  c.fillStyle="#111e30";c.fillRect(0,0,w,h);const left=98,right=w-18;
  const x=t=>left+(t-domain[0])/(domain[1]-domain[0])*(right-left);
  c.font="12px system-ui";c.textBaseline="middle";
  panels.forEach((panel,i)=>{
    const y0=CHART_TOP+i*(PANEL_H+PANEL_GAP),bottom=y0+PANEL_H-25;
    const traces=tracesFor(panel);
    const values=traces.flatMap(trace=>trace.pts.filter(point=>point.t>=domain[0]&&point.t<=domain[1]).map(point=>point.value));let lo=Math.min(...values),hi=Math.max(...values);
    if(!values.length){lo=0;hi=1;} if(lo===hi){const pad=Math.abs(lo)*0.02||1;lo-=pad;hi+=pad;}else{const pad=(hi-lo)*0.08;lo-=pad;hi+=pad;}
    const y=v=>bottom-(v-lo)/(hi-lo)*(PANEL_H-48);
    panelRanges[i]={lo,hi,bottom};
    c.fillStyle="#b8c8d8";c.fillText(panel.title,10,y0+10);c.fillStyle="#7790a9";
    c.fillText(axisValue(panel,hi),10,y0+32);c.fillText(axisValue(panel,lo),10,bottom);
    c.strokeStyle="#2a4058";c.lineWidth=1;c.beginPath();c.moveTo(left,y0+23);c.lineTo(right,y0+23);c.moveTo(left,bottom);c.lineTo(right,bottom);c.stroke();
    if(!values.length){c.fillStyle="#c89965";c.fillText("无可绘制数据",left+12,y0+69);}
    traces.forEach(({s,pts})=>{const visible=pts.filter(point=>point.t>=domain[0]&&point.t<=domain[1]);if(!visible.length)return;c.strokeStyle=COLORS[s.metric]||"#ffffff";c.fillStyle=c.strokeStyle;c.lineWidth=1.8;c.setLineDash(venueDash(s.venue));c.beginPath();visible.forEach((point,k)=>{if(k)c.lineTo(x(point.t),y(point.value));else c.moveTo(x(point.t),y(point.value));});c.stroke();c.setLineDash([]);if(visible.length<=3){for(const point of visible){c.beginPath();c.arc(x(point.t),y(point.value),3,0,Math.PI*2);c.fill();}}});
    if(hoverTime!==null){c.strokeStyle="#91a7c4";c.setLineDash([3,3]);c.beginPath();c.moveTo(x(hoverTime),y0+23);c.lineTo(x(hoverTime),bottom);c.stroke();c.setLineDash([]);}
  });
  c.fillStyle="#99a9bc";c.textAlign="left";c.fillText(new Date(domain[0]).toISOString().slice(0,16)+" UTC",left,h-8);c.textAlign="right";c.fillText(new Date(domain[1]).toISOString().slice(0,16)+" UTC",right,h-8);c.textAlign="left";
}
function showHover(event){
  const rect=canvas.getBoundingClientRect(),left=98,right=rect.width-18,x=event.clientX-rect.left,y=event.clientY-rect.top;
  const panels=currentPanels(),index=Math.floor((y-CHART_TOP)/(PANEL_H+PANEL_GAP)),panel=panels[index];
  const y0=CHART_TOP+index*(PANEL_H+PANEL_GAP);
  if(x<left||x>right||!panel||y<y0+23||y>y0+PANEL_H-25){hoverTime=null;tooltip.style.display="none";draw();return;}
  hoverTime=domain[0]+(x-left)/(right-left)*(domain[1]-domain[0]);draw();
  const lines=[panel.title+" · "+new Date(hoverTime).toISOString().slice(5,16).replace("T"," ")+" UTC"];
  const maxGap=Math.max(8*60000,Math.min(20*60000,(domain[1]-domain[0])*0.1));
  const matches=[];
  for(const {s,pts} of tracesFor(panel)){let nearest=null,dist=Infinity;for(const point of pts){const gap=Math.abs(point.t-hoverTime);if(gap<dist){nearest=point;dist=gap;}}
    if(nearest&&dist<=maxGap)matches.push({s,point:nearest});}
  if(matches.length){const range=panelRanges[index],project=value=>range.bottom-(value-range.lo)/(range.hi-range.lo)*(PANEL_H-48);
    matches.sort((a,b)=>Math.abs(project(a.point.value)-y)-Math.abs(project(b.point.value)-y));
    const metric=matches[0].s.metric;
    for(const {s,point} of matches.filter(item=>item.s.metric===metric).slice(0,4)){let value=panelValue(panel,point.value);if(panel.relative)value+=" ("+compact(point.raw)+" tokens)";lines.push((LABELS[s.metric]||s.metric)+" · "+s.venue+"  "+value+" · "+new Date(point.t).toISOString().slice(11,16));}
  }else lines.push("该时间附近无观测");
  tooltip.textContent=lines.join("\n");tooltip.style.display="block";
  const width=tooltip.offsetWidth,height=tooltip.offsetHeight;
  let tipX=event.clientX+14;if(tipX+width>window.innerWidth-8)tipX=event.clientX-width-14;
  tooltip.style.left=Math.max(8,tipX)+"px";
  tooltip.style.top=Math.max(8,Math.min(event.clientY+12,window.innerHeight-height-8))+"px";
}
function populateSnapshot(payload){
  const body=document.getElementById("rows");body.replaceChildren();
  for(const row of payload.observations){const tr=document.createElement("tr");const cells=[LABELS[row.metric]||row.metric,row.venue,row.value+" "+row.unit+(row.related_value!==null?" / "+row.related_value+" "+row.related_unit:""),row.source+" / "+row.instrument,row.source_timestamp||"缺失（以接收时间代替）",row.received_at,delay(row.receipt_delay_seconds),age(row.age_seconds),row.quality_flags.length?row.quality_flags.join(", "):row.quality_status];
    cells.forEach((value,i)=>{const td=document.createElement("td");td.textContent=value;if(i===8&&row.quality_flags.length)td.className="flag";tr.appendChild(td);});body.appendChild(tr);}
  if(!payload.observations.length){const tr=document.createElement("tr"),td=document.createElement("td");td.colSpan=9;td.textContent="没有观测数据";tr.appendChild(td);body.appendChild(tr);}
  document.getElementById("missing").textContent=payload.missing.length?"缺失指标："+payload.missing.map(m=>LABELS[m]||m).join("、"):"";
}
const hasNumber=value=>value!==null&&value!==undefined&&value!==""&&Number.isFinite(Number(value));
function signedPct(value,digits=2){if(!hasNumber(value))return "—";const n=Number(value);return (n>0?"+":"")+n.toFixed(digits)+"%";}
function scoreRange(value){
  if(Array.isArray(value)&&value.length===2&&value.every(hasNumber))return fmt(Number(value[0]))+"–"+fmt(Number(value[1]));
  if(value&&typeof value==="object"&&hasNumber(value.min)&&hasNumber(value.max))return fmt(Number(value.min))+"–"+fmt(Number(value.max));
  return typeof value==="string"&&value.trim()?value:"—";
}
function tierLabel(value){
  const labels={severe:"极高",very_high:"极高",high:"高",elevated:"偏高",medium:"中",moderate:"中",low:"低",very_low:"很低",strong:"强",watch:"观察",weak:"弱",unknown:"未知",insufficient_data:"数据不足",insufficient:"数据不足"};
  return labels[value]||value||"未知";
}
function coverageLabel(value){if(!hasNumber(value))return "覆盖未知";const n=Number(value);return "覆盖 "+Math.round(n<=1?n*100:n)+"%";}
function scoreDisplay(track){
  if(!track)return {main:"—",sub:"数据未覆盖",known:false};
  const range=scoreRange(track.score_range),known=hasNumber(track.score);
  const observed=hasNumber(track.observed_score)?fmt(Number(track.observed_score)):"";
  const main=known?fmt(Number(track.score))+" / 100":observed?"≥"+observed+" / 100":range!=="—"?range+" / 100":"—";
  return {main,sub:known?tierLabel(track.tier)+" · "+coverageLabel(track.coverage_pct):"未完成 · "+coverageLabel(track.coverage_pct)+(range!=="—"?" · 区间 "+range:""),known};
}
function addLine(group,key,value){const line=document.createElement("p"),label=document.createElement("span");line.className="detail-line";label.className="key";label.textContent=key;line.appendChild(label);line.appendChild(document.createTextNode(value===null||value===undefined||value===""?"—":String(value)));group.appendChild(line);return line;}
function addGroup(parent,title,wide=false){const group=document.createElement("section"),heading=document.createElement("h3");group.className="detail-group"+(wide?" wide":"");heading.textContent=title;group.appendChild(heading);parent.appendChild(group);return group;}
function sourceLabel(value){if(!value)return "未知";const key=String(value).toLowerCase();if(key.includes("coinmarketcap")||key==="cmc")return "CoinMarketCap";if(key.includes("pattern"))return "PatternPlus";return String(value);}
function contributionLines(group,track,label){
  if(!track)return;
  const parts=track.contributions;
  if(!parts||typeof parts!=="object")return;
  const names={price:"价格",oi:"OI 变化",oi_change:"OI 变化",oi_absolute:"OI 绝对值",oi_to_float:"OI / 流通市值",oi_to_fdv:"OI / FDV",funding:"资金费率",funding_shift:"资金费率变化",premium:"溢价",unlock:"代币解锁",venue_confirmation:"跨场所确认",elevated_price:"价格高位",positioning:"建仓趋势"};
  const entries=Array.isArray(parts)?parts.map((part,index)=>[part.key||part.name||String(index+1),part]):Object.entries(parts);
  if(!entries.length)return;
  addLine(group,label,"分项依据");
  for(const [key,part] of entries){
    const name=names[key]||String(key).replaceAll("_"," ");
    if(part&&typeof part==="object"){
      const points=hasNumber(part.score)?fmt(Number(part.score)):hasNumber(part.points)?fmt(Number(part.points)):"—";
      const maximum=hasNumber(part.max_points)?part.max_points:part.max_score;
      const why=part.reason||part.detail||part.note||"";
      const missing=Array.isArray(part.missing_inputs)&&part.missing_inputs.length?" · 缺 "+part.missing_inputs.join("、"):"";
      addLine(group,"· "+name,points+(hasNumber(maximum)?" / "+fmt(Number(maximum)):"")+(why?" · "+why:"")+(part.status&&!(["ok","known"].includes(part.status))?" · "+part.status:"")+missing);
    }else addLine(group,"· "+name,part);
  }
}
function populateDiscovery(payload){
  const body=document.getElementById("candidates");body.replaceChildren();
  const flagLabels={bulk_scan:"批量采集",bybit_oi_both_sides:"Bybit OI 双边口径",missing_source_timestamp:"缺少来源时间",response_time_only:"仅有响应时间",usdt_quoted_usd_proxy:"USDT≈USD 名义换算",oi_unit_inferred_contract_count:"OI 单位推断",oi_unit_inferred_crosschecked:"OI 单位经交叉核验",volume_unit_inferred:"成交量单位推断",single_venue_oi_confirmation:"仅单所 OI 确认",turnover_unverified_or_missing:"成交额待核验"};
  for(const candidate of payload.candidates||[]){
    const tracks=candidate.structural_scores||{},oi=candidate.oi_structure||{},token=candidate.tokenomics||{},unlock=candidate.unlock||{},carry=candidate.funding_carry||{};
    const early=scoreDisplay(tracks.early_accumulation),shortWatch=scoreDisplay(tracks.elevated_short_watch),longRisk=scoreDisplay(tracks.long_risk),shortRisk=scoreDisplay(tracks.short_risk);
    const tr=document.createElement("tr");tr.className="signal-row";tr.dataset.asset=candidate.asset;tr.tabIndex=0;tr.setAttribute("role","button");tr.setAttribute("aria-label","查看 "+candidate.asset+" 的时间轴");
    if(candidate.asset===document.getElementById("asset").value.trim().toUpperCase())tr.classList.add("selected");
    const cells=[
      [candidate.asset,candidate.status==="candidate"?"结构候选":candidate.status==="lead"?"待核实线索":"观察"],
      [early.main,early.sub],[shortWatch.main,shortWatch.sub],[longRisk.main,longRisk.sub],[shortRisk.main,shortRisk.sub],
      [pct(candidate.price_change_1h_pct)+" / "+pct(candidate.oi_change_1h_pct),"价格 / OI"],
      [(candidate.venues||[]).join(" · ")||"—",hasNumber(candidate.turnover_24h_usd)?"24h "+compact(candidate.turnover_24h_usd)+" USDT":"成交额未知"],
      [shortUtc(candidate.updated_at),""]
    ];
    cells.forEach(([main,sub],i)=>{const td=document.createElement("td"),minor=document.createElement("span");td.textContent=main;minor.className="secondary";minor.textContent=sub;if(sub)td.appendChild(minor);if(i===0)td.className="asset-name";if(i===1||i===2)td.className="score";if(i===3||i===4){const tier=String((i===3?tracks.long_risk:tracks.short_risk)?.tier||"").toLowerCase();td.className="risk-"+(tier==="severe"||tier.includes("high")?"high":tier.includes("medium")||tier.includes("moderate")||tier.includes("elevated")?"medium":tier.includes("low")?"low":"unknown");}if(i===7)td.title=candidate.updated_at||"";tr.appendChild(td);});
    const detailRow=document.createElement("tr"),detailCell=document.createElement("td"),details=document.createElement("details"),summary=document.createElement("summary"),summaryText=document.createElement("span");detailRow.className="detail-row";detailCell.colSpan=8;summaryText.className="summary-text";summaryText.textContent="查看评分依据、OI 结构、解锁与持仓成本";summary.appendChild(summaryText);
    const flags=candidate.quality_flags||[];if(flags.length){const count=document.createElement("span");count.className="quality-count";count.textContent=flags.length+" 项数据提示";summary.appendChild(count);}details.appendChild(summary);
    const detail=document.createElement("div");detail.className="detail-content";
    const scoreGroup=addGroup(detail,"评分依据",true);
    for(const [label,track] of [["早期建仓观察",tracks.early_accumulation],["高位空头观察",tracks.elevated_short_watch],["多头持仓风险",tracks.long_risk],["空头持仓风险",tracks.short_risk]]){
      if(!track)continue;const display=scoreDisplay(track);addLine(scoreGroup,label,display.main+" · "+display.sub+(display.known&&scoreRange(track.score_range)!=="—"?" · 可能范围 "+scoreRange(track.score_range):""));contributionLines(scoreGroup,track,label);
      if(Array.isArray(track.unknown_inputs)&&track.unknown_inputs.length)addLine(scoreGroup,"缺失输入",track.unknown_inputs.join("、"));
    }
    if(!Object.keys(tracks).length&&hasNumber(candidate.score))addLine(scoreGroup,"旧版筛选分",fmt(Number(candidate.score))+" / 100；新结构评分尚未覆盖");
    for(const reason of candidate.reasons||[])addLine(scoreGroup,"观察",reason);
    if(!Object.keys(tracks).length&&!hasNumber(candidate.score))addLine(scoreGroup,"评分","数据不足");
    const oiGroup=addGroup(detail,"OI 与代币供给");
    addLine(oiGroup,"已观测 OI",hasNumber(oi.observed_oi_usd)?compact(oi.observed_oi_usd)+" USDT≈USD":"未知");
    addLine(oiGroup,"OI / 流通量",signedPct(oi.oi_to_float_pct));addLine(oiGroup,"OI / FDV 等价供给",signedPct(oi.oi_to_fdv_pct));
    addLine(oiGroup,"OI 趋势 1h / 4h / 24h",[oi.trend_1h_pct,oi.trend_4h_pct,oi.trend_24h_pct].map(value=>signedPct(value)).join(" / "));
    addLine(oiGroup,"覆盖口径",oi.coverage_label||"覆盖与单双边口径待核验，不能视为全市场 OI");
    addLine(oiGroup,"流通量",hasNumber(token.circulating_supply)?compact(token.circulating_supply)+" tokens":"未知");
    addLine(oiGroup,"市值 / FDV",(hasNumber(token.market_cap_usd)?compact(token.market_cap_usd):"—")+" / "+(hasNumber(token.fdv_usd)?compact(token.fdv_usd):"—")+" USD");
    addLine(oiGroup,"供给来源",sourceLabel(token.source)+" · "+shortUtc(token.as_of));
    const unlockGroup=addGroup(detail,"代币解锁");
    const unlockKnown=unlock.next_at||hasNumber(unlock.days_until)||hasNumber(unlock.pct_circulating);
    addLine(unlockGroup,"下次时间",unlockKnown?unlockDate(unlock.next_at):"未知（当前数据源可能未覆盖）");
    addLine(unlockGroup,"距离",hasNumber(unlock.days_until)?Number(unlock.days_until).toFixed(1)+" 天":"—");
    const verifiedUnlock=hasNumber(unlock.verified_pct_circulating);
    addLine(unlockGroup,"规模",(hasNumber(unlock.amount_tokens)?compact(unlock.amount_tokens)+" tokens":"—")+" · 占流通量 "+(verifiedUnlock?signedPct(unlock.verified_pct_circulating)+"（按 CMC 流通量重算）":hasNumber(unlock.pct_circulating)?signedPct(unlock.pct_circulating)+"（来源值，未核验）":"—"));
    if(verifiedUnlock&&hasNumber(unlock.pct_circulating))addLine(unlockGroup,"来源占比",signedPct(unlock.pct_circulating));
    if(Array.isArray(unlock.basis)&&unlock.basis.length)addLine(unlockGroup,"计算依据",unlock.basis.join("；"));
    if(Array.isArray(unlock.quality_flags)&&unlock.quality_flags.length)addLine(unlockGroup,"数据提示",unlock.quality_flags.map(flag=>({date_precision_day:"解锁日期仅精确到日",stale_unlock_calendar:"解锁日历已过期",date_only_today_time_unknown:"当日解锁时刻未知",unlock_float_source_disagreement:"来源占比与 CMC 流通量重算值不一致"}[flag]||flag)).join("；"));
    addLine(unlockGroup,"状态 / 来源",(unlock.status||"未知")+" · "+sourceLabel(unlock.source));
    if(String(unlock.source||"").toLowerCase().includes("pattern")){const note=document.createElement("p"),link=document.createElement("a");note.className="detail-note";link.href="https://pattern.plus";link.target="_blank";link.rel="noopener noreferrer";link.textContent="Data: PatternPlus";note.appendChild(link);unlockGroup.appendChild(note);}
    const fundingGroup=addGroup(detail,"Funding 持仓成本");
    addLine(fundingGroup,"当前费率",fundingPct(candidate.funding_rate)+" / "+(hasNumber(carry.interval_hours)?fmt(Number(carry.interval_hours))+" 小时结算周期":"期（结算周期未知）")+"；正值多头付费，负值空头付费");
    addLine(fundingGroup,"折算 8 小时",signedPct(carry.rate_8h_pct,4));
    addLine(fundingGroup,"多头年化情景",signedPct(carry.long_annualized_pct));
    addLine(fundingGroup,"空头年化情景",signedPct(carry.short_annualized_pct));
    addLine(fundingGroup,"Premium",hasNumber(candidate.premium_bps)?Number(candidate.premium_bps).toFixed(2)+" bps":"—");
    const caveat=document.createElement("p");caveat.className="detail-note";caveat.textContent="年化只是把当前费率恒定延续的情景计算；实际结算周期、费率和价格会变化。正收益表示该方向收取 funding。";fundingGroup.appendChild(caveat);
    if(flags.length){const qualityGroup=addGroup(detail,"数据质量",true),line=document.createElement("p");line.className="detail-line";for(const flag of flags){const tag=document.createElement("span");tag.className="quality-tag";tag.textContent=flagLabels[flag]||flag;tag.title=flag;line.appendChild(tag);}qualityGroup.appendChild(line);}
    addLine(scoreGroup,"市场更新",shortUtc(candidate.updated_at,true));
    details.appendChild(detail);detailCell.appendChild(details);detailRow.appendChild(detailCell);
    const select=()=>{document.getElementById("asset").value=candidate.asset;document.querySelectorAll(".discovery-table .signal-row").forEach(row=>row.classList.remove("selected"));tr.classList.add("selected");load();document.getElementById("chart").scrollIntoView({behavior:"smooth",block:"center"});};
    tr.addEventListener("click",select);tr.addEventListener("keydown",event=>{if(event.key==="Enter"||event.key===" "){event.preventDefault();select();}});body.appendChild(tr);body.appendChild(detailRow);
  }
  const coverage=payload.coverage||{};
  if(!(payload.candidates||[]).length){const tr=document.createElement("tr"),td=document.createElement("td");td.colSpan=8;td.textContent=coverage.assets_seen>0&&coverage.assets_with_1h_baseline===0?"正在积累约 1 小时的价格和持仓基线，请稍后刷新":"当前没有达到筛选阈值的标的";tr.appendChild(td);body.appendChild(tr);}
  const cards=[["覆盖标的",coverage.assets_seen],["有 1h 基线",coverage.assets_with_1h_baseline],["结构预筛",coverage.assets_structurally_enriched??coverage.assets_eligible],["候选 / 线索",String(coverage.candidate_count??0)+" / "+String(coverage.lead_count??0)]];
  const grid=document.getElementById("coverage");grid.replaceChildren();for(const [label,value] of cards){const card=document.createElement("div"),name=document.createElement("span"),number=document.createElement("strong");card.className="stat";name.className="stat-label";name.textContent=label;number.textContent=hasNumber(value)?fmt(Number(value)):value??"—";card.append(name,number);grid.appendChild(card);}
  document.getElementById("discovery-status").textContent="市场 "+shortUtc(coverage.latest_market_at)+" · 可比 "+String(coverage.assets_eligible??"—")+" · 结构预筛 "+String(coverage.assets_structurally_enriched??"—")+" · 展示 "+(payload.candidates||[]).length;
}
async function loadDiscovery(){
  const status=document.getElementById("discovery-status");status.textContent="读取榜单中…";status.className="muted";
  try{const response=await fetch("/api/discovery?limit=50");if(!response.ok)throw Error("榜单 HTTP "+response.status);populateDiscovery(await response.json());}
  catch(err){status.textContent="榜单读取失败："+err.message;status.className="error";}
}
async function load(){
  const asset=document.getElementById("asset").value.trim().toUpperCase(),venue=document.getElementById("venue").value.trim();
  const status=document.getElementById("status");status.textContent="读取中…";status.className="status";
  if(!/^[A-Z0-9][A-Z0-9_-]{0,23}$/.test(asset)||venue&&!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(venue)){status.textContent="资产或场所格式无效";status.className="status error";return;}
  document.querySelectorAll(".discovery-table .signal-row").forEach(row=>row.classList.toggle("selected",row.dataset.asset===asset));
  const base=new URLSearchParams({asset});if(venue)base.set("venue",venue);
  const hoursValue=document.getElementById("hours").value,windowHours=hoursValue==="auto"?24:Number(hoursValue);
  const since=new Date(Date.now()-windowHours*3600000).toISOString();
  try{
    const snap=await fetch("/api/snapshot?"+base);if(!snap.ok)throw Error("快照 HTTP "+snap.status);const snapshot=await snap.json();populateSnapshot(snapshot);
    const responses=await Promise.all(METRICS.map(async metric=>{const q=new URLSearchParams(base);q.set("metric",metric);q.set("since",since);q.set("limit","2000");const r=await fetch("/api/history?"+q);if(!r.ok)throw Error(metric+" HTTP "+r.status);return r.json();}));
    allSeries=[];for(const result of responses){const groups=new Map();for(const row of result.observations){const key=row.metric+"/"+row.venue;if(!groups.has(key))groups.set(key,[]);groups.get(key).push(row);}for(const rows of groups.values())allSeries.push({metric:rows[0].metric,venue:rows[0].venue,rows});}
    const times=allSeries.flatMap(s=>s.rows.filter(r=>r.chartable).map(r=>Date.parse(r.as_of)).filter(Number.isFinite)),end=Date.now(),selectedWindow=windowHours*3600000;
    domain=[end-selectedWindow,end];if(hoursValue==="auto"&&times.length){const earliest=Math.min(...times),span=Math.min(selectedWindow,Math.max(10*60000,(end-earliest)*1.1));domain=[end-span,end];}if(domain[0]===domain[1])domain[1]++;
    hoverTime=null;tooltip.style.display="none";renderLegend();draw();status.textContent="更新于 "+shortUtc(snapshot.generated_at,true)+" · "+snapshot.observations.length+" 条最新观测 · "+allSeries.reduce((n,s)=>n+s.rows.length,0)+" 个历史点";
    if(snapshot.missing.length)status.className="status warning";
  }catch(err){status.textContent="读取失败："+err.message;status.className="status error";}
}
canvas.addEventListener("mousemove",showHover);canvas.addEventListener("mouseleave",()=>{hoverTime=null;tooltip.style.display="none";draw();});
window.addEventListener("resize",draw);document.getElementById("chart-view").addEventListener("change",()=>{hoverTime=null;tooltip.style.display="none";renderLegend();draw();});document.getElementById("hours").addEventListener("change",load);document.getElementById("refresh").addEventListener("click",()=>{loadDiscovery();load();});loadDiscovery();load();
</script></body></html>"""


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def make_handler(
    store: NormalizedSqliteStore,
    *,
    data_max_age_seconds: int = DEFAULT_DATA_MAX_AGE_SECONDS,
) -> type[BaseHTTPRequestHandler]:
    """Bind a store to a GET-only handler without process-global state."""
    if data_max_age_seconds <= 0:
        raise ValueError("data_max_age_seconds must be positive")

    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if content_type.startswith("text/html"):
                self.send_header(
                    "Content-Security-Policy",
                    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                    "connect-src 'self'; base-uri 'none'; form-action 'none'",
                )
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
            self._send(status, body, "application/json; charset=utf-8")

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            try:
                if parsed.path == "/":
                    self._send(HTTPStatus.OK, HTML.encode("utf-8"), "text/html; charset=utf-8")
                    return
                if parsed.path == "/healthz":
                    now = datetime.now(timezone.utc)
                    try:
                        received_at = _latest_received_at(store, now)
                    except (sqlite3.Error, OSError, ValueError):
                        received_at = None
                    age_seconds = (
                        round((now - received_at).total_seconds(), 3)
                        if received_at is not None else None
                    )
                    self._json(HTTPStatus.OK, {
                        "ok": True,
                        "service": "squeeze-research-dashboard",
                        "read_only": True,
                        "asset": None,
                        "latest_received_at": _iso(received_at),
                        "age_seconds": age_seconds,
                        "data_ready": (
                            age_seconds is not None and 0 <= age_seconds <= data_max_age_seconds
                        ),
                        "max_age_seconds": data_max_age_seconds,
                    })
                    return
                if parsed.path not in ("/api/discovery", "/api/snapshot", "/api/history"):
                    self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                    return
                query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=20)
                if parsed.path == "/api/discovery":
                    self._json(HTTPStatus.OK, discovery_payload(
                        store, now=datetime.now(timezone.utc), limit=_discovery_limit(query)
                    ))
                    return
                asset = _asset(query)
                venue = _venue(query)
                now = datetime.now(timezone.utc)
                if parsed.path == "/api/snapshot":
                    as_of = _time(query, "as_of") or now
                    records = store.list_as_of(asset, as_of, metrics=CHART_METRICS, venue=venue)
                    present = {record.metric for record in records}
                    self._json(HTTPStatus.OK, {
                        "asset": asset,
                        "venue": venue,
                        "generated_at": _iso(now),
                        "as_of": _iso(as_of),
                        "observations": [snapshot_json(record, now=now) for record in records],
                        "missing": [metric for metric in CHART_METRICS if metric not in present],
                    })
                    return
                metric = _single(query, "metric")
                if metric not in CHART_METRICS:
                    raise ValueError("metric must be a supported chart metric")
                since, until = _time(query, "since"), _time(query, "until")
                if since is not None and until is not None and since > until:
                    raise ValueError("since must be no later than until")
                limit = _limit(query)
                records = sorted(
                    store.history(asset, metric, venue=venue, since=since, until=until, limit=limit),
                    key=_event_time,
                )
                self._json(HTTPStatus.OK, {
                    "asset": asset,
                    "metric": metric,
                    "venue": venue,
                    "generated_at": _iso(now),
                    "filter_time": "available_at",
                    "axis_time": "source_timestamp_or_received_at",
                    "observations": [snapshot_json(record, now=now) for record in records],
                })
            except ValueError as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            except Exception:
                # Database paths, SQL details, and host environment stay private.
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "dashboard read failed"})

        def do_POST(self) -> None:
            self._json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "read-only dashboard"})

        do_PUT = do_POST
        do_PATCH = do_POST
        do_DELETE = do_POST

    return Handler


def make_server(
    store: NormalizedSqliteStore,
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    data_max_age_seconds: int = DEFAULT_DATA_MAX_AGE_SECONDS,
) -> DashboardHTTPServer:
    if host != DEFAULT_HOST:
        raise ValueError("dashboard may bind only to 127.0.0.1")
    return DashboardHTTPServer(
        (host, port), make_handler(store, data_max_age_seconds=data_max_age_seconds)
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only local perp market abuse research dashboard")
    parser.add_argument("--db", type=Path, required=True, help="normalized SQLite database path")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--data-max-age-seconds", type=int, default=DEFAULT_DATA_MAX_AGE_SECONDS)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be from 1 to 65535")
    if args.data_max_age_seconds <= 0:
        parser.error("--data-max-age-seconds must be positive")
    with NormalizedSqliteStore(args.db, read_only=True) as store:
        server = make_server(store, port=args.port, data_max_age_seconds=args.data_max_age_seconds)
        print(f"Perp Market Abuse Monitor: http://{DEFAULT_HOST}:{args.port}", flush=True)
        try:
            server.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
