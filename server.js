const express = require("express");
const path = require("path");

const app = express();
const PORT = process.env.PORT || 10000;

const ALPACA_API_KEY = process.env.ALPACA_API_KEY || "";
const ALPACA_SECRET_KEY = process.env.ALPACA_SECRET_KEY || "";
const ALPACA_FEED = (process.env.ALPACA_FEED || "iex").toLowerCase();
const ALPACA_TRADING_BASE_URL =
  process.env.ALPACA_TRADING_BASE_URL || "https://paper-api.alpaca.markets";

app.use(express.json());
app.use(express.static(__dirname));

const DEFAULT_SYMBOLS = [
  "MYSE", "WSHP", "RMSG", "ROLR", "RAYA", "CUE", "ELAB", "CYCN",
  "VCX", "LPA", "CTMX", "SOPA", "RR", "TNON", "SIDU", "SQFT"
];

const ALLOWED_EXCHANGES = new Set(["NASDAQ", "NYSE", "AMEX", "ARCA", "BATS"]);
const BAD_NAME_TOKENS = [
  "WARRANT", "RIGHT", "UNIT", "ETF", "FUND", "TRUST",
  "ETN", "PREFERRED", "PFD", "DEPOSITARY", "ADR"
];

const CATALYST_GRADES = {
  none: 0,
  generic_sector_news: 8,
  compliance_regime_change: 18,
  strategic_collaboration: 20,
  ai_transition: 18,
  prediction_market_pivot: 18,
  narrative_pivot: 22,
  biotech_data: 26,
  fda: 30,
  contract_award: 24,
  balance_sheet: 12,
  financing: -12,
  offering: -18,
  reverse_split: -30
};

const SYMBOL_FLAGS = {
  RMSG: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: true,
    catalystFresh: true,
    catalystType: "strategic_collaboration",
    allowAbove8: false,
    marketCapOverride: 25800000,
    floatOverride: 6500000
  },
  ROLR: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: true,
    catalystType: "compliance_regime_change",
    allowAbove8: true,
    marketCapOverride: 80696000,
    floatOverride: 9000000
  },
  RAYA: {
    catalystFresh: true,
    catalystType: "contract_award",
    cleanRecoveryPreferred: true
  },
  CUE: {
    catalystFresh: true,
    catalystType: "biotech_data"
  },
  ELAB: {
    catalystFresh: true,
    catalystType: "biotech_data"
  },
  CYCN: {
    catalystFresh: true,
    catalystType: "biotech_data"
  },
  IREN: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: true,
    catalystType: "ai_transition",
    allowAbove8: true,
    marketCapOverride: 14289000000,
    cleanRecoveryPreferred: true
  },
  BIRD: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: true,
    catalystType: "narrative_pivot",
    allowAbove8: true,
    marketCapOverride: 147951000,
    financingRisk: true,
    narrativePivot: true
  },
  MYSE: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: true,
    catalystType: "narrative_pivot",
    allowAbove8: false,
    marketCapOverride: 6227000,
    financingRisk: false,
    narrativePivot: true,
    cleanRecoveryPreferred: true
  },
  WSHP: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: true,
    catalystType: "balance_sheet",
    allowAbove8: true,
    marketCapOverride: 89957000,
    financingRisk: false,
    narrativePivot: false,
    cleanRecoveryPreferred: false,
    manualReject: false,
    cleanRecoveryPreferred: true
  },
  WNW: {
    recentReverseSplit: true,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: false,
    catalystType: "reverse_split",
    allowAbove8: false,
    marketCapOverride: 1916000,
    financingRisk: true,
    narrativePivot: false,
    manualReject: true
  },
  CTNT: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: false,
    catalystType: "none",
    allowAbove8: false,
    marketCapOverride: 6293000,
    financingRisk: false,
    narrativePivot: false,
    cleanRecoveryPreferred: false,
    manualReject: false,
    manualReject: true
  }
};

const FAMILY_MODELS = [
  {
    name: "COLLAPSE_RECOVERY",
    features: {
      price: { weight: 10, ideal: [0.10, 3.00], hard: [0.05, 6.00] },
      drawdown90: { weight: 12, ideal: [-92, -35], hard: [-99, -5] },
      baseTightness10: { weight: 10, ideal: [4, 28], hard: [0, 80] },
      prevDayRet: { weight: 8, ideal: [5, 35], hard: [-10, 90] },
      prevVolRatio: { weight: 10, ideal: [2, 15], hard: [0.5, 50] },
      prevDollarShock: { weight: 10, ideal: [2, 15], hard: [0.5, 50] },
      prevCloseStrength: { weight: 10, ideal: [72, 100], hard: [40, 100] },
      breakout20: { weight: 6, ideal: [1, 1], hard: [0, 1] },
      gapPct: { weight: 6, ideal: [5, 60], hard: [-10, 150] },
      preVolRatio: { weight: 8, ideal: [1, 8], hard: [0.2, 30] },
      holdQuality: { weight: 10, ideal: [65, 100], hard: [35, 100] }
    }
  },
  {
    name: "SQUEEZE_RECLAIM",
    features: {
      price: { weight: 8, ideal: [0.20, 5.00], hard: [0.10, 8.00] },
      drawdown90: { weight: 6, ideal: [-80, -15], hard: [-99, 0] },
      baseTightness10: { weight: 9, ideal: [3, 24], hard: [0, 60] },
      prevDayRet: { weight: 12, ideal: [12, 70], hard: [-5, 150] },
      prevVolRatio: { weight: 12, ideal: [2, 20], hard: [0.5, 80] },
      prevDollarShock: { weight: 10, ideal: [2, 20], hard: [0.5, 80] },
      prevCloseStrength: { weight: 12, ideal: [78, 100], hard: [45, 100] },
      breakout20: { weight: 8, ideal: [1, 1], hard: [0, 1] },
      gapPct: { weight: 7, ideal: [5, 50], hard: [-10, 120] },
      preVolRatio: { weight: 6, ideal: [1, 10], hard: [0.2, 35] },
      holdQuality: { weight: 10, ideal: [70, 100], hard: [40, 100] }
    }
  },
  {
    name: "REGIME_CHANGE",
    features: {
      price: { weight: 8, ideal: [1.00, 8.00], hard: [0.20, 12.00] },
      drawdown90: { weight: 4, ideal: [-70, -5], hard: [-99, 20] },
      baseTightness10: { weight: 6, ideal: [5, 35], hard: [0, 80] },
      prevDayRet: { weight: 12, ideal: [15, 120], hard: [-5, 250] },
      prevVolRatio: { weight: 12, ideal: [3, 30], hard: [0.5, 150] },
      prevDollarShock: { weight: 14, ideal: [4, 40], hard: [0.5, 150] },
      prevCloseStrength: { weight: 8, ideal: [65, 100], hard: [35, 100] },
      breakout20: { weight: 6, ideal: [1, 1], hard: [0, 1] },
      gapPct: { weight: 10, ideal: [10, 120], hard: [-10, 250] },
      preVolRatio: { weight: 8, ideal: [1, 12], hard: [0.2, 40] },
      holdQuality: { weight: 12, ideal: [60, 100], hard: [35, 100] }
    }
  },
  {
    name: "NARRATIVE_PIVOT",
    features: {
      price: { weight: 6, ideal: [0.80, 10.00], hard: [0.20, 18.00] },
      drawdown90: { weight: 5, ideal: [-92, -25], hard: [-99, 20] },
      baseTightness10: { weight: 4, ideal: [8, 55], hard: [0, 120] },
      prevDayRet: { weight: 12, ideal: [20, 180], hard: [-5, 450] },
      prevVolRatio: { weight: 12, ideal: [4, 80], hard: [0.5, 300] },
      prevDollarShock: { weight: 12, ideal: [4, 80], hard: [0.5, 300] },
      prevCloseStrength: { weight: 7, ideal: [60, 100], hard: [20, 100] },
      breakout20: { weight: 4, ideal: [1, 1], hard: [0, 1] },
      gapPct: { weight: 10, ideal: [15, 180], hard: [-10, 450] },
      preVolRatio: { weight: 6, ideal: [1, 16], hard: [0.1, 60] },
      holdQuality: { weight: 7, ideal: [55, 100], hard: [20, 100] },
      noReverseSplit: { weight: 15, ideal: [1, 1], hard: [0, 1] }
    }
  },
  {
    name: "CLEAN_RECOVERY",
    features: {
      price: { weight: 8, ideal: [0.20, 6.00], hard: [0.10, 10.00] },
      drawdown90: { weight: 10, ideal: [-90, -35], hard: [-99, -5] },
      drawdown252: { weight: 12, ideal: [-96, -45], hard: [-99, -10] },
      baseTightness10: { weight: 12, ideal: [3, 28], hard: [0, 70] },
      prevDayRet: { weight: 8, ideal: [5, 60], hard: [-20, 150] },
      prevVolRatio: { weight: 8, ideal: [2, 18], hard: [0.5, 60] },
      prevDollarShock: { weight: 8, ideal: [2, 18], hard: [0.5, 60] },
      prevCloseStrength: { weight: 10, ideal: [72, 100], hard: [40, 100] },
      breakout20: { weight: 6, ideal: [1, 1], hard: [0, 1] },
      holdQuality: { weight: 4, ideal: [55, 100], hard: [30, 100] },
      noReverseSplit: { weight: 14, ideal: [1, 1], hard: [0, 1] }
    }
  }
];

const ASSET_CACHE = { expiresAt: 0, data: null };
const DISCOVERY_CACHE = { expiresAt: 0, key: "", data: null };


const CONFIRM_RULES = {
  triggerTolerance: 0.995,
  nearTolerance: 0.985,
  minHoldQuality: 55,
  strongHoldQuality: 65
};

const CONFIRM_VOLUME_TIERS = [
  {
    maxPrice: 0.5,
    minPremarketVolume: 200000,
    betterPremarketVolume: 500000,
    minPremarketDollarVolume: 100000,
    betterPremarketDollarVolume: 250000,
    failBelowMultiplier: 0.94
  },
  {
    maxPrice: 1,
    minPremarketVolume: 250000,
    betterPremarketVolume: 500000,
    minPremarketDollarVolume: 150000,
    betterPremarketDollarVolume: 300000,
    failBelowMultiplier: 0.94
  },
  {
    maxPrice: 3,
    minPremarketVolume: 100000,
    betterPremarketVolume: 250000,
    minPremarketDollarVolume: 175000,
    betterPremarketDollarVolume: 350000,
    failBelowMultiplier: 0.95
  },
  {
    maxPrice: 8,
    minPremarketVolume: 30000,
    betterPremarketVolume: 60000,
    minPremarketDollarVolume: 200000,
    betterPremarketDollarVolume: 400000,
    failBelowMultiplier: 0.97
  },
  {
    maxPrice: 15,
    minPremarketVolume: 20000,
    betterPremarketVolume: 40000,
    minPremarketDollarVolume: 250000,
    betterPremarketDollarVolume: 500000,
    failBelowMultiplier: 0.97
  },
  {
    maxPrice: Number.POSITIVE_INFINITY,
    minPremarketVolume: 15000,
    betterPremarketVolume: 30000,
    minPremarketDollarVolume: 300000,
    betterPremarketDollarVolume: 600000,
    failBelowMultiplier: 0.975
  }
];

function safeNum(v, fallback = 0) {
  if (v === null || v === undefined || v === "") return fallback;
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function roundSmart(v) {
  if (v === null || v === undefined || !Number.isFinite(v)) return null;
  if (Math.abs(v) < 1) return Number(v.toFixed(4));
  return Number(v.toFixed(2));
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function avg(arr) {
  if (!arr || !arr.length) return 0;
  return arr.reduce((s, x) => s + safeNum(x, 0), 0) / arr.length;
}

function median(arr) {
  if (!arr || !arr.length) return 0;
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function maxOf(arr) {
  return arr && arr.length ? Math.max(...arr) : null;
}

function minOf(arr) {
  return arr && arr.length ? Math.min(...arr) : null;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function parseSymbols(raw) {
  return String(raw || "")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

function getFlags(symbol) {
  return {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: false,
    catalystType: "none",
    allowAbove8: false,
    marketCapOverride: null,
    floatOverride: null,
    financingRisk: false,
    narrativePivot: false,
    cleanRecoveryPreferred: false,
    manualReject: false,
    ...(SYMBOL_FLAGS[symbol] || {})
  };
}

function catalystGrade(type) {
  return safeNum(CATALYST_GRADES[type], 0);
}

function timeZoneParts(date, timeZone = "America/New_York") {
  const dtf = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });
  const parts = dtf.formatToParts(date);
  const out = {};
  for (const p of parts) {
    if (p.type !== "literal") out[p.type] = p.value;
  }
  return out;
}

function getTimeZoneOffsetMs(date, timeZone = "America/New_York") {
  const parts = timeZoneParts(date, timeZone);
  const asUTC = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
    Number(parts.second)
  );
  return asUTC - date.getTime();
}

function zonedDateTimeToUtcISO(dateStr, timeStr, timeZone = "America/New_York") {
  const [year, month, day] = dateStr.split("-").map(Number);
  const [hour, minute] = timeStr.split(":").map(Number);

  let guess = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
  for (let i = 0; i < 3; i++) {
    const offset = getTimeZoneOffsetMs(guess, timeZone);
    guess = new Date(guess.getTime() - offset);
  }
  return guess.toISOString();
}

function addDaysIso(dateStr, days) {
  const d = new Date(`${dateStr}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function isoDateNY(iso) {
  const d = new Date(iso);
  const p = timeZoneParts(d, "America/New_York");
  return `${p.year}-${p.month}-${p.day}`;
}

function isoTimeNY(iso) {
  const d = new Date(iso);
  const p = timeZoneParts(d, "America/New_York");
  return `${p.hour}:${p.minute}:${p.second}`;
}

function getTodayNyDate() {
  const p = timeZoneParts(new Date(), "America/New_York");
  return `${p.year}-${p.month}-${p.day}`;
}

function getNowNyTime() {
  const p = timeZoneParts(new Date(), "America/New_York");
  return `${p.hour}:${p.minute}:${p.second}`;
}

function getSessionLabelNow() {
  const now = new Date();
  const p = timeZoneParts(now, "America/New_York");
  const hh = Number(p.hour);
  const mm = Number(p.minute);
  const mins = hh * 60 + mm;

  const weekday = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short"
  }).format(now);

  if (weekday === "Sat" || weekday === "Sun") return "weekend";
  if (mins >= 240 && mins < 570) return "premarket";
  if (mins >= 570 && mins < 960) return "open";
  if (mins >= 960 && mins < 1200) return "afterhours";
  return "closed";
}

function decisionRank(decision) {
  if (decision === "SUPERNOVA_CONFIRMED" || decision === "GÜÇLÜ AL") return 5;
  if (decision === "AL") return 4;
  if (decision === "MANUAL_CONFIRM_REQUIRED" || decision === "SPARSE_PREMARKET" || decision === "WATCH_TRIGGER" || decision === "İZLE") return 2;
  return 1;
}

async function alpacaGetJson(url, isArray = false) {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) {
    throw new Error("ALPACA_API_KEY / ALPACA_SECRET_KEY eksik");
  }

  const response = await fetch(url, {
    headers: {
      "APCA-API-KEY-ID": ALPACA_API_KEY,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      Accept: "application/json"
    }
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Alpaca ${response.status}: ${text}`);
  }

  if (!text) return isArray ? [] : {};
  return JSON.parse(text);
}

async function fetchAssets() {
  if (ASSET_CACHE.data && Date.now() < ASSET_CACHE.expiresAt) return ASSET_CACHE.data;

  const url = new URL("/v2/assets", ALPACA_TRADING_BASE_URL);
  url.searchParams.set("status", "active");
  url.searchParams.set("asset_class", "us_equity");

  const assets = await alpacaGetJson(url.toString(), true);

  const filtered = (assets || []).filter((a) => {
    const symbol = String(a.symbol || "").toUpperCase();
    const name = String(a.name || "").toUpperCase();

    if (!a.tradable) return false;
    if (!ALLOWED_EXCHANGES.has(String(a.exchange || "").toUpperCase())) return false;
    if (!/^[A-Z]{1,5}$/.test(symbol)) return false;
    if (BAD_NAME_TOKENS.some((x) => name.includes(x))) return false;
    return true;
  });

  ASSET_CACHE.data = filtered;
  ASSET_CACHE.expiresAt = Date.now() + 12 * 60 * 60 * 1000;
  return filtered;
}

async function fetchSnapshotsBatch(symbols, feed = ALPACA_FEED) {
  if (!symbols.length) return {};
  const url = new URL("https://data.alpaca.markets/v2/stocks/snapshots");
  url.searchParams.set("symbols", symbols.join(","));
  url.searchParams.set("feed", feed);
  return alpacaGetJson(url.toString());
}

async function fetchSnapshotsBatched(symbols, feed = ALPACA_FEED, batchSize = 80, concurrency = 6) {
  const result = {};
  const chunks = chunkArray(symbols, batchSize);

  for (let i = 0; i < chunks.length; i += concurrency) {
    const group = chunks.slice(i, i + concurrency);
    const responses = await Promise.all(group.map((c) => fetchSnapshotsBatch(c, feed)));
    for (const resp of responses) {
      for (const [symbol, snap] of Object.entries(resp || {})) {
        result[symbol] = snap;
      }
    }
  }

  return result;
}

async function fetchBarsBatch(symbols, timeframe, startISO, endISO, feed = ALPACA_FEED, limit = 10000) {
  const barsBySymbol = {};
  let pageToken = null;

  while (true) {
    const url = new URL("https://data.alpaca.markets/v2/stocks/bars");
    url.searchParams.set("symbols", symbols.join(","));
    url.searchParams.set("timeframe", timeframe);
    url.searchParams.set("start", startISO);
    url.searchParams.set("end", endISO);
    url.searchParams.set("feed", feed);
    url.searchParams.set("adjustment", "raw");
    url.searchParams.set("sort", "asc");
    url.searchParams.set("limit", String(limit));
    if (pageToken) url.searchParams.set("page_token", pageToken);

    const json = await alpacaGetJson(url.toString());
    const bars = json.bars || {};

    for (const [symbol, arr] of Object.entries(bars)) {
      if (!barsBySymbol[symbol]) barsBySymbol[symbol] = [];
      barsBySymbol[symbol].push(...arr);
    }

    if (!json.next_page_token) break;
    pageToken = json.next_page_token;
  }

  for (const symbol of symbols) {
    if (!barsBySymbol[symbol]) barsBySymbol[symbol] = [];
    barsBySymbol[symbol].sort((a, b) => new Date(a.t) - new Date(b.t));
  }

  return barsBySymbol;
}

async function fetchBarsBatched(symbols, timeframe, startISO, endISO, feed = ALPACA_FEED, batchSize = 50, concurrency = 4) {
  const out = {};
  const chunks = chunkArray(symbols, batchSize);

  for (let i = 0; i < chunks.length; i += concurrency) {
    const group = chunks.slice(i, i + concurrency);
    const responses = await Promise.all(
      group.map((c) => fetchBarsBatch(c, timeframe, startISO, endISO, feed))
    );

    for (const resp of responses) {
      for (const [symbol, arr] of Object.entries(resp || {})) {
        if (!out[symbol]) out[symbol] = [];
        out[symbol].push(...arr);
      }
    }
  }

  for (const symbol of symbols) {
    if (!out[symbol]) out[symbol] = [];
    out[symbol].sort((a, b) => new Date(a.t) - new Date(b.t));
  }

  return out;
}

function getBarsForDate(allBars, dateStr) {
  return (allBars || []).filter((b) => isoDateNY(b.t) === dateStr);
}

function filterBarsByTime(bars, startTime, endTime) {
  return (bars || []).filter((b) => {
    const t = isoTimeNY(b.t);
    return t >= startTime && t <= endTime;
  });
}

function computeVWAP(bars) {
  if (!bars || !bars.length) return null;
  let pv = 0;
  let vv = 0;
  for (const b of bars) {
    const h = safeNum(b.h, 0);
    const l = safeNum(b.l, 0);
    const c = safeNum(b.c, 0);
    const v = safeNum(b.v, 0);
    const tp = (h + l + c) / 3;
    pv += tp * v;
    vv += v;
  }
  return vv > 0 ? pv / vv : null;
}

function computeCloseStrength(bar) {
  if (!bar) return null;
  const high = safeNum(bar.h, null);
  const low = safeNum(bar.l, null);
  const close = safeNum(bar.c, null);
  if ([high, low, close].some((v) => v == null)) return null;
  const range = high - low;
  if (range <= 0) return 50;
  return ((close - low) / range) * 100;
}

function computeRangePct(high, low, ref) {
  high = safeNum(high, null);
  low = safeNum(low, null);
  ref = safeNum(ref, null);
  if ([high, low, ref].some((v) => v == null) || ref <= 0) return null;
  return ((high - low) / ref) * 100;
}

function buildDailyHistoryContext(dailyBars, tradeDate) {
  const sorted = [...(dailyBars || [])].sort((a, b) => new Date(a.t) - new Date(b.t));
  const priorBars = sorted.filter((b) => isoDateNY(b.t) < tradeDate);
  if (priorBars.length < 25) return null;

  const last = priorBars[priorBars.length - 1];
  const prev = priorBars[priorBars.length - 2];
  const hist10 = priorBars.slice(-10);
  const hist20 = priorBars.slice(-20);
  const hist60 = priorBars.slice(-60);
  const hist90 = priorBars.slice(-90);
  const hist252 = priorBars.slice(-252);
  const priorDates = [...new Set(priorBars.map((b) => isoDateNY(b.t)))].slice(-10);

  return { last, prev, hist10, hist20, hist60, hist90, hist252, priorDates };
}

function buildStructuralMetrics(ctx, flags) {
  const price = safeNum(ctx.last.c, 0);
  const hist10High = maxOf(ctx.hist10.map((b) => safeNum(b.h, 0)));
  const hist10Low = minOf(ctx.hist10.map((b) => safeNum(b.l, 0)));
  const high20ExLast = maxOf(ctx.hist20.slice(0, -1).map((b) => safeNum(b.h, 0)));
  const low30 = minOf(ctx.hist60.slice(-30).map((b) => safeNum(b.l, 0)));
  const high90 = maxOf(ctx.hist90.map((b) => safeNum(b.h, 0)));
  const high252 = maxOf(ctx.hist252.map((b) => safeNum(b.h, 0)));
  const low252 = minOf(ctx.hist252.map((b) => safeNum(b.l, 0)));

  const drawdown90 = high90 > 0 ? ((price / high90) - 1) * 100 : 0;
  const drawdown252 = high252 > 0 ? ((price / high252) - 1) * 100 : 0;
  const reboundFrom30Low = low30 > 0 ? ((price - low30) / low30) * 100 : 0;
  const historicalRecoveryFrom252Low = low252 > 0 ? ((price - low252) / low252) * 100 : 0;
  const baseTightness10 = price > 0 ? ((hist10High - hist10Low) / price) * 100 : 999;
  const breakout20 = high20ExLast > 0 && price > high20ExLast ? 1 : 0;
  const historicalHighMultiple = price > 0 ? high252 / price : 0;
  const likelyReverseSplitProxy =
    historicalHighMultiple >= 80 ||
    (historicalHighMultiple >= 45 && price >= 1.5 && safeNum(flags.marketCapOverride, 0) <= 30000000 && safeNum(flags.marketCapOverride, 0) > 0);

  return {
    price,
    drawdown90,
    drawdown252,
    reboundFrom30Low,
    historicalRecoveryFrom252Low,
    baseTightness10,
    breakout20,
    historicalHighMultiple,
    likelyReverseSplitProxy,
    recentReverseSplit: !!flags.recentReverseSplit,
    otcRisk: !!flags.otcRisk,
    recentDeficiency: !!flags.recentDeficiency,
    allowAbove8: !!flags.allowAbove8,
    marketCap: flags.marketCapOverride,
    floatShares: flags.floatOverride,
    cleanRecoveryPreferred: !!flags.cleanRecoveryPreferred,
    manualReject: !!flags.manualReject
  };
}

function buildIgnitionMetrics(ctx, flags) {
  const prevClose = safeNum(ctx.prev.c, 0);
  const price = safeNum(ctx.last.c, 0);
  const prevDayRet = prevClose > 0 ? ((price - prevClose) / prevClose) * 100 : 0;
  const prevCloseStrength = computeCloseStrength(ctx.last);

  const avgVol20 = Math.max(avg(ctx.hist20.slice(0, -1).map((b) => safeNum(b.v, 0))), 1);
  const prevVol = safeNum(ctx.last.v, 0);
  const prevVolRatio = prevVol / avgVol20;

  const prevDollarVol = prevVol * price;
  const avgDollarVol20 = Math.max(
    avg(ctx.hist20.slice(0, -1).map((b) => safeNum(b.v, 0) * safeNum(b.c, 0))),
    1
  );
  const prevDollarShock = prevDollarVol / avgDollarVol20;

  const avgRange20 = Math.max(
    avg(ctx.hist20.slice(0, -1).map((b) => computeRangePct(b.h, b.l, b.c) || 0)),
    0.0001
  );

  const prevRangePct = computeRangePct(ctx.last.h, ctx.last.l, ctx.last.c);
  const rangeExpansion = prevRangePct != null ? prevRangePct / avgRange20 : 0;
  const collapseTrap = prevDayRet <= -60;
  const fadeRisk = prevDayRet >= 140 && safeNum(prevCloseStrength, 0) < 70;

  return {
    prevDayRet,
    prevCloseStrength,
    prevVolRatio,
    prevDollarVol,
    prevDollarShock,
    prevRangePct,
    rangeExpansion,
    catalystFresh: !!flags.catalystFresh,
    catalystType: flags.catalystType || "none",
    catalystGrade: catalystGrade(flags.catalystType || "none"),
    financingRisk: !!flags.financingRisk,
    narrativePivot: !!flags.narrativePivot,
    collapseTrap,
    fadeRisk
  };
}

function buildFormerRunnerMetrics(ctx) {
  const hist = ctx.hist252 || [];
  if (!hist.length) {
    return {
      explosiveDays252: 0,
      explosiveDays63: 0,
      haltProxyDays252: 0,
      haltProxyDays63: 0,
      maxExpansion252: 0,
      maxVolShock252: 0,
      formerRunnerFlag: false
    };
  }

  const volSma20Arr = [];
  for (let i = 0; i < hist.length; i++) {
    const start = Math.max(0, i - 20);
    const sample = hist.slice(start, i).map((b) => safeNum(b.v, 0));
    volSma20Arr.push(sample.length ? avg(sample) : 0);
  }

  let explosiveDays252 = 0;
  let explosiveDays63 = 0;
  let haltProxyDays252 = 0;
  let haltProxyDays63 = 0;
  let maxExpansion252 = 0;
  let maxVolShock252 = 0;

  hist.forEach((b, i) => {
    const open = safeNum(b.o, 0);
    const high = safeNum(b.h, 0);
    const close = safeNum(b.c, 0);
    const vol = safeNum(b.v, 0);
    const volSma20 = Math.max(safeNum(volSma20Arr[i], 0), 1);
    const expFromOpen = open > 0 ? ((high / open) - 1) * 100 : 0;
    const closeFromOpen = open > 0 ? ((close / open) - 1) * 100 : 0;
    const volShock = vol / volSma20;
    const closeStrength = computeCloseStrength(b) || 0;

    maxExpansion252 = Math.max(maxExpansion252, expFromOpen);
    maxVolShock252 = Math.max(maxVolShock252, volShock);

    const explosive =
      expFromOpen >= 60 &&
      volShock >= 8 &&
      closeStrength >= 55;

    const haltProxy =
      expFromOpen >= 70 &&
      volShock >= 10 &&
      closeFromOpen >= 25;

    if (explosive) {
      explosiveDays252 += 1;
      if (i >= hist.length - 63) explosiveDays63 += 1;
    }

    if (haltProxy) {
      haltProxyDays252 += 1;
      if (i >= hist.length - 63) haltProxyDays63 += 1;
    }
  });

  return {
    explosiveDays252,
    explosiveDays63,
    haltProxyDays252,
    haltProxyDays63,
    maxExpansion252,
    maxVolShock252,
    formerRunnerFlag: explosiveDays252 >= 1 || haltProxyDays252 >= 1
  };
}

function buildRotationMetrics(structuralMetrics, ignitionMetrics, preMetrics) {
  const price = safeNum(structuralMetrics.price, 0);
  const marketCap = safeNum(structuralMetrics.marketCap, 0);
  const explicitFloat = safeNum(structuralMetrics.floatShares, 0);

  let floatBase = explicitFloat;
  let usingProxyFloat = false;

  if (!floatBase && marketCap > 0 && price > 0) {
    floatBase = marketCap / price;
    usingProxyFloat = true;
  }

  const prevTurnover =
    floatBase > 0
      ? safeNum(ignitionMetrics.prevDollarVol, 0) /
        Math.max(marketCap || (floatBase * price), 1)
      : 0;

  const preTurnover =
    floatBase > 0
      ? safeNum(preMetrics.preDollarVol, 0) /
        Math.max(marketCap || (floatBase * safeNum(preMetrics.price, price)), 1)
      : 0;

  const volumeFloatRotation =
    floatBase > 0 ? safeNum(ignitionMetrics.prevVolRatio, 0) * 0.25 : 0;

  const proxyRotationComposite =
    0.45 * safeNum(ignitionMetrics.prevVolRatio, 0) +
    0.40 * safeNum(ignitionMetrics.prevDollarShock, 0) +
    0.15 * safeNum(preMetrics.preVolRatio, 0);

  return {
    floatBase,
    usingProxyFloat,
    prevTurnover,
    preTurnover,
    volumeFloatRotation,
    proxyRotationComposite
  };
}

function computeCumulativePremarketVolumeForDate(minuteBars, dateStr, cutoffTime = "09:25:00") {
  const dayBars = getBarsForDate(minuteBars, dateStr);
  const preBars = filterBarsByTime(dayBars, "04:00:00", cutoffTime);
  return preBars.reduce((sum, b) => sum + safeNum(b.v, 0), 0);
}

function computeSameTimePremarketBaseline(minuteBars, priorDates, cutoffTime = "09:25:00") {
  const vols = [];
  for (const d of priorDates || []) {
    const v = computeCumulativePremarketVolumeForDate(minuteBars, d, cutoffTime);
    if (v > 0) vols.push(v);
  }
  return {
    baselineMedian: vols.length ? median(vols) : 0,
    baselineAvg: vols.length ? avg(vols) : 0,
    samples: vols.length
  };
}

function buildPremarketMetrics(minuteBars, tradeDate, cutoffTime, prevClose, prevHigh) {
  const dayBars = getBarsForDate(minuteBars, tradeDate);
  const preBars = filterBarsByTime(dayBars, "04:00:00", cutoffTime);

  if (!preBars.length) {
    return {
      source: "NONE",
      price: null,
      gapPct: null,
      preVol: 0,
      preVWAP: null,
      holdQuality: null,
      preRangePct: null,
      preVolRatio: 0,
      preDollarVol: 0,
      abovePreVWAP: false,
      abovePrevHigh: false
    };
  }

  const preLast = safeNum(preBars[preBars.length - 1].c, null);
  const preHigh = maxOf(preBars.map((b) => safeNum(b.h, 0)));
  const preLow = minOf(preBars.map((b) => safeNum(b.l, 0)));
  const preVol = preBars.reduce((s, b) => s + safeNum(b.v, 0), 0);
  const preVWAP = computeVWAP(preBars);
  const holdQuality =
    preHigh > preLow ? ((preLast - preLow) / (preHigh - preLow)) * 100 : 50;
  const preRangePct = computeRangePct(preHigh, preLow, preLast);
  const gapPct = prevClose > 0 ? ((preLast - prevClose) / prevClose) * 100 : null;
  const preDollarVol = preLast != null ? preLast * preVol : 0;
  const abovePreVWAP = preVWAP != null && preLast != null ? preLast > preVWAP : false;
  const abovePrevHigh = prevHigh > 0 && preLast != null ? preLast > prevHigh : false;

  return {
    source: "REAL_PREMARKET",
    price: preLast,
    gapPct,
    preVol,
    preVWAP,
    holdQuality,
    preRangePct,
    preVolRatio: 0,
    preDollarVol,
    abovePreVWAP,
    abovePrevHigh
  };
}

function scoreStructural(m, flags) {
  let score = 0;
  const notes = [];

  if (flags.manualReject || m.manualReject) {
    notes.push("Manuel reject");
    return { score: 0, notes, hardReject: true };
  }

  if (flags.otcRisk) {
    notes.push("OTC riski");
    return { score: 0, notes, hardReject: true };
  }

  if (m.price < 0.10) {
    notes.push("0.10 altı fiyat");
    return { score: 0, notes, hardReject: true };
  }

  if (flags.recentReverseSplit || m.recentReverseSplit) {
    notes.push("Yakın reverse split");
    return { score: 0, notes, hardReject: true };
  }

  if (m.likelyReverseSplitProxy) {
    notes.push("Muhtemel reverse split proxy");
    return { score: 0, notes, hardReject: true };
  }

  if (m.price > 12 && !flags.allowAbove8) {
    notes.push("12 dolar üstü");
    return { score: 0, notes, hardReject: true };
  }

  if (m.price >= 0.10 && m.price <= 1) score += 22;
  else if (m.price > 1 && m.price <= 3) score += 18;
  else if (m.price > 3 && m.price <= 5) score += 12;
  else if (m.price > 5 && m.price <= 8) score += 6;
  else if (m.price > 8 && m.price <= 12) {
    score += 2;
    notes.push("8-12 bandı");
  }

  if (m.marketCap != null && m.marketCap > 0) {
    if (m.marketCap >= 5000000 && m.marketCap <= 120000000) score += 12;
    else if (m.marketCap > 120000000 && m.marketCap <= 300000000) score += 6;
    else if (m.marketCap < 3000000) {
      score -= 12;
      notes.push("Aşırı küçük market cap");
    } else if (m.marketCap > 600000000) {
      score -= 8;
      notes.push("Market cap büyük");
    }
  }

  if (m.drawdown90 >= -90 && m.drawdown90 <= -30) score += 14;
  else if (m.drawdown90 > -30 && m.drawdown90 <= -5) score += 6;
  else if (m.drawdown90 < -95) {
    score -= 6;
    notes.push("Aşırı çökmüş");
  }

  if (m.drawdown252 <= -45 && m.drawdown252 >= -97) {
    score += 10;
    notes.push("Long-term collapse");
  }

  if (m.baseTightness10 >= 3 && m.baseTightness10 <= 28) score += 16;
  else if (m.baseTightness10 > 28 && m.baseTightness10 <= 45) score += 8;
  else if (m.baseTightness10 > 70) {
    score -= 8;
    notes.push("Base gevşek");
  }

  if (m.reboundFrom30Low >= 5 && m.reboundFrom30Low <= 140) score += 8;
  else if (m.reboundFrom30Low > 220) {
    score -= 5;
    notes.push("Rebound fazla uzamış");
  }

  if (m.breakout20) {
    score += 10;
    notes.push("20g high reclaim");
  }

  if (flags.recentDeficiency) {
    score -= 6;
    notes.push("Deficiency notice");
  }

  if (m.cleanRecoveryPreferred) {
    score += 8;
    notes.push("Clean recovery preferansı");
  }

  score = clamp(Math.round(score), 0, 100);
  return { score, notes, hardReject: false };
}

function scoreIgnition(m) {
  let score = 0;
  const notes = [];

  if (m.prevDayRet >= 4 && m.prevDayRet < 15) score += 14;
  else if (m.prevDayRet >= 15 && m.prevDayRet < 45) score += 20;
  else if (m.prevDayRet >= 45 && m.prevDayRet < 120) score += 14;
  else if (m.prevDayRet >= 120 && m.prevDayRet < 220) {
    score += 6;
    notes.push("Aşırı sıcak ilk gün");
  } else if (m.prevDayRet >= 220) {
    score -= 4;
    notes.push("Mania spike");
  } else if (m.prevDayRet < 0) {
    score -= 12;
  }

  if (m.prevVolRatio >= 1.5 && m.prevVolRatio < 3) score += 10;
  else if (m.prevVolRatio >= 3 && m.prevVolRatio < 10) score += 18;
  else if (m.prevVolRatio >= 10 && m.prevVolRatio < 40) {
    score += 20;
    notes.push("Vol shock");
  } else if (m.prevVolRatio >= 40) {
    score += 8;
    notes.push("Aşırı hacim / mania");
  } else if (m.prevVolRatio < 0.8) {
    score -= 8;
  }

  if (m.prevDollarShock >= 1.5 && m.prevDollarShock < 4) score += 10;
  else if (m.prevDollarShock >= 4 && m.prevDollarShock < 15) score += 18;
  else if (m.prevDollarShock >= 15 && m.prevDollarShock < 60) {
    score += 20;
    notes.push("Dollar shock");
  } else if (m.prevDollarShock >= 60) {
    score += 8;
    notes.push("Aşırı dollar shock");
  } else if (m.prevDollarShock < 0.8) {
    score -= 8;
  }

  if (m.prevCloseStrength >= 80) score += 16;
  else if (m.prevCloseStrength >= 65) score += 10;
  else if (m.prevCloseStrength < 45) {
    score -= 12;
    notes.push("Weak close");
  }

  if (m.prevDollarVol >= 150000 && m.prevDollarVol < 600000) score += 8;
  else if (m.prevDollarVol >= 600000 && m.prevDollarVol < 3000000) score += 14;
  else if (m.prevDollarVol >= 3000000 && m.prevDollarVol < 30000000) score += 18;
  else if (m.prevDollarVol >= 30000000) {
    score += 6;
    notes.push("Çok kalabalık tape");
  } else if (m.prevDollarVol < 50000) {
    score -= 12;
    notes.push("Dollar vol zayıf");
  }

  if (m.rangeExpansion >= 1.3 && m.rangeExpansion < 2.5) score += 8;
  else if (m.rangeExpansion >= 2.5 && m.rangeExpansion < 6) score += 10;
  else if (m.rangeExpansion >= 6) {
    score -= 6;
    notes.push("Aşırı range expansion");
  }

  if (m.catalystFresh && m.catalystGrade > 0) {
    score += Math.min(m.catalystGrade, 18);
    notes.push(`Fresh catalyst: ${m.catalystType}`);
  } else if (m.catalystGrade < 0) {
    score += m.catalystGrade;
    notes.push(`Negative catalyst: ${m.catalystType}`);
  }

  if (m.narrativePivot && !m.financingRisk && m.prevCloseStrength >= 70) {
    score += 2;
    notes.push("Narrative pivot");
  }

  if (m.financingRisk) {
    score -= 14;
    notes.push("Financing/dilution riski");
  }

  if (m.collapseTrap) {
    score -= 30;
    notes.push("Collapse rebound trap");
  }

  if (m.fadeRisk) {
    score -= 10;
    notes.push("Fade riski");
  }

  score = clamp(Math.round(score), 0, 100);
  return { score, notes };
}

function scoreFormerRunner(m) {
  let score = 0;
  const notes = [];

  if (m.explosiveDays252 >= 1) {
    score += 18;
    notes.push("Former runner");
  }
  if (m.explosiveDays252 >= 2) score += 8;

  if (m.explosiveDays63 >= 1) {
    score += 18;
    notes.push("Recent explosive history");
  }
  if (m.haltProxyDays252 >= 1) {
    score += 14;
    notes.push("Halt proxy history");
  }
  if (m.haltProxyDays63 >= 1) score += 12;

  if (m.maxExpansion252 >= 80 && m.maxExpansion252 < 150) score += 8;
  else if (m.maxExpansion252 >= 150) score += 12;

  if (m.maxVolShock252 >= 8 && m.maxVolShock252 < 20) score += 8;
  else if (m.maxVolShock252 >= 20) score += 12;

  score = clamp(Math.round(score), 0, 100);
  return { score, notes };
}


function computeCleanRecoveryScore(structuralMetrics, ignitionMetrics, flags, bestFamilyHint = null) {
  let score = 0;
  const notes = [];

  if (!flags.recentReverseSplit && !safeNum(structuralMetrics.likelyReverseSplitProxy ? 1 : 0, 0)) score += 28;
  else notes.push("Split riski");

  if (flags.cleanRecoveryPreferred) {
    score += 12;
    notes.push("Clean recovery tercih");
  }

  if (structuralMetrics.drawdown252 <= -45 && structuralMetrics.drawdown252 >= -97) {
    score += 18;
    notes.push("Derin ama temiz çöküş");
  } else if (structuralMetrics.drawdown252 > -25) {
    score -= 8;
  }

  if (structuralMetrics.baseTightness10 >= 3 && structuralMetrics.baseTightness10 <= 28) score += 15;
  else if (structuralMetrics.baseTightness10 > 45) score -= 10;

  if (structuralMetrics.breakout20) score += 10;
  if (ignitionMetrics.prevCloseStrength >= 72) score += 10;
  if (ignitionMetrics.prevDayRet >= 5 && ignitionMetrics.prevDayRet <= 80) score += 8;
  if (ignitionMetrics.prevVolRatio >= 2 && ignitionMetrics.prevVolRatio <= 20) score += 8;

  if (ignitionMetrics.financingRisk) {
    score -= 18;
    notes.push("Financing riski");
  }
  if (ignitionMetrics.collapseTrap) {
    score -= 28;
    notes.push("Collapse rebound trap");
  }
  if (ignitionMetrics.fadeRisk) {
    score -= 10;
    notes.push("Fade riski");
  }
  if (bestFamilyHint === "NARRATIVE_PIVOT" && !flags.narrativePivot) {
    score -= 18;
    notes.push("Auto narrative pivot: continuation için temkin");
  }

  return { score: clamp(Math.round(score), 0, 100), notes };
}

function scoreRotation(m, source) {
  let score = 0;
  const notes = [];

  if (m.floatBase > 0) {
    if (m.usingProxyFloat) notes.push("Proxy float");
    else notes.push("Known float");

    if (m.prevTurnover >= 0.2 && m.prevTurnover < 0.5) score += 10;
    else if (m.prevTurnover >= 0.5 && m.prevTurnover < 1.0) score += 18;
    else if (m.prevTurnover >= 1.0) {
      score += 24;
      notes.push("Prev turnover strong");
    }

    if (source === "REAL_PREMARKET") {
      if (m.preTurnover >= 0.05 && m.preTurnover < 0.15) score += 10;
      else if (m.preTurnover >= 0.15 && m.preTurnover < 0.35) score += 18;
      else if (m.preTurnover >= 0.35) {
        score += 24;
        notes.push("Premarket turnover strong");
      }
    }

    if (m.volumeFloatRotation >= 1 && m.volumeFloatRotation < 2.5) score += 8;
    else if (m.volumeFloatRotation >= 2.5) score += 12;
  } else {
    if (m.proxyRotationComposite >= 4 && m.proxyRotationComposite < 10) score += 8;
    else if (m.proxyRotationComposite >= 10 && m.proxyRotationComposite < 20) score += 16;
    else if (m.proxyRotationComposite >= 20) {
      score += 22;
      notes.push("Rotation proxy strong");
    }
  }

  score = clamp(Math.round(score), 0, 100);
  return { score, notes };
}

function scorePremarket(m, feed) {
  let score = 0;
  const notes = [];

  if (m.source !== "REAL_PREMARKET") {
    notes.push("Premarket veri yok");
    return { score: 0, notes, hardReject: false };
  }

  if (feed === "sip") score += 3;
  else notes.push("IEX feed");

  if (m.gapPct >= 3 && m.gapPct < 15) score += 10;
  else if (m.gapPct >= 15 && m.gapPct < 60) score += 18;
  else if (m.gapPct >= 60 && m.gapPct < 180) {
    score += 10;
    notes.push("Aşırı sıcak gap");
  } else if (m.gapPct < 0) {
    score -= 10;
  }

  if (m.preVolRatio >= 0.8 && m.preVolRatio < 2) score += 10;
  else if (m.preVolRatio >= 2 && m.preVolRatio < 8) score += 18;
  else if (m.preVolRatio >= 8) {
    score += 22;
    notes.push("Premarket vol shock");
  } else if (m.preVolRatio < 0.4) {
    score -= 8;
  }

  if (m.preDollarVol >= 150000 && m.preDollarVol < 600000) score += 10;
  else if (m.preDollarVol >= 600000 && m.preDollarVol < 3000000) score += 16;
  else if (m.preDollarVol >= 3000000) score += 20;
  else if (m.preDollarVol < 75000) {
    score -= 12;
    notes.push("Premarket dollar vol zayıf");
  }

  if (m.holdQuality >= 80) score += 18;
  else if (m.holdQuality >= 65) score += 10;
  else if (m.holdQuality < 45) {
    score -= 12;
    notes.push("Hold zayıf");
  }

  if (m.abovePreVWAP) score += 14;
  else {
    score -= 12;
    notes.push("VWAP altı");
  }

  if (m.abovePrevHigh) {
    score += 8;
    notes.push("Prev high reclaim");
  }

  if (m.preRangePct != null && m.preRangePct > 65) {
    score -= 8;
    notes.push("Premarket range çok geniş");
  }

  if (
    feed === "iex" &&
    m.preVolRatio < 0.8 &&
    m.preDollarVol >= 400000 &&
    m.holdQuality >= 80 &&
    m.abovePreVWAP
  ) {
    score += 6;
    notes.push("IEX tolerance");
  }

  score = clamp(Math.round(score), 0, 100);

  const hardReject =
    m.source === "REAL_PREMARKET" &&
    (!m.abovePreVWAP || safeNum(m.holdQuality, 0) < 45 || safeNum(m.preDollarVol, 0) < 50000);

  return { score, notes, hardReject };
}

function bandSimilarity(value, idealLow, idealHigh, hardLow, hardHigh) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return 0;
  value = Number(value);

  if (value >= idealLow && value <= idealHigh) return 1;
  if (value < hardLow || value > hardHigh) return 0;

  if (value < idealLow) return (value - hardLow) / (idealLow - hardLow);
  return (hardHigh - value) / (hardHigh - idealHigh);
}

function boolSimilarity(value, idealLow, idealHigh, hardLow, hardHigh) {
  return bandSimilarity(value ? 1 : 0, idealLow, idealHigh, hardLow, hardHigh);
}

function computeFamilyFits(featureSet) {
  const fits = FAMILY_MODELS.map((family) => {
    let weighted = 0;
    let totalWeight = 0;

    for (const [key, cfg] of Object.entries(family.features)) {
      const val = featureSet[key];
      const hasValue =
        typeof val === "boolean" ||
        (val !== null && val !== undefined && Number.isFinite(Number(val)));

      if (!hasValue) continue;

      const sim = typeof val === "boolean"
        ? boolSimilarity(val, cfg.ideal[0], cfg.ideal[1], cfg.hard[0], cfg.hard[1])
        : bandSimilarity(val, cfg.ideal[0], cfg.ideal[1], cfg.hard[0], cfg.hard[1]);

      weighted += sim * cfg.weight;
      totalWeight += cfg.weight;
    }

    return {
      name: family.name,
      score: totalWeight > 0 ? Math.round((weighted / totalWeight) * 100) : 0
    };
  }).sort((a, b) => b.score - a.score);

  return {
    bestFamily: fits[0]?.name || null,
    familyScore: fits[0]?.score || 0,
    topFamilies: fits.slice(0, 3)
  };
}

function buildEntryPlan(decision, source, price, preVWAP, prevHigh) {
  if (!["SUPERNOVA_CONFIRMED", "GÜÇLÜ AL", "AL", "MANUAL_CONFIRM_REQUIRED", "SPARSE_PREMARKET", "WATCH_TRIGGER", "İZLE"].includes(decision)) {
    return {
      entryType: "NO_TRADE",
      entryIdea: null,
      stop: null,
      tp1: null,
      tp2: null
    };
  }

  const reclaim = prevHigh > 0 ? prevHigh * 1.01 : null;
  const vwapEntry = preVWAP != null ? preVWAP * 1.01 : null;

  let entryIdea = null;
  let entryType = "WATCH";

  if (source !== "REAL_PREMARKET") {
    entryIdea = reclaim;
    entryType = "WATCH_RECLAIM";
  } else {
    entryIdea = Math.max(safeNum(reclaim, 0), safeNum(vwapEntry, 0));

    if (price != null && entryIdea != null && entryIdea > 0) {
      const distance = ((price - entryIdea) / entryIdea) * 100;
      if (distance <= 4) entryType = "BUY_NEAR_ENTRY";
      else if (distance <= 12) entryType = "WAIT_RETEST";
      else entryType = "TOO_EXTENDED";
    }
  }

  if (!entryIdea || !Number.isFinite(entryIdea) || entryIdea <= 0) {
    return {
      entryType,
      entryIdea: null,
      stop: null,
      tp1: null,
      tp2: null
    };
  }

  return {
    entryType,
    entryIdea: roundSmart(entryIdea),
    stop: roundSmart(entryIdea * 0.88),
    tp1: roundSmart(entryIdea * 1.20),
    tp2: roundSmart(entryIdea * 1.50)
  };
}


function getConfirmTier(entryPrice) {
  const p = safeNum(entryPrice, 0);
  return CONFIRM_VOLUME_TIERS.find((tier) => p <= tier.maxPrice) || CONFIRM_VOLUME_TIERS[CONFIRM_VOLUME_TIERS.length - 1];
}

function buildConfirmProfile({ entryIdea, bestFamily }) {
  const entry = safeNum(entryIdea, null);
  if (entry == null || !Number.isFinite(entry) || entry <= 0) {
    return {
      triggerZoneLow: null,
      triggerZoneHigh: null,
      confirmAbove: null,
      strongConfirmAbove: null,
      failBelow: null,
      minPremarketVolume: null,
      betterPremarketVolume: null,
      minPremarketDollarVolume: null,
      betterPremarketDollarVolume: null,
      sparseVolume: null,
      sparseDollarVolume: null,
      minHoldQuality: CONFIRM_RULES.minHoldQuality,
      strongHoldQuality: CONFIRM_RULES.strongHoldQuality
    };
  }

  const tier = getConfirmTier(entry);
  const familyVolumeBoost =
    bestFamily === "NARRATIVE_PIVOT" ? 1.20 :
    bestFamily === "CLEAN_RECOVERY" ? 0.95 : 1;
  const strongMultiplier =
    bestFamily === "NARRATIVE_PIVOT" ? 1.03 :
    bestFamily === "CLEAN_RECOVERY" ? 1.015 : 1.02;

  return {
    triggerZoneLow: roundSmart(entry * CONFIRM_RULES.nearTolerance),
    triggerZoneHigh: roundSmart(entry),
    confirmAbove: roundSmart(entry),
    strongConfirmAbove: roundSmart(entry * strongMultiplier),
    failBelow: roundSmart(entry * tier.failBelowMultiplier),
    minPremarketVolume: Math.round(tier.minPremarketVolume * familyVolumeBoost),
    betterPremarketVolume: Math.round(tier.betterPremarketVolume * familyVolumeBoost),
    minPremarketDollarVolume: Math.round(tier.minPremarketDollarVolume * familyVolumeBoost),
    betterPremarketDollarVolume: Math.round(tier.betterPremarketDollarVolume * familyVolumeBoost),
    sparseVolume: Math.max(5000, Math.round(tier.minPremarketVolume * 0.25)),
    sparseDollarVolume: Math.max(25000, Math.round(tier.minPremarketDollarVolume * 0.25)),
    minHoldQuality: CONFIRM_RULES.minHoldQuality,
    strongHoldQuality: CONFIRM_RULES.strongHoldQuality
  };
}

function isNightlyWatchEligible({
  structuralScore,
  ignitionScore,
  formerRunnerScore,
  familyScore,
  supernovaScore,
  cleanRecoveryScore,
  bestFamily,
  explicitNarrativePivot
}) {
  if (bestFamily === "NARRATIVE_PIVOT" && !explicitNarrativePivot) return false;
  return (
    structuralScore >= 40 &&
    ignitionScore >= 42 &&
    formerRunnerScore >= 16 &&
    familyScore >= 56 &&
    supernovaScore >= 58 &&
    cleanRecoveryScore >= 45
  );
}


function buildMorningConfirmation({
  baseDecision,
  setupEligible,
  pre,
  entryIdea,
  confirmProfile
}) {
  const price = safeNum(pre.price, null);
  const hasPrice = price != null && Number.isFinite(price);
  const triggerHit = hasPrice && confirmProfile.confirmAbove != null && price >= confirmProfile.confirmAbove;
  const triggerNear = hasPrice && confirmProfile.triggerZoneLow != null && price >= confirmProfile.triggerZoneLow;
  const strongPriceConfirm = hasPrice && confirmProfile.strongConfirmAbove != null && price >= confirmProfile.strongConfirmAbove;
  const failBelow = hasPrice && confirmProfile.failBelow != null && price < confirmProfile.failBelow;

  const liquidityStrong =
    safeNum(pre.preVol, 0) >= safeNum(confirmProfile.betterPremarketVolume, Number.POSITIVE_INFINITY) ||
    safeNum(pre.preDollarVol, 0) >= safeNum(confirmProfile.betterPremarketDollarVolume, Number.POSITIVE_INFINITY);
  const liquidityOk =
    safeNum(pre.preVol, 0) >= safeNum(confirmProfile.minPremarketVolume, Number.POSITIVE_INFINITY) ||
    safeNum(pre.preDollarVol, 0) >= safeNum(confirmProfile.minPremarketDollarVolume, Number.POSITIVE_INFINITY);
  const liquiditySparse =
    safeNum(pre.preVol, 0) >= safeNum(confirmProfile.sparseVolume, Number.POSITIVE_INFINITY) ||
    safeNum(pre.preDollarVol, 0) >= safeNum(confirmProfile.sparseDollarVolume, Number.POSITIVE_INFINITY);

  const qualityOk = !!pre.abovePreVWAP && safeNum(pre.holdQuality, 0) >= safeNum(confirmProfile.minHoldQuality, CONFIRM_RULES.minHoldQuality);
  const qualityStrong = !!pre.abovePreVWAP && safeNum(pre.holdQuality, 0) >= safeNum(confirmProfile.strongHoldQuality, CONFIRM_RULES.strongHoldQuality);

  let confirmScore = 0;
  if (strongPriceConfirm) confirmScore += 50;
  else if (triggerHit) confirmScore += 40;
  else if (triggerNear) confirmScore += 18;
  if (liquidityStrong) confirmScore += 28;
  else if (liquidityOk) confirmScore += 20;
  else if (liquiditySparse) confirmScore += 10;
  if (qualityStrong) confirmScore += 17;
  else if (qualityOk) confirmScore += 10;
  if (pre.abovePrevHigh) confirmScore += 5;
  if (failBelow) confirmScore -= 20;
  confirmScore = clamp(Math.round(confirmScore), 0, 100);

  if (pre.source !== "REAL_PREMARKET") {
    return {
      decision: setupEligible ? "MANUAL_CONFIRM_REQUIRED" : baseDecision,
      confirmStatus: "NO_PREMARKET_CONFIRM",
      confirmScore,
      triggerHit,
      triggerNear,
      notes: setupEligible
        ? ["Premarket teyidi yok: manuel kontrol gerekli"]
        : ["Premarket teyidi yok"]
    };
  }

  if (failBelow) {
    return {
      decision: setupEligible ? "İZLE" : baseDecision,
      confirmStatus: "FAIL_BELOW",
      confirmScore,
      triggerHit,
      triggerNear,
      notes: ["Fiyat fail below altında", "Kurulum zayıfladı"]
    };
  }

  if (triggerHit && liquidityStrong && qualityStrong) {
    return {
      decision: "SUPERNOVA_CONFIRMED",
      confirmStatus: "CONFIRMED",
      confirmScore,
      triggerHit,
      triggerNear,
      notes: ["Entry tetiklendi", "Premarket likiditesi güçlü", "VWAP üstünde teyit"]
    };
  }

  if (triggerHit && liquidityOk && qualityOk) {
    return {
      decision: "AL",
      confirmStatus: "TRIGGER_OK",
      confirmScore,
      triggerHit,
      triggerNear,
      notes: ["Entry tetiklendi", "Likidite yeterli"]
    };
  }

  if (setupEligible && (triggerNear || liquiditySparse)) {
    return {
      decision: "SPARSE_PREMARKET",
      confirmStatus: "SPARSE_DATA",
      confirmScore,
      triggerHit,
      triggerNear,
      notes: ["Kurulum korunuyor", "Veri/hacim sınırlı", "Confirmed deme"]
    };
  }

  return {
    decision: baseDecision,
    confirmStatus: "NO_TRIGGER",
    confirmScore,
    triggerHit,
    triggerNear,
    notes: ["Tetik gelmedi"]
  };
}

function computeSupernovaScoreNightly(structuralScore, ignitionScore, formerRunnerScore, rotationScore, familyScore) {
  return roundSmart(
    0.20 * structuralScore +
    0.25 * ignitionScore +
    0.25 * formerRunnerScore +
    0.15 * rotationScore +
    0.15 * familyScore
  );
}

function computeSupernovaScoreLive(structuralScore, ignitionScore, formerRunnerScore, rotationScore, premarketScore, familyScore) {
  return roundSmart(
    0.15 * structuralScore +
    0.20 * ignitionScore +
    0.20 * formerRunnerScore +
    0.15 * rotationScore +
    0.15 * premarketScore +
    0.15 * familyScore
  );
}

function finalNightlyDecision({
  structuralScore,
  ignitionScore,
  formerRunnerScore,
  familyScore,
  supernovaScore,
  cleanRecoveryScore,
  bestFamily,
  explicitNarrativePivot
}) {
  if (bestFamily === "NARRATIVE_PIVOT" && !explicitNarrativePivot) return "ALMA";
  if (
    structuralScore >= 40 &&
    ignitionScore >= 42 &&
    formerRunnerScore >= 16 &&
    familyScore >= 56 &&
    supernovaScore >= 58 &&
    cleanRecoveryScore >= 45
  ) {
    return "İZLE";
  }
  return "ALMA";
}

function finalLiveDecision({
  structuralScore,
  ignitionScore,
  formerRunnerScore,
  rotationScore,
  premarketScore,
  familyScore,
  supernovaScore,
  source,
  cleanRecoveryScore,
  bestFamily,
  explicitNarrativePivot
}) {
  if (bestFamily === "NARRATIVE_PIVOT" && !explicitNarrativePivot) {
    return source === "REAL_PREMARKET" ? "MANUAL_CONFIRM_REQUIRED" : "ALMA";
  }

  if (source !== "REAL_PREMARKET") {
    if (
      structuralScore >= 40 &&
      ignitionScore >= 42 &&
      formerRunnerScore >= 16 &&
      familyScore >= 56 &&
      supernovaScore >= 58 &&
      cleanRecoveryScore >= 45
    ) return "İZLE";
    return "ALMA";
  }

  if (
    structuralScore >= 42 &&
    ignitionScore >= 50 &&
    formerRunnerScore >= 18 &&
    rotationScore >= 10 &&
    premarketScore >= 64 &&
    familyScore >= 60 &&
    supernovaScore >= 66 &&
    cleanRecoveryScore >= 52
  ) return "SUPERNOVA_CONFIRMED";

  if (
    structuralScore >= 40 &&
    ignitionScore >= 44 &&
    formerRunnerScore >= 16 &&
    premarketScore >= 52 &&
    familyScore >= 56 &&
    supernovaScore >= 58 &&
    cleanRecoveryScore >= 45
  ) return "AL";

  if (
    structuralScore >= 40 &&
    ignitionScore >= 42 &&
    formerRunnerScore >= 16 &&
    familyScore >= 56 &&
    supernovaScore >= 58 &&
    cleanRecoveryScore >= 45
  ) return "İZLE";

  return "ALMA";
}

function buildNightlyRocketRow({ symbol, dailyBars, referenceTradeDate }) {
  const flags = getFlags(symbol);
  const ctx = buildDailyHistoryContext(dailyBars, referenceTradeDate);
  if (!ctx) return null;

  const structuralMetrics = buildStructuralMetrics(ctx, flags);
  const ignitionMetrics = buildIgnitionMetrics(ctx, flags);
  const formerRunnerMetrics = buildFormerRunnerMetrics(ctx);

  const structural = scoreStructural(structuralMetrics, flags);
  const ignition = scoreIgnition(ignitionMetrics);
  const formerRunner = scoreFormerRunner(formerRunnerMetrics);

  const dummyPre = {
    source: "NONE",
    price: null,
    preDollarVol: 0
  };

  const rotationMetrics = buildRotationMetrics(structuralMetrics, ignitionMetrics, dummyPre);
  const rotation = scoreRotation(rotationMetrics, "NONE");

  const family = computeFamilyFits({
    price: structuralMetrics.price,
    drawdown90: structuralMetrics.drawdown90,
    baseTightness10: structuralMetrics.baseTightness10,
    prevDayRet: ignitionMetrics.prevDayRet,
    prevVolRatio: ignitionMetrics.prevVolRatio,
    prevDollarShock: ignitionMetrics.prevDollarShock,
    prevCloseStrength: ignitionMetrics.prevCloseStrength,
    breakout20: !!structuralMetrics.breakout20,
    drawdown252: structuralMetrics.drawdown252,
    noReverseSplit: !(flags.recentReverseSplit || structuralMetrics.likelyReverseSplitProxy)
  });
  const cleanRecovery = computeCleanRecoveryScore(structuralMetrics, ignitionMetrics, flags, family.bestFamily);

  const supernovaScore = computeSupernovaScoreNightly(
    structural.score,
    ignition.score,
    formerRunner.score,
    rotation.score,
    family.familyScore
  );

  const confirmProfile = buildConfirmProfile({
    entryIdea: buildEntryPlan("İZLE", "NONE", null, null, safeNum(ctx.last.h, 0)).entryIdea,
    bestFamily: family.bestFamily
  });

  const decision = structural.hardReject
    ? "ALMA"
    : finalNightlyDecision({
        structuralScore: structural.score,
        ignitionScore: ignition.score,
        formerRunnerScore: formerRunner.score,
        familyScore: family.familyScore,
        supernovaScore
      });

  const plan = buildEntryPlan(decision, "NONE", null, null, safeNum(ctx.last.h, 0));

  return {
    symbol,
    decision,
    supernovaScore,
    structuralScore: structural.score,
    ignitionScore: ignition.score,
    formerRunnerScore: formerRunner.score,
    rotationScore: rotation.score,
    premarketScore: 0,
    cleanRecoveryScore: cleanRecovery.score,
    familyScore: family.familyScore,
    bestFamily: family.bestFamily,
    familyMatches: family.topFamilies.map((x) => `${x.name}:${x.score}`).join(" | "),

    source: "NONE",
    price: null,
    prevClose: roundSmart(ctx.last.c),
    prevHigh: roundSmart(ctx.last.h),

    gapPct: null,
    preVol: 0,
    preVolBaselineMedian: 0,
    preVolRatio: 0,
    preDollarVol: 0,
    preVWAP: null,
    abovePreVWAP: false,
    holdQuality: null,
    preRangePct: null,
    abovePrevHigh: false,

    drawdown90: roundSmart(structuralMetrics.drawdown90),
    reboundFrom30Low: roundSmart(structuralMetrics.reboundFrom30Low),
    baseTightness10: roundSmart(structuralMetrics.baseTightness10),
    breakout20: structuralMetrics.breakout20 ? "YES" : "NO",

    prevDayRet: roundSmart(ignitionMetrics.prevDayRet),
    prevCloseStrength: roundSmart(ignitionMetrics.prevCloseStrength),
    prevVolRatio: roundSmart(ignitionMetrics.prevVolRatio),
    prevDollarVol: Math.round(ignitionMetrics.prevDollarVol || 0),
    prevDollarShock: roundSmart(ignitionMetrics.prevDollarShock),
    rangeExpansion: roundSmart(ignitionMetrics.rangeExpansion),

    explosiveDays252: formerRunnerMetrics.explosiveDays252,
    explosiveDays63: formerRunnerMetrics.explosiveDays63,
    haltProxyDays252: formerRunnerMetrics.haltProxyDays252,
    haltProxyDays63: formerRunnerMetrics.haltProxyDays63,
    maxExpansion252: roundSmart(formerRunnerMetrics.maxExpansion252),

    prevTurnover: roundSmart(rotationMetrics.prevTurnover),
    preTurnover: roundSmart(rotationMetrics.preTurnover),

    entryType: plan.entryType,
    entryIdea: plan.entryIdea,
    stop: plan.stop,
    tp1: plan.tp1,
    tp2: plan.tp2,

    triggerZoneLow: confirmProfile.triggerZoneLow,
    triggerZoneHigh: confirmProfile.triggerZoneHigh,
    confirmAbove: confirmProfile.confirmAbove,
    strongConfirmAbove: confirmProfile.strongConfirmAbove,
    failBelow: confirmProfile.failBelow,
    minPremarketVolume: confirmProfile.minPremarketVolume,
    betterPremarketVolume: confirmProfile.betterPremarketVolume,
    minPremarketDollarVolume: confirmProfile.minPremarketDollarVolume,
    betterPremarketDollarVolume: confirmProfile.betterPremarketDollarVolume,

    notes: [
      ...structural.notes,
      ...ignition.notes,
      ...formerRunner.notes,
      ...cleanRecovery.notes,
      ...rotation.notes
    ].join(" | ")
  };
}

function buildFullRocketRow({ symbol, dailyBars, minuteBars, tradeDate, cutoffTime }) {
  const flags = getFlags(symbol);
  const ctx = buildDailyHistoryContext(dailyBars, tradeDate);
  if (!ctx) return null;

  const structuralMetrics = buildStructuralMetrics(ctx, flags);
  const ignitionMetrics = buildIgnitionMetrics(ctx, flags);
  const formerRunnerMetrics = buildFormerRunnerMetrics(ctx);

  const structural = scoreStructural(structuralMetrics, flags);
  const ignition = scoreIgnition(ignitionMetrics);
  const formerRunner = scoreFormerRunner(formerRunnerMetrics);

  const baseline = computeSameTimePremarketBaseline(minuteBars, ctx.priorDates, cutoffTime);
  const pre = buildPremarketMetrics(
    minuteBars,
    tradeDate,
    cutoffTime,
    safeNum(ctx.last.c, 0),
    safeNum(ctx.last.h, 0)
  );

  const preVolRatio =
    pre.preVol > 0 && baseline.baselineMedian > 0
      ? pre.preVol / baseline.baselineMedian
      : 0;

  const preMetrics = { ...pre, preVolRatio };
  const premarket = scorePremarket(preMetrics, ALPACA_FEED);

  const rotationMetrics = buildRotationMetrics(structuralMetrics, ignitionMetrics, preMetrics);
  const rotation = scoreRotation(rotationMetrics, pre.source);

  const family = computeFamilyFits({
    price: structuralMetrics.price,
    drawdown90: structuralMetrics.drawdown90,
    baseTightness10: structuralMetrics.baseTightness10,
    prevDayRet: ignitionMetrics.prevDayRet,
    prevVolRatio: ignitionMetrics.prevVolRatio,
    prevDollarShock: ignitionMetrics.prevDollarShock,
    prevCloseStrength: ignitionMetrics.prevCloseStrength,
    breakout20: !!structuralMetrics.breakout20,
    gapPct: preMetrics.gapPct,
    preVolRatio: preMetrics.preVolRatio,
    holdQuality: preMetrics.holdQuality,
    abovePrevHigh: !!preMetrics.abovePrevHigh,
    drawdown252: structuralMetrics.drawdown252,
    noReverseSplit: !(flags.recentReverseSplit || structuralMetrics.likelyReverseSplitProxy)
  });
  const cleanRecovery = computeCleanRecoveryScore(structuralMetrics, ignitionMetrics, flags, family.bestFamily);

  const supernovaScore = computeSupernovaScoreLive(
    structural.score,
    ignition.score,
    formerRunner.score,
    rotation.score,
    premarket.score,
    family.familyScore
  );

  let baseDecision = "ALMA";
  if (!structural.hardReject && !premarket.hardReject) {
    baseDecision = finalLiveDecision({
      structuralScore: structural.score,
      ignitionScore: ignition.score,
      formerRunnerScore: formerRunner.score,
      rotationScore: rotation.score,
      premarketScore: premarket.score,
      familyScore: family.familyScore,
      supernovaScore,
      source: pre.source,
      cleanRecoveryScore: cleanRecovery.score,
      bestFamily: family.bestFamily,
      explicitNarrativePivot: !!flags.narrativePivot
    });
  }

  const setupEligible = isNightlyWatchEligible({
    structuralScore: structural.score,
    ignitionScore: ignition.score,
    formerRunnerScore: formerRunner.score,
    familyScore: family.familyScore,
    supernovaScore,
    cleanRecoveryScore: cleanRecovery.score,
    bestFamily: family.bestFamily,
    explicitNarrativePivot: !!flags.narrativePivot
  });

  const provisionalPlan = buildEntryPlan(baseDecision === "ALMA" && setupEligible ? "İZLE" : baseDecision, pre.source, pre.price, pre.preVWAP, safeNum(ctx.last.h, 0));
  const confirmProfile = buildConfirmProfile({
    entryIdea: provisionalPlan.entryIdea,
    bestFamily: family.bestFamily
  });
  const confirmation = buildMorningConfirmation({
    baseDecision,
    setupEligible,
    pre: preMetrics,
    entryIdea: provisionalPlan.entryIdea,
    confirmProfile
  });

  const decision = confirmation.decision;
  const plan = buildEntryPlan(decision, pre.source, pre.price, pre.preVWAP, safeNum(ctx.last.h, 0));

  return {
    symbol,
    decision,
    baseDecision,
    confirmStatus: confirmation.confirmStatus,
    confirmScore: confirmation.confirmScore,
    triggerHit: confirmation.triggerHit,
    triggerNear: confirmation.triggerNear,
    supernovaScore,
    structuralScore: structural.score,
    ignitionScore: ignition.score,
    formerRunnerScore: formerRunner.score,
    rotationScore: rotation.score,
    premarketScore: premarket.score,
    cleanRecoveryScore: cleanRecovery.score,
    familyScore: family.familyScore,
    bestFamily: family.bestFamily,
    familyMatches: family.topFamilies.map((x) => `${x.name}:${x.score}`).join(" | "),

    source: pre.source,
    price: roundSmart(pre.price),
    prevClose: roundSmart(ctx.last.c),
    prevHigh: roundSmart(ctx.last.h),

    gapPct: roundSmart(pre.gapPct),
    preVol: Math.round(pre.preVol || 0),
    preVolBaselineMedian: Math.round(baseline.baselineMedian || 0),
    preVolRatio: roundSmart(preVolRatio),
    preDollarVol: Math.round(pre.preDollarVol || 0),
    preVWAP: roundSmart(pre.preVWAP),
    abovePreVWAP: !!pre.abovePreVWAP,
    holdQuality: roundSmart(pre.holdQuality),
    preRangePct: roundSmart(pre.preRangePct),
    abovePrevHigh: !!pre.abovePrevHigh,

    drawdown90: roundSmart(structuralMetrics.drawdown90),
    reboundFrom30Low: roundSmart(structuralMetrics.reboundFrom30Low),
    baseTightness10: roundSmart(structuralMetrics.baseTightness10),
    breakout20: structuralMetrics.breakout20 ? "YES" : "NO",

    prevDayRet: roundSmart(ignitionMetrics.prevDayRet),
    prevCloseStrength: roundSmart(ignitionMetrics.prevCloseStrength),
    prevVolRatio: roundSmart(ignitionMetrics.prevVolRatio),
    prevDollarVol: Math.round(ignitionMetrics.prevDollarVol || 0),
    prevDollarShock: roundSmart(ignitionMetrics.prevDollarShock),
    rangeExpansion: roundSmart(ignitionMetrics.rangeExpansion),

    explosiveDays252: formerRunnerMetrics.explosiveDays252,
    explosiveDays63: formerRunnerMetrics.explosiveDays63,
    haltProxyDays252: formerRunnerMetrics.haltProxyDays252,
    haltProxyDays63: formerRunnerMetrics.haltProxyDays63,
    maxExpansion252: roundSmart(formerRunnerMetrics.maxExpansion252),

    prevTurnover: roundSmart(rotationMetrics.prevTurnover),
    preTurnover: roundSmart(rotationMetrics.preTurnover),

    entryType: plan.entryType,
    entryIdea: plan.entryIdea,
    stop: plan.stop,
    tp1: plan.tp1,
    tp2: plan.tp2,

    triggerZoneLow: confirmProfile.triggerZoneLow,
    triggerZoneHigh: confirmProfile.triggerZoneHigh,
    confirmAbove: confirmProfile.confirmAbove,
    strongConfirmAbove: confirmProfile.strongConfirmAbove,
    failBelow: confirmProfile.failBelow,
    minPremarketVolume: confirmProfile.minPremarketVolume,
    betterPremarketVolume: confirmProfile.betterPremarketVolume,
    minPremarketDollarVolume: confirmProfile.minPremarketDollarVolume,
    betterPremarketDollarVolume: confirmProfile.betterPremarketDollarVolume,

    notes: [
      ...structural.notes,
      ...ignition.notes,
      ...formerRunner.notes,
      ...rotation.notes,
      ...premarket.notes,
      ...confirmation.notes
    ].join(" | ")
  };
}

function buildBacktestOutcome(tradeDayMinuteBars, tradeDate, entryIdea) {
  if (entryIdea == null || !Number.isFinite(entryIdea)) {
    return {
      entryReached: false,
      realizedEntryToHighPct: null,
      realizedOpenToHighPct: null
    };
  }

  const dayBars = getBarsForDate(tradeDayMinuteBars, tradeDate);
  const openBars = filterBarsByTime(dayBars, "09:30:00", "12:00:00");
  if (!openBars.length) {
    return {
      entryReached: false,
      realizedEntryToHighPct: null,
      realizedOpenToHighPct: null
    };
  }

  const open = safeNum(openBars[0].o, null);
  const high120 = maxOf(openBars.map((b) => safeNum(b.h, 0)));
  const low120 = minOf(openBars.map((b) => safeNum(b.l, 999999)));

  const entryReached = low120 != null && low120 <= entryIdea;
  const realizedEntryToHighPct =
    entryReached && high120 != null && entryIdea > 0
      ? ((high120 - entryIdea) / entryIdea) * 100
      : null;

  const realizedOpenToHighPct =
    open != null && high120 != null && open > 0
      ? ((high120 - open) / open) * 100
      : null;

  return {
    entryReached,
    realizedEntryToHighPct: roundSmart(realizedEntryToHighPct),
    realizedOpenToHighPct: roundSmart(realizedOpenToHighPct)
  };
}


function computeRowSortKey(row) {
  const cleanRecovery = safeNum(row.cleanRecoveryScore, 0);
  const familyPenalty =
    row.bestFamily === "NARRATIVE_PIVOT" && !row.explicitNarrativePivot ? 14 : 0;
  const confirmBoost =
    row.decision === "SUPERNOVA_CONFIRMED" ? 24 :
    row.decision === "AL" ? 12 : 0;
  return (
    decisionRank(row.decision) * 100000 +
    (safeNum(row.supernovaScore, 0) + 0.35 * cleanRecovery + confirmBoost - familyPenalty) * 100 +
    safeNum(row.familyScore, 0)
  );
}

function summarizeRows(rows) {
  const strongSet = new Set(["SUPERNOVA_CONFIRMED", "GÜÇLÜ AL"]);
  const buySet = new Set(["AL"]);
  const watchSet = new Set(["İZLE", "MANUAL_CONFIRM_REQUIRED", "SPARSE_PREMARKET", "WATCH_TRIGGER"]);

  const picks = rows.filter((r) => strongSet.has(r.decision) || buySet.has(r.decision)).slice(0, 3);
  const vals = picks
    .map((r) => safeNum(r.realizedEntryToHighPct, null))
    .filter((v) => v != null);

  return {
    total: rows.length,
    strong: rows.filter((r) => strongSet.has(r.decision)).length,
    buy: rows.filter((r) => buySet.has(r.decision)).length,
    watch: rows.filter((r) => watchSet.has(r.decision)).length,
    topPicks: picks.length,
    avgEntryToHigh: vals.length ? roundSmart(avg(vals)) : null,
    hit15: vals.filter((v) => v >= 15).length,
    hit25: vals.filter((v) => v >= 25).length
  };
}

function scoreQuickDiscovery(asset, snap) {
  const latestTrade = snap?.latestTrade || {};
  const dailyBar = snap?.dailyBar || {};
  const prevDailyBar = snap?.prevDailyBar || {};

  const price = safeNum(latestTrade.p, safeNum(dailyBar.c, safeNum(prevDailyBar.c, null)));
  const prevClose = safeNum(prevDailyBar.c, null);
  const dayVol = Math.max(safeNum(dailyBar.v, 0), safeNum(prevDailyBar.v, 0));
  const dollarVol = price != null ? price * dayVol : 0;
  const dayRet = prevClose && price ? ((price - prevClose) / prevClose) * 100 : 0;

  if (price == null || price < 0.10 || price > 12.00) return null;
  if (dayVol < 50000) return null;
  if (dollarVol < 100000) return null;

  let score = 0;

  if (price >= 0.10 && price <= 1) score += 18;
  else if (price > 1 && price <= 3) score += 15;
  else if (price > 3 && price <= 5) score += 10;
  else if (price > 5 && price <= 8) score += 6;
  else if (price > 8 && price <= 12) score += 3;

  if (dayRet >= 4 && dayRet < 15) score += 10;
  else if (dayRet >= 15 && dayRet < 60) score += 18;
  else if (dayRet >= 60 && dayRet < 180) score += 16;
  else if (dayRet >= 180) score += 8;

  if (dayVol >= 100000 && dayVol < 500000) score += 8;
  else if (dayVol >= 500000 && dayVol < 5000000) score += 14;
  else if (dayVol >= 5000000) score += 20;

  if (dollarVol >= 100000 && dollarVol < 500000) score += 8;
  else if (dollarVol >= 500000 && dollarVol < 5000000) score += 14;
  else if (dollarVol >= 5000000 && dollarVol < 50000000) score += 18;
  else if (dollarVol >= 50000000) score += 8;

  if (String(asset.exchange || "").toUpperCase() === "NASDAQ") score += 2;
  if (String(asset.exchange || "").toUpperCase() === "AMEX") score += 2;

  return {
    symbol: asset.symbol,
    price: roundSmart(price),
    dayRet: roundSmart(dayRet),
    dayVol: Math.round(dayVol),
    dollarVol: Math.round(dollarVol),
    quickScore: score
  };
}

async function discoverUniverseCandidates(session) {
  const cacheKey = `${session}:${ALPACA_FEED}`;
  if (
    DISCOVERY_CACHE.data &&
    DISCOVERY_CACHE.key === cacheKey &&
    Date.now() < DISCOVERY_CACHE.expiresAt
  ) {
    return DISCOVERY_CACHE.data;
  }

  const assets = await fetchAssets();
  const symbols = assets.map((a) => a.symbol);
  const snapshots = await fetchSnapshotsBatched(symbols, ALPACA_FEED, 80, 6);

  const quick = [];
  for (const asset of assets) {
    const row = scoreQuickDiscovery(asset, snapshots[asset.symbol]);
    if (row) quick.push(row);
  }

  quick.sort((a, b) => b.quickScore - a.quickScore);

  const data = {
    totalUniverse: assets.length,
    passedQuick: quick.length,
    quickCandidates: quick.slice(0, 180),
    quickTop20: quick.slice(0, 20)
  };

  DISCOVERY_CACHE.key = cacheKey;
  DISCOVERY_CACHE.data = data;
  DISCOVERY_CACHE.expiresAt = Date.now() + 5 * 60 * 1000;

  return data;
}

async function buildLiveAutoUniverse(session, today, cutoffTime) {
  const discovery = await discoverUniverseCandidates(session);
  const quickSymbols = discovery.quickCandidates.map((x) => x.symbol);

  if (!quickSymbols.length) {
    return {
      mode: "AUTO_DISCOVERY_EMPTY",
      session,
      feed: ALPACA_FEED,
      cutoffTime,
      universeMode: "AUTO_DISCOVERY",
      discoveredUniverse: discovery.totalUniverse,
      quickPassed: discovery.passedQuick,
      quickTop20: discovery.quickTop20,
      rows: [],
      summary: summarizeRows([]),
      message: "Quick discovery aday bulamadı."
    };
  }

  const dailyLookbackStart = new Date(Date.now() - 280 * 86400000);
  const dailyStart = zonedDateTimeToUtcISO(dailyLookbackStart.toISOString().slice(0, 10), "00:00");
  const dailyEnd = new Date().toISOString();

  const referenceTradeDate =
    session === "afterhours" || session === "closed"
      ? addDaysIso(today, 1)
      : today;

  const dailyBarsMap = await fetchBarsBatched(
    quickSymbols,
    "1Day",
    dailyStart,
    dailyEnd,
    ALPACA_FEED,
    50,
    4
  );

  const nightlyRows = [];
  for (const symbol of quickSymbols) {
    const row = buildNightlyRocketRow({
      symbol,
      dailyBars: dailyBarsMap[symbol] || [],
      referenceTradeDate
    });
    if (row) nightlyRows.push(row);
  }

  nightlyRows.sort((a, b) => {
    const aKey = computeRowSortKey(a);
      const bKey = computeRowSortKey(b);
      return bKey - aKey;
  });

  if (session === "afterhours" || session === "closed") {
    const topNightly = nightlyRows.slice(0, 40);
    return {
      mode: "AUTO_ROCKET_NIGHTLY_V35",
      session,
      feed: ALPACA_FEED,
      cutoffTime: null,
      universeMode: "AUTO_DISCOVERY",
      discoveredUniverse: discovery.totalUniverse,
      quickPassed: discovery.passedQuick,
      quickTop20: discovery.quickTop20,
      rows: topNightly,
      summary: summarizeRows(topNightly),
      message: "Auto-discovery + nightly family shortlist üretildi."
    };
  }

  const topNightlySymbols = nightlyRows.slice(0, 60).map((r) => r.symbol);

  const minuteLookbackStart = new Date(Date.now() - 12 * 86400000);
  const priorMinuteStart = zonedDateTimeToUtcISO(minuteLookbackStart.toISOString().slice(0, 10), "04:00");
  const priorMinuteEnd = zonedDateTimeToUtcISO(today, "03:59");
  const tradeMinuteStart = zonedDateTimeToUtcISO(today, "04:00");
  const tradeMinuteEnd = new Date().toISOString();

  const priorStartMs = new Date(priorMinuteStart).getTime();
  const priorEndMs = new Date(priorMinuteEnd).getTime();
  const tradeStartMs = new Date(tradeMinuteStart).getTime();
  const tradeEndMs = new Date(tradeMinuteEnd).getTime();

  if (!(tradeEndMs > tradeStartMs)) {
    return {
      mode: "WAITING_WINDOW",
      session,
      feed: ALPACA_FEED,
      cutoffTime,
      universeMode: "AUTO_DISCOVERY",
      discoveredUniverse: discovery.totalUniverse,
      quickPassed: discovery.passedQuick,
      quickTop20: discovery.quickTop20,
      rows: [],
      summary: summarizeRows([]),
      message: "Trade minute penceresi henüz oluşmadı."
    };
  }

  let prior5MinMap = {};
  for (const s of topNightlySymbols) prior5MinMap[s] = [];

  const trade1MinMap = await fetchBarsBatched(
    topNightlySymbols,
    "1Min",
    tradeMinuteStart,
    tradeMinuteEnd,
    ALPACA_FEED,
    50,
    4
  );

  if (priorEndMs > priorStartMs) {
    prior5MinMap = await fetchBarsBatched(
      topNightlySymbols,
      "5Min",
      priorMinuteStart,
      priorMinuteEnd,
      ALPACA_FEED,
      50,
      4
    );
  }

  const fullRows = [];
  for (const symbol of topNightlySymbols) {
    const dailyBars = dailyBarsMap[symbol] || [];
    const prior5 = prior5MinMap[symbol] || [];
    const trade1 = trade1MinMap[symbol] || [];
    const mergedMinuteBars = [...prior5, ...trade1].sort((a, b) => new Date(a.t) - new Date(b.t));

    const row = buildFullRocketRow({
      symbol,
      dailyBars,
      minuteBars: mergedMinuteBars,
      tradeDate: today,
      cutoffTime
    });
    if (row) fullRows.push(row);
  }

  fullRows.sort((a, b) => {
    const aKey = computeRowSortKey(a);
      const bKey = computeRowSortKey(b);
      return bKey - aKey;
  });

  return {
    mode: "AUTO_ROCKET_PREMARKET_V35",
    session,
    feed: ALPACA_FEED,
    cutoffTime,
    universeMode: "AUTO_DISCOVERY",
    discoveredUniverse: discovery.totalUniverse,
    quickPassed: discovery.passedQuick,
    quickTop20: discovery.quickTop20,
    rows: fullRows,
    summary: summarizeRows(fullRows),
    message: null
  };
}

async function buildLiveManual(symbols, session, today, cutoffTime) {
  const dailyLookbackStart = new Date(Date.now() - 280 * 86400000);
  const dailyStart = zonedDateTimeToUtcISO(dailyLookbackStart.toISOString().slice(0, 10), "00:00");
  const dailyEnd = new Date().toISOString();

  const referenceTradeDate =
    session === "afterhours" || session === "closed"
      ? addDaysIso(today, 1)
      : today;

  const dailyBarsMap = await fetchBarsBatched(symbols, "1Day", dailyStart, dailyEnd, ALPACA_FEED, 50, 4);

  if (session === "afterhours" || session === "closed") {
    const rows = [];
    for (const symbol of symbols) {
      const row = buildNightlyRocketRow({
        symbol,
        dailyBars: dailyBarsMap[symbol] || [],
        referenceTradeDate
      });
      if (row) rows.push(row);
    }

    rows.sort((a, b) => {
      const aKey = computeRowSortKey(a);
      const bKey = computeRowSortKey(b);
      return bKey - aKey;
    });

    return {
      mode: "MANUAL_ROCKET_NIGHTLY_V35",
      session,
      feed: ALPACA_FEED,
      cutoffTime: null,
      universeMode: "MANUAL_LIST",
      discoveredUniverse: symbols.length,
      quickPassed: symbols.length,
      quickTop20: [],
      rows,
      summary: summarizeRows(rows),
      message: "Manuel liste nightly shortlist olarak işlendi."
    };
  }

  const minuteLookbackStart = new Date(Date.now() - 12 * 86400000);
  const priorMinuteStart = zonedDateTimeToUtcISO(minuteLookbackStart.toISOString().slice(0, 10), "04:00");
  const priorMinuteEnd = zonedDateTimeToUtcISO(today, "03:59");
  const tradeMinuteStart = zonedDateTimeToUtcISO(today, "04:00");
  const tradeMinuteEnd = new Date().toISOString();

  const priorStartMs = new Date(priorMinuteStart).getTime();
  const priorEndMs = new Date(priorMinuteEnd).getTime();
  const tradeStartMs = new Date(tradeMinuteStart).getTime();
  const tradeEndMs = new Date(tradeMinuteEnd).getTime();

  if (!(tradeEndMs > tradeStartMs)) {
    return {
      mode: "WAITING_WINDOW",
      session,
      feed: ALPACA_FEED,
      cutoffTime,
      universeMode: "MANUAL_LIST",
      discoveredUniverse: symbols.length,
      quickPassed: symbols.length,
      quickTop20: [],
      rows: [],
      summary: summarizeRows([]),
      message: "Trade minute penceresi henüz oluşmadı."
    };
  }

  let prior5MinMap = {};
  for (const s of symbols) prior5MinMap[s] = [];

  const trade1MinMap = await fetchBarsBatched(symbols, "1Min", tradeMinuteStart, tradeMinuteEnd, ALPACA_FEED, 50, 4);

  if (priorEndMs > priorStartMs) {
    prior5MinMap = await fetchBarsBatched(symbols, "5Min", priorMinuteStart, priorMinuteEnd, ALPACA_FEED, 50, 4);
  }

  const rows = [];
  for (const symbol of symbols) {
    const dailyBars = dailyBarsMap[symbol] || [];
    const prior5 = prior5MinMap[symbol] || [];
    const trade1 = trade1MinMap[symbol] || [];
    const mergedMinuteBars = [...prior5, ...trade1].sort((a, b) => new Date(a.t) - new Date(b.t));

    const row = buildFullRocketRow({
      symbol,
      dailyBars,
      minuteBars: mergedMinuteBars,
      tradeDate: today,
      cutoffTime
    });
    if (row) rows.push(row);
  }

  rows.sort((a, b) => {
    const aKey = computeRowSortKey(a);
      const bKey = computeRowSortKey(b);
      return bKey - aKey;
  });

  return {
    mode: "MANUAL_ROCKET_PREMARKET_V35",
    session,
    feed: ALPACA_FEED,
    cutoffTime,
    universeMode: "MANUAL_LIST",
    discoveredUniverse: symbols.length,
    quickPassed: symbols.length,
    quickTop20: [],
    rows,
    summary: summarizeRows(rows),
    message: null
  };
}

async function buildLive(manualSymbolsRaw) {
  const session = getSessionLabelNow();
  const today = getTodayNyDate();
  const nowNy = getNowNyTime();
  const manualSymbols = parseSymbols(manualSymbolsRaw);

  if (session === "weekend") {
    return {
      mode: "NO_MARKET",
      session,
      feed: ALPACA_FEED,
      cutoffTime: null,
      universeMode: manualSymbols.length ? "MANUAL_LIST" : "AUTO_DISCOVERY",
      discoveredUniverse: 0,
      quickPassed: 0,
      quickTop20: [],
      rows: [],
      summary: summarizeRows([]),
      message: "Hafta sonu."
    };
  }

  if (session !== "afterhours" && session !== "closed" && nowNy < "04:00:00") {
    return {
      mode: "WAIT_PREMARKET",
      session,
      feed: ALPACA_FEED,
      cutoffTime: null,
      universeMode: manualSymbols.length ? "MANUAL_LIST" : "AUTO_DISCOVERY",
      discoveredUniverse: 0,
      quickPassed: 0,
      quickTop20: [],
      rows: [],
      summary: summarizeRows([]),
      message: "Premarket henüz başlamadı. 04:00 ET sonrası tekrar dene."
    };
  }

  let cutoffTime = "09:25:00";
  if (session === "premarket") {
    cutoffTime = nowNy > "09:25:00" ? "09:25:00" : nowNy;
  } else if (session === "open") {
    cutoffTime = "09:25:00";
  } else {
    cutoffTime = null;
  }

  if (manualSymbols.length) {
    return buildLiveManual(manualSymbols, session, today, cutoffTime);
  }

  return buildLiveAutoUniverse(session, today, cutoffTime);
}

async function buildBacktest(dateStr, symbolsRaw) {
  const symbols = parseSymbols(symbolsRaw);
  if (!symbols.length) {
    throw new Error("Backtest için sembol listesi gerekli.");
  }

  const dailyLookbackStart = new Date(new Date(dateStr).getTime() - 280 * 86400000);
  const minuteLookbackStart = new Date(new Date(dateStr).getTime() - 12 * 86400000);

  const dailyStart = zonedDateTimeToUtcISO(dailyLookbackStart.toISOString().slice(0, 10), "00:00");
  const dailyEnd = zonedDateTimeToUtcISO(dateStr, "23:59");

  const priorMinuteStart = zonedDateTimeToUtcISO(minuteLookbackStart.toISOString().slice(0, 10), "04:00");
  const priorMinuteEnd = zonedDateTimeToUtcISO(dateStr, "03:59");

  const tradeMinuteStart = zonedDateTimeToUtcISO(dateStr, "04:00");
  const tradeMinuteEnd = zonedDateTimeToUtcISO(dateStr, "12:00");

  const [dailyBarsMap, prior5MinMap, trade1MinMap] = await Promise.all([
    fetchBarsBatched(symbols, "1Day", dailyStart, dailyEnd, ALPACA_FEED, 50, 4),
    fetchBarsBatched(symbols, "5Min", priorMinuteStart, priorMinuteEnd, ALPACA_FEED, 50, 4),
    fetchBarsBatched(symbols, "1Min", tradeMinuteStart, tradeMinuteEnd, ALPACA_FEED, 50, 4)
  ]);

  const rows = [];

  for (const symbol of symbols) {
    const dailyBars = dailyBarsMap[symbol] || [];
    const prior5 = prior5MinMap[symbol] || [];
    const trade1 = trade1MinMap[symbol] || [];
    const mergedMinuteBars = [...prior5, ...trade1].sort((a, b) => new Date(a.t) - new Date(b.t));

    const row = buildFullRocketRow({
      symbol,
      dailyBars,
      minuteBars: mergedMinuteBars,
      tradeDate: dateStr,
      cutoffTime: "09:25:00"
    });

    if (!row) continue;

    const outcome = buildBacktestOutcome(trade1, dateStr, row.entryIdea);

    rows.push({
      ...row,
      entryReached: outcome.entryReached,
      realizedEntryToHighPct: outcome.realizedEntryToHighPct,
      realizedOpenToHighPct: outcome.realizedOpenToHighPct
    });
  }

  rows.sort((a, b) => {
    const aKey = computeRowSortKey(a);
      const bKey = computeRowSortKey(b);
      return bKey - aKey;
  });

  return {
    mode: "ROCKET_BACKTEST_V35",
    tradeDate: dateStr,
    feed: ALPACA_FEED,
    cutoffTime: "09:25:00",
    universeMode: "MANUAL_LIST",
    discoveredUniverse: symbols.length,
    quickPassed: symbols.length,
    quickTop20: [],
    rows,
    summary: summarizeRows(rows)
  };
}

app.get("/test", (req, res) => {
  res.json({ status: "SERVER OK" });
});

app.get("/api/default-symbols", (req, res) => {
  res.json({ symbols: DEFAULT_SYMBOLS });
});

app.get("/api/live-supernova-v35", async (req, res) => {
  try {
    const data = await buildLive(req.query.symbols || "");
    res.json(data);
  } catch (err) {
    console.error("LIVE_SUPERNOVA_V35 error:", err);
    res.status(500).json({ error: "server error", detail: err.message });
  }
});

app.get("/api/backtest-supernova-v35", async (req, res) => {
  try {
    const dateStr = String(req.query.date || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return res.status(400).json({ error: "date parametresi YYYY-MM-DD formatında olmalı" });
    }

    const data = await buildBacktest(dateStr, req.query.symbols || "");
    res.json(data);
  } catch (err) {
    console.error("BACKTEST_SUPERNOVA_V35 error:", err);
    res.status(500).json({ error: "server error", detail: err.message });
  }
});

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.use((req, res) => {
  res.status(404).send(`Route not found: ${req.method} ${req.originalUrl}`);
});

app.listen(PORT, () => {
  console.log(`Rocket Engine v3.5 running on port ${PORT}`);
});
