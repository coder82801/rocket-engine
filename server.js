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
  "RMSG","CTMX","SOPA","RR","TNON","SIDU",
  "SQFT","FUSE","CREG","SKYQ","GN","MAXN",
  "IPST","TPST","RAYA","CUE"
];

const ALLOWED_EXCHANGES = new Set(["NASDAQ", "NYSE", "AMEX", "ARCA", "BATS"]);
const BAD_NAME_TOKENS = [
  "WARRANT", "RIGHT", "UNIT", "ETF", "FUND", "TRUST",
  "ETN", "PREFERRED", "PFD", "DEPOSITARY", "ADR"
];

const SYMBOL_FLAGS = {
  RMSG: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: true,
    catalystFresh: true,
    catalystType: "strategic_collaboration",
    allowAbove5: false
  },
  SOPA: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: false,
    catalystType: "none",
    allowAbove5: false
  },
  TNON: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: false,
    catalystType: "none",
    allowAbove5: false
  },
  IREN: {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: true,
    catalystType: "ai_transition",
    allowAbove5: true
  }
};

const ROCKET_PROTOTYPES = [
  {
    name: "RMSG_STYLE_SUPERNOVA",
    weights: {
      price: 10,
      drawdown90: 8,
      baseTightness10: 8,
      prevDayRet: 9,
      prevVolRatio: 10,
      prevCloseStrength: 10,
      breakout20: 6,
      gapPct: 8,
      preVolRatio: 10,
      holdQuality: 10,
      preDollarVol: 8,
      abovePrevHigh: 3
    },
    bands: {
      price: [0.10, 3.50, 0.05, 5.00],
      drawdown90: [-90, -25, -99, -5],
      baseTightness10: [5, 35, 0, 80],
      prevDayRet: [5, 45, -10, 90],
      prevVolRatio: [2, 15, 0.5, 40],
      prevCloseStrength: [75, 100, 45, 100],
      breakout20: [1, 1, 0, 1],
      gapPct: [10, 80, -10, 150],
      preVolRatio: [1.2, 10, 0.2, 30],
      holdQuality: [65, 100, 40, 100],
      preDollarVol: [200000, 20000000, 50000, 80000000],
      abovePrevHigh: [1, 1, 0, 1]
    }
  },
  {
    name: "SQUEEZE_RECLAIM_STYLE",
    weights: {
      price: 9,
      drawdown90: 8,
      baseTightness10: 9,
      prevDayRet: 10,
      prevVolRatio: 10,
      prevCloseStrength: 10,
      breakout20: 7,
      gapPct: 7,
      preVolRatio: 8,
      holdQuality: 9,
      preDollarVol: 8,
      abovePrevHigh: 5
    },
    bands: {
      price: [0.25, 5.00, 0.10, 7.00],
      drawdown90: [-80, -20, -99, 0],
      baseTightness10: [3, 25, 0, 60],
      prevDayRet: [12, 60, -5, 120],
      prevVolRatio: [2, 12, 0.5, 35],
      prevCloseStrength: [78, 100, 50, 100],
      breakout20: [1, 1, 0, 1],
      gapPct: [5, 50, -10, 100],
      preVolRatio: [1.0, 8, 0.2, 20],
      holdQuality: [70, 100, 45, 100],
      preDollarVol: [150000, 15000000, 40000, 60000000],
      abovePrevHigh: [1, 1, 0, 1]
    }
  },
  {
    name: "RECOVERY_IGNITION_STYLE",
    weights: {
      price: 8,
      drawdown90: 10,
      baseTightness10: 7,
      prevDayRet: 8,
      prevVolRatio: 10,
      prevCloseStrength: 8,
      breakout20: 6,
      gapPct: 6,
      preVolRatio: 8,
      holdQuality: 8,
      preDollarVol: 10,
      abovePrevHigh: 3
    },
    bands: {
      price: [0.20, 5.00, 0.05, 7.00],
      drawdown90: [-95, -40, -99, -10],
      baseTightness10: [4, 30, 0, 70],
      prevDayRet: [4, 30, -10, 70],
      prevVolRatio: [1.5, 8, 0.3, 25],
      prevCloseStrength: [70, 100, 40, 100],
      breakout20: [0, 1, 0, 1],
      gapPct: [2, 30, -10, 80],
      preVolRatio: [0.8, 6, 0.1, 20],
      holdQuality: [60, 95, 35, 100],
      preDollarVol: [100000, 12000000, 30000, 50000000],
      abovePrevHigh: [0, 1, 0, 1]
    }
  }
];

const ASSET_CACHE = {
  expiresAt: 0,
  data: null
};

const DISCOVERY_CACHE = {
  expiresAt: 0,
  key: "",
  data: null
};

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
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function parseSymbols(raw) {
  const src = String(raw || "")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
  return [...new Set(src)];
}

function getFlags(symbol) {
  return {
    recentReverseSplit: false,
    otcRisk: false,
    recentDeficiency: false,
    catalystFresh: false,
    catalystType: "none",
    allowAbove5: false,
    ...(SYMBOL_FLAGS[symbol] || {})
  };
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
  if (decision === "GÜÇLÜ AL") return 4;
  if (decision === "AL") return 3;
  if (decision === "İZLE") return 2;
  return 1;
}

async function alpacaGetJson(url, isTrading = false) {
  if (!ALPACA_API_KEY || !ALPACA_SECRET_KEY) {
    throw new Error("ALPACA_API_KEY / ALPACA_SECRET_KEY eksik");
  }

  const response = await fetch(url, {
    headers: {
      "APCA-API-KEY-ID": ALPACA_API_KEY,
      "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
      "Accept": "application/json"
    }
  });

  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Alpaca ${response.status}: ${text}`);
  }

  if (!text) return isTrading ? [] : {};
  return JSON.parse(text);
}

async function fetchAssets() {
  if (ASSET_CACHE.data && Date.now() < ASSET_CACHE.expiresAt) {
    return ASSET_CACHE.data;
  }

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
  const priorDates = [...new Set(priorBars.map((b) => isoDateNY(b.t)))].slice(-10);

  return { last, prev, hist10, hist20, hist60, hist90, priorDates };
}

function buildStructuralMetrics(ctx, flags) {
  const price = safeNum(ctx.last.c, 0);
  const hist10High = maxOf(ctx.hist10.map((b) => safeNum(b.h, 0)));
  const hist10Low = minOf(ctx.hist10.map((b) => safeNum(b.l, 0)));
  const high20ExLast = maxOf(ctx.hist20.slice(0, -1).map((b) => safeNum(b.h, 0)));
  const low30 = minOf(ctx.hist60.slice(-30).map((b) => safeNum(b.l, 0)));
  const high90 = maxOf(ctx.hist90.map((b) => safeNum(b.h, 0)));

  const drawdown90 = high90 > 0 ? ((price / high90) - 1) * 100 : 0;
  const reboundFrom30Low = low30 > 0 ? ((price - low30) / low30) * 100 : 0;
  const baseTightness10 = price > 0 ? ((hist10High - hist10Low) / price) * 100 : 999;
  const breakout20 = high20ExLast > 0 && price > high20ExLast ? 1 : 0;

  return {
    price,
    drawdown90,
    reboundFrom30Low,
    baseTightness10,
    breakout20,
    recentReverseSplit: !!flags.recentReverseSplit,
    otcRisk: !!flags.otcRisk,
    recentDeficiency: !!flags.recentDeficiency,
    allowAbove5: !!flags.allowAbove5
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

  const avgRange20 = Math.max(
    avg(ctx.hist20.slice(0, -1).map((b) => computeRangePct(b.h, b.l, b.c) || 0)),
    0.0001
  );

  const prevRangePct = computeRangePct(ctx.last.h, ctx.last.l, ctx.last.c);
  const rangeExpansion = prevRangePct != null ? prevRangePct / avgRange20 : 0;

  return {
    prevDayRet,
    prevCloseStrength,
    prevVolRatio,
    prevDollarVol,
    prevRangePct,
    rangeExpansion,
    catalystFresh: !!flags.catalystFresh,
    catalystType: flags.catalystType || "none"
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
  const holdQuality = preHigh > preLow ? ((preLast - preLow) / (preHigh - preLow)) * 100 : 50;
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

  if (flags.otcRisk) {
    notes.push("OTC riski");
    return { score: 0, notes, hardReject: true };
  }

  if (m.price < 0.10) {
    notes.push("0.10 altı fiyat");
    return { score: 0, notes, hardReject: true };
  }

  if (m.price > 5 && !flags.allowAbove5) {
    notes.push("5 dolar üstü");
    return { score: 0, notes, hardReject: true };
  }

  if (m.price >= 0.10 && m.price <= 1) score += 24;
  else if (m.price > 1 && m.price <= 3) score += 20;
  else if (m.price > 3 && m.price <= 5) score += 12;

  if (m.drawdown90 >= -90 && m.drawdown90 <= -35) score += 18;
  else if (m.drawdown90 > -35 && m.drawdown90 <= -10) score += 8;
  else if (m.drawdown90 < -95) score -= 8;

  if (m.baseTightness10 >= 4 && m.baseTightness10 <= 28) score += 16;
  else if (m.baseTightness10 > 28 && m.baseTightness10 <= 50) score += 8;
  else if (m.baseTightness10 > 70) {
    score -= 8;
    notes.push("Base gevşek");
  }

  if (m.reboundFrom30Low >= 5 && m.reboundFrom30Low <= 120) score += 10;
  else if (m.reboundFrom30Low > 200) {
    score -= 5;
    notes.push("Zaten çok şişmiş rebound");
  }

  if (m.breakout20) {
    score += 12;
    notes.push("20g high reclaim");
  }

  if (flags.recentReverseSplit) {
    score -= 28;
    notes.push("Yakın reverse split");
  }

  if (flags.recentDeficiency) {
    score -= 6;
    notes.push("Deficiency notice");
  }

  score = clamp(Math.round(score), 0, 100);
  return { score, notes, hardReject: false };
}

function scoreIgnition(m) {
  let score = 0;
  const notes = [];

  if (m.prevDayRet >= 4 && m.prevDayRet < 15) score += 16;
  else if (m.prevDayRet >= 15 && m.prevDayRet < 45) score += 22;
  else if (m.prevDayRet >= 45 && m.prevDayRet < 100) score += 14;
  else if (m.prevDayRet < 0) score -= 12;

  if (m.prevVolRatio >= 1.5 && m.prevVolRatio < 3) score += 12;
  else if (m.prevVolRatio >= 3 && m.prevVolRatio < 8) score += 20;
  else if (m.prevVolRatio >= 8) {
    score += 24;
    notes.push("Vol shock");
  } else if (m.prevVolRatio < 0.8) {
    score -= 8;
  }

  if (m.prevCloseStrength >= 80) score += 18;
  else if (m.prevCloseStrength >= 65) score += 10;
  else if (m.prevCloseStrength < 45) {
    score -= 10;
    notes.push("Weak close");
  }

  if (m.prevDollarVol >= 150000 && m.prevDollarVol < 600000) score += 10;
  else if (m.prevDollarVol >= 600000 && m.prevDollarVol < 3000000) score += 16;
  else if (m.prevDollarVol >= 3000000) score += 20;
  else if (m.prevDollarVol < 50000) {
    score -= 10;
    notes.push("Dollar vol zayıf");
  }

  if (m.rangeExpansion >= 1.3 && m.rangeExpansion < 2.5) score += 8;
  else if (m.rangeExpansion >= 2.5) score += 14;

  if (m.catalystFresh) {
    score += 12;
    notes.push(`Fresh catalyst: ${m.catalystType}`);
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

  if (feed === "sip") score += 4;
  else notes.push("IEX feed");

  if (m.gapPct >= 3 && m.gapPct < 15) score += 12;
  else if (m.gapPct >= 15 && m.gapPct < 50) score += 20;
  else if (m.gapPct >= 50 && m.gapPct < 150) {
    score += 12;
    notes.push("Aşırı sıcak gap");
  } else if (m.gapPct < 0) {
    score -= 10;
  }

  if (m.preVolRatio >= 0.8 && m.preVolRatio < 2) score += 10;
  else if (m.preVolRatio >= 2 && m.preVolRatio < 6) score += 20;
  else if (m.preVolRatio >= 6) {
    score += 24;
    notes.push("Premarket vol shock");
  } else if (m.preVolRatio < 0.4) {
    score -= 8;
  }

  if (m.preDollarVol >= 150000 && m.preDollarVol < 500000) score += 12;
  else if (m.preDollarVol >= 500000 && m.preDollarVol < 2000000) score += 18;
  else if (m.preDollarVol >= 2000000) score += 22;
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
    score += 10;
    notes.push("Prev high reclaim");
  }

  if (m.preRangePct != null && m.preRangePct > 60) {
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
    score += 8;
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

  if (value < idealLow) {
    return (value - hardLow) / (idealLow - hardLow);
  }
  return (hardHigh - value) / (hardHigh - idealHigh);
}

function boolSimilarity(value, idealLow, idealHigh, hardLow, hardHigh) {
  return bandSimilarity(value ? 1 : 0, idealLow, idealHigh, hardLow, hardHigh);
}

function computePatternSimilarity(featureSet) {
  const results = ROCKET_PROTOTYPES.map((proto) => {
    let weighted = 0;
    let totalWeight = 0;

    for (const [key, weight] of Object.entries(proto.weights)) {
      const band = proto.bands[key];
      if (!band) continue;

      const val = featureSet[key];
      const hasValue =
        typeof val === "boolean" ||
        (val !== null && val !== undefined && Number.isFinite(Number(val)));

      if (!hasValue) continue;

      const sim = typeof val === "boolean"
        ? boolSimilarity(val, band[0], band[1], band[2], band[3])
        : bandSimilarity(val, band[0], band[1], band[2], band[3]);

      weighted += sim * weight;
      totalWeight += weight;
    }

    const score = totalWeight > 0 ? (weighted / totalWeight) * 100 : 0;

    return {
      name: proto.name,
      score: Math.round(score)
    };
  }).sort((a, b) => b.score - a.score);

  return {
    bestName: results[0]?.name || null,
    bestScore: results[0]?.score || 0,
    topMatches: results.slice(0, 3)
  };
}

function buildEntryPlan(decision, source, price, preVWAP, prevHigh) {
  if (!["GÜÇLÜ AL", "AL", "İZLE"].includes(decision)) {
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
    tp1: roundSmart(entryIdea * 1.15),
    tp2: roundSmart(entryIdea * 1.30)
  };
}

function finalNightlyDecision({ structuralScore, ignitionScore, patternScore }) {
  const composite = 0.35 * structuralScore + 0.35 * ignitionScore + 0.30 * patternScore;
  if (composite >= 70 && structuralScore >= 40 && ignitionScore >= 50 && patternScore >= 58) {
    return "İZLE";
  }
  return "ALMA";
}

function finalLiveDecision({ structuralScore, ignitionScore, premarketScore, patternScore, source }) {
  const composite =
    0.25 * structuralScore +
    0.25 * ignitionScore +
    0.25 * premarketScore +
    0.25 * patternScore;

  if (source !== "REAL_PREMARKET") {
    if (0.4 * structuralScore + 0.3 * ignitionScore + 0.3 * patternScore >= 70 && patternScore >= 58) {
      return "İZLE";
    }
    return "ALMA";
  }

  if (
    structuralScore >= 50 &&
    ignitionScore >= 60 &&
    premarketScore >= 72 &&
    patternScore >= 72 &&
    composite >= 72
  ) {
    return "GÜÇLÜ AL";
  }

  if (
    structuralScore >= 40 &&
    ignitionScore >= 50 &&
    premarketScore >= 58 &&
    patternScore >= 60 &&
    composite >= 60
  ) {
    return "AL";
  }

  if (0.4 * structuralScore + 0.3 * ignitionScore + 0.3 * patternScore >= 70 && patternScore >= 58) {
    return "İZLE";
  }

  return "ALMA";
}

function buildNightlyRocketRow({ symbol, dailyBars, referenceTradeDate }) {
  const flags = getFlags(symbol);
  const ctx = buildDailyHistoryContext(dailyBars, referenceTradeDate);
  if (!ctx) return null;

  const structuralMetrics = buildStructuralMetrics(ctx, flags);
  const ignitionMetrics = buildIgnitionMetrics(ctx, flags);

  const structural = scoreStructural(structuralMetrics, flags);
  if (structural.hardReject) {
    return {
      symbol,
      decision: "ALMA",
      structuralScore: structural.score,
      ignitionScore: 0,
      premarketScore: 0,
      patternScore: 0,
      bestPattern: null,
      topMatches: "",
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
      rangeExpansion: roundSmart(ignitionMetrics.rangeExpansion),
      entryType: "NO_TRADE",
      entryIdea: null,
      stop: null,
      tp1: null,
      tp2: null,
      notes: structural.notes.join(" | ")
    };
  }

  const ignition = scoreIgnition(ignitionMetrics);

  const featureSet = {
    price: structuralMetrics.price,
    drawdown90: structuralMetrics.drawdown90,
    baseTightness10: structuralMetrics.baseTightness10,
    prevDayRet: ignitionMetrics.prevDayRet,
    prevVolRatio: ignitionMetrics.prevVolRatio,
    prevCloseStrength: ignitionMetrics.prevCloseStrength,
    breakout20: !!structuralMetrics.breakout20
  };

  const pattern = computePatternSimilarity(featureSet);

  const decision = finalNightlyDecision({
    structuralScore: structural.score,
    ignitionScore: ignition.score,
    patternScore: pattern.bestScore
  });

  const plan = buildEntryPlan(
    decision,
    "NONE",
    null,
    null,
    safeNum(ctx.last.h, 0)
  );

  return {
    symbol,
    decision,

    structuralScore: structural.score,
    ignitionScore: ignition.score,
    premarketScore: 0,
    patternScore: pattern.bestScore,
    bestPattern: pattern.bestName,
    topMatches: pattern.topMatches.map((x) => `${x.name}:${x.score}`).join(" | "),

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
    rangeExpansion: roundSmart(ignitionMetrics.rangeExpansion),

    entryType: plan.entryType,
    entryIdea: plan.entryIdea,
    stop: plan.stop,
    tp1: plan.tp1,
    tp2: plan.tp2,

    notes: [...structural.notes, ...ignition.notes].join(" | ")
  };
}

function buildFullRocketRow({ symbol, dailyBars, minuteBars, tradeDate, cutoffTime }) {
  const flags = getFlags(symbol);
  const ctx = buildDailyHistoryContext(dailyBars, tradeDate);
  if (!ctx) return null;

  const structuralMetrics = buildStructuralMetrics(ctx, flags);
  const ignitionMetrics = buildIgnitionMetrics(ctx, flags);

  const structural = scoreStructural(structuralMetrics, flags);
  const ignition = scoreIgnition(ignitionMetrics);

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

  const preMetrics = {
    ...pre,
    preVolRatio
  };

  const premarket = scorePremarket(preMetrics, ALPACA_FEED);

  const featureSet = {
    price: structuralMetrics.price,
    drawdown90: structuralMetrics.drawdown90,
    baseTightness10: structuralMetrics.baseTightness10,
    prevDayRet: ignitionMetrics.prevDayRet,
    prevVolRatio: ignitionMetrics.prevVolRatio,
    prevCloseStrength: ignitionMetrics.prevCloseStrength,
    breakout20: !!structuralMetrics.breakout20,
    gapPct: preMetrics.gapPct,
    preVolRatio: preMetrics.preVolRatio,
    holdQuality: preMetrics.holdQuality,
    preDollarVol: preMetrics.preDollarVol,
    abovePrevHigh: !!preMetrics.abovePrevHigh
  };

  const pattern = computePatternSimilarity(featureSet);

  let decision = "ALMA";
  if (!structural.hardReject && !premarket.hardReject) {
    decision = finalLiveDecision({
      structuralScore: structural.score,
      ignitionScore: ignition.score,
      premarketScore: premarket.score,
      patternScore: pattern.bestScore,
      source: pre.source
    });
  }

  const plan = buildEntryPlan(
    decision,
    pre.source,
    pre.price,
    pre.preVWAP,
    safeNum(ctx.last.h, 0)
  );

  return {
    symbol,
    decision,

    structuralScore: structural.score,
    ignitionScore: ignition.score,
    premarketScore: premarket.score,
    patternScore: pattern.bestScore,
    bestPattern: pattern.bestName,
    topMatches: pattern.topMatches.map((x) => `${x.name}:${x.score}`).join(" | "),

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
    rangeExpansion: roundSmart(ignitionMetrics.rangeExpansion),

    entryType: plan.entryType,
    entryIdea: plan.entryIdea,
    stop: plan.stop,
    tp1: plan.tp1,
    tp2: plan.tp2,

    notes: [
      ...structural.notes,
      ...ignition.notes,
      ...premarket.notes
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

function summarizeRows(rows) {
  const picks = rows.filter((r) => r.decision === "GÜÇLÜ AL" || r.decision === "AL").slice(0, 3);
  const vals = picks
    .map((r) => safeNum(r.realizedEntryToHighPct, null))
    .filter((v) => v != null);

  return {
    total: rows.length,
    strong: rows.filter((r) => r.decision === "GÜÇLÜ AL").length,
    buy: rows.filter((r) => r.decision === "AL").length,
    watch: rows.filter((r) => r.decision === "İZLE").length,
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

  if (price == null || price < 0.10 || price > 5.00) return null;
  if (dayVol < 25000) return null;
  if (dollarVol < 50000) return null;

  let score = 0;

  if (price >= 0.10 && price <= 1) score += 18;
  else if (price <= 3) score += 14;
  else score += 8;

  if (dayRet >= 3 && dayRet < 15) score += 10;
  else if (dayRet >= 15 && dayRet < 60) score += 18;
  else if (dayRet >= 60) score += 12;
  else if (dayRet < -20) score -= 8;

  if (dayVol >= 100000 && dayVol < 500000) score += 10;
  else if (dayVol >= 500000 && dayVol < 5000000) score += 16;
  else if (dayVol >= 5000000) score += 22;

  if (dollarVol >= 100000 && dollarVol < 400000) score += 8;
  else if (dollarVol >= 400000 && dollarVol < 2000000) score += 14;
  else if (dollarVol >= 2000000) score += 20;

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
      rows: [],
      summary: summarizeRows([]),
      message: "Quick discovery aday bulamadı."
    };
  }

  const dailyLookbackStart = new Date(Date.now() - 140 * 86400000);
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
    const aKey =
      decisionRank(a.decision) * 100000 +
      safeNum(a.patternScore, 0) * 100 +
      safeNum(a.ignitionScore, 0);

    const bKey =
      decisionRank(b.decision) * 100000 +
      safeNum(b.patternScore, 0) * 100 +
      safeNum(b.ignitionScore, 0);

    return bKey - aKey;
  });

  if (session === "afterhours" || session === "closed") {
    return {
      mode: "AUTO_ROCKET_NIGHTLY",
      session,
      feed: ALPACA_FEED,
      cutoffTime: null,
      universeMode: "AUTO_DISCOVERY",
      discoveredUniverse: discovery.totalUniverse,
      quickPassed: discovery.passedQuick,
      quickTop20: discovery.quickTop20,
      rows: nightlyRows.slice(0, 40),
      summary: summarizeRows(nightlyRows.slice(0, 40)),
      message: "Auto-discovery + nightly rocket shortlist üretildi."
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
    const aKey =
      decisionRank(a.decision) * 100000 +
      safeNum(a.patternScore, 0) * 100 +
      safeNum(a.premarketScore, 0);

    const bKey =
      decisionRank(b.decision) * 100000 +
      safeNum(b.patternScore, 0) * 100 +
      safeNum(b.premarketScore, 0);

    return bKey - aKey;
  });

  return {
    mode: "AUTO_ROCKET_PREMARKET",
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
  const dailyLookbackStart = new Date(Date.now() - 140 * 86400000);
  const dailyStart = zonedDateTimeToUtcISO(dailyLookbackStart.toISOString().slice(0, 10), "00:00");
  const dailyEnd = new Date().toISOString();

  const referenceTradeDate =
    session === "afterhours" || session === "closed"
      ? addDaysIso(today, 1)
      : today;

  const dailyBarsMap = await fetchBarsBatched(
    symbols,
    "1Day",
    dailyStart,
    dailyEnd,
    ALPACA_FEED,
    50,
    4
  );

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
      const aKey =
        decisionRank(a.decision) * 100000 +
        safeNum(a.patternScore, 0) * 100 +
        safeNum(a.ignitionScore, 0);

      const bKey =
        decisionRank(b.decision) * 100000 +
        safeNum(b.patternScore, 0) * 100 +
        safeNum(b.ignitionScore, 0);

      return bKey - aKey;
    });

    return {
      mode: "MANUAL_ROCKET_NIGHTLY",
      session,
      feed: ALPACA_FEED,
      cutoffTime: null,
      universeMode: "MANUAL_LIST",
      discoveredUniverse: symbols.length,
      quickPassed: symbols.length,
      quickTop20: [],
      rows,
      summary: summarizeRows(rows),
      message: "Manuel liste nightly rocket watchlist olarak işlendi."
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

  const trade1MinMap = await fetchBarsBatched(
    symbols,
    "1Min",
    tradeMinuteStart,
    tradeMinuteEnd,
    ALPACA_FEED,
    50,
    4
  );

  if (priorEndMs > priorStartMs) {
    prior5MinMap = await fetchBarsBatched(
      symbols,
      "5Min",
      priorMinuteStart,
      priorMinuteEnd,
      ALPACA_FEED,
      50,
      4
    );
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
    const aKey =
      decisionRank(a.decision) * 100000 +
      safeNum(a.patternScore, 0) * 100 +
      safeNum(a.premarketScore, 0);

    const bKey =
      decisionRank(b.decision) * 100000 +
      safeNum(b.patternScore, 0) * 100 +
      safeNum(b.premarketScore, 0);

    return bKey - aKey;
  });

  return {
    mode: "MANUAL_ROCKET_PREMARKET",
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
    throw new Error("Backtest için manuel sembol listesi gerekli.");
  }

  const dailyLookbackStart = new Date(new Date(dateStr).getTime() - 140 * 86400000);
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
    const aKey =
      decisionRank(a.decision) * 100000 +
      safeNum(a.patternScore, 0) * 100 +
      safeNum(a.premarketScore, 0);

    const bKey =
      decisionRank(b.decision) * 100000 +
      safeNum(b.patternScore, 0) * 100 +
      safeNum(b.premarketScore, 0);

    return bKey - aKey;
  });

  return {
    mode: "ROCKET_BACKTEST",
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

app.get("/api/live-supernova", async (req, res) => {
  try {
    const data = await buildLive(req.query.symbols || "");
    res.json(data);
  } catch (err) {
    console.error("LIVE_SUPERNOVA error:", err);
    res.status(500).json({ error: "server error", detail: err.message });
  }
});

app.get("/api/backtest-supernova", async (req, res) => {
  try {
    const dateStr = String(req.query.date || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return res.status(400).json({ error: "date parametresi YYYY-MM-DD formatında olmalı" });
    }

    const data = await buildBacktest(dateStr, req.query.symbols || "");
    res.json(data);
  } catch (err) {
    console.error("BACKTEST_SUPERNOVA error:", err);
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
  console.log(`Rocket Engine v2 running on port ${PORT}`);
});
