// server.js — Finnhub -> WebSocket relay for the trading game frontend
// Forwards live price ticks to connected clients in the exact shape the
// frontend expects, including the "source": "broker" field that unlocks
// the Buy/Sell buttons client-side.

// Load variables from a local .env file if present (no-op in production
// environments like Render, which inject env vars directly).
try {
  require("dotenv").config();
} catch (err) {
  // dotenv not installed — fine in production, just skip.
}

const WebSocket = require("ws");
const http = require("http");
const https = require("https");
const { createClient } = require("@supabase/supabase-js");

// --- Config ------------------------------------------------------------
const PORT = process.env.PORT || 8787;            // Render assigns PORT automatically
const FINNHUB_TOKEN = process.env.FINNHUB_TOKEN;  // Set this in Render's Environment tab
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const ASSET_SYMBOLS = {
  GOLD: "OANDA:XAU_USD",
  NASDAQ: "FXCM:NAS100"
};

const rooms = {
  GOLD: new Set(),
  NASDAQ: new Set()
};

const lastPrice = {
  GOLD: null,
  NASDAQ: null
};

if (!FINNHUB_TOKEN) {
  console.error(
    "[config] Missing FINNHUB_TOKEN environment variable. " +
    "The HTTP/WebSocket server will still start, but no price data will be " +
    "relayed until FINNHUB_TOKEN is set (Render: Environment tab -> Add Environment Variable)."
  );
}

// Initialize Supabase admin client for backend TP/SL automation
let supabaseAdmin = null;
if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
  supabaseAdmin = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  console.log("[supabase] Admin client initialized for TP/SL monitoring");
} else {
  console.error("[supabase] Missing credentials. Automatic TP/SL execution disabled.");
}

// --- Candle history endpoint (Twelve Data passthrough + backfill) -------
// Per-asset config: Twelve Data ticker, decimal precision for that symbol's
// quote convention, per-5m-bar volatility as a % of price (this is what
// makes NAS100 swing by tens of points per bar while EURUSD only moves by
// fractions of a cent — same % move, very different price scale), and a
// fallback anchor price used only if the provider returns literally nothing
// for that symbol. Add new assets here — everything below (routing,
// backfill) reads from this table, nothing is hardcoded to Gold/Nasdaq.
//
// NOTE: tdSymbol values below are Twelve Data's expected tickers for each
// instrument as of this writing — double check against Twelve Data's own
// symbol search if any of these come back empty in practice, since index
// tickers in particular vary by data vendor.
const SYMBOL_CONFIG = {
  GOLD:   { tdSymbol: "XAU/USD", decimals: 2, volatilityPct: 0.0012, fallbackPrice: 4068 },  // was 2350 — stale by thousands of dollars vs. current spot, which is exactly what produced the cliff at the synthetic/live join
  NASDAQ: { tdSymbol: "NDX",     decimals: 1, volatilityPct: 0.0010, fallbackPrice: 19850 }, // NAS100 — was 29200 (~47% too high); keep near current spot, matching the frontend's own base price, and decimals matching the frontend's 1-decimal NAS100 display
  US30:   { tdSymbol: "DJI",     decimals: 2, volatilityPct: 0.0008, fallbackPrice: 39500 }, // Dow / US30
  US500:  { tdSymbol: "SPX",     decimals: 2, volatilityPct: 0.0009, fallbackPrice: 5300 },  // S&P 500
  EURUSD: { tdSymbol: "EUR/USD", decimals: 5, volatilityPct: 0.0006, fallbackPrice: 1.085 },
  GBPUSD: { tdSymbol: "GBP/USD", decimals: 5, volatilityPct: 0.0006, fallbackPrice: 1.27 },
  USDJPY: { tdSymbol: "USD/JPY", decimals: 3, volatilityPct: 0.0006, fallbackPrice: 155 }
};

// Keep the key out of source control, same pattern as FINNHUB_TOKEN /
// SUPABASE_SERVICE_ROLE_KEY above. Falls back to the key you shared so this
// keeps working immediately — move it into Render's Environment tab (and
// rotate it, since it's now been pasted into a chat) when you get a chance.
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY || "af0b3fc31acb4ee8a372f586d23b1b51";

const HISTORY_BACKFILL_THRESHOLD = 300; // below this many real candles, pad out to a full week
const SEVEN_DAYS_SECONDS = 7 * 24 * 60 * 60;

// Resolves any of the query param spellings ("NASDAQ", "NAS100", "US30",
// "EURUSD", "EUR/USD", ...) to one of the SYMBOL_CONFIG keys above.
function resolveAssetKey(symbolParam) {
  const s = (symbolParam || "GOLD").toUpperCase().replace(/[^A-Z0-9]/g, "");
  if (SYMBOL_CONFIG[s]) return s; // exact key match
  if (s.includes("NAS")) return "NASDAQ";
  if (s.includes("DOW") || s.includes("US30")) return "US30";
  if (s.includes("SPX") || s.includes("US500") || s.includes("SP500")) return "US500";
  if (s.includes("EUR") && s.includes("USD")) return "EURUSD";
  if (s.includes("GBP") && s.includes("USD")) return "GBPUSD";
  if (s.includes("JPY")) return "USDJPY";
  if (s.includes("XAU") || s.includes("GOLD")) return "GOLD";
  return "GOLD"; // default, same fallback as before
}

function roundToDecimals(value, decimals) {
  const f = Math.pow(10, decimals);
  return Math.round(value * f) / f;
}

// Pads `realCandles` (oldest-first) out to a full 7-day window of
// `intervalSeconds`-spaced bars for the given symbol config, generating
// synthetic bars walking backward in time from whatever the earliest real
// candle is (or from "now" using the fallback price if there's no real data
// at all). The synthetic run is chained close-to-open back through time —
// same technique the frontend already uses for its own warm-up bars — so
// the newest synthetic bar closes exactly where real history begins and
// there's no visible seam at the join.
function backfillHistoricalCandles(realCandles, cfg, intervalSeconds, assetKey) {
  const targetCount = Math.floor(SEVEN_DAYS_SECONDS / intervalSeconds);
  const needed = targetCount - realCandles.length;
  if (needed <= 0) return realCandles;

  const anchor = realCandles[0]; // earliest real candle, since realCandles is oldest-first

  // Only trust the real anchor candle's open if it's an actual usable price.
  // Falling through on anything else (missing candle, NaN/0 from a
  // malformed API response, etc.) is what prevents the synthetic run from
  // chaining off garbage and creating a vertical seam against real data (or
  // against the separate live tick feed). When there's no usable real
  // candle at all, prefer the most recent live tick we've actually seen
  // (lastPrice[assetKey], updated continuously by the WebSocket feed) over
  // the static cfg.fallbackPrice — a live tick can never go stale the way a
  // hardcoded number in source can, which is exactly what caused the
  // multi-thousand-point cliff this function is meant to prevent. The
  // static fallback is now only a last resort for the brief window right
  // after a fresh server restart before any tick has arrived.
  const anchorOpen = anchor ? anchor.open : null;
  const liveAnchor = assetKey && Number.isFinite(lastPrice[assetKey]) && lastPrice[assetKey] > 0 ? lastPrice[assetKey] : null;
  const anchorPrice = Number.isFinite(anchorOpen) && anchorOpen > 0
    ? anchorOpen
    : (liveAnchor ?? cfg.fallbackPrice);
  const anchorTime = anchor ? anchor.time : Math.floor(Date.now() / 1000);
  const vol = anchorPrice * cfg.volatilityPct;

  const synthetic = new Array(needed);
  let closeCursor = anchorPrice;
  let t = anchorTime;
  for (let i = needed - 1; i >= 0; i--) {
    t -= intervalSeconds;
    const close = closeCursor;
    const open = close + (Math.random() - 0.5) * vol;
    const high = Math.max(open, close) + Math.random() * vol * 0.4;
    const low = Math.min(open, close) - Math.random() * vol * 0.4;
    synthetic[i] = {
      time: t,
      open: roundToDecimals(open, cfg.decimals),
      high: roundToDecimals(high, cfg.decimals),
      low: roundToDecimals(low, cfg.decimals),
      close: roundToDecimals(close, cfg.decimals)
    };
    closeCursor = open;
  }

  return synthetic.concat(realCandles);
}

// Twelve Data's `datetime` field ("YYYY-MM-DD HH:mm:ss" intraday, or
// "YYYY-MM-DD" for daily+) never includes a timezone designator. Even with
// &timezone=UTC on the request (see the url below), new Date(datetimeStr)
// alone would still parse that bare string in whatever timezone the Node
// process happens to default to — which is exactly the kind of silent skew
// that desyncs historical candle times from the live Finnhub feed (always
// UTC via Date.now()). Appending an explicit "Z" pins the parse to UTC
// regardless of hosting environment, independent of the query param.
function parseTwelveDataTimeUTC(datetimeStr) {
  const iso = String(datetimeStr).includes('T') ? datetimeStr : String(datetimeStr).replace(' ', 'T');
  const withZone = /[Zz]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : iso + 'Z';
  return Math.floor(new Date(withZone).getTime() / 1000);
}

function handleCandlesRequest(searchParams, res) {
  const symbolParam = (searchParams.get("symbol") || "GOLD").toUpperCase();
  const intervalParam = searchParams.get("interval") || "5m";
  const assetKey = resolveAssetKey(symbolParam);
  const cfg = SYMBOL_CONFIG[assetKey];

  // Twelve Data uses "1min"/"5min" rather than our "1m"/"5m" shorthand.
  const tdInterval = intervalParam === "1m" ? "1min" : "5min";
  const intervalSeconds = intervalParam === "1m" ? 60 : 300;
  const outputsize = Number(searchParams.get("outputsize")) || 500;

  // timezone=UTC is what actually pins Twelve Data's `datetime` values to
  // UTC. Without it, intraday datetimes come back in the exchange's local
  // timezone with no offset marker — silently skewed against the live
  // Finnhub feed below, which is always UTC via Date.now().
  const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(cfg.tdSymbol)}&interval=${tdInterval}&outputsize=${outputsize}&timezone=UTC&apikey=${TWELVE_DATA_API_KEY}`;

  const sendJson = (candles) => {
    const payload = JSON.stringify(candles);
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      // Explicit Content-Length (rather than chunked encoding) so large
      // multi-thousand-candle payloads aren't cut short client-side.
      "Content-Length": Buffer.byteLength(payload),
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*"
    });
    res.end(payload);
  };

  // Every exit path (provider error, malformed response, network failure,
  // or just a thin real history) funnels through here so every symbol —
  // not only Gold — reliably gets a full 7-day array back.
  const finalize = (realCandles) => {
    const candles = realCandles.length < HISTORY_BACKFILL_THRESHOLD
      ? backfillHistoricalCandles(realCandles, cfg, intervalSeconds, assetKey)
      : realCandles;
    sendJson(candles);
  };

  https.get(url, (apiRes) => {
    let body = "";
    apiRes.setEncoding("utf8");
    apiRes.on("data", (chunk) => { body += chunk; });
    apiRes.on("end", () => {
      try {
        const json = JSON.parse(body);
        if (json.status === "error" || json.code) {
          console.error(`[candles] Twelve Data error for ${assetKey}:`, json.message || body);
          return finalize([]);
        }
        if (!json.values || !Array.isArray(json.values)) return finalize([]);

        // Twelve Data returns newest-first; reverse for Lightweight Charts.
        const formattedCandles = json.values.slice().reverse().map((c) => ({
          time: parseTwelveDataTimeUTC(c.datetime),
          open: roundToDecimals(parseFloat(c.open), cfg.decimals),
          high: roundToDecimals(parseFloat(c.high), cfg.decimals),
          low: roundToDecimals(parseFloat(c.low), cfg.decimals),
          close: roundToDecimals(parseFloat(c.close), cfg.decimals)
        }));
        finalize(formattedCandles);
      } catch (err) {
        console.error(`[candles] Failed to parse Twelve Data response for ${assetKey}:`, err.message);
        finalize([]);
      }
    });
  }).on("error", (err) => {
    console.error(`[candles] Twelve Data request failed for ${assetKey}:`, err.message);
    finalize([]);
  });
}

// --- HTTP server (Render needs something bound to PORT) ----------------
const server = http.createServer((req, res) => {
  let parsedUrl;
  try {
    parsedUrl = new URL(req.url, "http://localhost");
  } catch (err) {
    res.writeHead(400, { "Content-Type": "text/plain" });
    return res.end("Bad request\n");
  }

  if (parsedUrl.pathname === "/api/candles") {
    return handleCandlesRequest(parsedUrl.searchParams, res);
  }

  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("price-server relay is running\n");
});

// --- WebSocket server for game clients -----------------------------------
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws, req) => {
  let requestedAsset = "GOLD";
  try {
    const url = new URL(req.url, "http://localhost");
    const param = url.searchParams.get("symbol");
    if (param && param.toUpperCase() === "NASDAQ") {
      requestedAsset = "NASDAQ";
    }
  } catch (err) {
    // Fallback to GOLD on parsing errors
  }

  const assetRoom = rooms[requestedAsset] ? requestedAsset : "GOLD";
  rooms[assetRoom].add(ws);
  ws.asset = assetRoom;

  console.log(`[clients] Connected to ${assetRoom}. Total in room: ${rooms[assetRoom].size}`);

  if (lastPrice[assetRoom] !== null) {
    ws.send(JSON.stringify({
      symbol: ASSET_SYMBOLS[assetRoom],
      asset: assetRoom,
      price: lastPrice[assetRoom],
      source: "broker",
      timestamp: Date.now()
    }));
  }

  ws.on("close", () => {
    rooms[assetRoom].delete(ws);
    console.log(`[clients] Disconnected from ${assetRoom}. Total remaining: ${rooms[assetRoom].size}`);
  });

  ws.on("error", (err) => {
    console.error(`[clients] Socket error in room ${assetRoom}:`, err.message);
  });
});

function broadcastToRoom(asset, payload) {
  const message = JSON.stringify(payload);
  if (rooms[asset]) {
    for (const client of rooms[asset]) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  }
}

// --- Automated Engine to monitor and execute TP/SL targets ----------------
async function checkTpSl(asset, price) {
  if (!supabaseAdmin) return;

  try {
    const res = await supabaseAdmin
      .from("duel_trades")
      .select("id, side, entry_price, tp, sl, duel_id, trading_duels!inner(asset,status)")
      .eq("status", "open")
      .eq("trading_duels.asset", asset)
      .eq("trading_duels.status", "active");

    if (res.error || !res.data || res.data.length === 0) return;

    for (let i = 0; i < res.data.length; i++) {
      const t = res.data[i];
      let hit = null;

      if (t.side === "buy") {
        if (t.tp != null && price >= t.tp) hit = "tp";
        else if (t.sl != null && price <= t.sl) hit = "sl";
      } else {
        if (t.tp != null && price <= t.tp) hit = "tp";
        else if (t.sl != null && price >= t.sl) hit = "sl";
      }

      if (hit) {
        const updateRes = await supabaseAdmin
          .from("duel_trades")
          .update({
            status: "closed",
            close_price: price,
            closed_at: new Date().toISOString(),
            closed_reason: hit
          })
          .eq("id", t.id)
          .eq("status", "open");

        if (!updateRes.error) {
          console.log(`[engine] Auto-closed trade ${t.id} due to ${hit.toUpperCase()} hit at ${price}`);
          broadcastToRoom(asset, {
            type: "trade_closed",
            tradeId: t.id,
            reason: hit,
            price: price,
            asset: asset
          });
        }
      }
    }
  } catch (err) {
    console.error("[engine] Error executing target checks:", err.message);
  }
}

// --- Finnhub upstream connection with exponential backoff ----------------
let finnhubSocket = null;
let reconnectAttempt = 0;
const BASE_DELAY_MS = 1000;
const MAX_DELAY_MS = 30000;

function getBackoffDelay() {
  const delay = Math.min(BASE_DELAY_MS * Math.pow(2, reconnectAttempt), MAX_DELAY_MS);
  return delay + Math.floor(Math.random() * 500);
}

function connectToFinnhub() {
  if (!FINNHUB_TOKEN) {
    console.log("[finnhub] No token set yet — retrying in 10s.");
    setTimeout(connectToFinnhub, 10000);
    return;
  }

  finnhubSocket = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_TOKEN}`);

  finnhubSocket.on("open", () => {
    console.log("[finnhub] Connected");
    reconnectAttempt = 0;
    Object.values(ASSET_SYMBOLS).forEach((sym) => {
      finnhubSocket.send(JSON.stringify({ type: "subscribe", symbol: sym }));
      console.log(`[finnhub] Subscribed to ${sym}`);
    });
  });

  finnhubSocket.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (err) {
      console.error("[finnhub] Failed to parse message:", err.message);
      return;
    }

    if (data.type === "trade" && Array.isArray(data.data)) {
      for (const tick of data.data) {
        let asset = null;
        if (tick.s === ASSET_SYMBOLS.NASDAQ) asset = "NASDAQ";
        else if (tick.s === ASSET_SYMBOLS.GOLD) asset = "GOLD";

        if (!asset) continue;

        const price = tick.p;
        lastPrice[asset] = price;

        const outgoing = {
          symbol: tick.s,
          asset: asset,
          price: price,
          timestamp: Date.now(),
          source: "broker"
        };

        broadcastToRoom(asset, outgoing);
        checkTpSl(asset, price);
      }
    }
  });

  finnhubSocket.on("close", () => {
    const delay = getBackoffDelay();
    console.log(`[finnhub] Connection closed. Reconnecting in ${delay}ms (attempt ${reconnectAttempt + 1}).`);
    reconnectAttempt++;
    setTimeout(connectToFinnhub, delay);
  });

  finnhubSocket.on("error", (err) => {
    console.error("[finnhub] Socket error:", err.message);
    finnhubSocket.close();
  });
}

connectToFinnhub();

// --- Start listening -----------------------------------------------------
server.listen(PORT, () => {
  console.log(`[server] price-server relay listening on port ${PORT}`);
});