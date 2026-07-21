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

// --- Candle history endpoint (Twelve Data passthrough) ------------------
// Maps our internal asset names to Twelve Data's symbol format.
const TWELVE_DATA_SYMBOLS = {
  GOLD: "XAU/USD",
  NASDAQ: "NDX"
};

// Keep the key out of source control, same pattern as FINNHUB_TOKEN /
// SUPABASE_SERVICE_ROLE_KEY above. Falls back to the key you shared so this
// keeps working immediately — move it into Render's Environment tab (and
// rotate it, since it's now been pasted into a chat) when you get a chance.
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY || "af0b3fc31acb4ee8a372f586d23b1b51";

function resolveTwelveDataSymbol(symbolParam) {
  const s = (symbolParam || "GOLD").toUpperCase();
  if (s.includes("NAS") || s.includes("NASDAQ")) return TWELVE_DATA_SYMBOLS.NASDAQ;
  return TWELVE_DATA_SYMBOLS.GOLD; // default / GOLD / XAU
}

function handleCandlesRequest(searchParams, res) {
  const symbolParam = (searchParams.get("symbol") || "GOLD").toUpperCase();
  const intervalParam = searchParams.get("interval") || "5m";
  const tdSymbol = resolveTwelveDataSymbol(symbolParam);

  // Twelve Data uses "1min"/"5min" rather than our "1m"/"5m" shorthand.
  const tdInterval = intervalParam === "1m" ? "1min" : "5min";
  const outputsize = Number(searchParams.get("outputsize")) || 500;

  const url = `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(tdSymbol)}&interval=${tdInterval}&outputsize=${outputsize}&apikey=${TWELVE_DATA_API_KEY}`;

  const sendJson = (candles) => {
    const payload = JSON.stringify(candles);
    res.writeHead(200, {
      "Content-Type": "application/json; charset=utf-8",
      // Explicit Content-Length (rather than chunked encoding) so large
      // 500-candle payloads aren't cut short client-side.
      "Content-Length": Buffer.byteLength(payload),
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*"
    });
    res.end(payload);
  };

  https.get(url, (apiRes) => {
    let body = "";
    apiRes.setEncoding("utf8");
    apiRes.on("data", (chunk) => { body += chunk; });
    apiRes.on("end", () => {
      try {
        const json = JSON.parse(body);
        if (json.status === "error" || json.code) {
          console.error("[candles] Twelve Data error:", json.message || body);
          return sendJson([]);
        }
        if (!json.values || !Array.isArray(json.values)) return sendJson([]);

        // Twelve Data returns newest-first; reverse for Lightweight Charts.
        const formattedCandles = json.values.slice().reverse().map((c) => ({
          time: Math.floor(new Date(c.datetime).getTime() / 1000),
          open: Number(parseFloat(c.open).toFixed(2)),
          high: Number(parseFloat(c.high).toFixed(2)),
          low: Number(parseFloat(c.low).toFixed(2)),
          close: Number(parseFloat(c.close).toFixed(2))
        }));
        sendJson(formattedCandles);
      } catch (err) {
        console.error("[candles] Failed to parse Twelve Data response:", err.message);
        sendJson([]);
      }
    });
  }).on("error", (err) => {
    console.error("[candles] Twelve Data request failed:", err.message);
    sendJson([]);
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