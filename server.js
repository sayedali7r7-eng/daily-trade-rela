// server.js — Finnhub -> WebSocket relay for the trading game frontend
// Forwards live price ticks to connected clients in the exact shape the
// frontend expects, including the "source": "broker" field that unlocks
// the Buy/Sell buttons client-side.

try {
  require("dotenv").config();
} catch (err) {
  // dotenv not installed — fine in production, just skip.
}

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const https = require("https");
const { createClient } = require("@supabase/supabase-js");

// --- Config ------------------------------------------------------------
const PORT = process.env.PORT || 8787;
const FINNHUB_TOKEN = process.env.FINNHUB_TOKEN || process.env.FINNHUB_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const app = express();

// CORS Middleware للسماح للموقع بجلب البيانات بدون مشاكل Security
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

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
    "relayed until FINNHUB_TOKEN is set."
  );
}

// Initialize Supabase admin client
let supabaseAdmin = null;
if (SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY) {
  supabaseAdmin = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
  console.log("[supabase] Admin client initialized for TP/SL monitoring");
} else {
  console.error("[supabase] Missing credentials. Automatic TP/SL execution disabled.");
}

// --- HTTP Routes -------------------------------------------------------
app.get("/", (req, res) => {
  res.send("price-server relay is running\n");
});

// Endpoint لجلب الشموع التاريخية (محدثة لتعمل عبر Twelve Data للذهب والنازداك مجاناً)
app.get("/api/candles", (req, res) => {
  try {
    const symbolParam = req.query.symbol || "GOLD";
    let twelvedataSymbol = "XAU/USD";

    if (symbolParam.toUpperCase().includes("NAS") || symbolParam.toUpperCase().includes("NASDAQ")) {
      twelvedataSymbol = "NDX";
    }

    const apiKey = process.env.TWELVEDATA_API_KEY || "demo"; 
    const interval = req.query.resolution === "1" ? "1min" : "5min";

    const url = `https://api.twelvedata.com/time_series?symbol=${twelvedataSymbol}&interval=${interval}&outputsize=100&apikey=${apiKey}`;

    https.get(url, (apiRes) => {
      let body = "";
      apiRes.on("data", (chunk) => body += chunk);
      apiRes.on("end", () => {
        try {
          const data = JSON.parse(body);
          if (data.status === "error" || !data.values) {
            return res.status(400).json({ error: "Failed to fetch candles", details: data });
          }

          // ترتيب البيانات لتكون من القديم إلى الحديث بالشكل المناسب للشارت
          const formattedCandles = data.values.reverse().map((item) => ({
            time: Math.floor(new Date(item.datetime).getTime() / 1000),
            open: parseFloat(item.open),
            high: parseFloat(item.high),
            low: parseFloat(item.low),
            close: parseFloat(item.close)
          }));

          res.json(formattedCandles);
        } catch (e) {
          res.status(500).json({ error: "Failed to parse candle data" });
        }
      });
    }).on("error", (err) => {
      res.status(500).json({ error: err.message });
    });

  } catch (error) {
    res.status(500).json({ error: "Server error", details: error.message });
  }
});

// Create HTTP server wrapping Express app
const server = http.createServer(app);

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