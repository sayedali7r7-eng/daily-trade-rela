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

// --- Config ------------------------------------------------------------
const PORT = process.env.PORT || 8787;            // Render assigns PORT automatically
const FINNHUB_TOKEN = process.env.FINNHUB_TOKEN;  // Set this in Render's Environment tab
const SYMBOL = "OANDA:XAU_USD";

if (!FINNHUB_TOKEN) {
  console.error(
    "[config] Missing FINNHUB_TOKEN environment variable. " +
    "The HTTP/WebSocket server will still start, but no price data will be " +
    "relayed until FINNHUB_TOKEN is set (Render: Environment tab -> Add Environment Variable)."
  );
}

// --- HTTP server (Render needs something bound to PORT) ----------------
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("price-server relay is running\n");
});

// --- WebSocket server for game clients -----------------------------------
const wss = new WebSocket.Server({ server });
const clients = new Set();

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`[clients] Connected. Total: ${clients.size}`);

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`[clients] Disconnected. Total: ${clients.size}`);
  });

  ws.on("error", (err) => {
    console.error("[clients] Socket error:", err.message);
  });
});

function broadcast(payload) {
  const message = JSON.stringify(payload);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

// --- Finnhub upstream connection with exponential backoff ----------------
let finnhubSocket = null;
let reconnectAttempt = 0;
const BASE_DELAY_MS = 1000;
const MAX_DELAY_MS = 30000;

function getBackoffDelay() {
  const delay = Math.min(BASE_DELAY_MS * Math.pow(2, reconnectAttempt), MAX_DELAY_MS);
  // add jitter so multiple reconnects don't stack in lockstep
  return delay + Math.floor(Math.random() * 500);
}

function connectToFinnhub() {
  if (!FINNHUB_TOKEN) {
    // Keep retrying periodically in case the token is added later
    // (e.g. Render env var saved after the service already booted).
    console.log("[finnhub] No token set yet — retrying in 10s.");
    setTimeout(connectToFinnhub, 10000);
    return;
  }

  finnhubSocket = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_TOKEN}`);

  finnhubSocket.on("open", () => {
    console.log("[finnhub] Connected");
    reconnectAttempt = 0; // reset backoff on a successful connection
    finnhubSocket.send(JSON.stringify({ type: "subscribe", symbol: SYMBOL }));
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
        const outgoing = {
          symbol: tick.s || SYMBOL,
          price: tick.p,
          timestamp: Date.now(),
          source: "broker" // required by the frontend's gating check
        };
        broadcast(outgoing);
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
