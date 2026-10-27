# Binance Futures EMA Strategy Real-Time Scanner Dashboard

An ultra-responsive, real-time crypto scanner and terminal dashboard built with Python (`ccxt`, `aiohttp`, `pandas`) and Tailwind CSS. It scans Binance USDT perpetual futures contracts every 15 seconds using a dual-timeframe EMA reversal strategy with All-Time Low (ATL) tracking.

---

## ⚡ Strategy Blueprint

The scanner identifies **early momentum reversals on high-volume sub-dollar coins** using a 4H/1D EMA trend sandwich:

1. **Liquidity Filter**: Active linear USDT futures with **24h Volume $\ge$ $5,000,000 USDT**.
2. **Sub-Dollar Filter**: Current price **$<\$1.00$** (focuses on micro-caps with explosive potential).
3. **Macro Oversold Filter (1D Timeframe)**: Current price is **BELOW the 1D EMA(99)**, ensuring the asset is deeply discounted / in a long-term macro base.
4. **Breakout Trigger (4H Timeframe)**: Current price has crossed **ABOVE the 4H EMA(99)**, signaling a short-term trend reversal and bullish momentum surge.
5. **All-Time Low (ATL) Anchor**: Fetches historical monthly candles to determine the coin's All-Time Low, computing exact percentage distance above ATL.

---

## 🚀 Quick Start

### 1. Access the Running Dashboard
The dashboard is currently live and running locally at:
👉 **[http://localhost:8080](http://localhost:8080)**

### 2. Manual Start / Run Later
To restart the dashboard anytime:
```bash
python run.py
```
This will automatically launch the server and open your default web browser to the dashboard.

---

## 🖥️ Dashboard Features

- **Live 15-Second Refresh Cycle**: Continuously scans in the background with an animated circular countdown ring.
- **Real-Time WebSocket Updates**: Live state pushes directly to the browser without full page refreshes.
- **1-Click TradingView Watchlist**: Copies `BINANCE:SYMBOLUSDT.P` directly to clipboard formatted for instant import into TradingView.
- **Interactive Metric Range Visualizer**: Displays a progress bar showing where price sits between the 4H EMA support floor and 1D EMA resistance ceiling.
- **Audio Chime Alerts**: Synthesizes a clean two-tone chime via the Web Audio API whenever a new breakout coin is detected.
- **Live Configuration Drawer**: Adjust volume thresholds, maximum price, EMA span (e.g. 50, 99, 200), and candle mode (live unclosed vs closed candle).
- **Interactive Search & Multi-Column Sorting**: Sort by 24h volume, price, % above 4H EMA, or room to 1D EMA.
- **CSV Export**: Export discovered opportunities with complete metrics to a timestamped CSV file.

---

## 📁 Project Structure

```
binance-crypto-scanner/
├── scanner.py          # Asynchronous scanning engine with CCXT and ATL caching
├── server.py           # Aiohttp HTTP and WebSocket server
├── run.py              # Launcher script with auto browser popup
├── test_scan.py        # Standalone terminal test script
├── static/
│   └── index.html      # Glassmorphic dark terminal dashboard UI
└── README.md           # Documentation
```

---

## ⚙️ Configuration Endpoints

- `GET  /api/status` : Current scanner state, matches, metrics, and logs in JSON.
- `POST /api/scan`   : Trigger an immediate scan on demand.
- `POST /api/config` : Dynamically update parameters (`min_volume`, `max_price`, `ema_span`, `interval_seconds`, `use_closed_candle`, `paused`).
- `GET  /api/tradingview` : Raw comma-separated string for TradingView import.
- `GET  /ws`         : Real-time WebSocket event stream.
