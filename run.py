"""
Binance Futures Real-Time Scanner Launcher
Starts the dashboard server and opens http://localhost:8080 in your default browser.
"""

import os
import sys
import time
import threading
import webbrowser
from aiohttp import web
from server import create_app

PORT = int(os.environ.get("PORT", 8080))

def open_browser():
    time.sleep(1.2)
    url = f"http://localhost:{PORT}"
    print(f"\n[+] Opening Dashboard in Browser: {url}\n")
    webbrowser.open(url)

if __name__ == "__main__":
    print("=" * 65)
    print("  BINANCE FUTURES EMA STRATEGY SCANNER DASHBOARD")
    print("=" * 65)
    print(f"  * Mode: Real-time 15-second Continuous Background Scanner")
    print(f"  * Strategy: Price < $1.00 & Price < 1D EMA(99) & Price > 4H EMA(99)")
    print(f"  * Dashboard URL: http://localhost:{PORT}")
    print("=" * 65)

    threading.Thread(target=open_browser, daemon=True).start()
    app = create_app()
    web.run_app(app, host="127.0.0.1", port=PORT)
