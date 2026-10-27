"""
Aiohttp Web Server for the Binance Futures Crypto Scanner Dashboard.
Integrates real-time WebSocket push, REST APIs, and modern responsive UI.
"""

import asyncio
import json
import os
import sys
from aiohttp import web, WSMsgType
from scanner import CryptoScanner

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

scanner = CryptoScanner(
    min_volume=5_000_000.0,
    max_price=1.0,
    ema_span=99,
    use_closed_candle=False,
    interval_seconds=15,
    concurrency=20,
)

connected_websockets = set()


async def broadcast_to_clients(payload: dict):
    if not connected_websockets:
        return
    msg = json.dumps(payload)
    dead_sockets = set()
    for ws in list(connected_websockets):
        try:
            await ws.send_str(msg)
        except Exception:
            dead_sockets.add(ws)
    connected_websockets.difference_update(dead_sockets)


scanner.add_listener(broadcast_to_clients)


# --- HTTP Handlers ---

async def index_handler(request):
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return web.FileResponse(index_path)
    return web.Response(text="Dashboard static file missing", status=404)


async def api_status(request):
    return web.json_response(scanner.get_state())


async def api_scan(request):
    scanner.trigger_scan_now()
    return web.json_response({"status": "scan_triggered", "is_scanning": scanner.is_scanning})


async def api_config(request):
    try:
        data = await request.json()
        scanner.update_config(
            min_volume=data.get("min_volume"),
            max_price=data.get("max_price"),
            ema_span=data.get("ema_span"),
            use_closed_candle=data.get("use_closed_candle"),
            interval_seconds=data.get("interval_seconds"),
            paused=data.get("paused"),
        )
        await scanner._notify("config_updated")
        return web.json_response({"status": "updated", "config": scanner.get_state()["config"]})
    except Exception as e:
        return web.json_response({"error": str(e)}, status=400)


async def api_tradingview(request):
    state = scanner.get_state()
    return web.Response(text=state["tv_watchlist"], content_type="text/plain")


async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=15.0)
    await ws.prepare(request)
    connected_websockets.add(ws)

    # Immediately push current state snapshot upon connect
    try:
        await ws.send_str(json.dumps({
            "event": "init",
            "data": scanner.get_state()
        }))
    except Exception:
        pass

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    payload = json.loads(msg.data)
                    action = payload.get("action")
                    if action == "scan_now":
                        scanner.trigger_scan_now()
                    elif action == "toggle_pause":
                        scanner.paused = not scanner.paused
                        scanner.add_log(f"Scanner {'PAUSED' if scanner.paused else 'RESUMED'} by user")
                        await scanner._notify("config_updated")
                    elif action == "update_config":
                        scanner.update_config(**payload.get("config", {}))
                        await scanner._notify("config_updated")
                except Exception as ex:
                    print(f"WS message error: {ex}")
            elif msg.type == WSMsgType.ERROR:
                break
    finally:
        connected_websockets.discard(ws)

    return ws


# --- Application Lifecycle ---

async def start_background_scanner(app):
    app["scanner_task"] = asyncio.create_task(scanner.run_loop())


async def cleanup_background_scanner(app):
    app["scanner_task"].cancel()
    try:
        await app["scanner_task"]
    except asyncio.CancelledError:
        pass
    await scanner.close()
    for ws in list(connected_websockets):
        await ws.close(code=1001, message=b"Server shutting down")


def create_app() -> web.Application:
    os.makedirs(STATIC_DIR, exist_ok=True)
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/api/status", api_status)
    app.router.add_post("/api/scan", api_scan)
    app.router.add_post("/api/config", api_config)
    app.router.add_get("/api/tradingview", api_tradingview)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_static("/static", STATIC_DIR)

    app.on_startup.append(start_background_scanner)
    app.on_cleanup.append(cleanup_background_scanner)
    return app


if __name__ == "__main__":
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8080))
    app = create_app()
    print(f"\n=======================================================")
    print(f" Binance Futures Scanner Dashboard Starting on Port {port}")
    print(f" Access URL: http://{host}:{port}")
    print(f" Auto-refresh cycle: 15 seconds")
    print(f"=======================================================\n")
    web.run_app(app, host=host, port=port)
