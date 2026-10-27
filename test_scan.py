import asyncio
import time
import ccxt.async_support as ccxt
import pandas as pd

USE_CLOSED_CANDLE = False

async def fetch_and_analyze(semaphore, exchange, symbol, volume_24h, atl_cache):
    async with semaphore:
        try:
            ohlcv_1d = await exchange.fetch_ohlcv(symbol, timeframe='1d', limit=100)
            if len(ohlcv_1d) < 100:
                return None
            
            df_1d = pd.DataFrame(ohlcv_1d, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            candle_index = -2 if USE_CLOSED_CANDLE else -1
            
            ema_99_1d = df_1d['close'].ewm(span=99, adjust=False).mean().iloc[candle_index]
            price_1d = df_1d['close'].iloc[candle_index]

            if price_1d >= 1:
                return None
            if price_1d >= ema_99_1d:
                return None 

            ohlcv_4h = await exchange.fetch_ohlcv(symbol, timeframe='4h', limit=100)
            if len(ohlcv_4h) < 100:
                return None

            df_4h = pd.DataFrame(ohlcv_4h, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            ema_99_4h = df_4h['close'].ewm(span=99, adjust=False).mean().iloc[candle_index]
            price_4h = df_4h['close'].iloc[candle_index]

            if price_4h > ema_99_4h:
                if symbol in atl_cache:
                    atl = min(atl_cache[symbol], price_4h)
                else:
                    ohlcv_monthly = await exchange.fetch_ohlcv(symbol, timeframe='1M', limit=1000)
                    atl = min(candle[3] for candle in ohlcv_monthly) if ohlcv_monthly else 0
                    atl_cache[symbol] = atl

                return {
                    'symbol': symbol,
                    'price': price_4h,
                    'atl': atl,
                    'ema_4h': ema_99_4h,
                    'ema_1d': ema_99_1d,
                    'volume_24h': volume_24h
                }
        except Exception as e:
            return None

async def run_scan():
    ex = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    await ex.load_markets()
    tickers = await ex.fetch_tickers()
    
    symbols_to_scan = []
    volumes_dict = {}
    for s, m in ex.markets.items():
        if m.get('active') and m.get('linear') and m.get('quote') == 'USDT':
            t = tickers.get(s, {})
            v = t.get('quoteVolume', 0)
            p = t.get('last')
            if v and v >= 5_000_000 and p is not None and p < 1.0:
                symbols_to_scan.append(s)
                volumes_dict[s] = v

    print(f"Scanning {len(symbols_to_scan)} candidate pairs...")
    t0 = time.time()
    semaphore = asyncio.Semaphore(15)
    atl_cache = {}
    tasks = [fetch_and_analyze(semaphore, ex, s, volumes_dict[s], atl_cache) for s in symbols_to_scan]
    results = await asyncio.gather(*tasks)
    matches = [r for r in results if r is not None]
    matches.sort(key=lambda x: x['volume_24h'], reverse=True)
    dt = time.time() - t0
    print(f"Done in {dt:.2f}s! Found {len(matches)} matches:")
    for m in matches:
        vol_m = m['volume_24h'] / 1_000_000
        print(f"  {m['symbol']:<12} Price: {m['price']:<8g} | ATL: {m['atl']:<8g} | 4H EMA: {m['ema_4h']:<8.4f} | 1D EMA: {m['ema_1d']:<8.4f} | Vol: ${vol_m:>6.2f}M")
    await ex.close()

if __name__ == '__main__':
    asyncio.run(run_scan())
