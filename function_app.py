# import logging
# import os
# import io
# import gzip
# import json
# import time
# import uuid
# from datetime import datetime, timedelta, timezone
# from collections import defaultdict

# import requests
# import azure.functions as func

# import pyodbc
# from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

# # ==========
# # Configuration
# # ==========
# BLOB_CONN_STR = os.getenv("AzureWebJobsStorage")
# BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "bitcoin-data")

# # Symbols and intervals for regular ingestion
# SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(",") if s.strip()]
# INTERVALS = [i.strip() for i in os.getenv("INTERVALS", "1m,1h,1d").split(",") if i.strip()]

# # Backfill configuration (default = 2 years, only 1h and 1d intervals for safety)
# BACKFILL_INTERVALS = [i.strip() for i in os.getenv("BACKFILL_INTERVALS", "1h,1d").split(",") if i.strip()]
# BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "730"))  # 2 years = 730 days

# # CoinGecko coin IDs
# COINGECKO_IDS = [c.strip() for c in os.getenv("COINGECKO_IDS", "bitcoin,ethereum,binancecoin").split(",") if c.strip()]

# # API endpoints
# BINANCE_SPOT = "https://api.binance.com/api/v3"
# BINANCE_FAPI = "https://fapi.binance.com/fapi/v1"
# FNG_URL = "https://api.alternative.me/fng/?limit=10"
# COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

# # ==========
# # Blob helpers
# # ==========
# _blob_service = None

# def _get_blob_service():
#     global _blob_service
#     if _blob_service is None:
#         _blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
#     return _blob_service

# def _ensure_container():
#     try:
#         svc = _get_blob_service()
#         cc = svc.get_container_client(BLOB_CONTAINER)
#         cc.create_container()  # no-op if exists
#     except Exception as e:
#         # if container already exists, ignore conflict
#         if "ContainerAlreadyExists" not in str(e):
#             raise

# def utcnow():
#     return datetime.now(timezone.utc)

# def utc_iso():
#     return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# def to_ms(dt: datetime) -> int:
#     return int(dt.timestamp() * 1000)

# def jsonl_path(prefix, **parts):
#     """
#     Build a partitioned blob path like:
#     raw/binance/klines/symbol=BTCUSDT/interval=1h/dt=2025-10-26/part-20251026T083000Z.jsonl.gz
#     """
#     p = prefix.rstrip("/")
#     # stable partition order
#     for k in ["symbol", "interval", "dt"]:
#         if k in parts and parts[k]:
#             p += f"/{k}={parts[k]}"
#     ts = utcnow().strftime("%Y%m%dT%H%M%SZ")
#     return f"{p}/part-{ts}.jsonl.gz"

# def upload_jsonl_gz(records, container, blob_path):
#     if not records:
#         return
#     buf = io.BytesIO()
#     with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
#         for r in records:
#             gz.write((json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))
#     buf.seek(0)
#     svc = _get_blob_service()
#     bc = svc.get_blob_client(container=container, blob=blob_path)
#     bc.upload_blob(buf, overwrite=True)
#     logging.info("Uploaded %d records -> %s/%s", len(records), container, blob_path)

# # ==========
# # HTTP helpers
# # ==========
# def get_json(url, params=None, retries=5, timeout=30):
#     for i in range(retries):
#         r = requests.get(url, params=params, timeout=timeout)
#         if r.status_code == 200:
#             return r.json()
#         if r.status_code in (418, 429, 451, 500, 503):
#             sleep_s = 2 ** i
#             logging.warning("HTTP %s on %s, retrying in %ss...", r.status_code, url, sleep_s)
#             time.sleep(sleep_s)
#             continue
#         r.raise_for_status()
#     raise RuntimeError(f"Failed GET {url} after {retries} attempts")

# # ==========
# # API callers
# # ==========
# def fetch_binance_klines(symbol, interval, start_ms=None, end_ms=None, limit=1000):
#     params = {"symbol": symbol, "interval": interval, "limit": limit}
#     if start_ms is not None:
#         params["startTime"] = start_ms
#     if end_ms is not None:
#         params["endTime"] = end_ms
#     return get_json(f"{BINANCE_SPOT}/klines", params=params)

# def fetch_binance_funding(symbol, start_ms=None, limit=1000):
#     params = {"symbol": symbol, "limit": limit}
#     if start_ms is not None:
#         params["startTime"] = start_ms
#     return get_json(f"{BINANCE_FAPI}/fundingRate", params=params)

# def fetch_fng():
#     return get_json(FNG_URL)

# def fetch_coingecko_markets(ids):
#     return get_json(COINGECKO_MARKETS, params={"vs_currency": "usd", "ids": ",".join(ids)})

# # ==========
# # Record mappers
# # ==========
# def klines_to_records(symbol, interval, rows):
#     ts = utc_iso()
#     out = []
#     for k in rows:
#         # Binance kline array indices:
#         # 0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume,
#         # 6 closeTime, 7 quoteAssetVolume, 8 numberOfTrades,
#         # 9 takerBuyBaseVol, 10 takerBuyQuoteVol, 11 ignore
#         out.append({
#             "symbol": symbol,
#             "interval": interval,
#             "openTime": k[0],
#             "openPrice": float(k[1]),
#             "highPrice": float(k[2]),
#             "lowPrice": float(k[3]),
#             "closePrice": float(k[4]),
#             "baseVolume": float(k[5]),
#             "closeTime": k[6],
#             "quoteVolume": float(k[7]),
#             "tradeCount": int(k[8]),
#             "takerBuyBase": float(k[9]),
#             "takerBuyQuote": float(k[10]),
#             "ingestTs": ts,
#             "source": "binance_spot"
#         })
#     return out

# def funding_to_records(symbol, rows):
#     ts = utc_iso()
#     return [{
#         "symbol": symbol,
#         "fundingTime": int(r["fundingTime"]),
#         "fundingRate": float(r["fundingRate"]),
#         "ingestTs": ts,
#         "source": "binance_perp"
#     } for r in rows]

# def fng_to_records(payload):
#     ts = utc_iso()
#     data = payload.get("data", [])
#     return [{
#         "time": int(it["timestamp"]),
#         "indexValue": int(it["value"]),
#         "sentiment": it.get("value_classification"),
#         "source": "alternative.me",
#         "ingestTs": ts
#     } for it in data]

# def markets_to_records(rows, asof_iso):
#     return [{
#         "coinId": r["id"],
#         "symbol": r.get("symbol"),
#         "name": r.get("name"),
#         "price": r.get("current_price"),
#         "marketCap": r.get("market_cap"),
#         "circulatingSupply": r.get("circulating_supply"),
#         "totalVolume": r.get("total_volume"),
#         "asof": asof_iso,
#         "source": "coingecko"
#     } for r in rows]

# # ==========
# # Ingestion (incremental)
# # ==========
# def overlap_for(interval: str) -> timedelta:
#     """Fetch extra overlap (3 candles) to prevent gaps."""
#     if interval.endswith("m"):
#         return timedelta(minutes=int(interval[:-1]) * 3)
#     if interval.endswith("h"):
#         return timedelta(hours=int(interval[:-1]) * 3)
#     if interval.endswith("d"):
#         return timedelta(days=int(interval[:-1]) * 3)
#     return timedelta(hours=3)

# def run_klines_once(symbol: str, interval: str):
#     now = utcnow()
#     dt = now.strftime("%Y-%m-%d")
#     start_ms = to_ms(now - overlap_for(interval))
#     rows = fetch_binance_klines(symbol, interval, start_ms=start_ms)
#     recs = klines_to_records(symbol, interval, rows)
#     blob = jsonl_path("raw/binance/klines", symbol=symbol, interval=interval, dt=dt)
#     upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

# def run_funding_once(symbol: str = "BTCUSDT"):
#     now = utcnow()
#     dt = now.strftime("%Y-%m-%d")
#     rows = fetch_binance_funding(symbol, start_ms=to_ms(now - timedelta(days=30)))
#     recs = funding_to_records(symbol, rows)
#     blob = jsonl_path("raw/binance/funding", symbol=symbol, dt=dt)
#     upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

# def run_fng_once():
#     now = utcnow()
#     dt = now.strftime("%Y-%m-%d")
#     payload = fetch_fng()
#     recs = fng_to_records(payload)
#     blob = jsonl_path("raw/altme/fear_greed", dt=dt)
#     upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

# def run_coingecko_once():
#     now = utcnow()
#     dt = now.strftime("%Y-%m-%d")
#     asof_iso = utc_iso()
#     rows = fetch_coingecko_markets(COINGECKO_IDS)
#     recs = markets_to_records(rows, asof_iso)
#     blob = jsonl_path("raw/coingecko/markets", dt=dt)
#     upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

# # ==========
# # Function App
# # ==========
# app = func.FunctionApp()

# # ---- Regular Ingestion (every 6h) ----
# @app.timer_trigger(
#     schedule="0 0 */6 * * *",
#     arg_name="myTimer",
#     run_on_startup=False,
#     use_monitor=False
# )
# def IngestCrypto(myTimer: func.TimerRequest) -> None:
#     if myTimer.past_due:
#         logging.info("Timer is past due.")

#     logging.info("Starting incremental ingestion...")
#     _ensure_container()

#     try:
#         # 1) Binance klines for all symbols/intervals
#         for sym in SYMBOLS:
#             for itv in INTERVALS:
#                 try:
#                     run_klines_once(sym, itv)
#                 except Exception as e:
#                     logging.exception("Klines failed for %s %s: %s", sym, itv, e)

#         # 2) Funding (BTC perp)
#         try:
#             run_funding_once("BTCUSDT")
#         except Exception as e:
#             logging.exception("Funding fetch failed: %s", e)

#         # 3) Fear & Greed
#         try:
#             run_fng_once()
#         except Exception as e:
#             logging.exception("Fear & Greed fetch failed: %s", e)

#         # 4) CoinGecko markets
#         try:
#             run_coingecko_once()
#         except Exception as e:
#             logging.exception("CoinGecko fetch failed: %s", e)

#         logging.info("Ingestion cycle completed.")
#     except Exception as e:
#         logging.exception("Fatal error during ingestion: %s", e)

# # ==========
# # BACKFILL
# # ==========
# def backfill_klines(symbol: str, interval: str, start_dt: datetime, end_dt: datetime):
#     """
#     Fetch Binance historical klines and group by date before saving.
#     This prevents overwriting files and ensures all data is preserved.
#     """
#     start_ms = to_ms(start_dt)
#     end_ms = to_ms(end_dt)
#     logging.info("Backfilling %s %s from %s to %s", symbol, interval, start_dt, end_dt)

#     # Memory-efficient: flush every 10k records or 7 days span
#     records_by_date = defaultdict(list)
#     batch_count = 0
#     total_saved = 0
#     FLUSH_THRESHOLD = 10000
#     FLUSH_DAYS = 7

#     def flush_to_storage():
#         nonlocal total_saved
#         if not records_by_date:
#             return
        
#         for date_key in sorted(records_by_date.keys()):
#             date_recs = records_by_date[date_key]
            
#             # Generate unique filename to prevent overwrites
#             ts = utcnow().strftime("%Y%m%dT%H%M%S")
#             unique_id = str(uuid.uuid4())[:8]
            
#             blob = f"raw/binance/klines/symbol={symbol}/interval={interval}/dt={date_key}/part-{ts}-{unique_id}.jsonl.gz"
#             upload_jsonl_gz(date_recs, BLOB_CONTAINER, blob)
            
#             logging.info("Saved %d records for %s", len(date_recs), date_key)
#             total_saved += len(date_recs)
        
#         records_by_date.clear()

#     while True:
#         rows = fetch_binance_klines(symbol, interval, start_ms=start_ms, end_ms=end_ms, limit=1000)
#         if not rows:
#             break

#         batch_count += 1
#         recs = klines_to_records(symbol, interval, rows)
        
#         # Group records by their actual date (not just first record)
#         for rec in recs:
#             open_dt = datetime.fromtimestamp(rec["openTime"] / 1000, tz=timezone.utc)
#             date_key = open_dt.strftime("%Y-%m-%d")
#             records_by_date[date_key].append(rec)

#         # Check if we should flush to free memory
#         total_buffered = sum(len(v) for v in records_by_date.values())
#         date_span = len(records_by_date)
        
#         if total_buffered >= FLUSH_THRESHOLD or date_span >= FLUSH_DAYS:
#             logging.info("Flushing buffer: %d records across %d dates", total_buffered, date_span)
#             flush_to_storage()

#         # Progress logging
#         if batch_count % 10 == 0:
#             first_dt = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
#             last_dt = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
#             logging.info("Progress: batch %d, buffered %d records, current range: %s to %s", 
#                         batch_count, total_buffered, first_dt, last_dt)

#         # Move to next batch
#         last_open = rows[-1][0]
#         next_ms = last_open + 1
#         if next_ms >= end_ms:
#             break
#         start_ms = next_ms
#         time.sleep(0.1)  # Rate limiting

#     # Final flush
#     flush_to_storage()
    
#     logging.info("Backfill complete for %s %s: %d batches, %d total records saved", 
#                  symbol, interval, batch_count, total_saved)

# def backfill_funding(symbol: str, start_dt: datetime):
#     """
#     Fetch Binance perpetual funding historical data with proper date grouping.
#     """
#     start_ms = to_ms(start_dt)
#     now_ms = to_ms(utcnow())
#     logging.info("Backfilling funding rates for %s from %s", symbol, start_dt)

#     records_by_date = defaultdict(list)
#     total_saved = 0
#     FLUSH_THRESHOLD = 5000

#     def flush_to_storage():
#         nonlocal total_saved
#         if not records_by_date:
#             return
        
#         for date_key in sorted(records_by_date.keys()):
#             date_recs = records_by_date[date_key]
            
#             ts = utcnow().strftime("%Y%m%dT%H%M%S")
#             unique_id = str(uuid.uuid4())[:8]
            
#             blob = f"raw/binance/funding/symbol={symbol}/dt={date_key}/part-{ts}-{unique_id}.jsonl.gz"
#             upload_jsonl_gz(date_recs, BLOB_CONTAINER, blob)
            
#             logging.info("Saved %d funding records for %s", len(date_recs), date_key)
#             total_saved += len(date_recs)
        
#         records_by_date.clear()

#     while True:
#         rows = fetch_binance_funding(symbol, start_ms=start_ms, limit=1000)
#         if not rows:
#             break

#         recs = funding_to_records(symbol, rows)
        
#         # Group by actual funding date
#         for rec in recs:
#             funding_dt = datetime.fromtimestamp(rec["fundingTime"] / 1000, tz=timezone.utc)
#             date_key = funding_dt.strftime("%Y-%m-%d")
#             records_by_date[date_key].append(rec)

#         # Flush if buffer is large
#         total_buffered = sum(len(v) for v in records_by_date.values())
#         if total_buffered >= FLUSH_THRESHOLD:
#             logging.info("Flushing funding buffer: %d records", total_buffered)
#             flush_to_storage()

#         last_time = int(rows[-1]["fundingTime"]) + 1
#         if last_time > now_ms:
#             break
#         start_ms = last_time
#         time.sleep(0.2)

#     # Final flush
#     flush_to_storage()
#     logging.info("Funding backfill complete for %s: %d total records", symbol, total_saved)

# # ---- Timer-triggered Backfill (run once then disable) ----
# @app.timer_trigger(
#     schedule="0 10 3 * * *",  # 03:10 UTC daily
#     arg_name="myTimer",
#     run_on_startup=False,
#     use_monitor=False
# )
# def BackfillCrypto(myTimer: func.TimerRequest) -> None:
#     if myTimer.past_due:
#         logging.info("Backfill timer is past due.")

#     logging.info("Starting historical BACKFILL...")
#     _ensure_container()

#     try:
#         start_dt = utcnow() - timedelta(days=BACKFILL_DAYS)
#         end_dt = utcnow()
        
#         logging.info("Backfill period: %s to %s (%d days)", start_dt, end_dt, BACKFILL_DAYS)
#         logging.info("Symbols: %s", SYMBOLS)
#         logging.info("Intervals: %s", BACKFILL_INTERVALS)

#         # Backfill klines for each symbol/interval
#         for sym in SYMBOLS:
#             for itv in BACKFILL_INTERVALS:
#                 try:
#                     logging.info("Starting backfill: %s %s", sym, itv)
#                     backfill_klines(sym, itv, start_dt, end_dt)
#                 except Exception as e:
#                     logging.exception("Backfill klines failed for %s %s: %s", sym, itv, e)

#         # Backfill funding rates
#         try:
#             logging.info("Starting funding backfill for BTCUSDT")
#             backfill_funding("BTCUSDT", start_dt)
#         except Exception as e:
#             logging.exception("Backfill funding failed: %s", e)

#         logging.info("BACKFILL completed successfully.")
#     except Exception as e:
#         logging.exception("Fatal error during BACKFILL: %s", e)

import logging
import os
import io
import gzip
import json
import time
import uuid
import pyodbc
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

# ==========
# Configuration
# ==========
BLOB_CONN_STR = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "bitcoin-data")
SQL_CONN_STR = os.getenv("SQL_CONNECTION_STRING")

SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(",") if s.strip()]
INTERVALS = [i.strip() for i in os.getenv("INTERVALS", "1m,1h,1d").split(",") if i.strip()]
BACKFILL_INTERVALS = [i.strip() for i in os.getenv("BACKFILL_INTERVALS", "1h,1d").split(",") if i.strip()]
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "730"))

COINGECKO_IDS = [c.strip() for c in os.getenv("COINGECKO_IDS", "bitcoin,ethereum,binancecoin").split(",") if c.strip()]

# API endpoints
BINANCE_SPOT = "https://api.binance.com/api/v3"
BINANCE_FAPI = "https://fapi.binance.com/fapi/v1"
FNG_URL = "https://api.alternative.me/fng/?limit=10"
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

# ==========
# SQL Helpers
# ==========
def get_sql_connection():
    return pyodbc.connect(SQL_CONN_STR)

def get_or_create_symbol_id(symbol, is_perp=False):
    """Get existing symbol_id or create new one"""
    conn = get_sql_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT symbol_id FROM dbo.Symbol WHERE symbol = ? AND is_perp = ?", symbol, is_perp)
        result = cursor.fetchone()
        if result:
            return result[0]
        
        # Extract base/quote assets
        base_asset = symbol[:-4] if symbol.endswith('USDT') else symbol[:3]
        quote_asset = symbol[-4:] if symbol.endswith('USDT') else symbol[3:]
        
        cursor.execute("""
            INSERT INTO dbo.Symbol (symbol, base_asset, quote_asset, is_perp, is_active)
            OUTPUT INSERTED.symbol_id
            VALUES (?, ?, ?, ?, 1)
        """, symbol, base_asset, quote_asset, is_perp)
        
        symbol_id = cursor.fetchone()[0]
        conn.commit()
        logging.info(f"Created new symbol: {symbol} (ID: {symbol_id})")
        return symbol_id
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ==========
# Blob Storage Helpers (INGESTION)
# ==========
_blob_service = None

def _get_blob_service():
    global _blob_service
    if _blob_service is None:
        _blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    return _blob_service

def _ensure_container():
    try:
        svc = _get_blob_service()
        cc = svc.get_container_client(BLOB_CONTAINER)
        cc.create_container()
    except Exception as e:
        if "ContainerAlreadyExists" not in str(e):
            raise

def utcnow():
    return datetime.now(timezone.utc)

def utc_iso():
    return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def jsonl_path(prefix, **parts):
    p = prefix.rstrip("/")
    for k in ["symbol", "interval", "dt"]:
        if k in parts and parts[k]:
            p += f"/{k}={parts[k]}"
    ts = utcnow().strftime("%Y%m%dT%H%M%SZ")
    return f"{p}/part-{ts}.jsonl.gz"

def upload_jsonl_gz(records, container, blob_path):
    if not records:
        return
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in records:
            gz.write((json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8"))
    buf.seek(0)
    svc = _get_blob_service()
    bc = svc.get_blob_client(container=container, blob=blob_path)
    bc.upload_blob(buf, overwrite=True)
    logging.info("Uploaded %d records -> %s/%s", len(records), container, blob_path)

# ==========
# API Helpers (INGESTION)
# ==========
def get_json(url, params=None, retries=5, timeout=30):
    for i in range(retries):
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (418, 429, 451, 500, 503):
            sleep_s = 2 ** i
            logging.warning("HTTP %s on %s, retrying in %ss...", r.status_code, url, sleep_s)
            time.sleep(sleep_s)
            continue
        r.raise_for_status()
    raise RuntimeError(f"Failed GET {url} after {retries} attempts")

def fetch_binance_klines(symbol, interval, start_ms=None, end_ms=None, limit=1000):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    if end_ms is not None:
        params["endTime"] = end_ms
    return get_json(f"{BINANCE_SPOT}/klines", params=params)

def fetch_binance_funding(symbol, start_ms=None, limit=1000):
    params = {"symbol": symbol, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    return get_json(f"{BINANCE_FAPI}/fundingRate", params=params)

def fetch_fng():
    return get_json(FNG_URL)

def fetch_coingecko_markets(ids):
    return get_json(COINGECKO_MARKETS, params={"vs_currency": "usd", "ids": ",".join(ids)})

# ==========
# Data Mappers (INGESTION)
# ==========
def klines_to_records(symbol, interval, rows):
    ts = utc_iso()
    out = []
    for k in rows:
        out.append({
            "symbol": symbol,
            "interval": interval,
            "openTime": k[0],
            "openPrice": float(k[1]),
            "highPrice": float(k[2]),
            "lowPrice": float(k[3]),
            "closePrice": float(k[4]),
            "baseVolume": float(k[5]),
            "closeTime": k[6],
            "quoteVolume": float(k[7]),
            "tradeCount": int(k[8]),
            "takerBuyBase": float(k[9]),
            "takerBuyQuote": float(k[10]),
            "ingestTs": ts,
            "source": "binance_spot"
        })
    return out

def funding_to_records(symbol, rows):
    ts = utc_iso()
    return [{
        "symbol": symbol,
        "fundingTime": int(r["fundingTime"]),
        "fundingRate": float(r["fundingRate"]),
        "ingestTs": ts,
        "source": "binance_perp"
    } for r in rows]

def fng_to_records(payload):
    ts = utc_iso()
    data = payload.get("data", [])
    return [{
        "time": int(it["timestamp"]),
        "indexValue": int(it["value"]),
        "sentiment": it.get("value_classification"),
        "source": "alternative.me",
        "ingestTs": ts
    } for it in data]

def markets_to_records(rows, asof_iso):
    return [{
        "coinId": r["id"],
        "symbol": r.get("symbol"),
        "name": r.get("name"),
        "price": r.get("current_price"),
        "marketCap": r.get("market_cap"),
        "circulatingSupply": r.get("circulating_supply"),
        "totalVolume": r.get("total_volume"),
        "asof": asof_iso,
        "source": "coingecko"
    } for r in rows]

# ==========
# Data Loaders (LOADING - SQL)
# ==========
def parse_klines_record(data, symbol_id, interval):
    """Parse klines record from JSON to SQL format"""
    try:
        open_time = datetime.fromtimestamp(data['openTime'] / 1000, tz=timezone.utc)
        close_time = datetime.fromtimestamp(data['closeTime'] / 1000, tz=timezone.utc)
        
        return {
            'symbol_id': symbol_id,
            'interval': interval,
            'open_time': open_time,
            'close_time': close_time,
            'open_price': data['openPrice'],
            'high_price': data['highPrice'],
            'low_price': data['lowPrice'],
            'close_price': data['closePrice'],
            'base_volume': data['baseVolume'],
            'quote_volume': data.get('quoteVolume'),
            'trade_count': data.get('tradeCount'),
            'taker_buy_base_vol': data.get('takerBuyBase'),
            'taker_buy_quote_vol': data.get('takerBuyQuote')
        }
    except KeyError as e:
        logging.warning(f"Missing key in klines data: {e}")
        return None

def parse_funding_record(data, symbol_id):
    try:
        funding_time = datetime.fromtimestamp(data['fundingTime'] / 1000, tz=timezone.utc)
        return {
            'symbol_id': symbol_id,
            'funding_time': funding_time,
            'funding_rate': data['fundingRate']
        }
    except KeyError as e:
        logging.warning(f"Missing key in funding data: {e}")
        return None

def parse_fng_record(data):
    try:
        fng_time = datetime.fromtimestamp(data['time'], tz=timezone.utc)
        return {
            'fng_time': fng_time,
            'index_value': data['indexValue'],
            'sentiment_label': data.get('sentiment')
        }
    except KeyError as e:
        logging.warning(f"Missing key in FNG data: {e}")
        return None

def parse_coingecko_record(data):
    try:
        asof_time = datetime.fromisoformat(data['asof'].replace('Z', '+00:00'))
        return {
            'coin_id': data['coinId'],
            'asof_time': asof_time,
            'coin_symbol': data.get('symbol'),
            'coin_name': data.get('name'),
            'market_price': data.get('price'),
            'market_cap': data.get('marketCap'),
            'circulating_supply': data.get('circulatingSupply'),
            'total_volume': data.get('totalVolume')
        }
    except KeyError as e:
        logging.warning(f"Missing key in CoinGecko data: {e}")
        return None

def insert_klines_records(records):
    """Insert klines records into SQL with MERGE (upsert)"""
    if not records:
        return
    
    conn = get_sql_connection()
    cursor = conn.cursor()
    
    try:
        merge_sql = """
            MERGE dbo.Kline AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) 
                   AS source (symbol_id, interval, open_time, close_time, open_price, high_price, low_price, 
                            close_price, base_volume, quote_volume, trade_count, taker_buy_base_vol, taker_buy_quote_vol)
            ON target.symbol_id = source.symbol_id 
               AND target.interval = source.interval 
               AND target.open_time = source.open_time
            WHEN MATCHED THEN
                UPDATE SET 
                    close_time = source.close_time,
                    open_price = source.open_price,
                    high_price = source.high_price,
                    low_price = source.low_price,
                    close_price = source.close_price,
                    base_volume = source.base_volume,
                    quote_volume = source.quote_volume,
                    trade_count = source.trade_count,
                    taker_buy_base_vol = source.taker_buy_base_vol,
                    taker_buy_quote_vol = source.taker_buy_quote_vol,
                    load_timestamp = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (symbol_id, interval, open_time, close_time, open_price, high_price, low_price, 
                       close_price, base_volume, quote_volume, trade_count, taker_buy_base_vol, 
                       taker_buy_quote_vol, load_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """
        
        for record in records:
            params = [
                record['symbol_id'], record['interval'], record['open_time'], record['close_time'],
                record['open_price'], record['high_price'], record['low_price'], record['close_price'],
                record['base_volume'], record.get('quote_volume'), record.get('trade_count'),
                record.get('taker_buy_base_vol'), record.get('taker_buy_quote_vol')
            ]
            params.extend(params)  # Duplicate for INSERT part
            
            cursor.execute(merge_sql, params)
        
        conn.commit()
        logging.info(f"Upserted {len(records)} klines records")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting klines records: {e}")
        raise
    finally:
        conn.close()

def insert_funding_records(records):
    if not records:
        return
    
    conn = get_sql_connection()
    cursor = conn.cursor()
    
    try:
        merge_sql = """
            MERGE dbo.FundingRate AS target
            USING (VALUES (?, ?, ?)) 
                   AS source (symbol_id, funding_time, funding_rate)
            ON target.symbol_id = source.symbol_id 
               AND target.funding_time = source.funding_time
            WHEN MATCHED THEN
                UPDATE SET 
                    funding_rate = source.funding_rate,
                    load_timestamp = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (symbol_id, funding_time, funding_rate, load_timestamp)
                VALUES (?, ?, ?, SYSUTCDATETIME());
        """
        
        for record in records:
            params = [record['symbol_id'], record['funding_time'], record['funding_rate']]
            params.extend(params)
            cursor.execute(merge_sql, params)
        
        conn.commit()
        logging.info(f"Upserted {len(records)} funding records")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting funding records: {e}")
        raise
    finally:
        conn.close()

def insert_fng_records(records):
    if not records:
        return
    
    conn = get_sql_connection()
    cursor = conn.cursor()
    
    try:
        merge_sql = """
            MERGE dbo.FearGreed AS target
            USING (VALUES (?, ?, ?)) 
                   AS source (fng_time, index_value, sentiment_label)
            ON target.fng_time = source.fng_time
            WHEN MATCHED THEN
                UPDATE SET 
                    index_value = source.index_value,
                    sentiment_label = source.sentiment_label,
                    load_timestamp = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (fng_time, index_value, sentiment_label, load_timestamp)
                VALUES (?, ?, ?, SYSUTCDATETIME());
        """
        
        for record in records:
            params = [record['fng_time'], record['index_value'], record.get('sentiment_label')]
            params.extend(params)
            cursor.execute(merge_sql, params)
        
        conn.commit()
        logging.info(f"Upserted {len(records)} FNG records")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting FNG records: {e}")
        raise
    finally:
        conn.close()

def insert_coingecko_records(records):
    if not records:
        return
    
    conn = get_sql_connection()
    cursor = conn.cursor()
    
    try:
        merge_sql = """
            MERGE dbo.CoinMarket AS target
            USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) 
                   AS source (coin_id, asof_time, coin_symbol, coin_name, market_price, market_cap, circulating_supply, total_volume)
            ON target.coin_id = source.coin_id 
               AND target.asof_time = source.asof_time
            WHEN MATCHED THEN
                UPDATE SET 
                    coin_symbol = source.coin_symbol,
                    coin_name = source.coin_name,
                    market_price = source.market_price,
                    market_cap = source.market_cap,
                    circulating_supply = source.circulating_supply,
                    total_volume = source.total_volume,
                    load_timestamp = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (coin_id, asof_time, coin_symbol, coin_name, market_price, market_cap, circulating_supply, total_volume, load_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """
        
        for record in records:
            params = [
                record['coin_id'], record['asof_time'], record.get('coin_symbol'), 
                record.get('coin_name'), record.get('market_price'), record.get('market_cap'),
                record.get('circulating_supply'), record.get('total_volume')
            ]
            params.extend(params)
            cursor.execute(merge_sql, params)
        
        conn.commit()
        logging.info(f"Upserted {len(records)} CoinGecko records")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting CoinGecko records: {e}")
        raise
    finally:
        conn.close()

# ==========
# Blob Processing (LOADING)
# ==========
def process_klines_blob(blob_client):
    """Process klines data from blob and load to SQL"""
    logging.info(f"Processing klines blob: {blob_client.blob_name}")
    
    path_parts = blob_client.blob_name.split('/')
    symbol = path_parts[3].split('=')[1]
    interval = path_parts[4].split('=')[1]
    
    symbol_id = get_or_create_symbol_id(symbol, is_perp=False)
    
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    with gzip.GzipFile(fileobj=io.BytesIO(blob_content), mode='rb') as gz_file:
        content = gz_file.read().decode('utf-8')
    
    records = []
    for line in content.split('\n'):
        if line.strip():
            try:
                data = json.loads(line)
                record = parse_klines_record(data, symbol_id, interval)
                if record:
                    records.append(record)
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse JSON line: {e}")
                continue
    
    if records:
        insert_klines_records(records)
        logging.info(f"Successfully loaded {len(records)} klines records for {symbol} {interval}")

def process_funding_blob(blob_client):
    logging.info(f"Processing funding blob: {blob_client.blob_name}")
    
    path_parts = blob_client.blob_name.split('/')
    symbol = path_parts[3].split('=')[1]
    
    symbol_id = get_or_create_symbol_id(symbol, is_perp=True)
    
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    with gzip.GzipFile(fileobj=io.BytesIO(blob_content), mode='rb') as gz_file:
        content = gz_file.read().decode('utf-8')
    
    records = []
    for line in content.split('\n'):
        if line.strip():
            try:
                data = json.loads(line)
                record = parse_funding_record(data, symbol_id)
                if record:
                    records.append(record)
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse JSON line: {e}")
                continue
    
    if records:
        insert_funding_records(records)
        logging.info(f"Successfully loaded {len(records)} funding records for {symbol}")

def process_fng_blob(blob_client):
    logging.info(f"Processing FNG blob: {blob_client.blob_name}")
    
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    with gzip.GzipFile(fileobj=io.BytesIO(blob_content), mode='rb') as gz_file:
        content = gz_file.read().decode('utf-8')
    
    records = []
    for line in content.split('\n'):
        if line.strip():
            try:
                data = json.loads(line)
                record = parse_fng_record(data)
                if record:
                    records.append(record)
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse JSON line: {e}")
                continue
    
    if records:
        insert_fng_records(records)
        logging.info(f"Successfully loaded {len(records)} FNG records")

def process_coingecko_blob(blob_client):
    logging.info(f"Processing CoinGecko blob: {blob_client.blob_name}")
    
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    with gzip.GzipFile(fileobj=io.BytesIO(blob_content), mode='rb') as gz_file:
        content = gz_file.read().decode('utf-8')
    
    records = []
    for line in content.split('\n'):
        if line.strip():
            try:
                data = json.loads(line)
                record = parse_coingecko_record(data)
                if record:
                    records.append(record)
            except json.JSONDecodeError as e:
                logging.warning(f"Failed to parse JSON line: {e}")
                continue
    
    if records:
        insert_coingecko_records(records)
        logging.info(f"Successfully loaded {len(records)} CoinGecko records")

# ==========
# Ingestion Functions (INGESTION)
# ==========
def overlap_for(interval: str) -> timedelta:
    if interval.endswith("m"):
        return timedelta(minutes=int(interval[:-1]) * 3)
    if interval.endswith("h"):
        return timedelta(hours=int(interval[:-1]) * 3)
    if interval.endswith("d"):
        return timedelta(days=int(interval[:-1]) * 3)
    return timedelta(hours=3)

def run_klines_once(symbol: str, interval: str):
    now = utcnow()
    dt = now.strftime("%Y-%m-%d")
    start_ms = to_ms(now - overlap_for(interval))
    rows = fetch_binance_klines(symbol, interval, start_ms=start_ms)
    recs = klines_to_records(symbol, interval, rows)
    blob = jsonl_path("raw/binance/klines", symbol=symbol, interval=interval, dt=dt)
    upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

def run_funding_once(symbol: str = "BTCUSDT"):
    now = utcnow()
    dt = now.strftime("%Y-%m-%d")
    rows = fetch_binance_funding(symbol, start_ms=to_ms(now - timedelta(days=30)))
    recs = funding_to_records(symbol, rows)
    blob = jsonl_path("raw/binance/funding", symbol=symbol, dt=dt)
    upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

def run_fng_once():
    now = utcnow()
    dt = now.strftime("%Y-%m-%d")
    payload = fetch_fng()
    recs = fng_to_records(payload)
    blob = jsonl_path("raw/altme/fear_greed", dt=dt)
    upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

def run_coingecko_once():
    now = utcnow()
    dt = now.strftime("%Y-%m-%d")
    asof_iso = utc_iso()
    rows = fetch_coingecko_markets(COINGECKO_IDS)
    recs = markets_to_records(rows, asof_iso)
    blob = jsonl_path("raw/coingecko/markets", dt=dt)
    upload_jsonl_gz(recs, BLOB_CONTAINER, blob)

# ==========
# Backfill Functions (INGESTION)
# ==========
def backfill_klines(symbol: str, interval: str, start_dt: datetime, end_dt: datetime):
    start_ms = to_ms(start_dt)
    end_ms = to_ms(end_dt)
    logging.info("Backfilling %s %s from %s to %s", symbol, interval, start_dt, end_dt)

    records_by_date = defaultdict(list)
    batch_count = 0
    total_saved = 0
    FLUSH_THRESHOLD = 10000

    def flush_to_storage():
        nonlocal total_saved
        if not records_by_date:
            return
        
        for date_key in sorted(records_by_date.keys()):
            date_recs = records_by_date[date_key]
            ts = utcnow().strftime("%Y%m%dT%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            blob = f"raw/binance/klines/symbol={symbol}/interval={interval}/dt={date_key}/part-{ts}-{unique_id}.jsonl.gz"
            upload_jsonl_gz(date_recs, BLOB_CONTAINER, blob)
            logging.info("Saved %d records for %s", len(date_recs), date_key)
            total_saved += len(date_recs)
        records_by_date.clear()

    while True:
        rows = fetch_binance_klines(symbol, interval, start_ms=start_ms, end_ms=end_ms, limit=1000)
        if not rows:
            break

        batch_count += 1
        recs = klines_to_records(symbol, interval, rows)
        
        for rec in recs:
            open_dt = datetime.fromtimestamp(rec["openTime"] / 1000, tz=timezone.utc)
            date_key = open_dt.strftime("%Y-%m-%d")
            records_by_date[date_key].append(rec)

        total_buffered = sum(len(v) for v in records_by_date.values())
        if total_buffered >= FLUSH_THRESHOLD:
            logging.info("Flushing buffer: %d records", total_buffered)
            flush_to_storage()

        if batch_count % 10 == 0:
            first_dt = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
            logging.info("Progress: batch %d, buffered %d records", batch_count, total_buffered)

        last_open = rows[-1][0]
        next_ms = last_open + 1
        if next_ms >= end_ms:
            break
        start_ms = next_ms
        time.sleep(0.1)

    flush_to_storage()
    logging.info("Backfill complete for %s %s: %d batches, %d total records", 
                 symbol, interval, batch_count, total_saved)

def backfill_funding(symbol: str, start_dt: datetime):
    start_ms = to_ms(start_dt)
    now_ms = to_ms(utcnow())
    logging.info("Backfilling funding rates for %s from %s", symbol, start_dt)

    records_by_date = defaultdict(list)
    total_saved = 0
    FLUSH_THRESHOLD = 5000

    def flush_to_storage():
        nonlocal total_saved
        if not records_by_date:
            return
        
        for date_key in sorted(records_by_date.keys()):
            date_recs = records_by_date[date_key]
            ts = utcnow().strftime("%Y%m%dT%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            blob = f"raw/binance/funding/symbol={symbol}/dt={date_key}/part-{ts}-{unique_id}.jsonl.gz"
            upload_jsonl_gz(date_recs, BLOB_CONTAINER, blob)
            logging.info("Saved %d funding records for %s", len(date_recs), date_key)
            total_saved += len(date_recs)
        records_by_date.clear()

    while True:
        rows = fetch_binance_funding(symbol, start_ms=start_ms, limit=1000)
        if not rows:
            break

        recs = funding_to_records(symbol, rows)
        
        for rec in recs:
            funding_dt = datetime.fromtimestamp(rec["fundingTime"] / 1000, tz=timezone.utc)
            date_key = funding_dt.strftime("%Y-%m-%d")
            records_by_date[date_key].append(rec)

        total_buffered = sum(len(v) for v in records_by_date.values())
        if total_buffered >= FLUSH_THRESHOLD:
            logging.info("Flushing funding buffer: %d records", total_buffered)
            flush_to_storage()

        last_time = int(rows[-1]["fundingTime"]) + 1
        if last_time > now_ms:
            break
        start_ms = last_time
        time.sleep(0.2)

    flush_to_storage()
    logging.info("Funding backfill complete for %s: %d total records", symbol, total_saved)

# ==========
# Function App Triggers
# ==========
app = func.FunctionApp()

# ---- INGESTION: Regular Data Collection ----
@app.timer_trigger(
    schedule="0 0 */6 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def IngestCrypto(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("Timer is past due.")

    logging.info("Starting incremental ingestion...")
    _ensure_container()

    try:
        # Binance klines
        for sym in SYMBOLS:
            for itv in INTERVALS:
                try:
                    run_klines_once(sym, itv)
                except Exception as e:
                    logging.exception("Klines failed for %s %s: %s", sym, itv, e)

        # Funding, FNG, CoinGecko
        try:
            run_funding_once("BTCUSDT")
        except Exception as e:
            logging.exception("Funding fetch failed: %s", e)

        try:
            run_fng_once()
        except Exception as e:
            logging.exception("Fear & Greed fetch failed: %s", e)

        try:
            run_coingecko_once()
        except Exception as e:
            logging.exception("CoinGecko fetch failed: %s", e)

        logging.info("Ingestion cycle completed.")
    except Exception as e:
        logging.exception("Fatal error during ingestion: %s", e)

# ---- INGESTION: Backfill ----
@app.timer_trigger(
    schedule="0 10 3 * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def BackfillCrypto(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("Backfill timer is past due.")

    logging.info("Starting historical BACKFILL...")
    _ensure_container()

    try:
        start_dt = utcnow() - timedelta(days=BACKFILL_DAYS)
        end_dt = utcnow()
        
        for sym in SYMBOLS:
            for itv in BACKFILL_INTERVALS:
                try:
                    logging.info("Starting backfill: %s %s", sym, itv)
                    backfill_klines(sym, itv, start_dt, end_dt)
                except Exception as e:
                    logging.exception("Backfill klines failed for %s %s: %s", sym, itv, e)

        try:
            logging.info("Starting funding backfill for BTCUSDT")
            backfill_funding("BTCUSDT", start_dt)
        except Exception as e:
            logging.exception("Backfill funding failed: %s", e)

        logging.info("BACKFILL completed successfully.")
    except Exception as e:
        logging.exception("Fatal error during BACKFILL: %s", e)

# ---- LOADING: Blob Triggers to SQL ----
@app.blob_trigger(
    arg_name="myblob",
    path="bitcoin-data/raw/binance/klines/symbol={symbol}/interval={interval}/dt={date}/part-{timestamp}.jsonl.gz",
    connection="AzureWebJobsStorage"
)
def ProcessKlinesBlob(myblob: func.InputStream):
    """Trigger function for klines blobs - LOADS TO SQL"""
    logging.info(f"Klines blob trigger processed: {myblob.name}")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=myblob.name)
    
    process_klines_blob(blob_client)

@app.blob_trigger(
    arg_name="myblob",
    path="bitcoin-data/raw/binance/funding/symbol={symbol}/dt={date}/part-{timestamp}.jsonl.gz",
    connection="AzureWebJobsStorage"
)
def ProcessFundingBlob(myblob: func.InputStream):
    """Trigger function for funding rate blobs - LOADS TO SQL"""
    logging.info(f"Funding blob trigger processed: {myblob.name}")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=myblob.name)
    
    process_funding_blob(blob_client)

@app.blob_trigger(
    arg_name="myblob",
    path="bitcoin-data/raw/altme/fear_greed/dt={date}/part-{timestamp}.jsonl.gz",
    connection="AzureWebJobsStorage"
)
def ProcessFNGBlob(myblob: func.InputStream):
    """Trigger function for Fear & Greed blobs - LOADS TO SQL"""
    logging.info(f"FNG blob trigger processed: {myblob.name}")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=myblob.name)
    
    process_fng_blob(blob_client)

@app.blob_trigger(
    arg_name="myblob",
    path="bitcoin-data/raw/coingecko/markets/dt={date}/part-{timestamp}.jsonl.gz",
    connection="AzureWebJobsStorage"
)
def ProcessCoinGeckoBlob(myblob: func.InputStream):
    """Trigger function for CoinGecko blobs - LOADS TO SQL"""
    logging.info(f"CoinGecko blob trigger processed: {myblob.name}")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=myblob.name)
    
    process_coingecko_blob(blob_client)

# ---- LOADING: Backfill Processing for existing blobs ----
@app.timer_trigger(
    schedule="0 0 */4 * * *",
    arg_name="myTimer",
    run_on_startup=True  # Process existing blobs on startup
)
def ProcessExistingBlobs(myTimer: func.TimerRequest):
    """Process existing blobs that might have been missed by triggers"""
    logging.info("Processing existing blobs for backfill...")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service.get_container_client(BLOB_CONTAINER)
    
    # Process all existing blobs
    for blob_prefix in ["raw/binance/klines/", "raw/binance/funding/", "raw/altme/fear_greed/", "raw/coingecko/markets/"]:
        blobs = container_client.list_blobs(name_starts_with=blob_prefix)
        for blob in blobs:
            try:
                blob_client = container_client.get_blob_client(blob.name)
                
                if "klines" in blob_prefix:
                    process_klines_blob(blob_client)
                elif "funding" in blob_prefix:
                    process_funding_blob(blob_client)
                elif "fear_greed" in blob_prefix:
                    process_fng_blob(blob_client)
                elif "coingecko" in blob_prefix:
                    process_coingecko_blob(blob_client)
                    
            except Exception as e:
                logging.error(f"Failed to process blob {blob.name}: {e}")
    
    logging.info("Existing blobs processing completed")