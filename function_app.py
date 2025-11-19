import logging
import os
import io
import gzip
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests
import azure.functions as func

import pyodbc
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient

# ==========
# Configuration
# ==========
BLOB_CONN_STR = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "bitcoin-data")

# Symbols and intervals for regular ingestion
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(",") if s.strip()]
INTERVALS = [i.strip() for i in os.getenv("INTERVALS", "1m,1h,1d").split(",") if i.strip()]

# Backfill configuration (default = 2 years, only 1h and 1d intervals for safety)
BACKFILL_INTERVALS = [i.strip() for i in os.getenv("BACKFILL_INTERVALS", "1h,1d").split(",") if i.strip()]
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "730"))  # 2 years = 730 days

# CoinGecko coin IDs
COINGECKO_IDS = [c.strip() for c in os.getenv("COINGECKO_IDS", "bitcoin,ethereum,binancecoin").split(",") if c.strip()]

# API endpoints
BINANCE_SPOT = "https://api.binance.com/api/v3"
BINANCE_FAPI = "https://fapi.binance.com/fapi/v1"
FNG_URL = "https://api.alternative.me/fng/?limit=10"
COINGECKO_MARKETS = "https://api.coingecko.com/api/v3/coins/markets"

# ==========
# Blob helpers
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
        cc.create_container()  # no-op if exists
    except Exception as e:
        # if container already exists, ignore conflict
        if "ContainerAlreadyExists" not in str(e):
            raise

def utcnow():
    return datetime.now(timezone.utc)

def utc_iso():
    return utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def jsonl_path(prefix, **parts):
    """
    Build a partitioned blob path like:
    raw/binance/klines/symbol=BTCUSDT/interval=1h/dt=2025-10-26/part-20251026T083000Z.jsonl.gz
    """
    p = prefix.rstrip("/")
    # stable partition order
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
# HTTP helpers
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

# ==========
# API callers
# ==========
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
# Record mappers
# ==========
def klines_to_records(symbol, interval, rows):
    ts = utc_iso()
    out = []
    for k in rows:
        # Binance kline array indices:
        # 0 openTime, 1 open, 2 high, 3 low, 4 close, 5 volume,
        # 6 closeTime, 7 quoteAssetVolume, 8 numberOfTrades,
        # 9 takerBuyBaseVol, 10 takerBuyQuoteVol, 11 ignore
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
# Ingestion (incremental)
# ==========
def overlap_for(interval: str) -> timedelta:
    """Fetch extra overlap (3 candles) to prevent gaps."""
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
# Function App
# ==========
app = func.FunctionApp()

# ---- Regular Ingestion (every 6h) ----
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
        # 1) Binance klines for all symbols/intervals
        for sym in SYMBOLS:
            for itv in INTERVALS:
                try:
                    run_klines_once(sym, itv)
                except Exception as e:
                    logging.exception("Klines failed for %s %s: %s", sym, itv, e)

        # 2) Funding (BTC perp)
        try:
            run_funding_once("BTCUSDT")
        except Exception as e:
            logging.exception("Funding fetch failed: %s", e)

        # 3) Fear & Greed
        try:
            run_fng_once()
        except Exception as e:
            logging.exception("Fear & Greed fetch failed: %s", e)

        # 4) CoinGecko markets
        try:
            run_coingecko_once()
        except Exception as e:
            logging.exception("CoinGecko fetch failed: %s", e)

        logging.info("Ingestion cycle completed.")
    except Exception as e:
        logging.exception("Fatal error during ingestion: %s", e)

# ==========
# BACKFILL
# ==========
def backfill_klines(symbol: str, interval: str, start_dt: datetime, end_dt: datetime):
    """
    Fetch Binance historical klines and group by date before saving.
    This prevents overwriting files and ensures all data is preserved.
    """
    start_ms = to_ms(start_dt)
    end_ms = to_ms(end_dt)
    logging.info("Backfilling %s %s from %s to %s", symbol, interval, start_dt, end_dt)

    # Memory-efficient: flush every 10k records or 7 days span
    records_by_date = defaultdict(list)
    batch_count = 0
    total_saved = 0
    FLUSH_THRESHOLD = 10000
    FLUSH_DAYS = 7

    def flush_to_storage():
        nonlocal total_saved
        if not records_by_date:
            return
        
        for date_key in sorted(records_by_date.keys()):
            date_recs = records_by_date[date_key]
            
            # Generate unique filename to prevent overwrites
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
        
        # Group records by their actual date (not just first record)
        for rec in recs:
            open_dt = datetime.fromtimestamp(rec["openTime"] / 1000, tz=timezone.utc)
            date_key = open_dt.strftime("%Y-%m-%d")
            records_by_date[date_key].append(rec)

        # Check if we should flush to free memory
        total_buffered = sum(len(v) for v in records_by_date.values())
        date_span = len(records_by_date)
        
        if total_buffered >= FLUSH_THRESHOLD or date_span >= FLUSH_DAYS:
            logging.info("Flushing buffer: %d records across %d dates", total_buffered, date_span)
            flush_to_storage()

        # Progress logging
        if batch_count % 10 == 0:
            first_dt = datetime.fromtimestamp(rows[0][0] / 1000, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(rows[-1][0] / 1000, tz=timezone.utc)
            logging.info("Progress: batch %d, buffered %d records, current range: %s to %s", 
                        batch_count, total_buffered, first_dt, last_dt)

        # Move to next batch
        last_open = rows[-1][0]
        next_ms = last_open + 1
        if next_ms >= end_ms:
            break
        start_ms = next_ms
        time.sleep(0.1)  # Rate limiting

    # Final flush
    flush_to_storage()
    
    logging.info("Backfill complete for %s %s: %d batches, %d total records saved", 
                 symbol, interval, batch_count, total_saved)

def backfill_funding(symbol: str, start_dt: datetime):
    """
    Fetch Binance perpetual funding historical data with proper date grouping.
    """
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
        
        # Group by actual funding date
        for rec in recs:
            funding_dt = datetime.fromtimestamp(rec["fundingTime"] / 1000, tz=timezone.utc)
            date_key = funding_dt.strftime("%Y-%m-%d")
            records_by_date[date_key].append(rec)

        # Flush if buffer is large
        total_buffered = sum(len(v) for v in records_by_date.values())
        if total_buffered >= FLUSH_THRESHOLD:
            logging.info("Flushing funding buffer: %d records", total_buffered)
            flush_to_storage()

        last_time = int(rows[-1]["fundingTime"]) + 1
        if last_time > now_ms:
            break
        start_ms = last_time
        time.sleep(0.2)

    # Final flush
    flush_to_storage()
    logging.info("Funding backfill complete for %s: %d total records", symbol, total_saved)

# ---- Timer-triggered Backfill (run once then disable) ----
@app.timer_trigger(
    schedule="0 10 3 * * *",  # 03:10 UTC daily
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
        
        logging.info("Backfill period: %s to %s (%d days)", start_dt, end_dt, BACKFILL_DAYS)
        logging.info("Symbols: %s", SYMBOLS)
        logging.info("Intervals: %s", BACKFILL_INTERVALS)

        # Backfill klines for each symbol/interval
        for sym in SYMBOLS:
            for itv in BACKFILL_INTERVALS:
                try:
                    logging.info("Starting backfill: %s %s", sym, itv)
                    backfill_klines(sym, itv, start_dt, end_dt)
                except Exception as e:
                    logging.exception("Backfill klines failed for %s %s: %s", sym, itv, e)

        # Backfill funding rates
        try:
            logging.info("Starting funding backfill for BTCUSDT")
            backfill_funding("BTCUSDT", start_dt)
        except Exception as e:
            logging.exception("Backfill funding failed: %s", e)

        logging.info("BACKFILL completed successfully.")
    except Exception as e:
        logging.exception("Fatal error during BACKFILL: %s", e)

# ============================
# Timer-triggered SQL Loader with checkpoint
# ============================

SQL_CONN = os.getenv("SQL_CONN_STR")
CHECKPOINT_BLOB = os.getenv("SQL_LOADER_CHECKPOINT", "checkpoints/sql_loader_state.json")

# ---------------- Blob Helpers ----------------
def _container_client():
    return ContainerClient.from_connection_string(BLOB_CONN_STR, container_name=BLOB_CONTAINER)

def _blob_client(name: str):
    return BlobClient.from_connection_string(BLOB_CONN_STR, container_name=BLOB_CONTAINER, blob_name=name)

def _read_jsonl_gz(blob_name: str):
    cli = _container_client()
    data = cli.download_blob(blob_name).readall()
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
        for line in gz:
            if line.strip():
                yield json.loads(line)

def _parse_partitions(blob_name: str) -> dict:
    # expects .../symbol=BTCUSDT/interval=1h/dt=YYYY-MM-DD/part-....
    parts = {}
    for seg in blob_name.split("/"):
        if "=" in seg:
            k, v = seg.split("=", 1)
            parts[k] = v
    return parts

# ---------------- Checkpoint Helpers ----------------
def _load_state() -> dict:
    try:
        bc = _blob_client(CHECKPOINT_BLOB)
        data = bc.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return {}

def _save_state(state: dict):
    bc = _blob_client(CHECKPOINT_BLOB)
    data = json.dumps(state, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    bc.upload_blob(data, overwrite=True, content_type="application/json")

# ---------------- SQL Connection ----------------
def _sql_conn():
    if not SQL_CONN:
        raise RuntimeError("Missing SQL_CONN_STR app setting.")
    return pyodbc.connect(SQL_CONN, autocommit=False)

# ---------------- SQL Loaders ----------------
_symbol_cache = {}

def _ensure_symbol_id(cur, symbol: str, is_perp: int = 0) -> int:
    key = (symbol, is_perp)
    if key in _symbol_cache:
        return _symbol_cache[key]
    cur.execute("SELECT symbol_id FROM dbo.Symbol WHERE symbol=? AND is_perp=?", (symbol, is_perp))
    row = cur.fetchone()
    if not row:
        base, quote = symbol[:-4], symbol[-4:]
        cur.execute("INSERT INTO dbo.Symbol(symbol, base_asset, quote_asset, is_perp) VALUES (?,?,?,?)",
                    (symbol, base, quote, is_perp))
        cur.execute("SELECT SCOPE_IDENTITY()")
        row = cur.fetchone()
    _symbol_cache[key] = int(row[0])
    return _symbol_cache[key]


def _merge_klines(cur, rows):
    cur.execute("""
        IF OBJECT_ID('tempdb..#KlineStage') IS NOT NULL DROP TABLE #KlineStage;
        CREATE TABLE #KlineStage(
            symbol_id INT NOT NULL,
            interval NVARCHAR(10) NOT NULL,
            open_time DATETIME2 NOT NULL,
            close_time DATETIME2 NOT NULL,
            open_price DECIMAL(38,12) NOT NULL,
            high_price DECIMAL(38,12) NOT NULL,
            low_price  DECIMAL(38,12) NOT NULL,
            close_price DECIMAL(38,12) NOT NULL,
            base_volume DECIMAL(38,12) NOT NULL,
            quote_volume DECIMAL(38,12) NULL,
            trade_count BIGINT NULL,
            taker_buy_base_vol DECIMAL(38,12) NULL,
            taker_buy_quote_vol DECIMAL(38,12) NULL
        );
    """)
    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO #KlineStage(symbol_id,interval,open_time,close_time,open_price,high_price,low_price,close_price,
                                base_volume,quote_volume,trade_count,taker_buy_base_vol,taker_buy_quote_vol)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    cur.execute("""
        MERGE dbo.Kline WITH (HOLDLOCK) AS T
        USING #KlineStage AS S
          ON  T.symbol_id = S.symbol_id
          AND T.interval  = S.interval
          AND T.open_time = S.open_time
        WHEN MATCHED THEN UPDATE SET
            T.close_time = S.close_time,
            T.open_price = S.open_price,
            T.high_price = S.high_price,
            T.low_price  = S.low_price,
            T.close_price = S.close_price,
            T.base_volume = S.base_volume,
            T.quote_volume = S.quote_volume,
            T.trade_count = S.trade_count,
            T.taker_buy_base_vol = S.taker_buy_base_vol,
            T.taker_buy_quote_vol = S.taker_buy_quote_vol,
            T.load_timestamp = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT(symbol_id,interval,open_time,close_time,open_price,high_price,low_price,close_price,
                   base_volume,quote_volume,trade_count,taker_buy_base_vol,taker_buy_quote_vol,load_timestamp)
            VALUES(S.symbol_id,S.interval,S.open_time,S.close_time,S.open_price,S.high_price,S.low_price,S.close_price,
                   S.base_volume,S.quote_volume,S.trade_count,S.taker_buy_base_vol,S.taker_buy_quote_vol,SYSUTCDATETIME());
    """)

def _merge_funding(cur, rows):
    cur.execute("""
        IF OBJECT_ID('tempdb..#FundingStage') IS NOT NULL DROP TABLE #FundingStage;
        CREATE TABLE #FundingStage(
            symbol_id INT NOT NULL,
            funding_time DATETIME2 NOT NULL,
            funding_rate DECIMAL(18,10) NOT NULL
        );
    """)
    cur.fast_executemany = True
    cur.executemany("INSERT INTO #FundingStage(symbol_id,funding_time,funding_rate) VALUES (?,?,?)", rows)
    cur.execute("""
        MERGE dbo.FundingRate WITH (HOLDLOCK) AS T
        USING #FundingStage AS S
          ON  T.symbol_id = S.symbol_id AND T.funding_time = S.funding_time
        WHEN MATCHED THEN UPDATE SET
            T.funding_rate = S.funding_rate,
            T.load_timestamp = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT(symbol_id,funding_time,funding_rate,load_timestamp)
            VALUES(S.symbol_id,S.funding_time,S.funding_rate,SYSUTCDATETIME());
    """)

def _load_coingecko_blob(cur, blob_name: str):
    rows = []
    for rec in _read_jsonl_gz(blob_name):
        rows.append((
            rec.get("coinId"),
            rec.get("symbol"),
            rec.get("name"),
            rec.get("price"),
            rec.get("marketCap"),
            rec.get("circulatingSupply"),
            rec.get("totalVolume"),
            rec.get("asof"),
            rec.get("source", "coingecko")
        ))
    if not rows:
        return 0
    cur.execute("""
        IF OBJECT_ID('tempdb..#CoinMarketStage') IS NOT NULL DROP TABLE #CoinMarketStage;
        CREATE TABLE #CoinMarketStage(
            coin_id NVARCHAR(50),
            symbol NVARCHAR(50),
            name NVARCHAR(100),
            price DECIMAL(38,12),
            market_cap BIGINT,
            circulating_supply DECIMAL(38,12),
            total_volume BIGINT,
            asof DATETIME2,
            source NVARCHAR(50)
        );
    """)
    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO #CoinMarketStage
        (coin_id,symbol,name,price,market_cap,circulating_supply,total_volume,asof,source)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, rows)
    cur.execute("""
        MERGE dbo.CoinMarket WITH (HOLDLOCK) AS T
        USING #CoinMarketStage AS S
          ON  T.coin_id = S.coin_id AND T.asof = S.asof
        WHEN MATCHED THEN UPDATE SET
            T.price = S.price,
            T.market_cap = S.market_cap,
            T.circulating_supply = S.circulating_supply,
            T.total_volume = S.total_volume,
            T.load_timestamp = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (coin_id,symbol,name,price,market_cap,circulating_supply,total_volume,asof,source,load_timestamp)
            VALUES (S.coin_id,S.symbol,S.name,S.price,S.market_cap,S.circulating_supply,S.total_volume,S.asof,S.source,SYSUTCDATETIME());
    """)
    return len(rows)

def _load_fng_blob(cur, blob_name: str):
    rows = []
    for rec in _read_jsonl_gz(blob_name):
        dt = datetime.fromtimestamp(int(rec["time"]), tz=timezone.utc).replace(tzinfo=None)
        rows.append((dt, rec.get("indexValue"), rec.get("sentiment"), rec.get("source", "altme")))
    if not rows:
        return 0
    cur.execute("""
        IF OBJECT_ID('tempdb..#FearGreedStage') IS NOT NULL DROP TABLE #FearGreedStage;
        CREATE TABLE #FearGreedStage(
            timestamp DATETIME2,
            index_value INT,
            sentiment NVARCHAR(50),
            source NVARCHAR(50)
        );
    """)
    cur.fast_executemany = True
    cur.executemany("INSERT INTO #FearGreedStage(timestamp,index_value,sentiment,source) VALUES (?,?,?,?)", rows)
    cur.execute("""
        MERGE dbo.FearGreed WITH (HOLDLOCK) AS T
        USING #FearGreedStage AS S
          ON  T.timestamp = S.timestamp
        WHEN MATCHED THEN UPDATE SET
            T.index_value = S.index_value,
            T.sentiment = S.sentiment,
            T.load_timestamp = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (timestamp,index_value,sentiment,source,load_timestamp)
            VALUES (S.timestamp,S.index_value,S.sentiment,S.source,SYSUTCDATETIME());
    """)
    return len(rows)

def _load_klines_blob(cur, blob_name: str, partitions: dict):
    """Load klines data with correct field mapping"""
    rows = []
    for rec in _read_jsonl_gz(blob_name):
        # Use partitions first, fallback to record values
        symbol = partitions.get("symbol", rec.get("symbol"))
        interval = partitions.get("interval", rec.get("interval"))
        
        # Get symbol_id
        symbol_id = _ensure_symbol_id(cur, symbol, 0)
        
        # Convert timestamps to datetime
        open_time = datetime.fromtimestamp(rec["openTime"] / 1000, tz=timezone.utc).replace(tzinfo=None)
        close_time = datetime.fromtimestamp(rec["closeTime"] / 1000, tz=timezone.utc).replace(tzinfo=None)
        
        rows.append((
            symbol_id,
            interval,
            open_time,
            close_time,
            rec["openPrice"],
            rec["highPrice"],
            rec["lowPrice"],
            rec["closePrice"],
            rec["baseVolume"],
            rec.get("quoteVolume"),
            rec.get("tradeCount"),
            rec.get("takerBuyBase"),
            rec.get("takerBuyQuote")
        ))
    
    if rows:
        _merge_klines(cur, rows)
    return len(rows)

def _load_funding_blob(cur, blob_name: str, partitions: dict):
    """Load funding data with correct field mapping"""
    rows = []
    for rec in _read_jsonl_gz(blob_name):
        # Use partitions first, fallback to record values
        symbol = partitions.get("symbol", rec.get("symbol"))
        
        # Get symbol_id (perp = 1)
        symbol_id = _ensure_symbol_id(cur, symbol, 1)
        
        # Convert timestamp to datetime
        funding_time = datetime.fromtimestamp(rec["fundingTime"] / 1000, tz=timezone.utc).replace(tzinfo=None)
        
        rows.append((
            symbol_id,
            funding_time,
            rec["fundingRate"]
        ))
    
    if rows:
        _merge_funding(cur, rows)
    return len(rows)

@app.timer_trigger(
    schedule="0 0 1 * * *",  # 01:00 UTC daily
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False
)
def LoadSqlTimer(mytimer: func.TimerRequest) -> None:
    logging.info("Starting SQL loader function...")
    
    if not SQL_CONN:
        logging.error("SQL_CONN_STR not configured, skipping SQL load")
        return
    
    state = _load_state()
    last_processed = state.get("last_processed", "")

    container = _container_client()
    
    # Get all blobs that are newer than last checkpoint
    all_blobs = [b for b in container.list_blobs() if b.name > last_processed]
    
    # Filter only data blobs (not checkpoints)
    data_blobs = [b.name for b in all_blobs if not b.name.startswith("checkpoints/")]
    data_blobs = sorted(data_blobs)

    if not data_blobs:
        logging.info("No new blobs to process.")
        return

    logging.info(f"Found {len(data_blobs)} new blobs to process")

    conn = _sql_conn()
    cursor = conn.cursor()
    
    processed_count = 0
    error_count = 0

    try:
        for blob_name in data_blobs:
            try:
                logging.info(f"Processing blob: {blob_name}")
                partitions = _parse_partitions(blob_name)

                # Match the actual blob path structure: raw/binance/klines/...
                if "raw/binance/klines" in blob_name:
                    count = _load_klines_blob(cursor, blob_name, partitions)
                    logging.info(f"Loaded {count} kline records from {blob_name}")
                
                elif "raw/binance/funding" in blob_name:
                    count = _load_funding_blob(cursor, blob_name, partitions)
                    logging.info(f"Loaded {count} funding records from {blob_name}")
                
                elif "raw/coingecko/markets" in blob_name:
                    count = _load_coingecko_blob(cursor, blob_name)
                    logging.info(f"Loaded {count} coingecko records from {blob_name}")
                
                elif "raw/altme/fear_greed" in blob_name:
                    count = _load_fng_blob(cursor, blob_name)
                    logging.info(f"Loaded {count} fear & greed records from {blob_name}")
                
                else:
                    logging.warning(f"Unknown blob type: {blob_name}")
                    continue

                # Commit after each blob
                conn.commit()
                processed_count += 1
                last_processed = blob_name
                
                # Save checkpoint every 10 blobs
                if processed_count % 10 == 0:
                    _save_state({"last_processed": last_processed})
                    logging.info(f"Checkpoint saved: {processed_count} blobs processed")
                
            except Exception as e:
                logging.exception(f"Error processing blob {blob_name}: {e}")
                error_count += 1
                conn.rollback()
                # Continue processing other blobs
                continue

        # Final checkpoint save
        if processed_count > 0:
            _save_state({"last_processed": last_processed})
            logging.info(f"Final checkpoint: {last_processed}")

        logging.info(f"SQL loader completed: {processed_count} blobs processed, {error_count} errors")

    except Exception as e:
        logging.exception(f"Fatal error during SQL loading: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        logging.info("SQL loader function completed.")