import logging
import os
import io
import gzip
import json
import pyodbc
import azure.functions as func
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient

# ==========
# Configuration
# ==========
BLOB_CONN_STR = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "bitcoin-data")
SQL_CONN_STR = os.getenv("SQL_CONNECTION_STRING")

# ==========
# SQL Helpers
# ==========
def get_sql_connection():
    """Get SQL database connection"""
    return pyodbc.connect(SQL_CONN_STR)

def get_or_create_symbol_id(symbol, is_perp=False, base_asset=None, quote_asset=None):
    """Get existing symbol_id or create new one"""
    conn = get_sql_connection()
    cursor = conn.cursor()
    
    try:
        # Try to get existing symbol
        cursor.execute("""
            SELECT symbol_id FROM dbo.Symbol 
            WHERE symbol = ? AND is_perp = ?
        """, symbol, is_perp)
        
        result = cursor.fetchone()
        if result:
            return result[0]
        
        # If not exists, extract base/quote assets if not provided
        if not base_asset or not quote_asset:
            base_asset, quote_asset = extract_assets(symbol)
        
        # Insert new symbol
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

def extract_assets(symbol):
    """Extract base and quote assets from symbol (simple logic)"""
    # Common quote assets
    quote_assets = ['USDT', 'BUSD', 'USDC', 'BTC', 'ETH']
    
    for quote in quote_assets:
        if symbol.endswith(quote):
            base_asset = symbol[:-len(quote)]
            return base_asset, quote
    
    # Fallback: simple split for unknown patterns
    if len(symbol) >= 6:
        return symbol[:3], symbol[3:]
    return symbol, 'UNKNOWN'

# ==========
# Blob Processing
# ==========
def process_klines_blob(blob_client):
    """Process klines data from blob and load to SQL"""
    logging.info(f"Processing klines blob: {blob_client.blob_name}")
    
    # Extract parameters from blob path
    path_parts = blob_client.blob_name.split('/')
    symbol = path_parts[3].split('=')[1]  # symbol=BTCUSDT
    interval = path_parts[4].split('=')[1]  # interval=1h
    
    # Get symbol_id (assuming spot data for klines)
    symbol_id = get_or_create_symbol_id(symbol, is_perp=False)
    
    # Download and process blob
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    # Decompress and parse
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
    
    # Insert to SQL
    if records:
        insert_klines_records(records)
        logging.info(f"Successfully loaded {len(records)} klines records for {symbol} {interval}")
    else:
        logging.warning(f"No valid records found in blob: {blob_client.blob_name}")

def process_funding_blob(blob_client):
    """Process funding rate data from blob and load to SQL"""
    logging.info(f"Processing funding blob: {blob_client.blob_name}")
    
    # Extract parameters from blob path
    path_parts = blob_client.blob_name.split('/')
    symbol = path_parts[3].split('=')[1]  # symbol=BTCUSDT
    
    # Get symbol_id (perpetual for funding rates)
    symbol_id = get_or_create_symbol_id(symbol, is_perp=True)
    
    # Download and process blob
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    # Decompress and parse
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
    
    # Insert to SQL
    if records:
        insert_funding_records(records)
        logging.info(f"Successfully loaded {len(records)} funding records for {symbol}")
    else:
        logging.warning(f"No valid records found in blob: {blob_client.blob_name}")

def process_fng_blob(blob_client):
    """Process Fear & Greed data from blob and load to SQL"""
    logging.info(f"Processing FNG blob: {blob_client.blob_name}")
    
    # Download and process blob
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    # Decompress and parse
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
    
    # Insert to SQL
    if records:
        insert_fng_records(records)
        logging.info(f"Successfully loaded {len(records)} FNG records")
    else:
        logging.warning(f"No valid records found in blob: {blob_client.blob_name}")

def process_coingecko_blob(blob_client):
    """Process CoinGecko data from blob and load to SQL"""
    logging.info(f"Processing CoinGecko blob: {blob_client.blob_name}")
    
    # Download and process blob
    blob_data = blob_client.download_blob()
    blob_content = blob_data.readall()
    
    # Decompress and parse
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
    
    # Insert to SQL
    if records:
        insert_coingecko_records(records)
        logging.info(f"Successfully loaded {len(records)} CoinGecko records")
    else:
        logging.warning(f"No valid records found in blob: {blob_client.blob_name}")

# ==========
# Data Parsers
# ==========
def parse_klines_record(data, symbol_id, interval):
    """Parse klines record from JSON to SQL format"""
    try:
        # Convert timestamps from milliseconds to datetime
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
    """Parse funding rate record from JSON to SQL format"""
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
    """Parse Fear & Greed record from JSON to SQL format"""
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
    """Parse CoinGecko record from JSON to SQL format"""
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

# ==========
# SQL Inserters
# ==========
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
            # Duplicate parameters for INSERT part of MERGE
            params.extend(params)
            
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
    """Insert funding rate records into SQL with MERGE (upsert)"""
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
            params = [
                record['symbol_id'], record['funding_time'], record['funding_rate']
            ]
            # Duplicate parameters for INSERT part of MERGE
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
    """Insert Fear & Greed records into SQL with MERGE (upsert)"""
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
            params = [
                record['fng_time'], record['index_value'], record.get('sentiment_label')
            ]
            # Duplicate parameters for INSERT part of MERGE
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
    """Insert CoinGecko records into SQL with MERGE (upsert)"""
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
            # Duplicate parameters for INSERT part of MERGE
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
# Blob Trigger Function
# ==========
app = func.FunctionApp()

@app.blob_trigger(
    arg_name="myblob",
    path="bitcoin-data/raw/binance/klines/symbol={symbol}/interval={interval}/dt={date}/part-{timestamp}.jsonl.gz",
    connection="AzureWebJobsStorage"
)
def ProcessKlinesBlob(myblob: func.InputStream):
    """Trigger function for klines blobs"""
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
    """Trigger function for funding rate blobs"""
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
    """Trigger function for Fear & Greed blobs"""
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
    """Trigger function for CoinGecko blobs"""
    logging.info(f"CoinGecko blob trigger processed: {myblob.name}")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=myblob.name)
    
    process_coingecko_blob(blob_client)

# ==========
# Timer Trigger for Backfill Processing
# ==========
@app.timer_trigger(
    schedule="0 0 */4 * * *",  # Every 4 hours
    arg_name="myTimer",
    run_on_startup=False
)
def ProcessBackfillBlobs(myTimer: func.TimerRequest):
    """Process existing blobs that might have been missed by triggers"""
    logging.info("Processing existing blobs for backfill...")
    
    blob_service = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service.get_container_client(BLOB_CONTAINER)
    
    # Process klines blobs
    klines_blobs = container_client.list_blobs(name_starts_with="raw/binance/klines/")
    for blob in klines_blobs:
        try:
            blob_client = container_client.get_blob_client(blob.name)
            process_klines_blob(blob_client)
        except Exception as e:
            logging.error(f"Failed to process blob {blob.name}: {e}")
    
    # Process funding blobs
    funding_blobs = container_client.list_blobs(name_starts_with="raw/binance/funding/")
    for blob in funding_blobs:
        try:
            blob_client = container_client.get_blob_client(blob.name)
            process_funding_blob(blob_client)
        except Exception as e:
            logging.error(f"Failed to process blob {blob.name}: {e}")
    
    # Process FNG blobs
    fng_blobs = container_client.list_blobs(name_starts_with="raw/altme/fear_greed/")
    for blob in fng_blobs:
        try:
            blob_client = container_client.get_blob_client(blob.name)
            process_fng_blob(blob_client)
        except Exception as e:
            logging.error(f"Failed to process blob {blob.name}: {e}")
    
    # Process CoinGecko blobs
    coingecko_blobs = container_client.list_blobs(name_starts_with="raw/coingecko/markets/")
    for blob in coingecko_blobs:
        try:
            blob_client = container_client.get_blob_client(blob.name)
            process_coingecko_blob(blob_client)
        except Exception as e:
            logging.error(f"Failed to process blob {blob.name}: {e}")
    
    logging.info("Backfill processing completed")