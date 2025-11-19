-- =========================================
-- 1. Symbol Reference (Spot + Perp supported)
-- =========================================
CREATE TABLE dbo.Symbol (
    symbol_id INT IDENTITY(1,1) PRIMARY KEY,
    symbol NVARCHAR(30) NOT NULL,          -- e.g., 'BTCUSDT'
    base_asset NVARCHAR(20) NULL,          -- 'BTC'
    quote_asset NVARCHAR(20) NULL,         -- 'USDT'
    is_perp BIT NOT NULL DEFAULT(0),       -- 0 = Spot, 1 = Perpetual Futures
    is_active BIT NOT NULL DEFAULT(1),
    CONSTRAINT UQ_Symbol UNIQUE (symbol, is_perp) 
);
GO

-- =========================================
-- 2. Kline (Candlestick / OHLCV)
-- =========================================
CREATE TABLE dbo.Kline (
    symbol_id INT NOT NULL,
    interval NVARCHAR(10) NOT NULL,        -- e.g., '1m','1h','1d'
    open_time DATETIME2 NOT NULL,
    close_time DATETIME2 NOT NULL,
    open_price DECIMAL(38, 12) NOT NULL,
    high_price DECIMAL(38, 12) NOT NULL,
    low_price  DECIMAL(38, 12) NOT NULL,
    close_price DECIMAL(38, 12) NOT NULL,
    base_volume DECIMAL(38, 12) NOT NULL,
    quote_volume DECIMAL(38, 12) NULL,
    trade_count BIGINT NULL,
    taker_buy_base_vol DECIMAL(38, 12) NULL,
    taker_buy_quote_vol DECIMAL(38, 12) NULL,
    load_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_Kline PRIMARY KEY (symbol_id, interval, open_time)
);
GO

-- =========================================
-- 3. Funding Rate (Perpetual Futures)
-- =========================================
CREATE TABLE dbo.FundingRate (
    symbol_id INT NOT NULL,
    funding_time DATETIME2 NOT NULL,
    funding_rate DECIMAL(18, 10) NOT NULL,
    load_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FundingRate PRIMARY KEY (symbol_id, funding_time)
);
GO

-- =========================================
-- 4. Fear & Greed Index
-- =========================================
CREATE TABLE dbo.FearGreed (
    fng_time DATETIME2 NOT NULL,
    index_value INT NOT NULL,                  -- 0–100
    sentiment_label NVARCHAR(50) NULL,         -- e.g., 'Fear', 'Greed'
    load_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_FearGreed PRIMARY KEY (fng_time)
);
GO

-- =========================================
-- 5. Coin Market Snapshot (from CoinGecko)
-- =========================================
CREATE TABLE dbo.CoinMarket (
    coin_id NVARCHAR(60) NOT NULL,             -- e.g., 'bitcoin'
    asof_time DATETIME2 NOT NULL,
    coin_symbol NVARCHAR(20) NULL,             -- 'btc'
    coin_name NVARCHAR(120) NULL,              -- 'Bitcoin'
    market_price DECIMAL(38, 12) NULL,
    market_cap DECIMAL(38, 2) NULL,
    circulating_supply DECIMAL(38, 6) NULL,
    total_volume DECIMAL(38, 2) NULL,
    load_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_CoinMarket PRIMARY KEY (coin_id, asof_time)
);
GO
