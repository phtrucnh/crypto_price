CREATE OR ALTER VIEW dbo.v_CoinLatestPrice
AS
WITH x AS (
    SELECT
        s.symbol,
        k.interval,
        k.close_time,
        k.close_price,
        LAG(k.close_price, 24) OVER (
            PARTITION BY s.symbol, k.interval
            ORDER BY k.close_time
        ) AS price_24h_ago,
        ROW_NUMBER() OVER (
            PARTITION BY s.symbol, k.interval
            ORDER BY k.close_time DESC
        ) AS rn
    FROM dbo.Kline k
    JOIN dbo.Symbol s
      ON s.symbol_id = k.symbol_id
    WHERE k.interval = '1h'
)
SELECT
    symbol,
    interval,
    close_time AS latest_time,
    close_price AS current_price,
    ( (close_price / NULLIF(price_24h_ago, 0)) - 1 ) * 100 AS pct_change_24h
FROM x
WHERE rn = 1;


CREATE OR ALTER VIEW v_CoinATH_ATL AS
SELECT
    s.symbol,
    MAX(k.high_price) AS all_time_high,
    MIN(k.low_price)  AS all_time_low
FROM dbo.Kline k
JOIN dbo.Symbol s ON s.symbol_id = k.symbol_id
WHERE k.interval = '1d'
GROUP BY s.symbol;


CREATE OR ALTER VIEW v_FundingRateSummary AS
SELECT
    s.symbol,
    MAX(f.funding_time) AS last_funding_time,
    AVG(f.funding_rate) AS avg_funding_rate_7d,
    MAX(f.funding_rate) AS latest_funding_rate
FROM dbo.FundingRate f
JOIN dbo.Symbol s ON s.symbol_id = f.symbol_id
WHERE f.funding_time > DATEADD(DAY, -7, SYSUTCDATETIME())
GROUP BY s.symbol;


CREATE OR ALTER VIEW v_CryptoDashboard AS
SELECT
    p.symbol,
    p.current_price,
    p.pct_change_24h,
    ath.all_time_high,
    ath.all_time_low,
    f.avg_funding_rate_7d,
    f.latest_funding_rate
FROM v_CoinLatestPrice p
LEFT JOIN v_CoinATH_ATL ath ON ath.symbol = p.symbol
LEFT JOIN v_FundingRateSummary f ON f.symbol = p.symbol;

CREATE OR ALTER VIEW dbo.v_MarketSnapshot
AS
SELECT
    m.coin_id,
    m.coin_symbol,
    m.coin_name,
    m.market_price,
    m.market_cap,
    m.total_volume,
    m.asof_time,
    LAG(m.market_price, 1) OVER (
        PARTITION BY m.coin_id
        ORDER BY m.asof_time
    ) AS prev_price,
    (m.market_price - LAG(m.market_price, 1) OVER (
        PARTITION BY m.coin_id
        ORDER BY m.asof_time
    ))
        / NULLIF(LAG(m.market_price, 1) OVER (
            PARTITION BY m.coin_id
            ORDER BY m.asof_time
        ), 0) * 100 AS pct_change
FROM dbo.CoinMarket AS m;

