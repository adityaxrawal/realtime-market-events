-- data-pipeline/schema.sql
-- Defines the database schema for storing processed data in TimescaleDB/PostgreSQL.
-- Added market_analysis_results table.

-- Ensure TimescaleDB extension is enabled
-- In TimescaleDB Docker images (especially -ha variants), this is often enabled by default.
-- Running this command ensures it's available.
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Table for storing processed stock sentiment data
CREATE TABLE stock_sentiment (
    time TIMESTAMPTZ NOT NULL,          -- Timestamp of the sentiment event (when the news/post was created or processed)
    stock_symbol VARCHAR(20) NOT NULL, -- Stock symbol (e.g., RELIANCE, AAPL) - Increased length for safety
    source VARCHAR(50),                -- Source of the data (e.g., 'finnhub', 'reddit')
    sentiment_score DOUBLE PRECISION,  -- Sentiment score (e.g., VADER compound: -1 to 1)
    sentiment_label VARCHAR(10),       -- Sentiment label (e.g., 'positive', 'negative', 'neutral')
    headline TEXT,                     -- News headline or post title (can be long)
    summary TEXT,                      -- News summary or post body snippet (can be long)
    source_id VARCHAR(255),            -- Unique ID from the source (e.g., news ID, post ID)
    url TEXT                           -- URL to the source article/post (can be long)
);

-- Table for storing detected market regime data
-- NOTE: This table might be used less directly if the final outcomes are stored in market_analysis_results
CREATE TABLE market_regimes (
    time TIMESTAMPTZ NOT NULL,          -- Timestamp of the regime detection
    stock_symbol VARCHAR(20) NOT NULL, -- Stock symbol - Increased length for safety
    regime_type VARCHAR(50) NOT NULL,  -- Type of regime (e.g., 'Trend', 'Volatility', 'Placeholder')
    regime_label VARCHAR(50) NOT NULL, -- Specific label (e.g., 'Trending Up', 'Low Volatility', 'Placeholder')
    score DOUBLE PRECISION,            -- Optional confidence score or associated value
    PRIMARY KEY (time, stock_symbol, regime_type) -- Example composite key, adjust if needed
);

-- *** ADDED: Table for storing the final 5 analysis outcomes from Flink ***
CREATE TABLE market_analysis_results (
    time TIMESTAMPTZ NOT NULL,          -- Timestamp of the latest analysis calculation
    stock_symbol VARCHAR(20) NOT NULL, -- The stock symbol
    sentiment_label VARCHAR(15),       -- Overall sentiment (e.g., Very Bearish, Bullish)
    short_term_label VARCHAR(10),      -- Short-term trend (e.g., Bearish, Neutral)
    volatility_label VARCHAR(15),      -- Volatility level (e.g., Low, High)
    risk_label VARCHAR(15),            -- Risk level (e.g., Low, High)
    liquidity_crunch BOOLEAN,          -- Flag indicating potential liquidity issues
    mood_index_label VARCHAR(15),      -- Overall mood index (e.g., Fear, Greed)
    -- Optional: Add underlying scores if calculated and stored by Flink
    sentiment_score DOUBLE PRECISION,
    volatility_score DOUBLE PRECISION,
    risk_score DOUBLE PRECISION,
    mood_index_score DOUBLE PRECISION,
    -- Define primary key for uniqueness and efficient lookups
    PRIMARY KEY (time, stock_symbol)
);


-- Convert tables to TimescaleDB hypertables, partitioning by the 'time' column.
-- This is ESSENTIAL for TimescaleDB performance and features.
SELECT create_hypertable('stock_sentiment', 'time', if_not_exists => TRUE);
SELECT create_hypertable('market_regimes', 'time', if_not_exists => TRUE);
-- *** ADDED: Convert the new table to a hypertable ***
SELECT create_hypertable('market_analysis_results', 'time', if_not_exists => TRUE);

-- Create indexes on frequently queried columns for better query performance.
-- TimescaleDB automatically creates an index on the 'time' column for hypertables.
CREATE INDEX IF NOT EXISTS idx_sentiment_symbol_time ON stock_sentiment (stock_symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_regime_symbol_time ON market_regimes (stock_symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_source ON stock_sentiment (source); -- Index on the data source
-- *** ADDED: Index for the new table ***
CREATE INDEX IF NOT EXISTS idx_analysis_symbol_time ON market_analysis_results (stock_symbol, time DESC);

-- Optional: Add unique constraint to prevent duplicate sentiment entries if needed.
-- This requires careful handling in the Flink job (e.g., exactly-once sinks or idempotence).
-- Consider if duplicates are acceptable vs. the overhead of ensuring uniqueness.
-- ALTER TABLE stock_sentiment ADD CONSTRAINT unique_sentiment_entry UNIQUE (time, stock_symbol, source, source_id);