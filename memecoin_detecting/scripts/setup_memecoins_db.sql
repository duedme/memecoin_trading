-- ============================================================
-- Memecoin Screener — schema oficial (local-only)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- TOKENS
-- ============================================================
CREATE TABLE IF NOT EXISTS tokens (
    id              BIGSERIAL PRIMARY KEY,
    mint_address    TEXT UNIQUE NOT NULL,
    symbol          TEXT,
    name            TEXT,
    decimals        INT DEFAULT 9,
    pool_address    TEXT,
    amm             TEXT,
    detected_at     TIMESTAMPTZ DEFAULT NOW(),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    status          TEXT DEFAULT 'active'  -- active | dead | blacklisted
);
CREATE INDEX IF NOT EXISTS idx_tokens_status    ON tokens(status);
CREATE INDEX IF NOT EXISTS idx_tokens_detected  ON tokens(detected_at DESC);

-- ============================================================
-- TOKEN METRICS (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS token_metrics (
    time            TIMESTAMPTZ NOT NULL,
    mint_address    TEXT NOT NULL,
    price_usd       DOUBLE PRECISION,
    price_sol       DOUBLE PRECISION,
    liquidity_sol   DOUBLE PRECISION,
    market_cap_usd  DOUBLE PRECISION,
    volume_5m       DOUBLE PRECISION,
    volume_1h       DOUBLE PRECISION,
    volume_6h       DOUBLE PRECISION,
    volume_24h      DOUBLE PRECISION,
    change_5m       DOUBLE PRECISION,
    change_1h       DOUBLE PRECISION,
    change_6h       DOUBLE PRECISION,
    change_24h      DOUBLE PRECISION,
    txns_24h        INT,
    makers_24h      INT
);
SELECT create_hypertable('token_metrics', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_metrics_mint_time
    ON token_metrics(mint_address, time DESC);

-- ============================================================
-- WALLETS
-- ============================================================
CREATE TABLE IF NOT EXISTS wallets (
    id              BIGSERIAL PRIMARY KEY,
    wallet_address  TEXT UNIQUE NOT NULL,
    first_seen      TIMESTAMPTZ DEFAULT NOW(),
    last_seen       TIMESTAMPTZ DEFAULT NOW(),
    tags            TEXT
);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen ON wallets(last_seen DESC);

-- ============================================================
-- TRACKED WALLETS (cola del tracker)
-- ============================================================
CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet_id       BIGINT PRIMARY KEY REFERENCES wallets(id) ON DELETE CASCADE,
    wallet_address  TEXT UNIQUE NOT NULL,
    last_signature  TEXT,
    last_checked    TIMESTAMPTZ DEFAULT NOW(),
    is_enabled      BOOLEAN DEFAULT TRUE,
    priority        INT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_tracked_enabled
    ON tracked_wallets(is_enabled, priority DESC, last_checked ASC);

-- ============================================================
-- WALLET TRANSACTIONS (hypertable)
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_transactions (
    time            TIMESTAMPTZ NOT NULL,
    signature       TEXT NOT NULL,
    wallet_address  TEXT NOT NULL,
    mint_address    TEXT NOT NULL,
    side            TEXT NOT NULL,        -- buy | sell
    amount_token    DOUBLE PRECISION,
    amount_sol      DOUBLE PRECISION,
    price_sol       DOUBLE PRECISION,
    PRIMARY KEY (time, signature, wallet_address, mint_address)
);
SELECT create_hypertable('wallet_transactions', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_wtx_wallet_time
    ON wallet_transactions(wallet_address, time DESC);
CREATE INDEX IF NOT EXISTS idx_wtx_mint_time
    ON wallet_transactions(mint_address, time DESC);

-- ============================================================
-- WALLET POSITIONS (estado actual)
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_positions (
    wallet_address  TEXT NOT NULL,
    mint_address    TEXT NOT NULL,
    amount_token    DOUBLE PRECISION DEFAULT 0,
    invested_sol    DOUBLE PRECISION DEFAULT 0,
    realized_sol    DOUBLE PRECISION DEFAULT 0,
    last_update     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (wallet_address, mint_address)
);

-- ============================================================
-- WALLET CLASSIFICATIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_classifications (
    wallet_address   TEXT PRIMARY KEY,
    behavior         TEXT,          -- human | bot | suspicious
    investor_type    TEXT,          -- elite | profitable | regular | bot-profitable | bot-regular | losing | casual | unclassified
    investor_score   INT DEFAULT 0,
    total_trades     INT DEFAULT 0,
    win_rate         DOUBLE PRECISION DEFAULT 0,
    total_pnl_sol    DOUBLE PRECISION DEFAULT 0,
    roi_percentage   DOUBLE PRECISION DEFAULT 0,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_class_type  ON wallet_classifications(investor_type);
CREATE INDEX IF NOT EXISTS idx_class_score ON wallet_classifications(investor_score DESC);