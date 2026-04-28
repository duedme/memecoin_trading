-- Schema v8: migrado a Birdeye + tope MAX_TRACKED_WALLETS con rotación.

-- 1. Tokens detectados (metadata + estado)
CREATE TABLE IF NOT EXISTS tokens (
    token_id SERIAL PRIMARY KEY,
    mint_address VARCHAR(44) UNIQUE NOT NULL,
    name VARCHAR(255),
    symbol VARCHAR(40),
    decimals INTEGER DEFAULT 9,
    liquidity NUMERIC(20,4),
    detected_at TIMESTAMP DEFAULT NOW(),
    source VARCHAR(30) DEFAULT 'birdeye_new_listing',
    status VARCHAR(20) DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_tokens_mint ON tokens(mint_address);
CREATE INDEX IF NOT EXISTS idx_tokens_detected ON tokens(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);

-- 2. Caché de precio / market data (última foto)
CREATE TABLE IF NOT EXISTS token_market_cache (
    token_id INTEGER PRIMARY KEY REFERENCES tokens(token_id) ON DELETE CASCADE,
    price_usd NUMERIC(30,12),
    price_sol NUMERIC(30,12),
    market_cap NUMERIC(20,2),
    fdv NUMERIC(20,2),
    liquidity NUMERIC(20,4),
    volume_24h NUMERIC(20,4),
    holder_count INTEGER,
    raw_json JSONB,
    last_updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_market_updated ON token_market_cache(last_updated DESC);

-- 3. Histórico de precio (snapshots ligeros)
CREATE TABLE IF NOT EXISTS token_price_history (
    id BIGSERIAL PRIMARY KEY,
    token_id INTEGER REFERENCES tokens(token_id) ON DELETE CASCADE,
    time TIMESTAMP DEFAULT NOW(),
    price_usd NUMERIC(30,12),
    volume_24h NUMERIC(20,4)
);
CREATE INDEX IF NOT EXISTS idx_price_history_token_time ON token_price_history(token_id, time DESC);

-- 4. Wallets (stats agregadas propias)
CREATE TABLE IF NOT EXISTS wallets (
    wallet_id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) UNIQUE NOT NULL,
    first_seen TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    tags TEXT
);
CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(wallet_address);

-- 5. Wallets que el usuario marca manualmente
CREATE TABLE IF NOT EXISTS tracked_wallets (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) UNIQUE NOT NULL,
    label VARCHAR(255),
    reason TEXT,
    priority INTEGER DEFAULT 3,
    is_active BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMP DEFAULT NOW()
);

-- 6. Caché de PnL por wallet
CREATE TABLE IF NOT EXISTS wallet_pnl_cache (
    wallet_address VARCHAR(44) PRIMARY KEY,
    realized_pnl_usd NUMERIC(20,4),
    unrealized_pnl_usd NUMERIC(20,4),
    total_pnl_usd NUMERIC(20,4),
    roi_pct NUMERIC(10,4),
    trade_count INTEGER,
    win_rate NUMERIC(5,2),
    raw_json JSONB,
    last_updated TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pnl_total ON wallet_pnl_cache(total_pnl_usd DESC);

-- 7. Cola de sincronización de wallets (prioridad + tope + next_sync_at)
CREATE TABLE IF NOT EXISTS wallet_sync_queue (
    wallet_address VARCHAR(44) PRIMARY KEY,
    priority INTEGER DEFAULT 5,          -- 1=más alta, 10=más baja
    refresh_interval_sec INTEGER DEFAULT 3600,
    active BOOLEAN NOT NULL DEFAULT TRUE, -- FALSE = desbancada por rotación
    next_sync_at TIMESTAMP DEFAULT NOW(),
    last_synced_at TIMESTAMP,
    fail_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_queue_active_next
    ON wallet_sync_queue(active, next_sync_at) WHERE active = TRUE;
CREATE INDEX IF NOT EXISTS idx_queue_prio
    ON wallet_sync_queue(priority ASC, next_sync_at ASC) WHERE active = TRUE;

-- 8. Top traders por token (cache)
CREATE TABLE IF NOT EXISTS token_top_traders_cache (
    token_id INTEGER REFERENCES tokens(token_id) ON DELETE CASCADE,
    wallet_address VARCHAR(44),
    rank INTEGER,
    volume_usd NUMERIC(20,4),
    total_pnl NUMERIC(20,4),
    trade_count INTEGER,
    last_updated TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (token_id, wallet_address)
);
CREATE INDEX IF NOT EXISTS idx_ttc_token ON token_top_traders_cache(token_id, rank);

-- 9. Clasificación de inversores
CREATE TABLE IF NOT EXISTS wallet_classifications (
    classification_id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) UNIQUE REFERENCES wallet_pnl_cache(wallet_address) ON DELETE CASCADE,
    behavior_type VARCHAR(20),
    consistency_level VARCHAR(20),
    profit_tier VARCHAR(20),
    investor_type VARCHAR(20),
    investor_score INTEGER DEFAULT 0,
    investor_label VARCHAR(100),
    classified_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_class_type ON wallet_classifications(investor_type);
CREATE INDEX IF NOT EXISTS idx_class_score ON wallet_classifications(investor_score DESC);

-- 10. Contador de consumo de CUs (kill switch diario)
CREATE TABLE IF NOT EXISTS birdeye_usage (
    day DATE PRIMARY KEY,
    cu_consumed BIGINT DEFAULT 0,
    requests_total BIGINT DEFAULT 0,
    wallet_requests BIGINT DEFAULT 0,
    last_updated TIMESTAMP DEFAULT NOW()
);