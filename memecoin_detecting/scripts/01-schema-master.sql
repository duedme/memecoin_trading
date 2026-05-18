-- ============================================================
-- Memecoin Screener - Esquema Maestro Unificado (Geyser Ready)
-- ============================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 1. TABLAS BASE
CREATE TABLE IF NOT EXISTS tokens (
    id BIGSERIAL PRIMARY KEY,
    mintaddress TEXT UNIQUE NOT NULL,
    symbol TEXT,
    name TEXT,
    decimals INT DEFAULT 9,
    pooladdress TEXT,
    amm TEXT,
    detectedat TIMESTAMPTZ DEFAULT NOW(),
    createdat TIMESTAMPTZ DEFAULT NOW(),
    status TEXT DEFAULT 'active'
);
CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);

CREATE TABLE IF NOT EXISTS wallets (
    id BIGSERIAL PRIMARY KEY,
    walletaddress TEXT UNIQUE NOT NULL,
    firstseen TIMESTAMPTZ DEFAULT NOW(),
    lastseen TIMESTAMPTZ DEFAULT NOW(),
    tags TEXT
);

CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet_id BIGINT PRIMARY KEY REFERENCES wallets(id) ON DELETE CASCADE,
    walletaddress TEXT UNIQUE NOT NULL,
    last_signature TEXT,
    last_checked TIMESTAMPTZ DEFAULT NOW(),
    is_enabled BOOLEAN DEFAULT TRUE,
    priority INT DEFAULT 0
);

-- 2. TRANSACCIONES (HYPERTABLE)
CREATE TABLE IF NOT EXISTS wallettransactions (
    time TIMESTAMPTZ NOT NULL,
    signature TEXT NOT NULL,
    walletaddress TEXT NOT NULL,
    mintaddress TEXT NOT NULL,
    side TEXT NOT NULL,
    amounttoken DOUBLE PRECISION,
    amountsol DOUBLE PRECISION,
    pricesol DOUBLE PRECISION,
    slot BIGINT,
    amm TEXT,
    source TEXT DEFAULT 'local',
    raw_json JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, signature, walletaddress, mintaddress)
);
SELECT create_hypertable('wallettransactions', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_wtx_sig ON wallettransactions(signature);
CREATE INDEX IF NOT EXISTS idx_wtx_wallet ON wallettransactions(walletaddress, time DESC);
CREATE INDEX IF NOT EXISTS idx_wtx_mint ON wallettransactions(mintaddress, time DESC);

-- 3. POSICIONES Y PNL
CREATE TABLE IF NOT EXISTS walletpositions (
    walletaddress TEXT NOT NULL,
    mintaddress TEXT NOT NULL,
    amounttoken DOUBLE PRECISION DEFAULT 0,
    investedsol DOUBLE PRECISION DEFAULT 0,
    realizedsol DOUBLE PRECISION DEFAULT 0,
    total_bought_token DOUBLE PRECISION DEFAULT 0,
    total_sold_token DOUBLE PRECISION DEFAULT 0,
    total_bought_sol DOUBLE PRECISION DEFAULT 0,
    total_sold_sol DOUBLE PRECISION DEFAULT 0,
    avg_buy_price_sol DOUBLE PRECISION DEFAULT 0,
    avg_sell_price_sol DOUBLE PRECISION DEFAULT 0,
    realized_pnl_sol DOUBLE PRECISION DEFAULT 0,
    unrealized_pnl_sol DOUBLE PRECISION DEFAULT 0,
    total_pnl_sol DOUBLE PRECISION DEFAULT 0,
    status TEXT DEFAULT 'open',
    first_buy_at TIMESTAMPTZ,
    last_buy_at TIMESTAMPTZ,
    last_sell_at TIMESTAMPTZ,
    last_tx_at TIMESTAMPTZ,
    lastupdate TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (walletaddress, mintaddress)
);

CREATE TABLE IF NOT EXISTS walletpnlcache (
    walletaddress TEXT PRIMARY KEY,
    totalpnlusd DOUBLE PRECISION DEFAULT 0,
    realizedpnlusd DOUBLE PRECISION DEFAULT 0,
    unrealizedpnlusd DOUBLE PRECISION DEFAULT 0,
    totalpnl_sol DOUBLE PRECISION DEFAULT 0,
    realizedpnl_sol DOUBLE PRECISION DEFAULT 0,
    unrealizedpnl_sol DOUBLE PRECISION DEFAULT 0,
    invested_sol DOUBLE PRECISION DEFAULT 0,
    realized_sol DOUBLE PRECISION DEFAULT 0,
    roipct DOUBLE PRECISION DEFAULT 0,
    tradecount INT DEFAULT 0,
    buycount INT DEFAULT 0,
    sellcount INT DEFAULT 0,
    tokenstraded INT DEFAULT 0,
    openpositions INT DEFAULT 0,
    winrate DOUBLE PRECISION DEFAULT 0,
    besttrade_sol DOUBLE PRECISION DEFAULT 0,
    worsttrade_sol DOUBLE PRECISION DEFAULT 0,
    firstactivity TIMESTAMPTZ,
    lastactivity TIMESTAMPTZ,
    lastupdated TIMESTAMPTZ DEFAULT NOW(),
    source TEXT DEFAULT 'local'
);

-- 4. CLASIFICACIÓN Y MERCADO
CREATE TABLE IF NOT EXISTS walletclassifications (
    walletaddress TEXT PRIMARY KEY,
    behavior TEXT,
    behaviortype TEXT,
    investortype TEXT,
    investorlabel TEXT,
    consistencylevel TEXT,
    profittier TEXT,
    investorscore INT DEFAULT 0,
    totaltrades INT DEFAULT 0,
    winrate DOUBLE PRECISION DEFAULT 0,
    totalpnlsol DOUBLE PRECISION DEFAULT 0,
    roipercentage DOUBLE PRECISION DEFAULT 0,
    updatedat TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS classifier_throttle (
    walletaddress TEXT PRIMARY KEY,
    last_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sol_price_cache (
    id INT PRIMARY KEY DEFAULT 1,
    priceusd DOUBLE PRECISION NOT NULL,
    source TEXT DEFAULT 'local',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO sol_price_cache (id, priceusd, source) VALUES (1, 150.0, 'seed') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS token_price_cache (
    mintaddress TEXT PRIMARY KEY,
    pricesol DOUBLE PRECISION,
    priceusd DOUBLE PRECISION,
    liquiditysol DOUBLE PRECISION,
    marketcapusd DOUBLE PRECISION,
    source TEXT DEFAULT 'pumpfun_curve',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tokenmarketcache (
    mintaddress TEXT PRIMARY KEY,
    volume5m DOUBLE PRECISION DEFAULT 0,
    volume1h DOUBLE PRECISION DEFAULT 0,
    volume6h DOUBLE PRECISION DEFAULT 0,
    volume24h DOUBLE PRECISION DEFAULT 0,
    change5m DOUBLE PRECISION DEFAULT 0,
    change1h DOUBLE PRECISION DEFAULT 0,
    change6h DOUBLE PRECISION DEFAULT 0,
    change24h DOUBLE PRECISION DEFAULT 0,
    txns24h INT DEFAULT 0,
    makers24h INT DEFAULT 0,
    tier TEXT DEFAULT 'monitor',
    lastupdated TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tokentoptraderscache (
    id BIGSERIAL PRIMARY KEY,
    mintaddress TEXT,
    tokenid BIGINT,
    walletaddress TEXT NOT NULL,
    rank INT,
    totalpnl DOUBLE PRECISION DEFAULT 0,
    totalpnl_sol DOUBLE PRECISION DEFAULT 0,
    volumeusd DOUBLE PRECISION DEFAULT 0,
    volumesol DOUBLE PRECISION DEFAULT 0,
    tradecount INT DEFAULT 0,
    current_balance_token DOUBLE PRECISION DEFAULT 0,
    avg_buy_price_sol DOUBLE PRECISION DEFAULT 0,
    lastactivity TIMESTAMPTZ,
    lastupdated TIMESTAMPTZ DEFAULT NOW(),
    source TEXT DEFAULT 'local'
);

CREATE TABLE IF NOT EXISTS tokeninvestorstatscache (
    mintaddress TEXT PRIMARY KEY,
    totalinvestors INT DEFAULT 0,
    elitecount INT DEFAULT 0,
    profitablecount INT DEFAULT 0,
    regularcount INT DEFAULT 0,
    casualcount INT DEFAULT 0,
    losingcount INT DEFAULT 0,
    humanscount INT DEFAULT 0,
    botscount INT DEFAULT 0,
    avgscore DOUBLE PRECISION DEFAULT 0,
    totalvolume_sol DOUBLE PRECISION DEFAULT 0,
    totalpnl_sol DOUBLE PRECISION DEFAULT 0,
    lastupdated TIMESTAMPTZ DEFAULT NOW()
);

-- 5. SISTEMA INTERNO Y COLAS
CREATE TABLE IF NOT EXISTS reducer_queue (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    walletaddress TEXT,
    mintaddress TEXT,
    signature TEXT,
    priority INT NOT NULL DEFAULT 5,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    CONSTRAINT reducer_queue_unique_event UNIQUE (event_type, signature, walletaddress, mintaddress)
);

CREATE TABLE IF NOT EXISTS process_heartbeat (
    process_name TEXT PRIMARY KEY,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 6. TABLAS LEGACY (Para que no fallen los scripts de limpieza)
CREATE TABLE IF NOT EXISTS chain_events_staging (
    id BIGSERIAL PRIMARY KEY,
    status TEXT,
    created_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    inserted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS datasource_audit (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ
);

-- 7. FUNCIONES DE MANTENIMIENTO
CREATE OR REPLACE FUNCTION reducer_queue_dead_letter(p_max_attempts INT DEFAULT 10)
RETURNS INT AS $$
DECLARE
    moved INT;
BEGIN
    UPDATE reducer_queue
       SET status = 'dead',
           updated_at = NOW(),
           last_error = COALESCE(last_error, 'max attempts exceeded')
     WHERE status IN ('pending','processing','error')
       AND attempts > p_max_attempts;
    GET DIAGNOSTICS moved = ROW_COUNT;
    RETURN moved;
END;
$$ LANGUAGE plpgsql;