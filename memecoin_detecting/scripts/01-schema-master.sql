-- ============================================================
-- Memecoin Screener — schema oficial (local-only)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- TOKENS
-- ============================================================
CREATE TABLE IF NOT EXISTS tokens (
    id              BIGSERIAL PRIMARY KEY,
    mintaddress    TEXT UNIQUE NOT NULL,
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
    mintaddress    TEXT NOT NULL,
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
    ON token_metrics(mintaddress, time DESC);

-- ============================================================
-- WALLETS
-- ============================================================
CREATE TABLE IF NOT EXISTS wallets (
    id              BIGSERIAL PRIMARY KEY,
    walletaddress  TEXT UNIQUE NOT NULL,
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
    walletaddress  TEXT UNIQUE NOT NULL,
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
    walletaddress  TEXT NOT NULL,
    mintaddress    TEXT NOT NULL,
    side            TEXT NOT NULL,        -- buy | sell
    amount_token    DOUBLE PRECISION,
    amount_sol      DOUBLE PRECISION,
    price_sol       DOUBLE PRECISION,
    PRIMARY KEY (time, signature, walletaddress, mintaddress)
);
SELECT create_hypertable('wallet_transactions', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_wtx_wallet_time
    ON wallet_transactions(walletaddress, time DESC);
CREATE INDEX IF NOT EXISTS idx_wtx_mint_time
    ON wallet_transactions(mintaddress, time DESC);

-- ============================================================
-- WALLET POSITIONS (estado actual)
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_positions (
    walletaddress  TEXT NOT NULL,
    mintaddress    TEXT NOT NULL,
    amount_token    DOUBLE PRECISION DEFAULT 0,
    invested_sol    DOUBLE PRECISION DEFAULT 0,
    realized_sol    DOUBLE PRECISION DEFAULT 0,
    last_update     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (walletaddress, mintaddress)
);

-- ============================================================
-- WALLET CLASSIFICATIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_classifications (
    walletaddress   TEXT PRIMARY KEY,
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

-- =====================================================================
-- 002_realtime_schema.sql
-- Migración idempotente para la arquitectura real-time (Fase 0).
-- Se puede correr múltiples veces sin romper nada.
-- =====================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------
-- 1. Staging de eventos crudos del chain-listener
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chain_events_staging (
    id              BIGSERIAL PRIMARY KEY,
    signature       TEXT NOT NULL,
    source          TEXT NOT NULL DEFAULT 'unknown',
    program_id      TEXT,
    program_name    TEXT,
    slot            BIGINT,
    block_time      TIMESTAMPTZ,
    commitment      TEXT DEFAULT 'confirmed',
    raw_json        JSONB,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    parse_error     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    CONSTRAINT chain_events_status_chk
        CHECK (status IN ('pending','processing','parsed','ignored','error','dead')),
    CONSTRAINT chain_events_unique_sig_source UNIQUE (signature, source)
);

CREATE INDEX IF NOT EXISTS idx_staging_status_created
    ON chain_events_staging (status, created_at);
CREATE INDEX IF NOT EXISTS idx_staging_signature
    ON chain_events_staging (signature);
CREATE INDEX IF NOT EXISTS idx_staging_program
    ON chain_events_staging (program_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_staging_slot
    ON chain_events_staging (slot DESC);

-- ---------------------------------------------------------------------
-- 2. Cola interna de reducers
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS reducer_queue (
    id              BIGSERIAL PRIMARY KEY,
    event_type      TEXT NOT NULL,
    walletaddress   TEXT,
    mintaddress     TEXT,
    signature       TEXT,
    priority        INT NOT NULL DEFAULT 5,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    CONSTRAINT reducer_queue_status_chk
        CHECK (status IN ('pending','processing','done','error','dead')),
    CONSTRAINT reducer_queue_unique_event
        UNIQUE (event_type, signature, walletaddress, mintaddress)
);

CREATE INDEX IF NOT EXISTS idx_reducer_queue_pick
    ON reducer_queue (status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_reducer_queue_wallet
    ON reducer_queue (walletaddress) WHERE walletaddress IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_reducer_queue_mint
    ON reducer_queue (mintaddress) WHERE mintaddress IS NOT NULL;

-- ---------------------------------------------------------------------
-- 3. Heartbeat de procesos
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS process_heartbeat (
    process_name    TEXT PRIMARY KEY,
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_process_heartbeat_last_seen
    ON process_heartbeat (last_seen_at DESC);

-- ---------------------------------------------------------------------
-- 4. Defensa: asegurar tablas base del stack (por si el init no corrió)
-- ---------------------------------------------------------------------
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
CREATE INDEX IF NOT EXISTS idx_tokens_detectedat ON tokens(detectedat DESC);

CREATE TABLE IF NOT EXISTS wallets (
    id BIGSERIAL PRIMARY KEY,
    walletaddress TEXT UNIQUE NOT NULL,
    firstseen TIMESTAMPTZ DEFAULT NOW(),
    lastseen TIMESTAMPTZ DEFAULT NOW(),
    tags TEXT
);

-- ---------------------------------------------------------------------
-- 5. wallettransactions (hypertable) + columnas extra
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wallettransactions (
    time TIMESTAMPTZ NOT NULL,
    signature TEXT NOT NULL,
    walletaddress TEXT NOT NULL,
    mintaddress TEXT NOT NULL,
    side TEXT NOT NULL,
    amounttoken DOUBLE PRECISION,
    amountsol DOUBLE PRECISION,
    pricesol DOUBLE PRECISION,
    PRIMARY KEY (time, signature, walletaddress, mintaddress)
);

ALTER TABLE wallettransactions ADD COLUMN IF NOT EXISTS slot BIGINT;
ALTER TABLE wallettransactions ADD COLUMN IF NOT EXISTS amm TEXT;
ALTER TABLE wallettransactions ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'local';
ALTER TABLE wallettransactions ADD COLUMN IF NOT EXISTS raw_json JSONB;
ALTER TABLE wallettransactions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();

DO $$
BEGIN
    PERFORM create_hypertable('wallettransactions', 'time',
                              if_not_exists => TRUE,
                              migrate_data => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'create_hypertable wallettransactions: %', SQLERRM;
END$$;

CREATE INDEX IF NOT EXISTS idx_wtx_signature ON wallettransactions(signature);
CREATE INDEX IF NOT EXISTS idx_wtx_wallet_time ON wallettransactions(walletaddress, time DESC);
CREATE INDEX IF NOT EXISTS idx_wtx_mint_time ON wallettransactions(mintaddress, time DESC);
CREATE INDEX IF NOT EXISTS idx_wtx_wallet_mint_time ON wallettransactions(walletaddress, mintaddress, time DESC);

-- ---------------------------------------------------------------------
-- 6. walletpositions extendida
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS walletpositions (
    walletaddress TEXT NOT NULL,
    mintaddress TEXT NOT NULL,
    amounttoken DOUBLE PRECISION DEFAULT 0,
    investedsol DOUBLE PRECISION DEFAULT 0,
    realizedsol DOUBLE PRECISION DEFAULT 0,
    lastupdate TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (walletaddress, mintaddress)
);

ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS total_bought_token DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS total_sold_token DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS total_bought_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS total_sold_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS avg_buy_price_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS avg_sell_price_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS realized_pnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS unrealized_pnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS total_pnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'open';
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS first_buy_at TIMESTAMPTZ;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS last_buy_at TIMESTAMPTZ;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS last_sell_at TIMESTAMPTZ;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS last_tx_at TIMESTAMPTZ;
ALTER TABLE walletpositions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_walletpositions_wallet ON walletpositions(walletaddress);
CREATE INDEX IF NOT EXISTS idx_walletpositions_mint ON walletpositions(mintaddress);
CREATE INDEX IF NOT EXISTS idx_walletpositions_status ON walletpositions(status);
CREATE INDEX IF NOT EXISTS idx_walletpositions_lastupdate ON walletpositions(lastupdate DESC);

-- ---------------------------------------------------------------------
-- 7. walletpnlcache extendida
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS walletpnlcache (
    walletaddress TEXT PRIMARY KEY,
    totalpnlusd DOUBLE PRECISION DEFAULT 0,
    realizedpnlusd DOUBLE PRECISION DEFAULT 0,
    unrealizedpnlusd DOUBLE PRECISION DEFAULT 0,
    roipct DOUBLE PRECISION DEFAULT 0,
    tradecount INT DEFAULT 0,
    winrate DOUBLE PRECISION DEFAULT 0,
    lastupdated TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS totalpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS realizedpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS unrealizedpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS invested_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS realized_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS buycount INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS sellcount INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS tokenstraded INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS openpositions INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS besttrade_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS worsttrade_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS firstactivity TIMESTAMPTZ;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS lastactivity TIMESTAMPTZ;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'local';

CREATE INDEX IF NOT EXISTS idx_walletpnlcache_lastupdated ON walletpnlcache(lastupdated DESC);
CREATE INDEX IF NOT EXISTS idx_walletpnlcache_totalpnl_sol ON walletpnlcache(totalpnl_sol DESC);

-- ---------------------------------------------------------------------
-- 8. tokentoptraderscache defensiva
-- ---------------------------------------------------------------------
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

CREATE INDEX IF NOT EXISTS idx_ttc_mint ON tokentoptraderscache(mintaddress);
CREATE INDEX IF NOT EXISTS idx_ttc_tokenid ON tokentoptraderscache(tokenid);
CREATE INDEX IF NOT EXISTS idx_ttc_wallet ON tokentoptraderscache(walletaddress);
CREATE INDEX IF NOT EXISTS idx_ttc_mint_rank ON tokentoptraderscache(mintaddress, rank);

-- ---------------------------------------------------------------------
-- 9. walletclassifications defensiva
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS walletclassifications (
    walletaddress TEXT PRIMARY KEY,
    behavior TEXT,
    investortype TEXT,
    investorscore INT DEFAULT 0,
    totaltrades INT DEFAULT 0,
    winrate DOUBLE PRECISION DEFAULT 0,
    totalpnlsol DOUBLE PRECISION DEFAULT 0,
    roipercentage DOUBLE PRECISION DEFAULT 0,
    updatedat TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wc_investortype ON walletclassifications(investortype);
CREATE INDEX IF NOT EXISTS idx_wc_score ON walletclassifications(investorscore DESC);

-- ---------------------------------------------------------------------
-- 10. Retention policies
-- ---------------------------------------------------------------------
DO $$
BEGIN
    PERFORM add_retention_policy('wallettransactions', INTERVAL '90 days', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'retention wallettransactions: %', SQLERRM;
END$$;

-- ---------------------------------------------------------------------
-- 11. Confirmación
-- ---------------------------------------------------------------------
INSERT INTO process_heartbeat (process_name, last_seen_at, metadata)
VALUES ('migration-002-realtime-schema', NOW(),
        jsonb_build_object('status','applied','version','002'))
ON CONFLICT (process_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

-- ============================================================
-- 003_walletpnlcache_compat.sql
-- Asegura columnas que escribe wallet_reducer.py y que lee apiserver.py
-- Idempotente.
-- ============================================================

-- Por si la tabla no tenía aún estas columnas (defensive)
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS totalpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS realizedpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS unrealizedpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS invested_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS realized_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS buycount INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS sellcount INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS tokenstraded INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS openpositions INT DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS besttrade_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS worsttrade_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS firstactivity TIMESTAMPTZ;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS lastactivity TIMESTAMPTZ;
ALTER TABLE walletpnlcache ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'local';

-- Índices para la API
CREATE INDEX IF NOT EXISTS idx_walletpnlcache_tradecount
    ON walletpnlcache (tradecount DESC);
CREATE INDEX IF NOT EXISTS idx_walletpnlcache_totalpnlusd
    ON walletpnlcache (totalpnlusd DESC);
CREATE INDEX IF NOT EXISTS idx_walletpnlcache_roipct
    ON walletpnlcache (roipct DESC);
CREATE INDEX IF NOT EXISTS idx_walletpnlcache_winrate
    ON walletpnlcache (winrate DESC);
CREATE INDEX IF NOT EXISTS idx_walletpnlcache_lastactivity
    ON walletpnlcache (lastactivity DESC);

-- Registro de migración
INSERT INTO process_heartbeat (process_name, last_seen_at, metadata)
VALUES ('migration-003-walletpnlcache-compat', NOW(),
        jsonb_build_object('status','applied','version','003'))
ON CONFLICT (process_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

-- ============================================================
-- 004_token_investors.sql
-- Agregado por token de: conteo por investortype y behavior.
-- Alimentado por token_trader_reducer.
-- Idempotente.
-- ============================================================

CREATE TABLE IF NOT EXISTS tokeninvestorstatscache (
    mintaddress      TEXT PRIMARY KEY,
    totalinvestors   INT DEFAULT 0,
    elitecount       INT DEFAULT 0,
    profitablecount  INT DEFAULT 0,
    regularcount     INT DEFAULT 0,
    casualcount      INT DEFAULT 0,
    losingcount      INT DEFAULT 0,
    humanscount      INT DEFAULT 0,
    botscount        INT DEFAULT 0,
    avgscore         DOUBLE PRECISION DEFAULT 0,
    totalvolume_sol  DOUBLE PRECISION DEFAULT 0,
    totalpnl_sol     DOUBLE PRECISION DEFAULT 0,
    lastupdated      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tokeninvestorstatscache_lastupdated
    ON tokeninvestorstatscache (lastupdated DESC);

-- Asegurar que tokentoptraderscache tenga todo lo que escribiremos
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS mintaddress TEXT;
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS totalpnl_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS volumesol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS current_balance_token DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS avg_buy_price_sol DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS lastactivity TIMESTAMPTZ;
ALTER TABLE tokentoptraderscache ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'local';

-- Índice para búsquedas por mint+rank (lo usa la API)
CREATE INDEX IF NOT EXISTS idx_ttc_mint_rank
    ON tokentoptraderscache (mintaddress, rank);

-- Registro de migración
INSERT INTO process_heartbeat (process_name, last_seen_at, metadata)
VALUES ('migration-004-token-investors', NOW(),
        jsonb_build_object('status','applied','version','004'))
ON CONFLICT (process_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    metadata = EXCLUDED.metadata,
    updated_at = NOW();

-- ============================================================
-- 005_market_aggregates.sql
-- Continuous aggregates Timescale + tablas de price feed.
-- Idempotente. Ejecutar como superuser (psql -U memecoin).
-- ============================================================

-- ---------------------------------------------------------------
-- 1. Price feed SOL/USD local
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sol_price_cache (
    id          INT PRIMARY KEY DEFAULT 1,
    priceusd    DOUBLE PRECISION NOT NULL,
    source      TEXT DEFAULT 'local',
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT sol_price_singleton CHECK (id = 1)
);

-- Seed
INSERT INTO sol_price_cache (id, priceusd, source)
VALUES (1, 150.0, 'seed')
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------
-- 2. Token price cache (precio actual por mint en SOL)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS token_price_cache (
    mintaddress     TEXT PRIMARY KEY,
    pricesol        DOUBLE PRECISION,
    priceusd        DOUBLE PRECISION,
    liquiditysol    DOUBLE PRECISION,
    marketcapusd    DOUBLE PRECISION,
    source          TEXT DEFAULT 'pumpfun_curve',
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_token_price_cache_updated
    ON token_price_cache (updated_at DESC);

-- ---------------------------------------------------------------
-- 3. Continuous aggregates de wallettransactions
-- ---------------------------------------------------------------
-- Drop si existen con otro esquema (idempotencia segura solo si son nuestras)
DO $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'token_metrics_5m'
    ) INTO v_exists;
    IF NOT v_exists THEN
        EXECUTE $sql$
        CREATE MATERIALIZED VIEW token_metrics_5m
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket(INTERVAL '5 minutes', time) AS bucket,
            mintaddress,
            SUM(amountsol)                     AS volume_sol,
            COUNT(*)                            AS txns,
            COUNT(DISTINCT walletaddress)       AS makers,
            SUM(amountsol) FILTER (WHERE side='buy')  AS buy_volume_sol,
            SUM(amountsol) FILTER (WHERE side='sell') AS sell_volume_sol,
            MAX(pricesol)                       AS high_price,
            MIN(pricesol)                       AS low_price,
            (ARRAY_AGG(pricesol ORDER BY time DESC))[1] AS close_price,
            (ARRAY_AGG(pricesol ORDER BY time ASC))[1]  AS open_price
        FROM wallettransactions
        GROUP BY bucket, mintaddress
        WITH NO DATA;
        $sql$;
    END IF;
END$$;

DO $$
DECLARE
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'token_metrics_1h'
    ) INTO v_exists;
    IF NOT v_exists THEN
        EXECUTE $sql$
        CREATE MATERIALIZED VIEW token_metrics_1h
        WITH (timescaledb.continuous) AS
        SELECT
            time_bucket(INTERVAL '1 hour', time) AS bucket,
            mintaddress,
            SUM(amountsol)                     AS volume_sol,
            COUNT(*)                            AS txns,
            COUNT(DISTINCT walletaddress)       AS makers,
            MAX(pricesol)                       AS high_price,
            MIN(pricesol)                       AS low_price,
            (ARRAY_AGG(pricesol ORDER BY time DESC))[1] AS close_price,
            (ARRAY_AGG(pricesol ORDER BY time ASC))[1]  AS open_price
        FROM wallettransactions
        GROUP BY bucket, mintaddress
        WITH NO DATA;
        $sql$;
    END IF;
END$$;

-- ---------------------------------------------------------------
-- 4. Refresh policies (Timescale refresca en background)
-- ---------------------------------------------------------------
DO $$
BEGIN
    PERFORM add_continuous_aggregate_policy('token_metrics_5m',
        start_offset => INTERVAL '2 hours',
        end_offset   => INTERVAL '1 minute',
        schedule_interval => INTERVAL '30 seconds',
        if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'policy 5m: %', SQLERRM;
END$$;

DO $$
BEGIN
    PERFORM add_continuous_aggregate_policy('token_metrics_1h',
        start_offset => INTERVAL '2 days',
        end_offset   => INTERVAL '5 minutes',
        schedule_interval => INTERVAL '5 minutes',
        if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'policy 1h: %', SQLERRM;
END$$;

-- ---------------------------------------------------------------
-- 5. Helper functions para la API (leen CA + fallback a raw)
-- ---------------------------------------------------------------
CREATE OR REPLACE FUNCTION token_volume_sol(p_mint TEXT, p_window INTERVAL)
RETURNS DOUBLE PRECISION AS $$
    SELECT COALESCE(SUM(amountsol), 0)::double precision
    FROM wallettransactions
    WHERE mintaddress = p_mint
      AND time > NOW() - p_window;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION token_txns_count(p_mint TEXT, p_window INTERVAL)
RETURNS INT AS $$
    SELECT COALESCE(COUNT(*), 0)::int
    FROM wallettransactions
    WHERE mintaddress = p_mint
      AND time > NOW() - p_window;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION token_makers_count(p_mint TEXT, p_window INTERVAL)
RETURNS INT AS $$
    SELECT COALESCE(COUNT(DISTINCT walletaddress), 0)::int
    FROM wallettransactions
    WHERE mintaddress = p_mint
      AND time > NOW() - p_window;
$$ LANGUAGE SQL STABLE;

-- ---------------------------------------------------------------
-- 6. tokenmarketcache: asegurar columnas que escribe market-collector
-- ---------------------------------------------------------------
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS volume5m DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS volume1h DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS volume6h DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS volume24h DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS change5m DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS change1h DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS change6h DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS change24h DOUBLE PRECISION DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS txns24h INT DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS makers24h INT DEFAULT 0;
ALTER TABLE tokenmarketcache ADD COLUMN IF NOT EXISTS tier TEXT DEFAULT 'monitor';

-- ---------------------------------------------------------------
-- 7. Registro
-- ---------------------------------------------------------------
INSERT INTO process_heartbeat (process_name, last_seen_at, metadata)
VALUES ('migration-005-market-aggregates', NOW(),
        jsonb_build_object('status','applied','version','005'))
ON CONFLICT (process_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    metadata     = EXCLUDED.metadata,
    updated_at   = NOW();

-- ============================================================
-- 006_classifier_incremental.sql
-- Asegura columnas que el classifier escribe y las que lee la API.
-- Añade tabla para throttling.
-- Idempotente.
-- ============================================================

-- Columnas adicionales para walletclassifications que lee el apiserver.py
ALTER TABLE walletclassifications ADD COLUMN IF NOT EXISTS behaviortype TEXT;
ALTER TABLE walletclassifications ADD COLUMN IF NOT EXISTS consistencylevel TEXT;
ALTER TABLE walletclassifications ADD COLUMN IF NOT EXISTS profittier TEXT;
ALTER TABLE walletclassifications ADD COLUMN IF NOT EXISTS investorlabel TEXT;

-- Sincroniza behaviortype <-> behavior si alguno falta
UPDATE walletclassifications
   SET behaviortype = behavior
 WHERE behaviortype IS NULL AND behavior IS NOT NULL;

UPDATE walletclassifications
   SET behavior = behaviortype
 WHERE behavior IS NULL AND behaviortype IS NOT NULL;

-- Índices útiles
CREATE INDEX IF NOT EXISTS idx_wc_behaviortype ON walletclassifications(behaviortype);
CREATE INDEX IF NOT EXISTS idx_wc_profittier ON walletclassifications(profittier);
CREATE INDEX IF NOT EXISTS idx_wc_updatedat ON walletclassifications(updatedat DESC);

-- Throttling: última vez que una wallet fue clasificada
CREATE TABLE IF NOT EXISTS classifier_throttle (
    walletaddress  TEXT PRIMARY KEY,
    last_run_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_classifier_throttle_last_run
    ON classifier_throttle (last_run_at DESC);

-- Registro
INSERT INTO process_heartbeat (process_name, last_seen_at, metadata)
VALUES ('migration-006-classifier-incremental', NOW(),
        jsonb_build_object('status','applied','version','006'))
ON CONFLICT (process_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    metadata     = EXCLUDED.metadata,
    updated_at   = NOW();

-- ============================================================
-- 008_cleanup_observability.sql
-- Dead letter, retention, índices de observabilidad.
-- Idempotente.
-- ============================================================

-- Dead letter: agregar 'dead' como status permitido (es TEXT, no CHECK).
-- Si tienes un CHECK constraint en reducer_queue.status, ajústalo aquí.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.check_constraints
        WHERE constraint_name LIKE '%reducer_queue%status%'
    ) THEN
        BEGIN
            ALTER TABLE reducer_queue DROP CONSTRAINT IF EXISTS reducer_queue_status_check;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END IF;
END$$;

-- Índices para observabilidad
CREATE INDEX IF NOT EXISTS idx_reducer_queue_status_type
    ON reducer_queue (status, event_type, created_at);
CREATE INDEX IF NOT EXISTS idx_reducer_queue_attempts
    ON reducer_queue (attempts DESC) WHERE status IN ('pending','processing');

-- Función: mover eventos con attempts > N a 'dead'
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

-- Función: retention de tablas operativas
CREATE OR REPLACE FUNCTION run_retention_cleanup()
RETURNS TABLE(table_name TEXT, rows_deleted BIGINT) AS $$
DECLARE
    n BIGINT;
BEGIN
    DELETE FROM reducer_queue
     WHERE status IN ('done','ignored','dead')
       AND COALESCE(processed_at, updated_at) < NOW() - INTERVAL '3 days';
    GET DIAGNOSTICS n = ROW_COUNT;
    table_name := 'reducer_queue'; rows_deleted := n; RETURN NEXT;

    DELETE FROM chain_events_staging
     WHERE status IN ('parsed','ignored','error')
       AND COALESCE(processed_at, inserted_at) < NOW() - INTERVAL '2 days';
    GET DIAGNOSTICS n = ROW_COUNT;
    table_name := 'chain_events_staging'; rows_deleted := n; RETURN NEXT;

    DELETE FROM datasource_audit
     WHERE created_at < NOW() - INTERVAL '14 days';
    GET DIAGNOSTICS n = ROW_COUNT;
    table_name := 'datasource_audit'; rows_deleted := n; RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Vista consolidada de salud
CREATE OR REPLACE VIEW system_health_view AS
WITH hb AS (
    SELECT process_name,
           EXTRACT(EPOCH FROM (NOW() - last_seen_at))::int AS seconds_since,
           metadata
    FROM process_heartbeat
    WHERE process_name IN (
        'chain-listener','tx-parser','position-reducer','wallet-reducer',
        'token-trader-reducer','market-collector','price-updater',
        'classifier-worker'
    )
),
staging AS (
    SELECT status, COUNT(*) AS n
    FROM chain_events_staging
    GROUP BY status
),
queue AS (
    SELECT event_type, status, COUNT(*) AS n
    FROM reducer_queue
    GROUP BY event_type, status
)
SELECT
    (SELECT jsonb_object_agg(process_name, jsonb_build_object(
        'seconds_since', seconds_since,
        'healthy', seconds_since < 60,
        'metadata', metadata))
     FROM hb) AS workers,
    (SELECT jsonb_object_agg(status, n) FROM staging) AS staging,
    (SELECT jsonb_object_agg(event_type || '/' || status, n)
     FROM queue) AS queue;

-- Registro
INSERT INTO process_heartbeat (process_name, last_seen_at, metadata)
VALUES ('migration-008-cleanup-observability', NOW(),
        jsonb_build_object('status','applied','version','008'))
ON CONFLICT (process_name) DO UPDATE SET
    last_seen_at = EXCLUDED.last_seen_at,
    metadata     = EXCLUDED.metadata,
    updated_at   = NOW();

