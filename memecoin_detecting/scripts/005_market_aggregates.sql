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