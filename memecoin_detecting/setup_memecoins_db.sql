-- ============================================================================
-- Script de Automatización: memecoins_db
-- PostgreSQL 14 + TimescaleDB 2.19.3
-- ============================================================================
-- PREREQUISITOS:
--   - PostgreSQL 14 instalado y corriendo
--   - TimescaleDB 2.19.3 extensión disponible
--   - Base de datos 'memecoins_db' creada
--
-- USO:
--   sudo -u postgres psql -d memecoins_db -f setup_memecoins_db.sql
-- ============================================================================

\echo '🚀 Iniciando creación de schema memecoins_db...'
\echo ''

-- Habilitar extensión TimescaleDB
\echo '📦 Habilitando extensión TimescaleDB...'
CREATE EXTENSION IF NOT EXISTS timescaledb;

\echo '✓ TimescaleDB habilitado'
\echo ''

-- ============================================================================
-- SECCIÓN 1: TABLAS MAESTRAS
-- ============================================================================

\echo '📋 Creando tablas maestras...'

-- 1.1 tokens
\echo '  → Creando tabla tokens...'
CREATE TABLE IF NOT EXISTS tokens (
    token_id        SERIAL PRIMARY KEY,
    mint_address    VARCHAR NOT NULL UNIQUE,
    name            VARCHAR,
    symbol          VARCHAR,
    total_supply    BIGINT,
    decimals        INTEGER,
    uri             TEXT,
    image_url       TEXT,
    amm             VARCHAR NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    pool_address    VARCHAR,
    status          VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_tokens_amm     ON tokens (amm);
CREATE INDEX IF NOT EXISTS idx_tokens_created ON tokens (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tokens_mint    ON tokens (mint_address);
CREATE INDEX IF NOT EXISTS idx_tokens_pool    ON tokens (pool_address);
CREATE INDEX IF NOT EXISTS idx_tokens_status  ON tokens (status);

\echo '  ✓ tokens creado'

-- 1.2 wallets
\echo '  → Creando tabla wallets...'
CREATE TABLE IF NOT EXISTS wallets (
    wallet_id            SERIAL PRIMARY KEY,
    wallet_address       VARCHAR(44) NOT NULL UNIQUE,
    total_trades         INTEGER DEFAULT 0,
    total_profit_loss    NUMERIC(30,8) DEFAULT 0,
    total_invested       NUMERIC(30,8) DEFAULT 0,
    total_realized       NUMERIC(30,8) DEFAULT 0,
    win_rate             NUMERIC(5,2) DEFAULT 0,
    avg_profit_per_trade NUMERIC(30,8) DEFAULT 0,
    best_trade           NUMERIC(30,8) DEFAULT 0,
    worst_trade          NUMERIC(30,8) DEFAULT 0,
    first_seen           TIMESTAMP DEFAULT NOW(),
    last_seen            TIMESTAMP DEFAULT NOW(),
    is_active            BOOLEAN DEFAULT TRUE,
    tags                 TEXT[],
    notes                TEXT
);

CREATE INDEX IF NOT EXISTS idx_wallets_address     ON wallets (wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen   ON wallets (last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_profit_loss ON wallets (total_profit_loss DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_win_rate    ON wallets (win_rate DESC);

\echo '  ✓ wallets creado'

-- 1.3 tracked_wallets
\echo '  → Creando tabla tracked_wallets...'
CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet_address VARCHAR PRIMARY KEY,
    is_active      BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_tracked_wallets_active
    ON tracked_wallets (is_active) WHERE (is_active = true);

\echo '  ✓ tracked_wallets creado'

-- 1.4 transactions
\echo '  → Creando tabla transactions...'
CREATE TABLE IF NOT EXISTS transactions (
    tx_id       SERIAL PRIMARY KEY,
    signature   VARCHAR NOT NULL UNIQUE,
    token_id    INTEGER NOT NULL REFERENCES tokens(token_id),
    block_time  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tx_signature ON transactions (signature);
CREATE INDEX IF NOT EXISTS idx_tx_token     ON transactions (token_id, block_time DESC);

\echo '  ✓ transactions creado'

-- 1.5 wallet_positions
\echo '  → Creando tabla wallet_positions...'
CREATE TABLE IF NOT EXISTS wallet_positions (
    position_id               SERIAL PRIMARY KEY,
    wallet_id                 INTEGER NOT NULL REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    token_id                  INTEGER NOT NULL REFERENCES tokens(token_id),
    current_balance           NUMERIC(30,8) DEFAULT 0,
    avg_buy_price             NUMERIC(30,8),
    total_cost                NUMERIC(30,8) DEFAULT 0,
    total_sold                NUMERIC(30,8) DEFAULT 0,
    realized_pnl              NUMERIC(30,8) DEFAULT 0,
    unrealized_pnl            NUMERIC(30,8) DEFAULT 0,
    status                    VARCHAR DEFAULT 'open',
    first_buy                 TIMESTAMP,
    last_buy                  TIMESTAMP,
    last_sell                 TIMESTAMP,
    closed_at                 TIMESTAMP,
    origin                    VARCHAR DEFAULT 'tracked',
    unrealized_roi_percentage NUMERIC(10,2),
    UNIQUE (wallet_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_status                ON wallet_positions (status);
CREATE INDEX IF NOT EXISTS idx_positions_token                 ON wallet_positions (token_id);
CREATE INDEX IF NOT EXISTS idx_positions_wallet                ON wallet_positions (wallet_id);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_wallet_token   ON wallet_positions (wallet_id, token_id);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_unrealized_pnl ON wallet_positions (unrealized_pnl DESC);
CREATE INDEX IF NOT EXISTS idx_walletpositions_origin          ON wallet_positions (origin);
CREATE INDEX IF NOT EXISTS idx_walletpositions_origin_pnl      ON wallet_positions (wallet_id, origin, realized_pnl)
    WHERE (origin = 'tracked');

\echo '  ✓ wallet_positions creado'
\echo ''

-- ============================================================================
-- SECCIÓN 2: HYPERTABLES (Series de tiempo)
-- ============================================================================

\echo '⏰ Creando hypertables...'

-- 2.1 token_metrics
\echo '  → Creando tabla token_metrics...'
CREATE TABLE IF NOT EXISTS token_metrics (
    time               TIMESTAMPTZ NOT NULL,
    token_id           INTEGER     NOT NULL REFERENCES tokens(token_id),
    price              NUMERIC,
    volume_24h         NUMERIC,
    liquidity          NUMERIC,
    market_cap         NUMERIC,
    fdv                NUMERIC,
    holders_count      INTEGER,
    buyers_count       INTEGER,
    sellers_count      INTEGER,
    transactions_count INTEGER,
    data_source        VARCHAR DEFAULT 'local_node',
    volume_10m         NUMERIC DEFAULT 0,
    volume_1h          NUMERIC DEFAULT 0,
    pool_address       VARCHAR,
    transaction_count  INTEGER DEFAULT 0,
    swap_count         INTEGER DEFAULT 0
);

-- Verificar si ya es hypertable antes de convertir
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables 
        WHERE hypertable_name = 'token_metrics'
    ) THEN
        PERFORM create_hypertable('token_metrics', 'time',
            chunk_time_interval => INTERVAL '7 days');
        RAISE NOTICE 'token_metrics convertido a hypertable';
    ELSE
        RAISE NOTICE 'token_metrics ya es hypertable, saltando...';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_token_metrics_time_token ON token_metrics (time, token_id);
CREATE INDEX IF NOT EXISTS idx_metrics_token_time       ON token_metrics (token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_token_metrics_time       ON token_metrics (time DESC);
CREATE INDEX IF NOT EXISTS idx_token_metrics_token_id   ON token_metrics (token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_pool             ON token_metrics (pool_address);

\echo '  ✓ token_metrics (hypertable) creado'

-- 2.2 wallet_transactions
\echo '  → Creando tabla wallet_transactions...'
CREATE TABLE IF NOT EXISTS wallet_transactions (
    transaction_id INTEGER     NOT NULL,
    time           TIMESTAMPTZ NOT NULL,
    wallet_id      INTEGER     NOT NULL REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    token_id       INTEGER     NOT NULL REFERENCES tokens(token_id),
    signature      VARCHAR,
    tx_type        VARCHAR,
    amount         NUMERIC(30,8),
    price          NUMERIC(30,8),
    sol_amount     NUMERIC(30,8),
    side           VARCHAR,
    order_id       VARCHAR,
    PRIMARY KEY (time, transaction_id)
);

-- Verificar si ya es hypertable antes de convertir
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables 
        WHERE hypertable_name = 'wallet_transactions'
    ) THEN
        PERFORM create_hypertable('wallet_transactions', 'time',
            chunk_time_interval => INTERVAL '1 day');
        RAISE NOTICE 'wallet_transactions convertido a hypertable';
    ELSE
        RAISE NOTICE 'wallet_transactions ya es hypertable, saltando...';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_wallet_tx_signature_unique ON wallet_transactions (signature, time);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_time_wallet      ON wallet_transactions (time DESC, wallet_id);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_token            ON wallet_transactions (token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_token_time       ON wallet_transactions (token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_type             ON wallet_transactions (tx_type);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_wallet           ON wallet_transactions (wallet_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_order_id         ON wallet_transactions (order_id) WHERE (order_id IS NOT NULL);

\echo '  ✓ wallet_transactions (hypertable) creado'
\echo ''

-- ============================================================================
-- SECCIÓN 3: COMPRESIÓN
-- ============================================================================

\echo '🗜️  Configurando compresión...'

-- Configurar compresión en token_metrics
DO $$
BEGIN
    -- Verificar si ya tiene compresión habilitada
    IF NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables 
        WHERE hypertable_name = 'token_metrics' AND compression_enabled = true
    ) THEN
        ALTER TABLE token_metrics SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'token_id',
            timescaledb.compress_orderby   = 'time DESC'
        );
        RAISE NOTICE 'Compresión habilitada en token_metrics';
    ELSE
        RAISE NOTICE 'token_metrics ya tiene compresión, saltando...';
    END IF;
END $$;

-- Agregar política de compresión (comprimir chunks > 2 días)
SELECT add_compression_policy('token_metrics', INTERVAL '2 days',
    if_not_exists => true,
    schedule_interval => INTERVAL '12 hours');

\echo '  ✓ Compresión configurada en token_metrics'
\echo ''

-- ============================================================================
-- SECCIÓN 4: POLÍTICAS DE RETENCIÓN
-- ============================================================================

\echo '🗑️  Configurando políticas de retención...'

-- Retención token_metrics (14 días)
SELECT add_retention_policy('token_metrics', INTERVAL '14 days',
    if_not_exists => true);

\echo '  ✓ Retención configurada: token_metrics (14 días)'

-- Retención wallet_transactions (30 días)
SELECT add_retention_policy('wallet_transactions', INTERVAL '30 days',
    if_not_exists => true);

\echo '  ✓ Retención configurada: wallet_transactions (30 días)'
\echo ''

-- ============================================================================
-- SECCIÓN 5: CONTINUOUS AGGREGATES
-- ============================================================================

\echo '📊 Creando continuous aggregates...'

-- 5.1 token_hourly_stats
\echo '  → Creando token_hourly_stats...'
CREATE MATERIALIZED VIEW IF NOT EXISTS token_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    token_id,
    time_bucket('1 hour', time)           AS hour,
    first(price, time)                    AS open_price,
    last(price, time)                     AS close_price,
    max(price)                            AS high_price,
    min(price)                            AS low_price,
    avg(volume_24h)                       AS avg_volume,
    max(holders_count)                    AS max_holders
FROM token_metrics
GROUP BY token_id, time_bucket('1 hour', time)
WITH NO DATA;

-- Agregar política de refresh
SELECT add_continuous_aggregate_policy('token_hourly_stats',
    start_offset  => INTERVAL '1 day',
    end_offset    => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => true);

\echo '  ✓ token_hourly_stats creado'

-- 5.2 token_volume_hourly
\echo '  → Creando token_volume_hourly...'
CREATE MATERIALIZED VIEW IF NOT EXISTS token_volume_hourly
WITH (timescaledb.continuous) AS
SELECT
    token_id,
    time_bucket('1 hour', time)           AS hour,
    avg(price)                            AS avg_price,
    sum(volume_10m)                       AS total_volume_10m,
    max(market_cap)                       AS max_market_cap
FROM token_metrics
GROUP BY token_id, time_bucket('1 hour', time)
WITH NO DATA;

-- Agregar política de refresh
SELECT add_continuous_aggregate_policy('token_volume_hourly',
    start_offset  => INTERVAL '1 day',
    end_offset    => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => true);

\echo '  ✓ token_volume_hourly creado'
\echo ''

-- ============================================================================
-- SECCIÓN 6: VISTAS
-- ============================================================================

\echo '👁️  Creando vistas...'

-- Vista active_positions
\echo '  → Creando active_positions...'
CREATE OR REPLACE VIEW active_positions AS
SELECT
    wp.position_id,
    w.wallet_address,
    t.mint_address,
    t.name           AS token_name,
    t.symbol         AS token_symbol,
    wp.current_balance,
    wp.avg_buy_price,
    wp.total_cost,
    wp.unrealized_pnl,
    wp.first_buy,
    wp.last_buy,
    tm.price         AS current_price,
    wp.unrealized_roi_percentage
FROM wallet_positions wp
JOIN wallets w  ON w.wallet_id  = wp.wallet_id
JOIN tokens  t  ON t.token_id   = wp.token_id
LEFT JOIN LATERAL (
    SELECT price FROM token_metrics
    WHERE token_id = wp.token_id
    ORDER BY time DESC LIMIT 1
) tm ON true
WHERE wp.status = 'open';

\echo '  ✓ active_positions creado'
\echo ''

-- ============================================================================
-- RESUMEN FINAL
-- ============================================================================

\echo '✅ Setup completado exitosamente!'
\echo ''
\echo '📊 Resumen de objetos creados:'
\echo ''

SELECT 
    'Tablas base' AS tipo,
    COUNT(*) AS cantidad
FROM information_schema.tables
WHERE table_schema = 'public' 
  AND table_type = 'BASE TABLE'
  AND table_name NOT LIKE '_timescaledb%'
UNION ALL
SELECT 
    'Hypertables',
    COUNT(*)
FROM timescaledb_information.hypertables
UNION ALL
SELECT 
    'Continuous Aggregates',
    COUNT(*)
FROM timescaledb_information.continuous_aggregates
UNION ALL
SELECT 
    'Vistas',
    COUNT(*)
FROM information_schema.views
WHERE table_schema = 'public'
  AND table_name NOT LIKE '_timescaledb%';

\echo ''
\echo '🔧 Políticas activas:'
\echo ''

SELECT 
    proc_name AS politica,
    hypertable_name AS tabla,
    schedule_interval AS intervalo
FROM timescaledb_information.jobs
WHERE hypertable_name IS NOT NULL
ORDER BY hypertable_name, proc_name;

\echo ''
\echo '🎯 Base de datos lista para usar!'
\echo ''
