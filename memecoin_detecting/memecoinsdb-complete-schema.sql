-- ============================================================
-- MEMECOINSDB - Schema Completo de Recreación
-- Sistema de Detección y Análisis de Memecoins en Solana
-- Fecha: 2026-02-20
-- Motor: PostgreSQL + TimescaleDB
-- ============================================================

-- ============================================================
-- PASO 0: Extensión TimescaleDB
-- ============================================================
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================
-- TABLA 1: tokens
-- Registro maestro de todos los tokens detectados
-- Alimentada por: detector_memecoins.py (~32K tokens/día)
-- Retención: permanente
-- ============================================================
CREATE TABLE IF NOT EXISTS tokens (
    token_id SERIAL PRIMARY KEY,
    mint_address VARCHAR(44) UNIQUE NOT NULL,

    -- Datos estáticos del token
    name VARCHAR(255),
    symbol VARCHAR(20),
    total_supply BIGINT,
    decimals INT,
    uri TEXT,
    image_url TEXT,

    -- Metadata de detección
    amm VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    detected_at TIMESTAMP DEFAULT NOW(),

    -- Transacción de creación
    creation_signature VARCHAR(88),
    creation_instruction VARCHAR(100),

    -- Estado del token
    status VARCHAR(20) DEFAULT 'active',
    retention_category VARCHAR(20) DEFAULT 'shortterm',

    -- Pool cacheado (optimización: evita 3-7 llamadas RPC por token)
    pool_address VARCHAR(44),

    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('active', 'dead', 'interesting')),
    CONSTRAINT valid_retention CHECK (retention_category IN ('shortterm', 'longterm'))
);

CREATE INDEX IF NOT EXISTS idx_tokens_mint ON tokens(mint_address);
CREATE INDEX IF NOT EXISTS idx_tokens_amm ON tokens(amm);
CREATE INDEX IF NOT EXISTS idx_tokens_created ON tokens(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);
CREATE INDEX IF NOT EXISTS idx_tokens_pool ON tokens(pool_address) WHERE pool_address IS NOT NULL;

-- ============================================================
-- TABLA 2: token_metrics (TimescaleDB Hypertable)
-- Serie temporal de precios, liquidez, volumen y holders
-- Alimentada por: metrics_collector.py (cada ~60s)
-- Retención: 3 días (auto-borrado)
-- Compresión: después de 2 días
-- ============================================================
CREATE TABLE IF NOT EXISTS token_metrics (
    time TIMESTAMPTZ NOT NULL,
    token_id INTEGER NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,

    -- Precio y liquidez
    price NUMERIC(30, 18),
    liquidity NUMERIC(30, 8),

    -- Volumen por ventana de tiempo
    volume_10m NUMERIC(30, 8) DEFAULT 0,
    volume_1m NUMERIC(30, 8) DEFAULT 0,
    volume_5m NUMERIC(30, 8) DEFAULT 0,
    volume_1h NUMERIC(30, 8) DEFAULT 0,
    volume_24h NUMERIC(30, 8) DEFAULT 0,

    -- Market Cap
    market_cap NUMERIC(30, 8),
    fdv NUMERIC(30, 8),

    -- Holders y transacciones
    holders_count INTEGER DEFAULT 0,
    buyers_count INTEGER,
    sellers_count INTEGER,
    transactions_count INTEGER DEFAULT 0,

    -- Metadata
    data_source VARCHAR(20) DEFAULT 'localnode',
    pool_address VARCHAR(44),

    CONSTRAINT token_metrics_pkey PRIMARY KEY (time, token_id)
);

-- Convertir a hypertable
SELECT create_hypertable('token_metrics', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

-- Índices
CREATE INDEX IF NOT EXISTS idx_metrics_token_time ON token_metrics(token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_token_metrics_time ON token_metrics(time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_pool ON token_metrics(pool_address);
CREATE INDEX IF NOT EXISTS idx_metrics_volume_time ON token_metrics(token_id, time DESC, volume_10m) WHERE volume_10m > 0;

-- Política de retención: borrado automático después de 3 días
SELECT add_retention_policy('token_metrics', INTERVAL '3 days', if_not_exists => TRUE);

-- Política de compresión: después de 2 días
ALTER TABLE token_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'token_id',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('token_metrics', INTERVAL '2 days', if_not_exists => TRUE);

-- ============================================================
-- TABLA 3: transactions
-- Transacciones detectadas por el detector de tokens
-- Alimentada por: detector_memecoins.py
-- Retención: permanente
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
    tx_id SERIAL PRIMARY KEY,
    token_id INTEGER NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,

    -- Datos de transacción
    signature VARCHAR(88) UNIQUE NOT NULL,
    blocktime TIMESTAMP NOT NULL,
    slot BIGINT NOT NULL,

    -- Tipo
    tx_type VARCHAR(20),

    -- Montos
    amount DECIMAL(30, 18),
    amount_usd DECIMAL(20, 8),

    -- Wallets involucradas
    from_wallet VARCHAR(44),
    to_wallet VARCHAR(44),

    -- Timestamp
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tx_token ON transactions(token_id, blocktime DESC);
CREATE INDEX IF NOT EXISTS idx_tx_signature ON transactions(signature);

-- ============================================================
-- TABLA 4: wallets
-- Perfil y estadísticas acumuladas de cada wallet
-- Alimentada por: enhanced_wallet_tracker.py vía process_transaction()
-- Retención: permanente
-- ============================================================
CREATE TABLE IF NOT EXISTS wallets (
    wallet_id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) UNIQUE NOT NULL,

    -- Estadísticas acumuladas
    total_trades INTEGER DEFAULT 0,
    total_profit_loss NUMERIC(30, 8) DEFAULT 0,
    total_invested NUMERIC(30, 8) DEFAULT 0,
    total_realized NUMERIC(30, 8) DEFAULT 0,

    -- Métricas de desempeño
    win_rate NUMERIC(5, 2) DEFAULT 0,           -- % de trades ganadores
    avg_profit_per_trade NUMERIC(30, 8) DEFAULT 0,
    best_trade NUMERIC(30, 8) DEFAULT 0,
    worst_trade NUMERIC(30, 8) DEFAULT 0,

    -- Control
    first_seen TIMESTAMP DEFAULT NOW(),
    last_seen TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,

    -- Etiquetas para categorizar
    tags TEXT,                                   -- Ej: 'whale', 'insider', 'bot'
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallets_profit_loss ON wallets(total_profit_loss DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_winrate ON wallets(win_rate DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_lastseen ON wallets(last_seen DESC);

-- ============================================================
-- TABLA 5: wallet_positions
-- Posición actual de cada wallet en cada token (P&L tracking)
-- Alimentada por: función SQL process_transaction()
-- Retención: permanente
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_positions (
    position_id SERIAL PRIMARY KEY,
    wallet_id INTEGER NOT NULL REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    token_id INTEGER NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,

    -- Cantidades
    total_bought NUMERIC(30, 8) DEFAULT 0,
    total_sold NUMERIC(30, 8) DEFAULT 0,
    current_balance NUMERIC(30, 8) DEFAULT 0,

    -- Costos
    total_cost NUMERIC(30, 8) DEFAULT 0,        -- Costo total en SOL
    avg_buy_price NUMERIC(30, 18) DEFAULT 0,

    -- Ingresos
    total_revenue NUMERIC(30, 8) DEFAULT 0,
    avg_sell_price NUMERIC(30, 18) DEFAULT 0,

    -- Ganancias/Pérdidas
    realized_pnl NUMERIC(30, 8) DEFAULT 0,      -- P&L realizado (ya vendido)
    unrealized_pnl NUMERIC(30, 8) DEFAULT 0,    -- P&L no realizado (aún en posesión)

    -- Control
    first_buy TIMESTAMP,
    last_buy TIMESTAMP,
    last_sell TIMESTAMP,
    status VARCHAR(20) DEFAULT 'open',           -- open, closed, partial

    CONSTRAINT wallet_positions_unique UNIQUE (wallet_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_wallet_positions_wallet ON wallet_positions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_token ON wallet_positions(token_id);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_status ON wallet_positions(status);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_unrealized_pnl ON wallet_positions(unrealized_pnl DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_positions_wallet_token ON wallet_positions(wallet_id, token_id);

-- ============================================================
-- TABLA 6: wallet_transactions (TimescaleDB Hypertable)
-- Historial completo de trades individuales
-- Alimentada por: enhanced_wallet_tracker.py vía process_transaction()
-- Retención: 30 días (auto-borrado)
-- ============================================================
CREATE TABLE IF NOT EXISTS wallet_transactions (
    time TIMESTAMPTZ NOT NULL,
    transaction_id SERIAL,
    wallet_id INTEGER NOT NULL REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    token_id INTEGER NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,

    -- Información de la transacción
    signature VARCHAR(88) NOT NULL,
    tx_type VARCHAR(10) NOT NULL,                -- 'buy' o 'sell'

    -- Cantidades
    token_amount NUMERIC(30, 8) NOT NULL,
    sol_amount NUMERIC(30, 8) NOT NULL,
    price NUMERIC(30, 18) NOT NULL,

    -- Fees
    fee NUMERIC(30, 8) DEFAULT 0,

    -- Órdenes parciales
    is_partial BOOLEAN DEFAULT FALSE,
    order_id VARCHAR(88),
    partial_fill_index INTEGER DEFAULT 1,

    -- Pool y programa
    pool_address VARCHAR(44),
    program_id VARCHAR(44),

    -- Metadatos
    instruction_index INTEGER,
    block_slot BIGINT,

    CONSTRAINT wallet_transactions_pkey PRIMARY KEY (time, transaction_id)
);

-- Convertir a hypertable
SELECT create_hypertable('wallet_transactions', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

-- Política de retención: 30 días
SELECT add_retention_policy('wallet_transactions', INTERVAL '30 days', if_not_exists => TRUE);

-- Índices
CREATE INDEX IF NOT EXISTS idx_wallet_tx_wallet ON wallet_transactions(wallet_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_token ON wallet_transactions(token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_signature ON wallet_transactions(signature);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_type ON wallet_transactions(tx_type);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_order_id ON wallet_transactions(order_id) WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wallet_tx_time_wallet ON wallet_transactions(time DESC, wallet_id);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_token_time ON wallet_transactions(token_id, time DESC);

-- ============================================================
-- TABLA 7: tracked_wallets
-- Lista de wallets que se rastrean manualmente (VIP)
-- Alimentada por: inserción manual o add_wallet_to_track()
-- Retención: permanente
-- ============================================================
CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet_address VARCHAR(44) PRIMARY KEY,
    label VARCHAR(100),                          -- Nombre/etiqueta del wallet
    reason TEXT,                                  -- Por qué lo estamos rastreando
    added_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_tracked_wallets_active ON tracked_wallets(is_active) WHERE is_active = TRUE;

-- ============================================================
-- FUNCIÓN: update_wallet_stats()
-- Recalcula estadísticas acumuladas del wallet
-- ============================================================
CREATE OR REPLACE FUNCTION update_wallet_stats(p_wallet_id INTEGER)
RETURNS VOID AS $$
BEGIN
    UPDATE wallets SET
        total_trades = (SELECT COUNT(*) FROM wallet_transactions WHERE wallet_id = p_wallet_id),
        total_profit_loss = (SELECT COALESCE(SUM(realized_pnl), 0) FROM wallet_positions WHERE wallet_id = p_wallet_id),
        win_rate = (
            SELECT CASE WHEN COUNT(*) > 0
                THEN ROUND(COUNT(*) FILTER (WHERE realized_pnl > 0)::numeric / COUNT(*)::numeric * 100, 2)
                ELSE 0 END
            FROM wallet_positions WHERE wallet_id = p_wallet_id AND status = 'closed'
        ),
        avg_profit_per_trade = (
            SELECT COALESCE(AVG(realized_pnl), 0)
            FROM wallet_positions WHERE wallet_id = p_wallet_id AND status = 'closed'
        ),
        last_seen = NOW()
    WHERE wallet_id = p_wallet_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FUNCIÓN: process_transaction()
-- Procesa un trade completo: crea wallet, inserta tx, actualiza
-- posición y calcula P&L en una sola llamada
-- ============================================================
CREATE OR REPLACE FUNCTION process_transaction(
    p_wallet_address VARCHAR(44),
    p_mint_address VARCHAR(44),
    p_signature VARCHAR(88),
    p_tx_type VARCHAR(10),
    p_token_amount NUMERIC(30, 8),
    p_sol_amount NUMERIC(30, 8),
    p_price NUMERIC(30, 18),
    p_time TIMESTAMPTZ,
    p_fee NUMERIC(30, 8) DEFAULT 0,
    p_is_partial BOOLEAN DEFAULT FALSE,
    p_order_id VARCHAR(88) DEFAULT NULL
)
RETURNS VOID AS $$
DECLARE
    v_wallet_id INTEGER;
    v_token_id INTEGER;
    v_position_id INTEGER;
    v_current_balance NUMERIC(30, 8);
    v_pnl NUMERIC(30, 8);
BEGIN
    -- Obtener o crear wallet
    INSERT INTO wallets (wallet_address)
    VALUES (p_wallet_address)
    ON CONFLICT (wallet_address) DO UPDATE SET last_seen = NOW()
    RETURNING wallet_id INTO v_wallet_id;

    -- Obtener token_id
    SELECT token_id INTO v_token_id FROM tokens WHERE mint_address = p_mint_address;
    IF v_token_id IS NULL THEN
        RAISE EXCEPTION 'Token no encontrado: %', p_mint_address;
    END IF;

    -- Insertar transacción
    INSERT INTO wallet_transactions (
        time, wallet_id, token_id, signature, tx_type,
        token_amount, sol_amount, price, fee, is_partial, order_id
    ) VALUES (
        p_time, v_wallet_id, v_token_id, p_signature, p_tx_type,
        p_token_amount, p_sol_amount, p_price, p_fee, p_is_partial, p_order_id
    );

    -- Obtener o crear posición
    INSERT INTO wallet_positions (wallet_id, token_id, status)
    VALUES (v_wallet_id, v_token_id, 'open')
    ON CONFLICT (wallet_id, token_id) DO NOTHING;

    SELECT position_id, current_balance INTO v_position_id, v_current_balance
    FROM wallet_positions
    WHERE wallet_id = v_wallet_id AND token_id = v_token_id;

    -- Actualizar posición según tipo de transacción
    IF p_tx_type = 'buy' THEN
        UPDATE wallet_positions SET
            total_bought = total_bought + p_token_amount,
            current_balance = current_balance + p_token_amount,
            total_cost = total_cost + p_sol_amount + p_fee,
            avg_buy_price = (total_cost + p_sol_amount + p_fee) / (total_bought + p_token_amount),
            last_buy = p_time,
            first_buy = COALESCE(first_buy, p_time)
        WHERE position_id = v_position_id;

    ELSIF p_tx_type = 'sell' THEN
        -- Calcular P&L de esta venta
        SELECT avg_buy_price INTO v_pnl FROM wallet_positions WHERE position_id = v_position_id;
        v_pnl := (p_price - v_pnl) * p_token_amount;

        UPDATE wallet_positions SET
            total_sold = total_sold + p_token_amount,
            current_balance = current_balance - p_token_amount,
            total_revenue = total_revenue + p_sol_amount - p_fee,
            avg_sell_price = (total_revenue + p_sol_amount - p_fee) / (total_sold + p_token_amount),
            realized_pnl = realized_pnl + v_pnl,
            last_sell = p_time,
            status = CASE
                WHEN current_balance - p_token_amount <= 0 THEN 'closed'
                ELSE 'partial'
            END
        WHERE position_id = v_position_id;
    END IF;

    -- Actualizar estadísticas del wallet
    PERFORM update_wallet_stats(v_wallet_id);
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- VISTA MATERIALIZADA: token_hourly_stats
-- OHLC de precios por hora (continuous aggregate)
-- Refresco automático: cada 1 hora
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS token_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    token_id,
    time_bucket('1 hour', time) AS hour,
    FIRST(price, time) as open_price,
    LAST(price, time) as close_price,
    MAX(price) as high_price,
    MIN(price) as low_price,
    AVG(volume_24h) as avg_volume,
    MAX(holders_count) as max_holders
FROM token_metrics
GROUP BY token_id, hour
WITH NO DATA;

SELECT add_continuous_aggregate_policy('token_hourly_stats',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ============================================================
-- VISTA MATERIALIZADA: token_volume_hourly
-- Volumen agregado por hora (continuous aggregate)
-- Refresco automático: cada 1 hora
-- ============================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS token_volume_hourly
WITH (timescaledb.continuous) AS
SELECT
    token_id,
    time_bucket('1 hour', time) AS hour,
    AVG(price) as avg_price,
    SUM(volume_10m) as total_volume_10m,
    MAX(market_cap) as max_market_cap,
    MAX(transactions_count) as total_transactions
FROM token_metrics
GROUP BY token_id, hour
WITH NO DATA;

SELECT add_continuous_aggregate_policy('token_volume_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ============================================================
-- VISTA: active_positions
-- Posiciones abiertas con precio actual
-- ============================================================
CREATE OR REPLACE VIEW active_positions AS
SELECT
    wp.position_id,
    w.wallet_address,
    t.mint_address,
    t.name AS token_name,
    t.symbol AS token_symbol,
    wp.current_balance,
    wp.avg_buy_price,
    wp.total_cost,
    wp.unrealized_pnl,
    wp.first_buy,
    wp.last_buy,
    (SELECT price FROM token_metrics tm
     WHERE tm.token_id = wp.token_id
     ORDER BY time DESC LIMIT 1) AS current_price,
    CASE WHEN wp.total_cost > 0
        THEN ROUND(wp.unrealized_pnl / wp.total_cost * 100::numeric, 2)
        ELSE 0
    END AS unrealized_roi_percentage
FROM wallet_positions wp
JOIN wallets w ON wp.wallet_id = w.wallet_id
JOIN tokens t ON wp.token_id = t.token_id
WHERE wp.status != 'closed' AND wp.current_balance > 0
ORDER BY wp.unrealized_pnl DESC;

-- ============================================================
-- VISTA: wallet_performance_summary
-- Resumen de rendimiento por wallet
-- ============================================================
CREATE OR REPLACE VIEW wallet_performance_summary AS
SELECT
    w.wallet_id,
    w.wallet_address,
    w.total_trades,
    w.total_profit_loss,
    w.total_invested,
    w.win_rate,
    w.avg_profit_per_trade,
    CASE WHEN w.total_invested > 0
        THEN ROUND(w.total_profit_loss / w.total_invested * 100::numeric, 2)
        ELSE 0
    END AS roi_percentage,
    COUNT(DISTINCT wp.position_id) FILTER (WHERE wp.status != 'closed') AS open_positions,
    SUM(wp.unrealized_pnl) AS total_unrealized_pnl,
    w.last_seen,
    DATE_PART('day', NOW() - w.last_seen) AS days_since_last_trade,
    w.tags
FROM wallets w
LEFT JOIN wallet_positions wp ON w.wallet_id = wp.wallet_id
GROUP BY w.wallet_id, w.wallet_address, w.total_trades,
         w.total_profit_loss, w.total_invested, w.win_rate,
         w.avg_profit_per_trade, w.last_seen, w.tags;

-- ============================================================
-- VISTA: top_traders
-- Top 100 traders por P&L (mínimo 5 trades)
-- ============================================================
CREATE OR REPLACE VIEW top_traders AS
SELECT
    w.wallet_address,
    w.total_profit_loss,
    w.win_rate,
    w.total_trades,
    ROUND(w.total_profit_loss / NULLIF(w.total_invested, 0) * 100::numeric, 2) AS roi_percentage,
    w.tags,
    w.last_seen
FROM wallets w
WHERE w.total_trades >= 5
ORDER BY w.total_profit_loss DESC
LIMIT 100;

-- ============================================================
-- COMENTARIOS
-- ============================================================
COMMENT ON TABLE tokens IS 'Registro maestro de todos los tokens detectados en Solana';
COMMENT ON TABLE token_metrics IS 'Serie temporal de precios, liquidez y volumen (hypertable, retención 3 días)';
COMMENT ON TABLE transactions IS 'Transacciones detectadas por el detector de tokens';
COMMENT ON TABLE wallets IS 'Perfil y estadísticas acumuladas de cada wallet rastreado';
COMMENT ON TABLE wallet_positions IS 'Posición actual de cada wallet en cada token con P&L';
COMMENT ON TABLE wallet_transactions IS 'Historial completo de trades individuales (hypertable, retención 30 días)';
COMMENT ON TABLE tracked_wallets IS 'Wallets específicos que queremos monitorear manualmente (VIP)';
COMMENT ON COLUMN wallet_transactions.is_partial IS 'TRUE si esta transacción es parte de una orden parcial';
COMMENT ON COLUMN wallet_transactions.order_id IS 'ID para agrupar transacciones de la misma orden parcial';
COMMENT ON COLUMN wallet_transactions.partial_fill_index IS 'Número secuencial dentro de la orden (1, 2, 3...)';
COMMENT ON COLUMN tokens.pool_address IS 'Pool AMM cacheado para evitar 3-7 llamadas RPC por consulta de precio';

-- ============================================================
-- FIN DEL SCHEMA
-- ============================================================
-- Para ejecutar:
--   sudo -u postgres createdb memecoinsdb
--   sudo -u postgres psql memecoinsdb -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
--   sudo -u postgres psql memecoinsdb -f memecoinsdb-complete-schema.sql
-- ============================================================
