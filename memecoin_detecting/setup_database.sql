-- ========================================
-- TABLA DE TOKENS (PostgreSQL normal)
-- ========================================

CREATE TABLE IF NOT EXISTS tokens (
    -- Identificadores
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
    retention_category VARCHAR(20) DEFAULT 'short_term',
    
    -- Constraints
    CONSTRAINT valid_status CHECK (status IN ('active', 'dead', 'interesting')),
    CONSTRAINT valid_retention CHECK (retention_category IN ('short_term', 'long_term'))
);

-- Índices para búsquedas rápidas
CREATE INDEX IF NOT EXISTS idx_tokens_mint ON tokens(mint_address);
CREATE INDEX IF NOT EXISTS idx_tokens_amm ON tokens(amm);
CREATE INDEX IF NOT EXISTS idx_tokens_created ON tokens(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);

-- ========================================
-- TABLA DE MÉTRICAS (TimescaleDB Hypertable)
-- ========================================

CREATE TABLE IF NOT EXISTS token_metrics (
    -- Serie temporal
    time TIMESTAMPTZ NOT NULL,
    token_id INTEGER NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,
    
    -- Métricas cada 10 segundos
    price DECIMAL(30, 18),
    volume_24h DECIMAL(30, 8),
    liquidity DECIMAL(30, 8),
    market_cap DECIMAL(30, 8),
    fdv DECIMAL(30, 8),
    
    -- Holders y transacciones
    holders_count INTEGER,
    buyers_count INTEGER,
    sellers_count INTEGER,
    transactions_count INTEGER,
    
    -- Metadata
    data_source VARCHAR(20) DEFAULT 'local_node'
);

-- Convertir a hypertable (solo si no existe)
SELECT create_hypertable('token_metrics', 'time', if_not_exists => TRUE);

-- Índice compuesto
CREATE INDEX IF NOT EXISTS idx_metrics_token_time 
ON token_metrics (token_id, time DESC);

-- Política de retención automática (3 días)
SELECT add_retention_policy('token_metrics', INTERVAL '3 days', if_not_exists => TRUE);

-- ========================================
-- TABLA DE TRANSACCIONES
-- ========================================

CREATE TABLE IF NOT EXISTS transactions (
    tx_id SERIAL PRIMARY KEY,
    token_id INTEGER NOT NULL REFERENCES tokens(token_id) ON DELETE CASCADE,
    
    -- Datos de transacción
    signature VARCHAR(88) UNIQUE NOT NULL,
    block_time TIMESTAMP NOT NULL,
    slot BIGINT NOT NULL,
    
    -- Tipo de transacción
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

CREATE INDEX IF NOT EXISTS idx_tx_token ON transactions(token_id, block_time DESC);
CREATE INDEX IF NOT EXISTS idx_tx_signature ON transactions(signature);

-- ========================================
-- VISTA MATERIALIZADA PARA ESTADÍSTICAS
-- ========================================

CREATE MATERIALIZED VIEW IF NOT EXISTS token_hourly_stats
WITH (timescaledb.continuous) AS
SELECT token_id,
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

-- Política de refresco automático
SELECT add_continuous_aggregate_policy('token_hourly_stats',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ========================================
-- FIN DEL SCHEMA
-- ========================================
