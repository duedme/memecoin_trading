-- ============================================
-- ACTUALIZACIÓN DEL SCHEMA - Fase 2
-- Aumentar retención + agregar columnas de volumen
-- ============================================

-- 1. Modificar tabla token_metrics para agregar columnas de volumen
ALTER TABLE token_metrics ADD COLUMN IF NOT EXISTS volume_10m DECIMAL(30, 8) DEFAULT 0;
ALTER TABLE token_metrics ADD COLUMN IF NOT EXISTS volume_1h DECIMAL(30, 8) DEFAULT 0;

-- Agregar índice para pool_address si no existe
CREATE INDEX IF NOT EXISTS idx_metrics_pool ON token_metrics(pool_address);

-- 2. Actualizar política de retención de 3 días a 14 días
-- Primero, eliminar la política existente
SELECT remove_retention_policy('token_metrics', if_exists => TRUE);

-- Luego, crear nueva política de 14 días
SELECT add_retention_policy('token_metrics', INTERVAL '14 days', if_not_exists => TRUE);

-- 3. Verificar la columna pool_address en tokens (debería existir)
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS pool_address VARCHAR(44);

-- Índice para búsquedas rápidas de pool
CREATE INDEX IF NOT EXISTS idx_tokens_pool ON tokens(pool_address) WHERE pool_address IS NOT NULL;

-- 4. Crear vista materializada para agregaciones de volumen por hora
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

-- Política de refresco automático para la vista de volumen
SELECT add_continuous_aggregate_policy('token_volume_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- 5. Crear índice compuesto para consultas de volumen por período
CREATE INDEX IF NOT EXISTS idx_metrics_volume_time 
ON token_metrics(token_id, time DESC, volume_10m) 
WHERE volume_10m > 0;

-- ============================================
-- VERIFICACIÓN
-- ============================================

-- Mostrar configuración de retención actual
SELECT 
    hypertable_name,
    older_than,
    drop_after
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention'
AND hypertable_name = 'token_metrics';

-- Mostrar columnas de token_metrics
SELECT 
    column_name, 
    data_type, 
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = 'token_metrics'
ORDER BY ordinal_position;

-- Verificar tamaño de la hypertable
SELECT 
    hypertable_name,
    pg_size_pretty(hypertable_size('token_metrics')) as size,
    num_chunks
FROM timescaledb_information.hypertables
WHERE hypertable_name = 'token_metrics';
