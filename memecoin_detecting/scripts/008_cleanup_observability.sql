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