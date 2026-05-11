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