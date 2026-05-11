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