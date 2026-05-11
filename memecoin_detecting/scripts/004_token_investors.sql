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