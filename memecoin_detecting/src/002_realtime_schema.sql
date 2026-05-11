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