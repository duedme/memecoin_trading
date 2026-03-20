-- setup_memecoins_db.sql
-- Schema de base de datos para sistema de detección de memecoins
-- VERSIÓN CORREGIDA:
--   - Añadidas columnas faltantes: creation_signature, creation_instruction, 
--     detected_at, retention_category
--   - Añadidas columnas faltantes en tracked_wallets: label, reason

-- ============================================================
-- 1. TABLA: tokens
-- ============================================================

CREATE TABLE IF NOT EXISTS tokens (
    token_id SERIAL PRIMARY KEY,
    mint_address VARCHAR(44) UNIQUE NOT NULL,
    name VARCHAR(255),
    symbol VARCHAR(20),
    decimals INTEGER DEFAULT 9,
    total_supply BIGINT,
    image_url TEXT,
    amm VARCHAR(50),
    created_at TIMESTAMP,
    detected_at TIMESTAMP DEFAULT NOW(),        -- FIX: Añadida
    creation_signature VARCHAR(88),             -- FIX: Añadida
    creation_instruction TEXT,                  -- FIX: Añadida (opcional)
    pool_address VARCHAR(44),
    status VARCHAR(20) DEFAULT 'active',
    retention_category VARCHAR(20) DEFAULT 'shortterm'  -- FIX: Añadida
);

CREATE INDEX IF NOT EXISTS idx_tokens_mint ON tokens(mint_address);
CREATE INDEX IF NOT EXISTS idx_tokens_status ON tokens(status);
CREATE INDEX IF NOT EXISTS idx_tokens_detected_at ON tokens(detected_at);
CREATE INDEX IF NOT EXISTS idx_tokens_retention ON tokens(retention_category);

-- ============================================================
-- 2. TABLA: token_metrics
-- ============================================================

CREATE TABLE IF NOT EXISTS token_metrics (
    metric_id SERIAL PRIMARY KEY,
    token_id INTEGER REFERENCES tokens(token_id) ON DELETE CASCADE,
    time TIMESTAMP DEFAULT NOW(),
    price NUMERIC(20, 10),
    market_cap NUMERIC(20, 2),
    fdv NUMERIC(20, 2),
    liquidity NUMERIC(20, 4),
    volume_10m NUMERIC(20, 4) DEFAULT 0,
    swap_count INTEGER DEFAULT 0,
    holders_count INTEGER DEFAULT 0,
    UNIQUE(token_id, time)
);

CREATE INDEX IF NOT EXISTS idx_metrics_token_time ON token_metrics(token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_time ON token_metrics(time);

-- ============================================================
-- 3. TABLA: wallets
-- ============================================================

CREATE TABLE IF NOT EXISTS wallets (
    wallet_id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) UNIQUE NOT NULL,
    total_trades INTEGER DEFAULT 0,
    total_profit_loss NUMERIC(20, 6) DEFAULT 0,
    total_invested NUMERIC(20, 6) DEFAULT 0,
    total_realized NUMERIC(20, 6) DEFAULT 0,
    win_rate NUMERIC(5, 2) DEFAULT 0,
    avg_profit_per_trade NUMERIC(20, 6) DEFAULT 0,
    best_trade NUMERIC(20, 6) DEFAULT 0,
    worst_trade NUMERIC(20, 6) DEFAULT 0,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(wallet_address);
CREATE INDEX IF NOT EXISTS idx_wallets_pnl ON wallets(total_profit_loss DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen ON wallets(last_seen);

-- ============================================================
-- 4. TABLA: wallet_transactions
-- ============================================================

CREATE TABLE IF NOT EXISTS wallet_transactions (
    transaction_id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    token_id INTEGER REFERENCES tokens(token_id) ON DELETE CASCADE,
    signature VARCHAR(88) UNIQUE NOT NULL,
    tx_type VARCHAR(10) NOT NULL,
    token_amount NUMERIC(30, 6),
    sol_amount NUMERIC(20, 6),
    price NUMERIC(20, 10),
    time TIMESTAMP,
    is_partial BOOLEAN DEFAULT FALSE,
    order_id VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON wallet_transactions(wallet_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_token ON wallet_transactions(token_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_signature ON wallet_transactions(signature);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON wallet_transactions(time DESC);

-- ============================================================
-- 5. TABLA: wallet_positions
-- ============================================================

CREATE TABLE IF NOT EXISTS wallet_positions (
    position_id SERIAL PRIMARY KEY,
    wallet_id INTEGER REFERENCES wallets(wallet_id) ON DELETE CASCADE,
    token_id INTEGER REFERENCES tokens(token_id) ON DELETE CASCADE,
    total_bought NUMERIC(30, 6) DEFAULT 0,
    total_sold NUMERIC(30, 6) DEFAULT 0,
    current_balance NUMERIC(30, 6) DEFAULT 0,
    avg_buy_price NUMERIC(20, 10),
    avg_sell_price NUMERIC(20, 10),
    realized_pnl NUMERIC(20, 6) DEFAULT 0,
    unrealized_pnl NUMERIC(20, 6) DEFAULT 0,
    first_buy TIMESTAMP,
    last_buy TIMESTAMP,
    first_sell TIMESTAMP,
    last_sell TIMESTAMP,
    status VARCHAR(20) DEFAULT 'open',
    UNIQUE(wallet_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_wallet ON wallet_positions(wallet_id);
CREATE INDEX IF NOT EXISTS idx_positions_token ON wallet_positions(token_id);

-- ============================================================
-- 6. TABLA: tracked_wallets
-- ============================================================

CREATE TABLE IF NOT EXISTS tracked_wallets (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(44) UNIQUE NOT NULL,
    label VARCHAR(255),           -- FIX: Añadida
    reason TEXT,                  -- FIX: Añadida
    is_active BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracked_wallets_active ON tracked_wallets(is_active);

-- ============================================================
-- 7. STORED PROCEDURE: process_transaction
-- ============================================================

CREATE OR REPLACE FUNCTION process_transaction(
    p_wallet_address VARCHAR(44),
    p_mint_address VARCHAR(44),
    p_signature VARCHAR(88),
    p_tx_type VARCHAR(10),
    p_token_amount NUMERIC(30, 6),
    p_sol_amount NUMERIC(20, 6),
    p_price NUMERIC(20, 10),
    p_time TIMESTAMP
)
RETURNS VOID AS $$
DECLARE
    v_wallet_id INTEGER;
    v_token_id INTEGER;
    v_position_id INTEGER;
    v_current_balance NUMERIC(30, 6);
    v_avg_buy_price NUMERIC(20, 10);
    v_avg_sell_price NUMERIC(20, 10);
    v_realized_pnl NUMERIC(20, 6);
    v_total_bought NUMERIC(30, 6);
    v_total_sold NUMERIC(30, 6);
BEGIN
    -- 1. Obtener o crear wallet
    INSERT INTO wallets (wallet_address, first_seen, last_seen, is_active)
    VALUES (p_wallet_address, p_time, p_time, TRUE)
    ON CONFLICT (wallet_address) DO UPDATE
    SET last_seen = p_time
    RETURNING wallet_id INTO v_wallet_id;

    IF v_wallet_id IS NULL THEN
        SELECT wallet_id INTO v_wallet_id
        FROM wallets
        WHERE wallet_address = p_wallet_address;
    END IF;

    -- 2. Obtener token_id
    SELECT token_id INTO v_token_id
    FROM tokens
    WHERE mint_address = p_mint_address;

    IF v_token_id IS NULL THEN
        RAISE EXCEPTION 'Token % no existe en la tabla tokens', p_mint_address;
    END IF;

    -- 3. Insertar transacción
    INSERT INTO wallet_transactions (
        wallet_id, token_id, signature, tx_type,
        token_amount, sol_amount, price, time
    )
    VALUES (
        v_wallet_id, v_token_id, p_signature, p_tx_type,
        p_token_amount, p_sol_amount, p_price, p_time
    )
    ON CONFLICT (signature) DO NOTHING;

    -- 4. Obtener o crear posición
    INSERT INTO wallet_positions (wallet_id, token_id, first_buy, last_buy)
    VALUES (v_wallet_id, v_token_id, p_time, p_time)
    ON CONFLICT (wallet_id, token_id) DO NOTHING
    RETURNING position_id INTO v_position_id;

    IF v_position_id IS NULL THEN
        SELECT position_id INTO v_position_id
        FROM wallet_positions
        WHERE wallet_id = v_wallet_id AND token_id = v_token_id;
    END IF;

    -- 5. Actualizar posición según tipo de transacción
    IF p_tx_type = 'buy' THEN
        -- COMPRA
        SELECT 
            current_balance,
            avg_buy_price,
            total_bought
        INTO 
            v_current_balance,
            v_avg_buy_price,
            v_total_bought
        FROM wallet_positions
        WHERE position_id = v_position_id;

        -- Calcular nuevo precio promedio de compra
        IF v_current_balance > 0 THEN
            v_avg_buy_price := (
                (v_avg_buy_price * v_current_balance) + (p_price * p_token_amount)
            ) / (v_current_balance + p_token_amount);
        ELSE
            v_avg_buy_price := p_price;
        END IF;

        -- Actualizar posición
        UPDATE wallet_positions
        SET
            total_bought = total_bought + p_token_amount,
            current_balance = current_balance + p_token_amount,
            avg_buy_price = v_avg_buy_price,
            last_buy = p_time,
            status = 'open'
        WHERE position_id = v_position_id;

    ELSIF p_tx_type = 'sell' THEN
        -- VENTA
        SELECT 
            current_balance,
            avg_buy_price,
            avg_sell_price,
            total_sold
        INTO 
            v_current_balance,
            v_avg_buy_price,
            v_avg_sell_price,
            v_total_sold
        FROM wallet_positions
        WHERE position_id = v_position_id;

        -- Calcular P&L realizado de esta venta
        IF v_avg_buy_price IS NOT NULL AND v_avg_buy_price > 0 THEN
            v_realized_pnl := (p_price - v_avg_buy_price) * p_token_amount;
        ELSE
            v_realized_pnl := 0;
        END IF;

        -- Calcular nuevo precio promedio de venta
        IF v_total_sold > 0 THEN
            v_avg_sell_price := (
                (v_avg_sell_price * v_total_sold) + (p_price * p_token_amount)
            ) / (v_total_sold + p_token_amount);
        ELSE
            v_avg_sell_price := p_price;
        END IF;

        -- Actualizar posición
        UPDATE wallet_positions
        SET
            total_sold = total_sold + p_token_amount,
            current_balance = current_balance - p_token_amount,
            avg_sell_price = v_avg_sell_price,
            realized_pnl = realized_pnl + v_realized_pnl,
            last_sell = p_time,
            status = CASE
                WHEN (current_balance - p_token_amount) <= 0 THEN 'closed'
                ELSE 'open'
            END
        WHERE position_id = v_position_id;

        -- Actualizar wallet P&L
        UPDATE wallets
        SET
            total_realized = total_realized + p_sol_amount,
            total_profit_loss = total_profit_loss + v_realized_pnl
        WHERE wallet_id = v_wallet_id;

    END IF;

    -- 6. Actualizar estadísticas del wallet
    UPDATE wallets
    SET
        total_trades = total_trades + 1,
        total_invested = CASE
            WHEN p_tx_type = 'buy' THEN total_invested + p_sol_amount
            ELSE total_invested
        END,
        best_trade = CASE
            WHEN p_tx_type = 'sell' AND v_realized_pnl > best_trade THEN v_realized_pnl
            ELSE best_trade
        END,
        worst_trade = CASE
            WHEN p_tx_type = 'sell' AND v_realized_pnl < worst_trade THEN v_realized_pnl
            ELSE worst_trade
        END,
        last_seen = p_time
    WHERE wallet_id = v_wallet_id;

    -- 7. Calcular win rate
    UPDATE wallets w
    SET win_rate = (
        SELECT
            CASE
                WHEN COUNT(*) > 0 THEN
                    (COUNT(*) FILTER (WHERE realized_pnl > 0)::NUMERIC / COUNT(*)::NUMERIC * 100)
                ELSE 0
            END
        FROM wallet_positions
        WHERE wallet_id = w.wallet_id
        AND status = 'closed'
    ),
    avg_profit_per_trade = (
        SELECT AVG(realized_pnl)
        FROM wallet_positions
        WHERE wallet_id = w.wallet_id
        AND status = 'closed'
    )
    WHERE w.wallet_id = v_wallet_id;

END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FIN DEL SCHEMA
-- ============================================================
