
# Base de Datos: memecoinsdb
## Guía de Recreación Completa

**Motor:** PostgreSQL + TimescaleDB  
**Base de datos:** `memecoinsdb`  
**Usuario:** `postgres`  
**Puerto:** `5432`

---

## Paso 0 — Crear la base de datos

```sql
sudo -u postgres createdb memecoinsdb
sudo -u postgres psql memecoinsdb
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

---

## Arquitectura de tablas

### Diagrama de relaciones

```
tokens (tabla central)
  │
  ├──→ token_metrics      (hypertable: precios cada 10s, retención 3 días)
  ├──→ transactions        (tabla regular: detecciones del detector)
  │
  ├──→ wallet_positions    (P&L por wallet×token)
  │       │
  │       └──→ wallets     (estadísticas acumuladas por wallet)
  │
  └──→ wallet_transactions (hypertable: trades individuales, retención 30 días)
          │
          └──→ wallets

tracked_wallets              (tabla independiente: wallets manuales)

Vistas:
  - token_hourly_stats       (materializada, TimescaleDB continuous aggregate)
  - token_volume_hourly      (materializada, TimescaleDB continuous aggregate)
  - active_positions         (vista regular)
  - wallet_performance_summary (vista regular)
  - top_traders              (vista regular)
```

---

## Tabla 1: `tokens` (PostgreSQL regular)

**Propósito:** Registro maestro de todos los tokens detectados.  
**Alimentada por:** `detector_memecoins.py`  
**Volumen:** ~32,000 tokens/día

| Columna | Tipo | Descripción |
|---|---|---|
| token_id | SERIAL PK | ID interno |
| mint_address | VARCHAR(44) UNIQUE | Dirección del token en Solana |
| name | VARCHAR(255) | Nombre del token |
| symbol | VARCHAR(20) | Símbolo (ticker) |
| total_supply | BIGINT | Supply total |
| decimals | INT | Decimales del token |
| uri | TEXT | URI de metadata |
| image_url | TEXT | URL de imagen |
| amm | VARCHAR(50) | AMM donde se detectó (Pump.fun, Raydium, etc.) |
| created_at | TIMESTAMP | Fecha de creación on-chain |
| detected_at | TIMESTAMP | Fecha de detección por nuestro sistema |
| creation_signature | VARCHAR(88) | Firma de la transacción de creación |
| creation_instruction | VARCHAR(100) | Instrucción de creación |
| status | VARCHAR(20) | `active`, `dead`, `interesting` |
| retention_category | VARCHAR(20) | `shortterm`, `longterm` |
| pool_address | VARCHAR(44) | Pool AMM cacheado (optimización) |

**Índices:** mint_address, amm, created_at DESC, status, pool_address

---

## Tabla 2: `token_metrics` (TimescaleDB Hypertable)

**Propósito:** Serie temporal de precios, liquidez, volumen y holders.  
**Alimentada por:** `metrics_collector.py` (cada ~60s)  
**Retención:** 3 días (auto-borrado) | Compresión: después de 2 días  
**Chunk interval:** 1 día

| Columna | Tipo | Descripción |
|---|---|---|
| time | TIMESTAMPTZ | Timestamp (parte del PK) |
| token_id | INTEGER FK→tokens | Token asociado |
| price | NUMERIC(30,18) | Precio en SOL |
| liquidity | NUMERIC(30,8) | Liquidez del pool (SOL×2) |
| volume_10m | NUMERIC(30,8) | Volumen 10 minutos |
| volume_1h | NUMERIC(30,8) | Volumen 1 hora |
| volume_24h | NUMERIC(30,8) | Volumen 24 horas |
| market_cap | NUMERIC(30,8) | Market cap |
| fdv | NUMERIC(30,8) | Fully diluted valuation |
| holders_count | INTEGER | Número de holders |
| transactions_count | INTEGER | Conteo de transacciones |
| pool_address | VARCHAR(44) | Pool address usado |

**Políticas automáticas:**
- Retención: borrado automático después de 3 días
- Compresión: después de 2 días (segmentado por token_id)

---

## Tabla 3: `transactions` (PostgreSQL regular)

**Propósito:** Transacciones detectadas por el detector de tokens.  
**Alimentada por:** `detector_memecoins.py`

| Columna | Tipo | Descripción |
|---|---|---|
| tx_id | SERIAL PK | ID interno |
| token_id | INTEGER FK→tokens | Token asociado |
| signature | VARCHAR(88) UNIQUE | Firma de la transacción |
| blocktime | TIMESTAMP | Timestamp del bloque |
| slot | BIGINT | Slot de Solana |
| tx_type | VARCHAR(20) | Tipo de transacción |
| amount | DECIMAL(30,18) | Monto |
| amount_usd | DECIMAL(20,8) | Monto en USD |
| from_wallet | VARCHAR(44) | Wallet origen |
| to_wallet | VARCHAR(44) | Wallet destino |

---

## Tabla 4: `wallets` (PostgreSQL regular)

**Propósito:** Perfil y estadísticas acumuladas de cada wallet.  
**Alimentada por:** `enhanced_wallet_tracker.py` vía función `process_transaction()`

| Columna | Tipo | Descripción |
|---|---|---|
| wallet_id | SERIAL PK | ID interno |
| wallet_address | VARCHAR(44) UNIQUE | Dirección Solana del wallet |
| total_trades | INTEGER | Total de operaciones |
| total_profit_loss | NUMERIC(30,8) | P&L total acumulado |
| total_invested | NUMERIC(30,8) | SOL total invertido |
| total_realized | NUMERIC(30,8) | Ganancias realizadas |
| win_rate | NUMERIC(5,2) | % de trades ganadores |
| avg_profit_per_trade | NUMERIC(30,8) | Ganancia promedio por trade |
| best_trade | NUMERIC(30,8) | Mejor trade |
| worst_trade | NUMERIC(30,8) | Peor trade |
| first_seen | TIMESTAMP | Primera vez detectado |
| last_seen | TIMESTAMP | Última actividad |
| is_active | BOOLEAN | Si está activo |
| tags | TEXT | Etiquetas (whale, bot, insider) |
| notes | TEXT | Notas libres |

**Índices:** wallet_address, total_profit_loss DESC, win_rate DESC, last_seen DESC

---

## Tabla 5: `wallet_positions` (PostgreSQL regular)

**Propósito:** Posición actual de cada wallet en cada token (P&L tracking).  
**Alimentada por:** función SQL `process_transaction()`

| Columna | Tipo | Descripción |
|---|---|---|
| position_id | SERIAL PK | ID interno |
| wallet_id | INTEGER FK→wallets | Wallet |
| token_id | INTEGER FK→tokens | Token |
| total_bought | NUMERIC(30,8) | Total comprado |
| total_sold | NUMERIC(30,8) | Total vendido |
| current_balance | NUMERIC(30,8) | Balance actual |
| total_cost | NUMERIC(30,8) | Costo total en SOL |
| avg_buy_price | NUMERIC(30,18) | Precio promedio de compra |
| total_revenue | NUMERIC(30,8) | Ingresos por ventas |
| avg_sell_price | NUMERIC(30,18) | Precio promedio de venta |
| realized_pnl | NUMERIC(30,8) | P&L realizado |
| unrealized_pnl | NUMERIC(30,8) | P&L no realizado |
| first_buy | TIMESTAMP | Primera compra |
| last_buy | TIMESTAMP | Última compra |
| last_sell | TIMESTAMP | Última venta |
| status | VARCHAR(20) | `open`, `closed`, `partial` |

**Constraint único:** (wallet_id, token_id)

---

## Tabla 6: `wallet_transactions` (TimescaleDB Hypertable)

**Propósito:** Historial completo de trades individuales.  
**Alimentada por:** `enhanced_wallet_tracker.py` vía `process_transaction()`  
**Retención:** 30 días (auto-borrado)  
**Chunk interval:** 1 día

| Columna | Tipo | Descripción |
|---|---|---|
| time | TIMESTAMPTZ | Timestamp (parte del PK) |
| transaction_id | SERIAL | ID secuencial |
| wallet_id | INTEGER FK→wallets | Wallet |
| token_id | INTEGER FK→tokens | Token |
| signature | VARCHAR(88) | Firma de la transacción |
| tx_type | VARCHAR(10) | `buy` o `sell` |
| token_amount | NUMERIC(30,8) | Cantidad de tokens |
| sol_amount | NUMERIC(30,8) | Cantidad de SOL |
| price | NUMERIC(30,18) | Precio unitario |
| fee | NUMERIC(30,8) | Fee pagado |
| is_partial | BOOLEAN | Si es orden parcial |
| order_id | VARCHAR(88) | ID de la orden (agrupar parciales) |
| partial_fill_index | INTEGER | Índice de llenado parcial |
| pool_address | VARCHAR(44) | Pool usado |
| program_id | VARCHAR(44) | Programa AMM |
| instruction_index | INTEGER | Índice de instrucción |
| block_slot | BIGINT | Slot del bloque |

---

## Tabla 7: `tracked_wallets` (PostgreSQL regular)

**Propósito:** Lista de wallets que se rastrean manualmente (VIP).  
**Alimentada por:** inserción manual o `add_wallet_to_track()`

| Columna | Tipo | Descripción |
|---|---|---|
| wallet_address | VARCHAR(44) PK | Dirección del wallet |
| label | VARCHAR(100) | Nombre/etiqueta |
| reason | TEXT | Razón del seguimiento |
| added_at | TIMESTAMP | Fecha de registro |
| is_active | BOOLEAN | Si está activo |

---

## Funciones SQL

### `process_transaction()`
Procesa una transacción completa: crea/actualiza wallet, inserta transacción, actualiza posición y calcula P&L.

### `update_wallet_stats()`
Recalcula estadísticas acumuladas del wallet (total_trades, win_rate, avg_profit, etc.).

---

## Vistas

| Vista | Tipo | Descripción |
|---|---|---|
| `token_hourly_stats` | Materializada (continuous aggregate) | OHLC de precios por hora |
| `token_volume_hourly` | Materializada (continuous aggregate) | Volumen agregado por hora |
| `active_positions` | Regular | Posiciones abiertas con precio actual |
| `wallet_performance_summary` | Regular | Resumen de rendimiento por wallet |
| `top_traders` | Regular | Top 100 traders por P&L (mín 5 trades) |

---

## Políticas automáticas de TimescaleDB

| Tabla | Retención | Compresión | Refresh (vistas) |
|---|---|---|---|
| token_metrics | 3 días | 2 días | token_hourly_stats: cada 1h |
| wallet_transactions | 30 días | — | token_volume_hourly: cada 1h |

---

## Para recrear desde cero

```bash
# 1. Crear BD
sudo -u postgres createdb memecoinsdb
sudo -u postgres psql memecoinsdb -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# 2. Ejecutar schemas
sudo -u postgres psql memecoinsdb -f memecoinsdb-complete-schema.sql

# 3. Verificar
sudo -u postgres psql memecoinsdb -c "\dt+"
```
