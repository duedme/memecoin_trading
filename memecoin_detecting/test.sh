#!/bin/bash
# test_nodo_rpc.sh - Script de diagnóstico del nodo RPC Solana
# Prueba todas las funcionalidades necesarias para el detector

echo "=========================================="
echo "🔍 DIAGNÓSTICO DEL NODO RPC SOLANA"
echo "=========================================="
echo ""

RPC_URL="http://127.0.0.1:7211"

# Colores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ========================================
# PRUEBA 1: Health Check
# ========================================
echo "📍 PRUEBA 1: Health del nodo"
echo "----------------------------------------"
HEALTH=$(curl -s $RPC_URL -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}')

echo "Request: getHealth"
echo "Response: $HEALTH"

if echo "$HEALTH" | grep -q '"result":"ok"'; then
    echo -e "${GREEN}✅ PASSED: Nodo está saludable${NC}"
else
    echo -e "${RED}❌ FAILED: Nodo no responde correctamente${NC}"
    echo "Verifica que el nodo esté corriendo: solv status"
    exit 1
fi
echo ""

# ========================================
# PRUEBA 2: Obtener Slot Actual
# ========================================
echo "📍 PRUEBA 2: Slot actual"
echo "----------------------------------------"
SLOT=$(curl -s $RPC_URL -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot"}')

echo "Request: getSlot"
echo "Response: $SLOT"

SLOT_NUM=$(echo "$SLOT" | grep -o '"result":[0-9]*' | grep -o '[0-9]*')
if [ ! -z "$SLOT_NUM" ]; then
    echo -e "${GREEN}✅ PASSED: Slot actual: $SLOT_NUM${NC}"
else
    echo -e "${RED}❌ FAILED: No se pudo obtener el slot${NC}"
fi
echo ""

# ========================================
# PRUEBA 3: Probar getSignaturesForAddress con Pump.fun
# ========================================
echo "📍 PRUEBA 3: Obtener firmas de Pump.fun"
echo "----------------------------------------"
PUMP_FUN="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
echo "Programa: Pump.fun ($PUMP_FUN)"

SIGNATURES=$(curl -s $RPC_URL -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc":"2.0",
    "id":1,
    "method":"getSignaturesForAddress",
    "params":[
      "'$PUMP_FUN'",
      {"limit":5}
    ]
  }')

echo "Request: getSignaturesForAddress (limit: 5)"
echo "Response (primeros 500 chars):"
echo "$SIGNATURES" | head -c 500
echo ""

# Verificar si hay resultados
if echo "$SIGNATURES" | grep -q '"result":\['; then
    SIG_COUNT=$(echo "$SIGNATURES" | grep -o '"signature"' | wc -l)
    echo -e "${GREEN}✅ PASSED: Se obtuvieron $SIG_COUNT firmas${NC}"
    
    # Extraer la primera firma para la siguiente prueba
    FIRST_SIG=$(echo "$SIGNATURES" | grep -o '"signature":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo "Primera firma: $FIRST_SIG"
else
    echo -e "${RED}❌ FAILED: No se obtuvieron firmas${NC}"
    echo "Posibles causas:"
    echo "  - El nodo no tiene historial completo"
    echo "  - La dirección de Pump.fun cambió"
    echo "  - Problemas de sincronización"
fi
echo ""

# ========================================
# PRUEBA 4: Obtener detalles de transacción
# ========================================
if [ ! -z "$FIRST_SIG" ]; then
    echo "📍 PRUEBA 4: Obtener detalles de transacción"
    echo "----------------------------------------"
    echo "Firma: $FIRST_SIG"
    
    TX_DETAIL=$(curl -s $RPC_URL -X POST \
      -H "Content-Type: application/json" \
      -d '{
        "jsonrpc":"2.0",
        "id":1,
        "method":"getTransaction",
        "params":[
          "'$FIRST_SIG'",
          {
            "encoding":"jsonParsed",
            "maxSupportedTransactionVersion":0
          }
        ]
      }')
    
    echo "Request: getTransaction"
    echo "Response (primeros 1000 chars):"
    echo "$TX_DETAIL" | head -c 1000
    echo ""
    
    if echo "$TX_DETAIL" | grep -q '"result":{'; then
        echo -e "${GREEN}✅ PASSED: Se obtuvieron detalles de transacción${NC}"
        
        # Verificar si tiene logs
        if echo "$TX_DETAIL" | grep -q '"logMessages"'; then
            echo -e "${GREEN}✅ Transacción contiene logs${NC}"
        else
            echo -e "${YELLOW}⚠️  Transacción sin logs${NC}"
        fi
        
        # Verificar si tiene postTokenBalances
        if echo "$TX_DETAIL" | grep -q '"postTokenBalances"'; then
            echo -e "${GREEN}✅ Transacción contiene postTokenBalances${NC}"
        else
            echo -e "${YELLOW}⚠️  Transacción sin postTokenBalances${NC}"
        fi
    else
        echo -e "${RED}❌ FAILED: No se pudieron obtener detalles${NC}"
        echo "Causa probable: El nodo no tiene historial de esta transacción"
    fi
    echo ""
fi

# ========================================
# PRUEBA 5: Probar todos los AMMs
# ========================================
echo "📍 PRUEBA 5: Verificar todos los AMMs"
echo "----------------------------------------"

declare -A AMMS=(
    ["Pump.fun"]="6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
    ["PumpSwap"]="pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
    ["Raydium AMM"]="CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
    ["Raydium LaunchLab"]="LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj"
    ["FluxBeam"]="FLUXubRmkEi2q6K3Y9kBPg9248ggaZVsoSFhtJHSrm1X"
    ["HeavenDEX"]="HEAVENoP2qxoeuF8Dj2oT1GHEnu49U5mJYkdeC8BAX2o"
    ["Meteora DLMM"]="LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
    ["Meteora DYN2"]="cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
    ["Meteora DYN"]="Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"
    ["Meteora DBC"]="dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN"
    ["Moonit"]="MoonCVVNZFSYkqNXP6bxHLPL6QQJiMagDL3qcqUQTrG"
    ["Orca"]="whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
)

for AMM_NAME in "${!AMMS[@]}"; do
    AMM_ADDR="${AMMS[$AMM_NAME]}"
    
    SIG_TEST=$(curl -s $RPC_URL -X POST \
      -H "Content-Type: application/json" \
      -d '{
        "jsonrpc":"2.0",
        "id":1,
        "method":"getSignaturesForAddress",
        "params":["'$AMM_ADDR'",{"limit":1}]
      }')
    
    if echo "$SIG_TEST" | grep -q '"result":\['; then
        COUNT=$(echo "$SIG_TEST" | grep -o '"signature"' | wc -l)
        if [ "$COUNT" -gt 0 ]; then
            echo -e "${GREEN}✅ $AMM_NAME: $COUNT transacciones encontradas${NC}"
        else
            echo -e "${YELLOW}⚠️  $AMM_NAME: 0 transacciones (puede ser normal)${NC}"
        fi
    else
        echo -e "${RED}❌ $AMM_NAME: Error al consultar${NC}"
    fi
done
echo ""

# ========================================
# PRUEBA 6: Verificar historial disponible
# ========================================
echo "📍 PRUEBA 6: Verificar rango de historial"
echo "----------------------------------------"

# Obtener el primer slot disponible
FIRST_SLOT=$(curl -s $RPC_URL -X POST \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"getFirstAvailableBlock"}')

echo "Request: getFirstAvailableBlock"
echo "Response: $FIRST_SLOT"

FIRST_NUM=$(echo "$FIRST_SLOT" | grep -o '"result":[0-9]*' | grep -o '[0-9]*')
if [ ! -z "$FIRST_NUM" ] && [ ! -z "$SLOT_NUM" ]; then
    DIFF=$((SLOT_NUM - FIRST_NUM))
    # Cada slot son ~400ms, calculamos días aproximados
    DAYS=$(echo "scale=2; $DIFF * 0.0004 / 86400" | bc)
    echo -e "${GREEN}✅ PASSED: Historial disponible${NC}"
    echo "   Primer slot: $FIRST_NUM"
    echo "   Slot actual: $SLOT_NUM"
    echo "   Diferencia: $DIFF slots (~$DAYS días)"
else
    echo -e "${YELLOW}⚠️  No se pudo calcular el rango de historial${NC}"
fi
echo ""

# ========================================
# RESUMEN FINAL
# ========================================
echo "=========================================="
echo "📊 RESUMEN DEL DIAGNÓSTICO"
echo "=========================================="
echo ""
echo "Si todas las pruebas pasaron (✅), el nodo está funcionando correctamente."
echo ""
echo "Problemas comunes y soluciones:"
echo ""
echo "1. ${YELLOW}Nodo no responde:${NC}"
echo "   → Verificar: solv status"
echo "   → Reiniciar: solv restart"
echo ""
echo "2. ${YELLOW}No se encuentran transacciones:${NC}"
echo "   → El nodo solo tiene historial desde su instalación (4 Feb 2026)"
echo "   → Espera unos minutos para que se acumule actividad nueva"
echo "   → Verifica que el nodo esté completamente sincronizado"
echo ""
echo "3. ${YELLOW}getTransaction devuelve null:${NC}"
echo "   → La transacción es muy antigua (antes del snapshot)"
echo "   → Usa solo transacciones recientes (últimas horas)"
echo ""
echo "4. ${YELLOW}Algunos AMMs no tienen transacciones:${NC}"
echo "   → Es normal, no todos tienen actividad constante"
echo "   → Pump.fun y Raydium suelen tener más actividad"
echo ""
echo "=========================================="
echo "✅ Diagnóstico completado"
echo "=========================================="

