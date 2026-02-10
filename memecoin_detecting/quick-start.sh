#!/bin/bash
# ================================================================
# QUICK START - Implementación Fase 2 en 5 minutos
# ================================================================

echo "🚀 FASE 2 - QUICK START"
echo "======================================"

# PASO 1: Instalar dependencias
echo ""
echo "📦 PASO 1: Instalando dependencias..."
cd /home/rebelforce/scripts/memecoin_detecting/
pip install psycopg2-binary requests python-dateutil tabulate colorama

# PASO 2: Crear schema de base de datos
echo ""
echo "🗄️  PASO 2: Creando schema de base de datos..."
sudo -u postgres psql -d memecoins_db -f schema-fase2.sql

# PASO 3: Verificar que todo está OK
echo ""
echo "✅ PASO 3: Verificando instalación..."

# Verificar tablas creadas
sudo -u postgres psql -d memecoins_db -c "SELECT COUNT(*) FROM wallets;" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "  ✅ Tabla 'wallets' creada"
else
    echo "  ❌ Error creando tabla 'wallets'"
    exit 1
fi

sudo -u postgres psql -d memecoins_db -c "SELECT COUNT(*) FROM wallet_transactions;" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "  ✅ Tabla 'wallet_transactions' creada"
else
    echo "  ❌ Error creando tabla 'wallet_transactions'"
    exit 1
fi

sudo -u postgres psql -d memecoins_db -c "SELECT COUNT(*) FROM wallet_positions;" > /dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "  ✅ Tabla 'wallet_positions' creada"
else
    echo "  ❌ Error creando tabla 'wallet_positions'"
    exit 1
fi

# Verificar funciones SQL
sudo -u postgres psql -d memecoins_db -c "SELECT proname FROM pg_proc WHERE proname = 'process_transaction';" | grep -q "process_transaction"
if [ $? -eq 0 ]; then
    echo "  ✅ Función 'process_transaction' creada"
else
    echo "  ❌ Error creando función 'process_transaction'"
    exit 1
fi

echo ""
echo "======================================"
echo "✨ ¡Instalación completada!"
echo "======================================"
echo ""
echo "📝 PRÓXIMOS PASOS:"
echo ""
echo "1️⃣  PROBAR MANUALMENTE (recomendado):"
echo "    Terminal 1: python3 metrics_collector.py"
echo "    Terminal 2: python3 wallet_tracker.py"
echo "    Terminal 3: python3 wallet_analytics.py top"
echo ""
echo "2️⃣  CREAR SERVICIOS SYSTEMD (para producción):"
echo "    Ver archivo: GUIA-INSTALACION.md (sección servicios)"
echo ""
echo "3️⃣  ANALIZAR DATOS:"
echo "    python3 wallet_analytics.py top"
echo "    python3 wallet_analytics.py activity --hours 24"
echo ""
echo "======================================"
echo ""
echo "📚 DOCUMENTACIÓN:"
echo "  - RESUMEN-EJECUTIVO.md  → Qué hace cada componente"
echo "  - GUIA-INSTALACION.md   → Guía paso a paso completa"
echo ""
echo "🎉 ¡Listo para empezar!"
