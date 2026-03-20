#!/bin/bash
# ============================================================================
# Script de Instalación Automatizada: memecoins_db
# ============================================================================

set -e  # Salir si hay error

echo "🚀 Instalador automático de memecoins_db"
echo "========================================"
echo ""

# Variables
DB_NAME="memecoins_db"
SCRIPT_SQL="setup_memecoins_db.sql"

# Verificar que el script SQL existe
if [ ! -f "$SCRIPT_SQL" ]; then
    echo "❌ Error: No se encuentra $SCRIPT_SQL"
    echo "   Asegúrate de tener el archivo en el mismo directorio"
    exit 1
fi

# Verificar que PostgreSQL está corriendo
if ! sudo systemctl is-active --quiet postgresql; then
    echo "❌ Error: PostgreSQL no está corriendo"
    echo "   Ejecuta: sudo systemctl start postgresql"
    exit 1
fi

echo "✓ PostgreSQL está corriendo"
echo ""

# Crear base de datos si no existe
echo "📦 Verificando base de datos..."
if sudo -u postgres psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "⚠️  La base de datos '$DB_NAME' ya existe"
    read -p "   ¿Deseas continuar? Esto creará las tablas si no existen (s/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        echo "Operación cancelada"
        exit 0
    fi
else
    echo "→ Creando base de datos '$DB_NAME'..."
    sudo -u postgres psql -c "CREATE DATABASE $DB_NAME;"
    echo "✓ Base de datos creada"
fi

echo ""
echo "🔧 Ejecutando script de setup..."
echo ""

# Ejecutar el script SQL
sudo -u postgres psql -d "$DB_NAME" -f "$SCRIPT_SQL"

# Resultado final
if [ $? -eq 0 ]; then
    echo ""
    echo "============================================"
    echo "✅ ¡Instalación completada exitosamente!"
    echo "============================================"
    echo ""
    echo "📋 Próximos pasos:"
    echo "   1. Conectarte: sudo -u postgres psql -d $DB_NAME"
    echo "   2. Ver tablas: \dt"
    echo "   3. Ver hypertables: SELECT * FROM timescaledb_information.hypertables;"
    echo ""
else
    echo ""
    echo "❌ Error durante la instalación"
    echo "   Revisa los mensajes de error arriba"
    exit 1
fi
