#!/bin/bash
# start_webhook_server.sh
# Script para iniciar el webhook server como servicio

# Cargar variables de entorno
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Activar entorno virtual si existe
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Iniciar servidor
echo "🚀 Iniciando Webhook Server..."
python3 webhook_server.py
