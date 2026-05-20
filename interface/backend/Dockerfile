# Usamos una imagen de Python ligera
FROM python:3.10-slim

WORKDIR /app

# Instalamos las dependencias del sistema necesarias para compilar psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Instalamos TODAS las librerías que necesitan el Firehose y la API
RUN pip install --no-cache-dir Flask Flask-SocketIO Flask-CORS psycopg2-binary
