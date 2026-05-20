# Usamos una imagen de Python ligera
FROM python:3.10-slim

WORKDIR /app

# Instalamos las dependencias del sistema necesarias para compilar psycopg2
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Instalamos las librerías de Python exactas que necesita tu Firehose
RUN pip install --no-cache-dir Flask Flask-SocketIO psycopg2-binary

# El comando que mantendrá vivo el servidor (la bandera -u es para ver los logs en tiempo real)
CMD ["python", "-u", "firehose_server.py"]
