FROM python:3.11-slim

# Evita archivos .pyc y buffers innecesarios
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalar dependencias del sistema mínimas
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copiar SOLO requirements del classifier
COPY docker/classifier/requirements.txt .

# Instalar dependencias Python
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# (Opcional pero recomendado) precargar modelo
RUN python - <<EOF
from pysentimiento import create_analyzer
create_analyzer(task="sentiment", lang="es")
EOF

# Copiar código de la app
COPY app ./app

CMD ["python", "-m", "app.classifier"]
