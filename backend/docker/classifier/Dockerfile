FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias nativas necesarias para ML
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

# Caché de modelos (mejora performance)
ENV HF_HOME=/app/.cache/huggingface
ENV TRANSFORMERS_CACHE=/app/.cache/huggingface
ENV PYSENTIMIENTO_CACHE=/app/.cache/pysentimiento

CMD ["python", "-m", "app.classifier"]
