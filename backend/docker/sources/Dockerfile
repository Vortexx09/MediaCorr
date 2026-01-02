FROM python:3.11-slim

# Evita buffers y reduce logs ruidosos
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código
COPY app /app/app

# Punto de entrada del Job
CMD ["python", "-m", "app.sources"]
