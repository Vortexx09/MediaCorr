FROM python:3.11-slim-bullseye

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencias para pandas, scipy, matplotlib
RUN apt-get update && apt-get install -y \
    gcc \
    gfortran \
    libblas-dev \
    liblapack-dev \
    libfreetype6-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

CMD ["python", "-m", "app.correlator"]
