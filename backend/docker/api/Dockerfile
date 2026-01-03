FROM python:3.11-slim

WORKDIR /app

COPY backend /app/backend

ENV PYTHONPATH=/app/backend

RUN pip install fastapi uvicorn kubernetes

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
