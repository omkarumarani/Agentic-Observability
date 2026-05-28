FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Ensure the obs_intelligence package is importable as a top-level module
ENV PYTHONPATH=/app/app

# Install dependencies first (layer-cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (includes the obs_intelligence package)
COPY app/ ./app/

# Expose the intelligence engine port
EXPOSE 9100

# Start uvicorn
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:9100/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9100", "--log-level", "info"]
