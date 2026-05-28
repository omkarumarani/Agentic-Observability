FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy shared obs_intelligence package first (required for -e install below)
COPY obs-intelligence/app/obs_intelligence /app/obs_intelligence

# Install service dependencies (includes -e /app/obs_intelligence)
COPY compute-agent/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY compute-agent/app/ ./app/

# Expose the compute-agent webhook port
EXPOSE 9000

# Start uvicorn with the FastAPI app
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:9000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9000", "--log-level", "info"]
