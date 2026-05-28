# backend-api/Dockerfile
# ----------------------
# Builds the Backend API container.
#
# python:3.11-slim keeps the image small while giving us a full CPython runtime.
# Dependencies are installed in a separate layer so Docker can cache them;
# the layer is only invalidated when requirements.txt changes.

FROM python:3.11-slim

# Set a clean working directory inside the container
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# ── Install Python dependencies ───────────────────────────────
# Copy requirements first to benefit from Docker layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy application source ────────────────────────────────────
COPY app/ ./app/

# ── Runtime ───────────────────────────────────────────────────
EXPOSE 8081

# uvicorn runs the FastAPI app.
# --host 0.0.0.0  → listen on all interfaces (required inside Docker)
# --port 8081     → must match EXPOSE and docker-compose ports mapping
# --log-level info → clean, readable logs; change to "debug" for more detail
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:8081/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8081", "--log-level", "info"]
