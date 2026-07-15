# Stage 1: Build stage - Install dependencies to a wheelhouse
FROM python:3.12-slim as builder

WORKDIR /app

# Install curl for healthcheck and build-essential for some python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt

# Stage 2: Final stage - Create the production image
FROM python:3.12-slim

WORKDIR /app

# Create a non-root user for security
RUN groupadd -r duka && useradd --no-create-home -r -g duka duka

# Install curl for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Copy installed wheels from builder stage and install them
COPY --from=builder /wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache --no-index --find-links=/wheels -r requirements.txt

# Copy application code and set ownership
COPY --chown=duka:duka app/ ./app/
COPY --chown=duka:duka configs/ ./configs/

USER duka

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
