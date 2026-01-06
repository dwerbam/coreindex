# Stage 1: Builder
FROM python:3.12-slim-bookworm AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency definitions
COPY pyproject.toml uv.lock ./

# Install dependencies into a virtual environment
# --no-dev: Production deps only
# --compile: Bytecode compile for faster startup
RUN uv sync --frozen --no-dev --compile

# Stage 2: Runner
FROM python:3.12-slim-bookworm

WORKDIR /app

# Copy the virtual environment from the builder stage
COPY --from=builder /app/.venv /app/.venv

# Copy application source code
COPY src /app/src

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH"
ENV DATA_DIR=/app/data
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV FORCE_COLOR=1

# Expose the Electrum server port
EXPOSE 50001

# Run the application using the python executable from the venv
CMD ["python", "src/main.py"]