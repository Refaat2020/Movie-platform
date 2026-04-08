# ===== Base Image =====
FROM python:3.12-slim

# ===== Env =====
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONPATH=/app/src

# ===== Workdir =====
WORKDIR /app

# ===== System deps =====
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# ===== Install Poetry =====
RUN pip install --no-cache-dir poetry

# ===== Copy dependency files =====
COPY pyproject.toml poetry.lock* ./

# ===== Install deps =====
RUN poetry install --no-interaction --no-ansi --no-root

# ===== Copy project =====
COPY . .

# ===== Static files =====
RUN python src/manage.py collectstatic --noinput || true

# ===== Default command =====
CMD ["gunicorn", "core.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "4"]