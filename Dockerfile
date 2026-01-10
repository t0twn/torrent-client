FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir uv \
    && uv sync --project /app --no-dev --no-cache

CMD ["uv", "run", "app.py"]
