FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENV PORT=8080
# keep glibc from ballooning RSS with many malloc arenas; unbuffered logs
ENV MALLOC_ARENA_MAX=2
ENV PYTHONUNBUFFERED=1
# where parsed schedules are cached — mount a PERSISTENT disk here on the host
ENV CACHE_DIR=/data
EXPOSE 8080

# 1 worker (memory-safe: heavy parsing runs in a separate short-lived process),
# threads so status polls + chunk serving stay responsive during a build.
CMD ["sh", "-c", "gunicorn -w 1 --threads 4 -t 120 -b 0.0.0.0:${PORT} app:app"]
