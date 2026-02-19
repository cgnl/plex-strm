FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY *.py requirements.txt /app/
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "plex_strm.py"]
