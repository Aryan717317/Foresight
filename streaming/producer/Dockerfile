FROM python:3.11-slim

WORKDIR /app

# snappy requires gcc + libsnappy-dev
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libsnappy-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

RUN useradd -m -u 1000 appuser
USER appuser

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "2"]
