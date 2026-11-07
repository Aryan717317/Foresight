FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# CPU-only PyTorch to keep image lean
RUN pip install --no-cache-dir torch==2.3.1+cpu \
        --index-url https://download.pytorch.org/whl/cpu && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV MLFLOW_TRACKING_URI=http://mlflow.monitoring.svc.cluster.local:5000

# Default entrypoint: training pipeline
# Override CMD for drift check or data generation
CMD ["python", "train.py"]
