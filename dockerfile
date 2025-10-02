# Dockerfile
FROM python:3.11-slim

# avoid prompts
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# system deps for confluent-kafka (librdkafka) and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    ca-certificates \
    librdkafka-dev \
    libsasl2-dev \
    libsasl2-modules \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# copy application code
COPY . .

# expose port if your app runs an HTTP server (example)
EXPOSE 8000

# default command (override in docker-compose or docker run)
CMD ["python", "main.py"]
