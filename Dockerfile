# Use an official lightweight Python runtime
FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8080
ENV HOST=0.0.0.0

WORKDIR /app

# Install system dependencies (build-essential needed for some ccxt/pandas binary wheels if required)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY scanner.py .
COPY server.py .
COPY static/ ./static/

# Expose port (Render/Railway map this dynamically)
EXPOSE 8080

# Run the async web server
CMD ["python", "server.py"]
