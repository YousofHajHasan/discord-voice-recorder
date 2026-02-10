FROM python:3.10-slim

# Install system dependencies (ffmpeg is required for audio)
RUN apt-get update && \
    apt-get install -y ffmpeg libopus0 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# We use an environment variable for the token, so no hardcoding!
CMD ["python", "-u", "app.py"]