FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY telegram_downloader_bot.py .

# Create download temp dir
RUN mkdir -p /tmp/tg_downloader

# Run the bot
CMD ["python", "-u", "telegram_downloader_bot.py"]
