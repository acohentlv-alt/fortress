# Fortress — Production Dockerfile for Render
# Includes Playwright Chromium + system dependencies for Maps scraping

FROM python:3.13-slim

# Install system dependencies required by Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 \
    libpango-1.0-0 libcairo2 libasound2 \
    libx11-xcb1 libxcb1 libxext6 libx11-6 \
    libdbus-1-3 libglib2.0-0 \
    fonts-liberation fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser
RUN playwright install chromium

# Copy application code
COPY . .

# Expose port (Render passes $PORT at runtime)
EXPOSE 10000

# Start command — Render overrides this with the Start Command setting
CMD ["sh", "-c", "uvicorn fortress.api.main:app --host 0.0.0.0 --port ${PORT:-10000}"]
