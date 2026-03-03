FROM python:3.11-slim

WORKDIR /app

# Dépendances système pour Playwright/Chromium
RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installe Chromium + deps Playwright
RUN python -m playwright install --with-deps chromium

COPY . .

CMD ["python", "comparateur_energie_BG.py"]