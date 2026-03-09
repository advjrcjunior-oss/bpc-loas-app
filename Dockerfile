FROM python:3.11-slim

WORKDIR /app

# Instala dependências do sistema necessárias para PyMuPDF e Pillow
RUN apt-get update && apt-get install -y --no-install-recommends \
    libmupdf-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p output

EXPOSE 8080

CMD gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120
