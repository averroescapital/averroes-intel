FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir google-cloud-storage openpyxl toml

# Copy dashboard and ingest parser (needed for GCS fallback)
COPY . .
COPY ../functions/ingest/phase1_parser.py ./phase1_parser.py
COPY ../functions/ingest/ma_parser.py ./ma_parser.py
COPY ../functions/ingest/parsers ./parsers

EXPOSE 8080

CMD ["streamlit", "run", "pe_app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
