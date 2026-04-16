FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all app files (flat build context)
COPY pe_app.py .
COPY phase1_parser.py .
COPY ma_parser.py .
COPY parsers/ ./parsers/

# Streamlit config
COPY .streamlit/ ./.streamlit/

EXPOSE 8080

CMD ["streamlit", "run", "pe_app.py", \
     "--server.port=8080", \
     "--server.address=0.0.0.0", \
     "--server.enableCORS=false", \
     "--server.enableXsrfProtection=false"]
