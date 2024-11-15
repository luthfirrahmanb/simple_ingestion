FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Run multiple Python files sequentially
CMD ["sh", "-c", "python /app/scripts/ingest_to_bigquery.py && python /app/scripts/staging_process.py && python /app/scripts/report_process.py"]
