FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY gdelt_incremental/ ./gdelt_incremental/
COPY main.py ./

RUN mkdir -p /tmp/gdelt
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/tmp/gdelt

ENTRYPOINT ["python", "main.py"]
