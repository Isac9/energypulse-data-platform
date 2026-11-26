# Image Python commune : ingestion, consumer, API, outils.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt/energypulse:/opt
WORKDIR /opt

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY ingestion/energypulse /opt/energypulse/energypulse
COPY api /opt/api
COPY scripts /opt/scripts

CMD ["python", "-m", "energypulse.pipeline", "--help"]
