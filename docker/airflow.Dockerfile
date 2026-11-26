# Airflow + les dépendances du paquet energypulse.
FROM apache/airflow:2.10.5-python3.12

RUN pip install --no-cache-dir "confluent-kafka==2.6.1"
ENV PYTHONPATH=/opt/energypulse
