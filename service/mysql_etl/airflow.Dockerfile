FROM apache/airflow:2.11.0-python3.12

### init airflow & create user
# RUN airflow standalone
RUN airflow db init
RUN airflow users create \
    --username airflow \
    --firstname airflow \
    --password airflow \
    --lastname airflow \
    --role Admin \
    --email your_email@example.com

### USER PERMISSION ISSUE?
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential vim curl git \
#   && rm -rf /var/lib/apt/lists/*

### Try to use requirement.txt later [COPY ...]
RUN pip install --upgrade pip
RUN pip install --no-cache-dir matplotlib seaborn pymongo \
    confluent_kafka kafka-python dotenv scrapy

CMD ["airflow", "standalone"]