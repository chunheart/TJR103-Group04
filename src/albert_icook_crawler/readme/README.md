🌱 Carbon Emission Recipe Pipeline

An automated data pipeline that tracks and analyzes carbon emissions from recipes

🧩 Project Overview

This project aims to build a data-driven carbon emission analysis system based on online recipe data.
The system automatically extracts, processes, and analyzes recipes to estimate their environmental impact.

💡 Key Features

🕷️ Scrapy for web scraping recipe data

⚙️ Kafka for real-time message streaming

🗄️ MySQL + MongoDB for raw and staged data storage

🧠 Airflow for orchestration and automation

🐳 Docker for full environment reproducibility

🧰 Poetry for dependency management

🌐 FastAPI / Flask (optional) for web service integration

Produce meaningful insights on sustainable food choices for personal updating

🔁 Data Flow Overview
```
flowchart TD
    A[Scrapy] -->|Export| B[CSV]
    A --> C[Kafka Topic]
    C <-->|Producer / Consumer| A
    C --> D[MySQL (raw)]
    D --> E[Transform step]
    E -->|cleaning / normalization / calculation| F[MongoDB (stage)]
    F --> G[Web Service]
```

🏗️ Project Architecture
```
carbon_emission_project/
│
├── dags/
│   └── carbon_pipeline_dag.py         # Airflow DAG (single-responsibility design)
│
├── src/
│   ├── pipeline/
│   │   ├── extract/
│   │   │   ├── scrapy_spider.py       # Scrapy spider for recipe data
│   │   │   └── csv_reader.py
│   │   ├── kafka/
│   │   │   ├── producer.py
│   │   │   └── consumer.py
│   │   ├── load/
│   │   │   ├── db_connection.py
│   │   │   ├── load_raw.py
│   │   │   └── load_stage.py
│   │   ├── transform/
│   │   │   ├── cleaning.py
│   │   │   ├── normalization.py
│   │   │   ├── calculation.py
│   │   │   └── enrichment.py
│   │   └── utils/
│   │       └── logger.py
│   └── web/
│       ├── app.py                     # Web API / data visualization
│       └── api.py
│
├── data/
│   ├── raw/
│   ├── stage/
│   └── processed/
│
├── tests/
│   ├── test_extract.py
│   ├── test_kafka.py
│   ├── test_load.py
│   └── test_transform.py
│
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── poetry.lock
└── README.md
```

🧠 Core Design Principles

| Principle                 | Description                                                        |
| ------------------------- | ------------------------------------------------------------------ |
| **Single Responsibility** | Each Airflow task does one thing (easy to debug & retry).          |
| **Stateless Design**      | Every step stores results externally (DB, file) instead of memory. |
| **Retry & Alert**         | Automatic retry on transient errors (Kafka, DB).                   |
| **Unified Logging**       | Centralized logger for consistent Airflow log messages.            |
| **Modularity**            | Each folder handles one part of the ETL pipeline.                  |
| **Dockerized**            | Ensures reproducible local and production environments.            |

⚙️ Tech Stack

| Category             | Technology                             |
| -------------------- | -------------------------------------- |
| **Orchestration**    | Apache Airflow                         |
| **Data Collection**  | Scrapy                                 |
| **Streaming**        | Apache Kafka                           |
| **Storage**          | MySQL (raw data), MongoDB (stage data) |
| **Transformation**   | Python (Pandas / custom logic)         |
| **Packaging**        | Poetry                                 |
| **Containerization** | Docker & docker-compose                |
| **Web Layer**        | Flask / FastAPI (optional)             |
