# 🚀 Airflow + Databricks ETL Pipeline (Production-Style)

End-to-end data pipeline using Apache Airflow (Docker + CeleryExecutor) and Databricks with Bronze–Silver–Gold architecture.
## 🏗 Architecture

Airflow (Scheduler + Worker)
        ↓
Redis Queue
        ↓
Databricks API (Token Auth)
        ↓
Databricks Jobs (Bronze → Silver → Gold)
        ↓
Delta Lake Storage
