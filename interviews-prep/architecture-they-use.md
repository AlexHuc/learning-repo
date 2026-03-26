## 1️⃣ Understand the Architecture They Use

Their stack likely looks something like this:k

Sources &rarr; Azure Data Factory &rarr; Databricks (Spark) &rarr; Delta Lake &rarr; Data Lake (S3/Azure Storage) &rarr; Clients

1. Data lands in S3 / Data Lake
2. Azure Data Factory triggers ingestion
3. Processing happens in Databricks using Spark/PySpark
4. Data stored in Delta Lake tables
5. Clients query curated datasets