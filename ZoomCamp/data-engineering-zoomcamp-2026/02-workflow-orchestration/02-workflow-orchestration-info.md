# Workflow Orchestration

Welcome to **Module 2 – Workflow Orchestration** of the Data Engineering Zoomcamp.

In this module, you will learn how to build, schedule, and monitor data pipelines using **Kestra**, an open-source workflow orchestration platform.

Kestra allows you to define workflows using **YAML**, following Infrastructure as Code principles. It supports scheduling, event-driven pipelines, retries, logging, and integrations with many tools used in data engineering.

---

## Course Structure

- [2.1 - Introduction to Workflow Orchestration](#21-introduction-to-workflow-orchestration)
- [2.2 - Getting Started with Kestra](#22-getting-started-with-kestra)
- [2.3 - Build ETL Pipelines with Kestra](#23-build-etl-pipelines-with-kestra)
- [2.4 - ELT Pipelines on Google Cloud](#24-elt-pipelines-on-google-cloud)
- [2.5 - Using AI in Kestra](#25-using-ai-in-kestra)
- [2.6 - Bonus: Deploy to the Cloud](#26-bonus-deploy-to-the-cloud-optional)

---

## 2.1 Introduction to Workflow Orchestration

### What is Workflow Orchestration?

In data engineering, we often need to run multiple steps in a specific order:

- download data  
- clean or transform it  
- load it into a database  
- run this process daily or monthly  

A **workflow orchestrator** is responsible for:

- running tasks in order  
- handling failures and retries  
- scheduling pipelines  
- providing logs and execution history  

Instead of running scripts manually, orchestration allows pipelines to run **automatically and reliably**.

---

### What is Kestra?

Kestra is an **open-source workflow orchestration platform** that:

- uses YAML to define workflows  
- supports both scheduled and event-based pipelines  
- works with any programming language  
- provides 1000+ plugins (Python, SQL, GCP, AWS, APIs, etc.)  

Kestra is especially beginner-friendly while still being production-ready.

Resources:
- https://kestra.io
- https://go.kestra.io/de-zoomcamp/docs

---

## 2.2 Getting Started with Kestra

### Installing Kestra

We will run Kestra locally using **Docker Compose**.

Kestra requires:
- Kestra server
- Postgres database (used internally by Kestra)

From the module folder:

```bash
cd 02-workflow-orchestration
docker compose up -d
```

Once started, open the UI:

```
http://localhost:8080
```

To stop Kestra:

```bash
docker compose down
```

---

### Adding Flows

Flows can be added in two ways:

1. Copy & paste YAML directly into the Kestra UI  
2. Import flows using the API  

Example API import:

```bash
curl -X POST -u 'admin@kestra.io:Admin1234' \
http://localhost:8080/api/v1/flows/import \
-F fileUpload=@flows/01_hello_world.yaml
```

---

### Core Kestra Concepts

| Concept | Description |
|------|------|
| Flow | A full workflow definition |
| Task | A single step inside a flow |
| Inputs | Parameters passed at runtime |
| Outputs | Values produced by tasks |
| Triggers | Automatically start flows |
| Execution | One run of a flow |
| Variables | Reusable key–value pairs |
| Concurrency | Limit parallel executions |

Example flow:  
`flows/01_hello_world.yaml`

This flow demonstrates:
- inputs
- variables
- outputs
- scheduling
- logging
- concurrency limits

---

### Running Python Code

Kestra allows executing Python code:

- inline in YAML  
- or from a separate Python file  

Example:
`flows/02_python.yaml`

This flow:
- runs Python code
- fetches data from an API
- returns outputs to Kestra
- makes results reusable in other tasks

This makes Kestra extremely flexible for real pipelines.

---

## 2.3 Build ETL Pipelines with Kestra

In this section, we build **ETL pipelines** using NYC Taxi data.

You will:
1. Extract CSV data from GitHub  
2. Transform it using Python  
3. Load it into Postgres  

---

### 2.3.1 Basic Pipeline

Flow:
`03_getting_started_data_pipeline.yaml`

Pipeline steps:
- download data via HTTP
- process data in Python
- query data using DuckDB

This flow is mainly for understanding how data moves through Kestra.

---

### 2.3.2 Load Taxi Data into Postgres

Flow:
`04_postgres_taxi.yaml`

This pipeline:
- selects year and month
- downloads taxi CSV files
- creates tables
- loads monthly data
- merges into final tables  

We use **CSV files** from:

https://github.com/DataTalksClub/nyc-tlc-data/releases

CSV is intentionally chosen because it’s easier to inspect and understand.

---

### 2.3.3 Scheduling and Backfills

Flow:
`05_postgres_taxi_scheduled.yaml`

This flow demonstrates:
- scheduling pipelines daily
- running historical backfills
- reprocessing past months

Backfills are essential in real data systems when pipelines fail or logic changes.

---

## 2.4 ELT Pipelines on Google Cloud

After running pipelines locally, we move to the cloud.

We will use:

- **GCS** → data lake  
- **BigQuery** → data warehouse  

---

### ETL vs ELT

**ETL (local):**
1. Extract
2. Transform
3. Load

**ELT (cloud):**
1. Extract
2. Load
3. Transform inside the warehouse

In cloud systems, ELT is preferred because:
- warehouses scale automatically
- transformations are much faster
- storage is cheap

---

### 2.4.2 GCP Setup

Flow:
`06_gcp_kv.yaml`

Used to store configuration values:
- project ID
- location
- dataset
- bucket name

Optional setup flow:
`07_gcp_setup.yaml`

This can automatically create:
- GCS bucket
- BigQuery dataset

---

### 2.4.3 Load Taxi Data to BigQuery

Flow:
`08_gcp_taxi.yaml`

Pipeline steps:
- download CSV
- upload to GCS
- create external tables
- create monthly tables
- merge into final tables
- clean up files

This represents a real-world ELT pipeline.

---

### 2.4.4 Scheduling and Full Backfills

Flow:
`09_gcp_taxi_scheduled.yaml`

This flow:
- schedules daily loads
- supports full historical backfills
- safely processes large datasets in the cloud

---

## 2.5 Using AI in Kestra

Kestra includes AI tools that help you build workflows faster.

You can use AI to:
- generate YAML flows
- avoid syntax errors
- speed up development

---

### AI Copilot

Kestra AI Copilot understands:
- latest plugin versions
- correct task syntax
- real Kestra documentation

Unlike generic LLMs, it avoids outdated or hallucinated code.

AI Copilot is configured using a Gemini API key and environment variables.

---

### Retrieval Augmented Generation (RAG)

RAG allows AI to answer questions using **real documentation** instead of training data alone.

How it works:
1. ingest documents
2. create embeddings
3. retrieve relevant context
4. generate accurate answers

Example flows:
- `10_chat_without_rag.yaml`
- `11_chat_with_rag.yaml`

RAG dramatically reduces hallucinations.

---

## 2.6 Bonus: Deploy to the Cloud (Optional)

Once pipelines work locally, Kestra can be deployed to the cloud.

You can:
- deploy on GCP
- sync flows from GitHub
- manage secrets securely
- run production pipelines

Resources:
- https://go.kestra.io/de-zoomcamp/gcp-install
- https://go.kestra.io/de-zoomcamp/git

---

## Troubleshooting Tips

Recommended versions:

- `kestra/kestra:v1.1`
- `postgres:18`

If port 8080 is already in use, change mapping to:

```
18080:8080
```

Then open:

```
http://localhost:18080
```

If BigQuery CSV errors appear, re-run the pipeline — most issues come from incomplete downloads or uploads.

---