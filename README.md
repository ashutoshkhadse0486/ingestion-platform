# Ingestion Platform

A full-stack data ingestion platform for CSV and JSON datasets. The project accepts a control file from an external client, validates the requested file and record count, optionally splits oversized files with PySpark, orchestrates processing with Apache Airflow, and loads cleaned records into PostgreSQL via an Apache Beam pipeline.

## Overview

This repository contains four main runtime components:

- `service-api`: Spring Boot REST API that accepts triggering requests and coordinates the ingestion flow.
- `beam-pipeline`: Java + Apache Beam pipeline that reads either CSV or JSON input, validates and cleans records, and inserts them into the warehouse table.
- `airflow`: DAG-based orchestration layer that invokes the Beam job and validates the number of inserted records.
- `database`: PostgreSQL schema initialization and warehouse metadata tables.

The end-to-end flow is:

1. A client calls the ingestion API with the location of a control file.
2. The service reads the control file and verifies the record count, file type, and source path.
3. If the file is large, the service runs a PySpark split step to create smaller chunks.
4. Airflow triggers the Beam ingestion DAG for each file chunk.
5. The Beam job writes valid rows into the warehouse and bad rows to error logs.
6. The DAG verifies the row count against the expected value and marks the ingestion status.

---

## Repository Structure

```text
ingestion-platform/
├── .env
├── Dockerfile
├── docker-compose.yml
├── README.md
├── Architecture.md
├── airflow/
│   └── dags/
│       └── ingestion_dag.py
├── beam-pipeline/
│   ├── pom.xml
│   └── src/main/java/
├── data/
│   ├── beam-jars/
│   ├── control-file/
│   ├── error-record/
│   └── ingestion-files/
├── database/
│   └── init.sql
├── service-api/
│   ├── pom.xml
│   └── src/main/java/
├── spark-job/
│   └── split_file.py
└── .gitignore
```

---

## Key Components

### 1) Service API

The API lives under `service-api` and exposes ingestion endpoints for orchestrating the pipeline.

Main responsibilities:

- Parse and validate the incoming control file
- Resolve local and container paths for source data files
- Split oversized files with PySpark
- Trigger an Airflow DAG run with execution metadata
- Read ingested warehouse records through a repository layer

### 2) Airflow DAG

The DAG is defined in `airflow/dags/ingestion_dag.py`.

It performs the following tasks:

- Create a metadata row in `ingestion_metadata` with status `RUNNING`
- Invoke the Java Beam ingestion jar with the source file path and execution ID
- Validate the final row count in `target_warehouse`
- Update the status to `COMPLETED` or `FAILED`

The Airflow UI is served on:

- `http://localhost:8081`

### 3) Beam Pipeline

The Java pipeline builds into a bundled JAR and runs from the Airflow task.

It:

- Reads CSV or JSON input files
- Cleans and normalizes records
- Writes bad records to files under `data/error-record/<execution_id>/`
- Inserts successful records into the PostgreSQL `target_warehouse` table


### 4) Database

Database setup is defined in `database/init.sql`.

Tables:

- `target_warehouse`: final ingested records
- `ingestion_metadata`: execution tracking and status metadata

The schema also creates an index on `execution_id` for easier per-run queries.

---

## Control File Contract

The ingestion API expects a properties-style control file. Example:

```properties
record_count=346
file_type=CSV
data_file_location=/opt/data/ingestion-files/Employee_200.csv
```

Required fields:

- `record_count`: expected number of rows to be ingested
- `file_type`: `CSV` or `JSON`
- `data_file_location`: absolute source file path in the container filesystem


---

## Run the Project

### Prerequisites

- Docker and Docker Compose
- Java 17+
- Maven 3.9+
- Python 3.10+
- Optional: PySpark if you plan to run the splitter manually outside Docker

### 1) Start infrastructure

From the repo root:

```bash
docker compose up --build -d
```

This starts:

- PostgreSQL on `localhost:5432`
- Airflow webserver on `localhost:8081`
- Airflow scheduler in the background

### 2) Build the Beam pipeline

```bash
cd beam-pipeline
mvn clean package
```

This produces a shaded JAR in `target/beam-ingestion-pipeline-bundled.jar`.

### 3) Start the service API

```bash
cd service-api
./mvnw spring-boot:run
```

The API runs by default on:

- `http://localhost:8080`

---

## API Usage

### Trigger ingestion

Request:

```bash
curl -X POST http://localhost:8080/api/v1/ingestions/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "controlFileLocation": "/opt/data/control-file/control.properties"
  }'
```

Expected response:

```json
{
  "executionId": "<uuid>",
  "status": "ACCEPTED",
  "timestamp": "2026-09-19T00:00:00Z",
  "summary": "Ingestion accepted for Airflow processing"
}
```

### Fetch warehouse records

```bash
curl http://localhost:8080/api/v1/warehouse-records
```

## Data Flow Details

### Large File Handling

If a source file exceeds the configured threshold, the service uses PySpark to split it before triggering Airflow.

This allows large datasets to be processed in smaller partitions instead of a single huge file.

### Error Handling

The Beam job writes invalid records into the `data/error-record/<execution_id>/` directory. This provides an auditable trail for malformed rows or rejected data.

### Validation

The Airflow DAG checks that the number of rows inserted into `target_warehouse` matches the expected count from the control file. If it does not match, the ingestion is marked failed.

---